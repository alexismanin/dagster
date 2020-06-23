import dask
import dask.distributed
from dagster_graphql.client.mutations import execute_execute_plan_mutation

# Dask resource requirements are specified under this key
from dagster import Executor, Field, Permissive, Selector, check, seven
from dagster.core.definitions.executor import check_cross_process_constraints, executor
from dagster.core.events import DagsterEvent
from dagster.core.execution.context.system import SystemPipelineExecutionContext
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.utils import frozentags
from dagster.utils.hosted_user_process import create_in_process_ephemeral_workspace

DASK_RESOURCE_REQUIREMENTS_KEY = 'dagster-dask/resource_requirements'


@executor(
    name='dask',
    config_schema={
        'cluster': Field(
            Selector(
                {
                    'local': Field(
                        Permissive(), is_required=False, description='Local cluster configuration.'
                    ),
                    'yarn': Field(
                        Permissive(), is_required=False, description='YARN cluster configuration.'
                    ),
                    'ssh': Field(
                        Permissive(), is_required=False, description='SSH cluster configuration.'
                    ),
                    'pbs': Field(
                        Permissive(), is_required=False, description='PBS cluster configuration.'
                    ),
                    'kube': Field(
                        Permissive(),
                        is_required=False,
                        description='Kubernetes cluster configuration.',
                    ),
                }
            )
        )
    },
)
def dask_executor(init_context):
    '''Dask-based executor.

    If the Dask executor is used without providing executor-specific config, a local Dask cluster
    will be created (as when calling :py:class:`dask.distributed.Client() <dask:distributed.Client>`
    without specifying the scheduler address).

    The Dask executor optionally takes the following config:

    .. code-block:: none

        cluster:
            {
                local?:  # The cluster type, one of the following ('local', 'yarn', 'ssh', 'pbs', 'kube').
                    {
                        address?: '127.0.0.1:8786',  # The address of a Dask scheduler
                        timeout?: 5,  # Timeout duration for initial connection to the scheduler
                        scheduler_file?: '/path/to/file'  # Path to a file with scheduler information
                        # Whether to connect directly to the workers, or ask the scheduler to serve as
                        # intermediary
                        direct_to_workers?: False,
                        heartbeat_interval?: 1000,  # Time in milliseconds between heartbeats to scheduler
                    }
            }

    If you'd like to configure a dask executor in addition to the
    :py:class:`~dagster.default_executors`, you should add it to the ``executor_defs`` defined on a
    :py:class:`~dagster.ModeDefinition` as follows:

    .. code-block:: python

        from dagster import ModeDefinition, default_executors, pipeline
        from dagster_dask import dask_executor

        @pipeline(mode_defs=[ModeDefinition(executor_defs=default_executors + [dask_executor])])
        def dask_enabled_pipeline():
            pass

    '''
    check_cross_process_constraints(init_context)
    ((cluster_type, cluster_configuration),) = init_context.executor_config['cluster'].items()
    return DaskExecutor(cluster_type, cluster_configuration)


def query_on_dask_worker(
    workspace, variables, dependencies, instance_ref=None
):  # pylint: disable=unused-argument
    '''Note that we need to pass "dependencies" to ensure Dask sequences futures during task
    scheduling, even though we do not use this argument within the function.
    '''
    return execute_execute_plan_mutation(workspace, variables, instance_ref=instance_ref)


def get_dask_resource_requirements(tags):
    check.inst_param(tags, 'tags', frozentags)
    req_str = tags.get(DASK_RESOURCE_REQUIREMENTS_KEY)
    if req_str is not None:
        return seven.json.loads(req_str)

    return {}


class DaskExecutor(Executor):
    def __init__(self, cluster_type, cluster_configuration):
        self.cluster_type = check.opt_str_param(cluster_type, 'cluster_type', default='local')
        self.cluster_configuration = check.opt_dict_param(
            cluster_configuration, 'cluster_configuration'
        )

    def execute(self, pipeline_context, execution_plan):
        check.inst_param(pipeline_context, 'pipeline_context', SystemPipelineExecutionContext)
        check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)
        check.param_invariant(
            isinstance(pipeline_context.executor, DaskExecutor),
            'pipeline_context',
            'Expected executor to be DaskExecutor got {}'.format(pipeline_context.executor),
        )

        # Checks to ensure storage is compatible with Dask configuration
        storage = pipeline_context.run_config.get('storage')
        check.invariant(storage.keys(), 'Must specify storage to use Dask execution')

        check.invariant(
            pipeline_context.instance.is_persistent,
            'Dask execution requires a persistent DagsterInstance',
        )

        # https://github.com/dagster-io/dagster/issues/2440
        check.invariant(
            pipeline_context.system_storage_def.is_persistent,
            'Cannot use in-memory storage with Dask, use filesystem, S3, or GCS',
        )

        step_levels = execution_plan.execution_step_levels()

        pipeline_name = pipeline_context.pipeline_def.name

        instance = pipeline_context.instance

        cluster_type = self.cluster_type
        if cluster_type == 'local':
            from dask.distributed import LocalCluster

            cluster = LocalCluster(**self.build_dict(pipeline_name))
        elif cluster_type == 'yarn':
            from dask_yarn import YarnCluster

            cluster = YarnCluster(**self.build_dict(pipeline_name))
        elif cluster_type == 'ssh':
            from dask.distributed import SSHCluster

            cluster = SSHCluster(**self.build_dict(pipeline_name))
        elif cluster_type == 'pbs':
            from dask_jobqueue import PBSCluster

            cluster = PBSCluster(**self.build_dict(pipeline_name))
        elif cluster_type == 'kube':
            from dask_kubernetes import KubeCluster

            cluster = KubeCluster(**self.build_dict(pipeline_name))
        else:
            raise ValueError(
                f"Must be providing one of the following ('local', 'yarn', 'ssh', 'pbs', 'kube') not {cluster_type}"
            )

        with dask.distributed.Client(cluster) as client:
            execution_futures = []
            execution_futures_dict = {}

            for step_level in step_levels:
                for step in step_level:
                    # We ensure correctness in sequencing by letting Dask schedule futures and
                    # awaiting dependencies within each step.
                    dependencies = []
                    for step_input in step.step_inputs:
                        for key in step_input.dependency_keys:
                            dependencies.append(execution_futures_dict[key])

                    run_config = dict(pipeline_context.run_config, execution={'in_process': {}})
                    recon_repo = pipeline_context.pipeline.get_reconstructable_repository()
                    variables = {
                        'executionParams': {
                            'selector': {
                                'pipelineName': pipeline_name,
                                'repositoryName': recon_repo.get_definition().name,
                                'repositoryLocationName': '<<in_process>>',
                            },
                            'runConfigData': run_config,
                            'mode': pipeline_context.mode_def.name,
                            'executionMetadata': {'runId': pipeline_context.pipeline_run.run_id},
                            'stepKeys': [step.key],
                        }
                    }

                    dask_task_name = '%s.%s' % (pipeline_name, step.key)

                    workspace = create_in_process_ephemeral_workspace(
                        pointer=pipeline_context.pipeline.get_reconstructable_repository().pointer
                    )

                    future = client.submit(
                        query_on_dask_worker,
                        workspace,
                        variables,
                        dependencies,
                        instance.get_ref(),
                        key=dask_task_name,
                        resources=get_dask_resource_requirements(step.tags),
                    )

                    execution_futures.append(future)
                    execution_futures_dict[step.key] = future

            # This tells Dask to awaits the step executions and retrieve their results to the
            # master
            for future in dask.distributed.as_completed(execution_futures):
                for step_event in future.result():
                    check.inst(step_event, DagsterEvent)

                    yield step_event

    def build_dict(self, pipeline_name):
        '''Returns a dict we can use for kwargs passed to dask client instantiation.

        Intended to be used like:

        with dask.distributed.Client(**cfg.build_dict()) as client:
            << use client here >>

        '''

        if self.cluster_type in ['yarn', 'kube', 'pbs']:
            dask_cfg = {'name': pipeline_name}
        else:
            dask_cfg = {}

        if self.cluster_configuration:
            for k, v in self.cluster_configuration.items():
                dask_cfg[k] = v

        # if address is set, don't add LocalCluster args
        # context: https://github.com/dask/distributed/issues/3313
        if (self.cluster_type == 'local') and ('address' not in dask_cfg):
            # We set threads_per_worker because Dagster is not thread-safe. Even though
            # environments=True by default, there is a clever piece of machinery
            # (dask.distributed.deploy.local.nprocesses_nthreads) that automagically makes execution
            # multithreaded by default when the number of available cores is greater than 4.
            # See: https://github.com/dagster-io/dagster/issues/2181
            # We may want to try to figure out a way to enforce this on remote Dask clusters against
            # which users run Dagster workloads.
            dask_cfg['threads_per_worker'] = 1

        return dask_cfg
