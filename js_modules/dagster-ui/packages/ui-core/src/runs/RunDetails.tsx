import {gql, useMutation} from '@apollo/client';
import {
  Button,
  Colors,
  DialogFooter,
  Dialog,
  Group,
  Icon,
  MenuItem,
  Menu,
  MetadataTable,
  Popover,
  Tooltip,
  Subheading,
  Box,
  StyledReadOnlyCodeMirror,
} from '@dagster-io/ui-components';
import * as React from 'react';
import {useHistory} from 'react-router-dom';
import styled from 'styled-components';

import {AppContext} from '../app/AppContext';
import {showSharedToaster} from '../app/DomUtils';
import {useFeatureFlags} from '../app/Flags';
import {useCopyToClipboard} from '../app/browser';
import {RunStatus} from '../graphql/types';
import {FREE_CONCURRENCY_SLOTS_FOR_RUN_MUTATION} from '../instance/InstanceConcurrency';
import {
  FreeConcurrencySlotsForRunMutation,
  FreeConcurrencySlotsForRunMutationVariables,
} from '../instance/types/InstanceConcurrency.types';
import {NO_LAUNCH_PERMISSION_MESSAGE} from '../launchpad/LaunchRootExecutionButton';
import {TimestampDisplay} from '../schedules/TimestampDisplay';
import {AnchorButton} from '../ui/AnchorButton';
import {workspacePathFromRunDetails, workspacePipelinePath} from '../workspace/workspacePath';

import {DeletionDialog} from './DeletionDialog';
import {doneStatuses} from './RunStatuses';
import {RunTags} from './RunTags';
import {RunsQueryRefetchContext} from './RunUtils';
import {TerminationDialog} from './TerminationDialog';
import {TimeElapsed} from './TimeElapsed';
import {RunDetailsFragment} from './types/RunDetails.types';
import {RunFragment} from './types/RunFragments.types';

export const timingStringForStatus = (status?: RunStatus) => {
  switch (status) {
    case RunStatus.QUEUED:
      return 'Queued';
    case RunStatus.CANCELED:
      return 'Canceled';
    case RunStatus.CANCELING:
      return 'Canceling…';
    case RunStatus.FAILURE:
      return 'Failed';
    case RunStatus.NOT_STARTED:
      return 'Waiting to start…';
    case RunStatus.STARTED:
      return 'Started…';
    case RunStatus.STARTING:
      return 'Starting…';
    case RunStatus.SUCCESS:
      return 'Succeeded';
    default:
      return 'None';
  }
};

const LoadingOrValue: React.FC<{
  loading: boolean;
  children: () => React.ReactNode;
}> = ({loading, children}) =>
  loading ? <div style={{color: Colors.Gray400}}>Loading…</div> : <div>{children()}</div>;

const TIME_FORMAT = {showSeconds: true, showTimezone: false};

export const RunDetails: React.FC<{
  loading: boolean;
  run: RunDetailsFragment | undefined;
}> = ({loading, run}) => {
  return (
    <MetadataTable
      spacing={0}
      rows={[
        {
          key: 'Started',
          value: (
            <LoadingOrValue loading={loading}>
              {() => {
                if (run?.startTime) {
                  return <TimestampDisplay timestamp={run.startTime} timeFormat={TIME_FORMAT} />;
                }
                return (
                  <div style={{color: Colors.Gray400}}>{timingStringForStatus(run?.status)}</div>
                );
              }}
            </LoadingOrValue>
          ),
        },
        {
          key: 'Ended',
          value: (
            <LoadingOrValue loading={loading}>
              {() => {
                if (run?.endTime) {
                  return <TimestampDisplay timestamp={run.endTime} timeFormat={TIME_FORMAT} />;
                }
                return (
                  <div style={{color: Colors.Gray400}}>{timingStringForStatus(run?.status)}</div>
                );
              }}
            </LoadingOrValue>
          ),
        },
        {
          key: 'Duration',
          value: (
            <LoadingOrValue loading={loading}>
              {() => {
                if (run?.startTime) {
                  return <TimeElapsed startUnix={run.startTime} endUnix={run.endTime} />;
                }
                return (
                  <div style={{color: Colors.Gray400}}>{timingStringForStatus(run?.status)}</div>
                );
              }}
            </LoadingOrValue>
          ),
        },
      ]}
    />
  );
};

type VisibleDialog = 'config' | 'delete' | 'terminate' | 'free_slots' | null;

export const RunConfigDialog: React.FC<{run: RunFragment; isJob: boolean}> = ({run, isJob}) => {
  const {runConfigYaml} = run;
  const {flagInstanceConcurrencyLimits} = useFeatureFlags();
  const [visibleDialog, setVisibleDialog] = React.useState<VisibleDialog>(null);

  const {rootServerURI} = React.useContext(AppContext);
  const {refetch} = React.useContext(RunsQueryRefetchContext);

  const copy = useCopyToClipboard();
  const history = useHistory();

  const [freeSlots] = useMutation<
    FreeConcurrencySlotsForRunMutation,
    FreeConcurrencySlotsForRunMutationVariables
  >(FREE_CONCURRENCY_SLOTS_FOR_RUN_MUTATION);

  const copyConfig = async () => {
    copy(runConfigYaml);
    await showSharedToaster({
      intent: 'success',
      icon: 'copy_to_clipboard_done',
      message: 'Copied!',
    });
  };

  const freeConcurrencySlots = async () => {
    const resp = await freeSlots({variables: {runId: run.id}});
    if (resp.data?.freeConcurrencySlotsForRun) {
      await showSharedToaster({
        intent: 'success',
        icon: 'check_circle',
        message: 'Freed concurrency slots',
      });
    }
  };

  const jobPath = workspacePathFromRunDetails({
    id: run.id,
    repositoryName: run.repositoryOrigin?.repositoryName,
    repositoryLocationName: run.repositoryOrigin?.repositoryLocationName,
    pipelineName: run.pipelineName,
    isJob,
  });

  return (
    <div>
      <Group direction="row" spacing={8}>
        {run.hasReExecutePermission ? (
          <AnchorButton icon={<Icon name="edit" />} to={jobPath}>
            Open in Launchpad
          </AnchorButton>
        ) : (
          <Tooltip content={NO_LAUNCH_PERMISSION_MESSAGE} useDisabledButtonTooltipFix>
            <Button icon={<Icon name="edit" />} disabled>
              Open in Launchpad
            </Button>
          </Tooltip>
        )}
        <Button icon={<Icon name="tag" />} onClick={() => setVisibleDialog('config')}>
          View tags and config
        </Button>
        <Popover
          position="bottom-right"
          content={
            <Menu>
              <Tooltip
                content="Loadable in dagster-webserver-debug"
                position="left"
                targetTagName="div"
              >
                <MenuItem
                  text="Download debug file"
                  icon={<Icon name="download_for_offline" />}
                  onClick={() => window.open(`${rootServerURI}/download_debug/${run.id}`)}
                />
              </Tooltip>
              {flagInstanceConcurrencyLimits &&
              run.hasConcurrencyKeySlots &&
              doneStatuses.has(run.status) ? (
                <MenuItem
                  text="Free concurrency slots"
                  icon={<Icon name="lock" />}
                  onClick={freeConcurrencySlots}
                />
              ) : null}
              {run.hasDeletePermission ? (
                <MenuItem
                  icon="delete"
                  text="Delete"
                  intent="danger"
                  onClick={() => setVisibleDialog('delete')}
                />
              ) : null}
            </Menu>
          }
        >
          <Button icon={<Icon name="expand_more" />} />
        </Popover>
      </Group>
      <Dialog
        isOpen={visibleDialog === 'config'}
        onClose={() => setVisibleDialog(null)}
        style={{
          width: '90vw',
          maxWidth: '1000px',
          minWidth: '600px',
          height: '90vh',
          maxHeight: '1000px',
          minHeight: '600px',
        }}
        title="Run configuration"
      >
        <Box flex={{direction: 'column'}} style={{flex: 1, overflow: 'hidden'}}>
          <Box flex={{direction: 'column', gap: 20}} style={{flex: 1, overflow: 'hidden'}}>
            <Box flex={{direction: 'column', gap: 12}} padding={{top: 16, horizontal: 24}}>
              <Subheading>Tags</Subheading>
              <div>
                <RunTags tags={run.tags} mode={isJob ? null : run.mode} />
              </div>
            </Box>
            <Box flex={{direction: 'column'}} style={{flex: 1, overflow: 'hidden'}}>
              <Box
                border={{side: 'bottom', width: 1, color: Colors.KeylineGray}}
                padding={{left: 24, bottom: 16}}
              >
                <Subheading>Config</Subheading>
              </Box>
              <CodeMirrorContainer>
                <StyledReadOnlyCodeMirror
                  value={runConfigYaml}
                  options={{lineNumbers: true, mode: 'yaml'}}
                  theme={['config-editor']}
                />
              </CodeMirrorContainer>
            </Box>
          </Box>
          <DialogFooter topBorder>
            <Button onClick={() => copyConfig()} intent="none">
              Copy config
            </Button>
            <Button onClick={() => setVisibleDialog(null)} intent="primary">
              OK
            </Button>
          </DialogFooter>
        </Box>
      </Dialog>
      {run.hasDeletePermission ? (
        <DeletionDialog
          isOpen={visibleDialog === 'delete'}
          onClose={() => setVisibleDialog(null)}
          onComplete={() => {
            if (run.repositoryOrigin) {
              history.push(
                workspacePipelinePath({
                  repoName: run.repositoryOrigin.repositoryName,
                  repoLocation: run.repositoryOrigin.repositoryLocationName,
                  pipelineName: run.pipelineName,
                  isJob,
                  path: '/runs',
                }),
              );
            } else {
              setVisibleDialog(null);
            }
          }}
          onTerminateInstead={() => setVisibleDialog('terminate')}
          selectedRuns={{[run.id]: run.canTerminate}}
        />
      ) : null}
      {run.hasTerminatePermission ? (
        <TerminationDialog
          isOpen={visibleDialog === 'terminate'}
          onClose={() => setVisibleDialog(null)}
          onComplete={() => {
            refetch();
          }}
          selectedRuns={{[run.id]: run.canTerminate}}
        />
      ) : null}
    </div>
  );
};

const CodeMirrorContainer = styled.div`
  flex: 1;
  overflow: hidden;

  .react-codemirror2,
  .CodeMirror {
    height: 100%;
  }
`;

export const RUN_DETAILS_FRAGMENT = gql`
  fragment RunDetailsFragment on Run {
    id
    startTime
    endTime
    status
    hasConcurrencyKeySlots
  }
`;