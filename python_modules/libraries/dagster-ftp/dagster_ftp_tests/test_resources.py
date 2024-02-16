import tempfile

from dagster._core.execution.context.init import build_init_resource_context
import pytest
from pytest import FixtureRequest, fixture

from dagster_ftp_example.resources import FTPResource


@fixture(name = "ftp_resource", params=[True, False])
@pytest.mark.parametrize("ssl", )
def init_ftp_content(request: FixtureRequest):
    server_name = "ftpserver_TLS" if request.param else "ftpserver"
    server = request.getfixturevalue(server_name)
    with tempfile.NamedTemporaryFile(mode="w+t", prefix="ftp_test", suffix=".txt") as f:
        f.write("test\nfile")
        f.flush()
        server.put_files({ "src":f.name, "dest": "test.txt"})
    with FTPResource(host="localhost",
                     port=server.server_port,
                     username=server.username,
                     password=server.password,
                     tls=request.param).yield_for_execution(build_init_resource_context()) as r:
        yield r


def test_upload_single_file(ftp_resource: FTPResource):
    pass


def test_upload_recursive(ftp_resource: FTPResource):
    pass


def test_download_single_file(ftp_resource: FTPResource):
    with tempfile.NamedTemporaryFile(mode="w+b", prefix="downloaded", suffix=".txt") as f:
        ftp_resource.download("test.txt", f)
        f.flush()
        f.seek(0)
        file_content = f.read()
        assert file_content == b"test\nfile"


def test_download_recursive(ftp_resource: FTPResource):
    pass
