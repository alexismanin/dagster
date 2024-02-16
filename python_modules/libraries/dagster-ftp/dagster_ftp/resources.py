import contextlib
import ftplib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PosixPath
from typing import BinaryIO

from dagster import ConfigurableResource, InitResourceContext
from pydantic import BaseModel, Field, PrivateAttr


class FileTransferReport(BaseModel):
    source_file: str
    destination_file: str
    nb_transferred_bytes: int


@dataclass(frozen=False)
class _ByteAccumulator:
    dest: BinaryIO
    nb_written_bytes: int = 0

    def write_and_count(self, data: bytes):
        self.nb_written_bytes += self.dest.write(data)


class FTPResource(ConfigurableResource):
    # Connect info
    tls: bool = Field(default=True, description="True to activate SSL (FTPS), False for unsecure FTP")
    host: str = Field(description="The host to connect to.")
    port: int = Field(
        default=21,
        description="The TCP port to connect to (default: 21, as specified by the FTP protocol specification)"
    )
    timeout: int | None = Field(
        default=None,
        description="A timeout in seconds for blocking operations (read, write, connect if no connect_timeout is set)"
    )
    connect_timeout: int | None = Field(
        default=None,
        description="timeout in seconds when connecting to the FTP server"
    )
    source_host: str | None = Field(
        default=None,
        description="Host for the socket to bind to as its source address before connecting"
    )
    source_port: int | None = Field(
        default=None,
        description="Port for the socket to bind to as its source address before connecting"
    )
    encoding: str = Field(default="utf-8", description="The encoding for directories and filenames")

    # Login info
    username: str = Field(default="anonymous", description="User name to login with")
    password: str | None = Field(default=None, description="User password to login with")
    acct: str | None = Field(
        default=None,
        description="Account information to be used for the ACCT FTP command. " +
                    "Few systems implement this. See RFC-959 for more details."
    )

    # Other
    debug_level: int = Field(
        default=0,
        description="Set debugging level for this FTP instance. 0=no debug output; 1=moderate; 2=maximum"
    )

    # internal state
    __cli = PrivateAttr()

    @contextlib.contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        kwargs = {}
        if self.source_host or self.source_port:
            kwargs["source_address"] = (self.source_host, self.source_port)
        if self.encoding is not None and len(self.encoding) > 0:
            kwargs["encoding"] = self.encoding
        with (ftplib.FTP_TLS(**kwargs) if self.tls else ftplib.FTP(**kwargs)) as cli:
            cli.set_debuglevel(self.debug_level)
            cli.connect(self.host,
                        self.port or 21,
                        self.connect_timeout or self.timeout)
            cli.login(self.username, self.password or "", self.acct or "")
            self.__cli = cli
            yield self

    def download(self, source: str, destination: BinaryIO) -> int:
        # TODO: manage reconnection
        acc = _ByteAccumulator(destination)
        return_code = self.__cli.retrbinary(f"RETR {source}", callback=acc.write_and_count)
        if not return_code.startswith("226 "):
            raise RuntimeError(f"Binary transfer of {source} failed. Return code: {return_code}")
        return acc.nb_written_bytes

    def recursive_download(self, source: str, destination: Path) -> Iterable[FileTransferReport]:
        return_code = self.__cli.cwd(source)
        # TODO: improve error detection
        if not return_code.startswith("2"):
            raise RuntimeError(f"Directory change failed. Return code: {return_code}")

        def file_with_type(entry: tuple[str,dict]):
            return entry[0], entry[1]["type"]

        for path, type in map(file_with_type, self.__cli.mlsd(facts=["type"])):
            file_name = PosixPath(path).name
            src_file = f"{source}/{file_name}"
            dest_file = destination/file_name
            if type == "file":
                with open(dest_file, mode="wb") as output_stream:
                    byte_count = self.download(src_file, output_stream)
                    yield FileTransferReport(src_file, dest_file, byte_count)
            elif type == "dir":
                dest_file.mkdir()
                yield from self.recursive_download(src_file, dest_file)
            # else: ignore cdir (current directory), pdir (parent directory) and os-related files
