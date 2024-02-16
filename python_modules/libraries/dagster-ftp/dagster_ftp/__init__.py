from dagster._core.libraries import DagsterLibraryRegistry

from .resources import FTPResource
from .version import __version__

DagsterLibraryRegistry.register("dagster-ftp", __version__)

__all__ = ["FTPResource"]
