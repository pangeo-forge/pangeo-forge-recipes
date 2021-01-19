import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import BinaryIO, NoReturn

import fsspec


@dataclass
class Target:
    """Representation of a storage target for Pangeo Forge.

    :param fs: The filesystem object we are writing to.
    :param path: The path where the target data will be saved.
    """

    fs: fsspec.AbstractFileSystem
    path: str

    def get_mapper(self) -> fsspec.mapping.FSMap:
        """Get a mutable mapping object suitable for storing Zarr data."""
        return self.fs.get_mapper(self.path)


def _hash_path(path: str) -> str:
    return str(hash(path))


@dataclass
class InputCache:
    """Representation of an intermediate storage location where remote files
    Can be cached locally.

    :param fs: The filesystem we are writing to.
    :param prefix: A path prepended to all paths.
    """

    fs: fsspec.AbstractFileSystem
    prefix: str = ""

    def _full_path(self, path):
        return os.path.join(self.prefix, _hash_path(path))

    def exists(self, path) -> bool:
        """Check that the file is in the cache."""
        return self.fs.exists(self._full_path(path))

    def rm(self, path) -> NoReturn:
        """Remove file from the cache."""
        self.fs.rm(self._full_path(path))

    @contextmanager
    def open(self, path, **kwargs) -> BinaryIO:
        """Open file with a context manager."""
        with self.fs.open(self._full_path(path), **kwargs) as f:
            yield f

    def __post_init__(self):
        if not self.fs.isdir(self.prefix):
            self.fs.mkdir(self.prefix)
