import os
import re
import unicodedata
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import BinaryIO, NoReturn

import fsspec


class AbstractTarget(ABC):
    @abstractmethod
    def get_mapper(self):
        pass

    @abstractmethod
    def exists(self, path) -> bool:
        """Check that the file exists."""
        pass

    @abstractmethod
    def rm(self, path) -> NoReturn:
        """Remove file."""
        pass

    @contextmanager
    def open(self, path, **kwargs) -> BinaryIO:
        """Open file with a context manager."""
        pass


def _hash_path(path: str) -> str:
    return str(hash(path))


@dataclass
class FSSpecTarget(AbstractTarget):
    """Representation of a storage target for Pangeo Forge.

    :param fs: The filesystem object we are writing to.
    :param root_path: The path under which the target data will be stored.
    """

    fs: fsspec.AbstractFileSystem
    root_path: str = ""

    def get_mapper(self) -> fsspec.mapping.FSMap:
        """Get a mutable mapping object suitable for storing Zarr data."""
        return self.fs.get_mapper(self.root_path)

    def _full_path(self, path):
        return os.path.join(self.root_path, path)

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
        if not self.fs.isdir(self.root_path):
            self.fs.mkdir(self.root_path)


class FlatFSSpecTarget(FSSpecTarget):
    """A target that sanitizes all the path names so that everthing is stored
    in a single directory.

    Designed to be used as a cache for inputs.
    """

    def _full_path(self, path):
        slug = _slugify(path)
        prefix = prefix = hex(hash(path))[2:10]
        new_path = "-".join([prefix, slug])
        return os.path.join(self.root_path, new_path)


class CacheFSSpecTarget(FlatFSSpecTarget):
    """Alias for FlatFSSpecTarget"""

    pass


def _slugify(value):
    # Adopted from
    # https://github.com/django/django/blob/master/django/utils/text.py
    # https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
    value = str(value)
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^.\w\s-]+", "_", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


class UninitializedTarget(AbstractTarget):
    def get_mapper(self):
        raise UninitializedTargetError

    def exists(self, path) -> bool:
        raise UninitializedTargetError

    def rm(self, path) -> NoReturn:
        raise UninitializedTargetError

    def open(self, path, **kwargs) -> BinaryIO:
        raise UninitializedTargetError


class TargetError(Exception):
    """Base class for exceptions in this module."""

    pass


class UninitializedTargetError(TargetError):
    """Operation on an uninitialized Target."""

    pass
