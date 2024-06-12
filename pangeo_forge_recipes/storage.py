from __future__ import annotations

import hashlib
import io
import logging
import os
import re
import time
import unicodedata
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterator, Optional, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import fsspec
from fsspec.implementations.local import LocalFileSystem
from zarr.storage import FSStore

logger = logging.getLogger(__name__)

OpenFileType = Union[fsspec.core.OpenFile, fsspec.spec.AbstractBufferedFile, io.IOBase]


def _copy_btw_filesystems(input_opener, output_opener, BLOCK_SIZE=10_000_000):
    try:
        with input_opener as source, output_opener as target:
            start = time.time()
            interval = 5  # seconds
            bytes_read = log_count = 0
            while True:
                data = source.read(BLOCK_SIZE)
                if not data:
                    break
                target.write(data)
                bytes_read += len(data)
                elapsed = time.time() - start
                throughput = bytes_read / elapsed
                if elapsed // interval >= log_count:
                    logger.debug(f"_copy_btw_filesystems total bytes copied: {bytes_read}")
                    logger.debug(
                        f"avg throughput over {elapsed/60:.2f} min: {throughput/1e6:.2f} MB/sec"
                    )
                    log_count += 1
    except Exception as e:
        logger.error(f"Failed during file copy after reading {bytes_read} bytes: {e}")
        raise e
    logger.debug("_copy_btw_filesystems completed successfully")


class AbstractTarget(ABC):
    @abstractmethod
    def get_mapper(self):
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check that the file exists."""
        pass

    @abstractmethod
    def rm(self, path: str) -> None:
        """Remove file."""
        pass

    @contextmanager
    def open(self, path: str, **kwargs):  # don't know how to type hint this
        """Open file with a context manager."""
        pass

    @abstractmethod
    def size(self, path: str) -> int:
        """Get file size"""
        pass


def _hash_path(path: str) -> str:
    return str(hash(path))


@dataclass
class FSSpecTarget(AbstractTarget):
    """
    Representation of a storage target for Pangeo Forge.

    :param fs: The filesystem object we are writing to.
    :param root_path: The path under which the target data will be stored.
    :param fsspec_kwargs: The fsspec kwargs that can be reused as
                          `target_options` and `remote_options` for fsspec class instantiation
    """

    fs: fsspec.AbstractFileSystem
    root_path: str = ""
    fsspec_kwargs: Dict[Any, Any] = field(default_factory=dict)

    def __truediv__(self, suffix: str) -> FSSpecTarget:
        """
        Support / operator so FSSpecTarget actslike a pathlib.path

        Only supports getting a string suffix added in.
        """
        return replace(self, root_path=os.path.join(self.root_path, suffix))

    @classmethod
    def from_url(cls, url: str):
        fs, _, root_paths = fsspec.get_fs_token_paths(url)
        assert len(root_paths) == 1
        return cls(fs, root_paths[0])

    def get_fsspec_remote_protocol(self):
        """fsspec implementation-specific remote protocal"""
        fsspec_protocol = self.fs.protocol
        if isinstance(fsspec_protocol, str):
            return fsspec_protocol
        elif isinstance(fsspec_protocol, tuple):
            return fsspec_protocol[0]
        elif isinstance(fsspec_protocol, list):
            return fsspec_protocol[0]
        else:
            raise ValueError(
                f"could not resolve fsspec protocol '{fsspec_protocol}' from underlying filesystem"
            )

    def get_mapper(self) -> fsspec.mapping.FSMap:
        """Get a mutable mapping object suitable for storing Zarr data."""
        return FSStore(self.root_path, fs=self.fs)

    def _full_path(self, path: str):
        return os.path.join(self.root_path, path)

    def exists(self, path: str) -> bool:
        """Check that the file is in the cache."""
        return self.fs.exists(self._full_path(path))

    def rm(self, path: str, recursive: Optional[bool] = False) -> None:
        """Remove file from the cache."""
        self.fs.rm(self._full_path(path), recursive=recursive)

    def size(self, path: str) -> int:
        return self.fs.size(self._full_path(path))

    def makedir(self, path: str) -> None:
        self.fs.makedir(self._full_path(path))

    @contextmanager
    def open(self, path: str, **kwargs) -> Iterator[OpenFileType]:
        """Open file with a context manager."""
        full_path = self._full_path(path)
        logger.debug(f"entering fs.open context manager for {full_path}")
        of = self.fs.open(full_path, **kwargs)
        logger.debug(f"FSSpecTarget.open yielding {of}")
        yield of
        logger.debug("FSSpecTarget.open yielded")
        of.close()

    def open_file(self, path: str, **kwargs) -> OpenFileType:
        """Returns an fsspec open file"""
        full_path = self._full_path(path)
        logger.debug(f"returning open file for {full_path}")
        return self.fs.open(full_path, **kwargs)

    def __post_init__(self):
        if not self.fs.isdir(self.root_path):
            self.fs.mkdir(self.root_path)


class FlatFSSpecTarget(FSSpecTarget):
    """A target that sanitizes all the path names so that everything is stored
    in a single directory.

    Designed to be used as a cache for inputs.
    """

    def _full_path(self, path: str) -> str:
        # this is just in case _slugify(path) is non-unique
        prefix = hashlib.md5(path.encode()).hexdigest()
        slug = _slugify(path)
        if isinstance(self.fs, LocalFileSystem) and len("-".join([prefix, slug])) > 255:
            drop_nchars = len("-".join([prefix, slug])) - 255
            slug = slug[drop_nchars:]
            logger.warning(
                "POSIX filesystems don't allow filenames to exceed 255 bytes in length. "
                f"Truncating the filename slug for path '{path}' to '{slug}' to accommodate this."
            )
        new_path = "-".join([prefix, slug])
        return os.path.join(self.root_path, new_path)


class CacheFSSpecTarget(FlatFSSpecTarget):
    """Alias for FlatFSSpecTarget"""

    def cache_file(
        self, fname: str, secrets: Optional[dict], fsspec_sync_patch=False, **open_kwargs
    ) -> None:
        # check and see if the file already exists in the cache
        logger.info(f"Caching file '{fname}'")
        input_opener = _get_opener(fname, secrets, fsspec_sync_patch, **open_kwargs)

        if self.exists(fname):
            cached_size = self.size(fname)
            with input_opener as of:
                remote_size = of.size
            if cached_size == remote_size:
                # TODO: add checksumming here
                logger.info(f"File '{fname}' is already cached")
                return

        target_opener = self.open(fname, mode="wb")
        logger.info(f"Copying remote file '{fname}' to cache")
        _copy_btw_filesystems(input_opener, target_opener)


def _slugify(value: str) -> str:
    # Adopted from
    # https://github.com/django/django/blob/master/django/utils/text.py
    # https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
    value = str(value)
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^.\w\s-]+", "_", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


def _add_query_string_secrets(fname: str, secrets: dict) -> str:
    parsed = urlparse(fname)
    query = parse_qs(parsed.query)
    for k, v in secrets.items():
        query.update({k: v})
    parsed = parsed._replace(query=urlencode(query, doseq=True))
    return urlunparse(parsed)


def _get_opener(fname, secrets, fsspec_sync_patch, **open_kwargs):
    if fsspec_sync_patch:
        logger.debug("Attempting to enable synchronous filesystem implementations in FSSpec")
        from httpfs_sync.core import SyncHTTPFileSystem

        SyncHTTPFileSystem.overwrite_async_registration()
        logger.debug("Synchronous HTTP implementation enabled.")

    fname = fname if not secrets else _add_query_string_secrets(fname, secrets)
    return fsspec.open(fname, mode="rb", **open_kwargs)


def file_opener(*args, **kwargs):
    # dummy function to keep test suite running
    pass
