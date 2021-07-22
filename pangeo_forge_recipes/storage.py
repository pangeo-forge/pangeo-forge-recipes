import hashlib
import json
import logging
import os
import re
import tempfile
import unicodedata
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional, Sequence, Union

import fsspec
from fsspec.implementations.http import BlockSizeError

logger = logging.getLogger(__name__)

# fsspec doesn't provide type hints, so I'm not sure what the write type is for open files
OpenFileType = Any


def _get_url_size(fname, **open_kwargs):
    with fsspec.open(fname, mode="rb", **open_kwargs) as of:
        size = of.size
    return size


def _copy_btw_filesystems(input_opener, output_opener, BLOCK_SIZE=10_000_000, **kwargs):
    streaming = True if kwargs.pop("block_size", True) == 0 else False
    if os.getenv("PANGEO_FORGE_BLOCK_SIZE"):
        BLOCK_SIZE = int(os.getenv("PANGEO_FORGE_BLOCK_SIZE"))
    with input_opener as source:
        with output_opener as target:
            count = summed_bytes = 0
            while True:
                logger.debug("_copy_btw_filesystems reading data") if not streaming else None
                try:
                    data = source.read(BLOCK_SIZE)
                except BlockSizeError as e:
                    raise ValueError(
                        "Server does not permit random access to this file via Range requests. "
                        'Try re-instantiating recipe with `fsspec_open_kwargs={"block_size": 0}`'
                    ) from e
                if not data:
                    break
                # strided logging pattern for streaming transfers to avoid excessive logs
                if streaming:
                    summed_bytes += len(data)
                    if summed_bytes // BLOCK_SIZE >= count:
                        logger.debug(f"copying block {count} of ~{BLOCK_SIZE} bytes")
                        count += 1
                else:
                    logger.debug(f"_copy_btw_filesystems copying block of {len(data)} bytes")
                target.write(data)
    logger.debug("_copy_btw_filesystems done")


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
    """Representation of a storage target for Pangeo Forge.

    :param fs: The filesystem object we are writing to.
    :param root_path: The path under which the target data will be stored.
    """

    fs: fsspec.AbstractFileSystem
    root_path: str = ""

    def get_mapper(self) -> fsspec.mapping.FSMap:
        """Get a mutable mapping object suitable for storing Zarr data."""
        return self.fs.get_mapper(self.root_path)

    def _full_path(self, path: str):
        return os.path.join(self.root_path, path)

    def exists(self, path: str) -> bool:
        """Check that the file is in the cache."""
        return self.fs.exists(self._full_path(path))

    def rm(self, path: str) -> None:
        """Remove file from the cache."""
        self.fs.rm(self._full_path(path))

    def size(self, path: str) -> int:
        return self.fs.size(self._full_path(path))

    @contextmanager
    def open(self, path: str, **kwargs) -> Iterator[None]:
        """Open file with a context manager."""
        full_path = self._full_path(path)
        logger.debug(f"entering fs.open context manager for {full_path}")
        with self.fs.open(full_path, **kwargs) as f:
            logger.debug(f"FSSpecTarget.open yielding {f}")
            yield f
            logger.debug("FSSpecTarget.open yielded")

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
        new_path = "-".join([prefix, slug])
        return os.path.join(self.root_path, new_path)


class CacheFSSpecTarget(FlatFSSpecTarget):
    """Alias for FlatFSSpecTarget"""

    def cache_file(self, fname: str, **open_kwargs) -> None:
        # check and see if the file already exists in the cache
        logger.info(f"Caching file '{fname}'")
        if self.exists(fname):
            cached_size = self.size(fname)
            remote_size = _get_url_size(fname, **open_kwargs)
            if cached_size == remote_size:
                # TODO: add checksumming here
                logger.info(f"File '{fname}' is already cached")
                return

        input_opener = fsspec.open(fname, mode="rb", **open_kwargs)
        target_opener = self.open(fname, mode="wb")
        logger.info(f"Coping remote file '{fname}' to cache")
        _copy_btw_filesystems(input_opener, target_opener, **open_kwargs)


class MetadataTarget(FSSpecTarget):
    """Target for storing metadata dictionaries as json."""

    def __setitem__(self, key: str, value: dict) -> None:
        mapper = self.get_mapper()
        mapper[key] = json.dumps(value).encode("utf-8")

    def __getitem__(self, key: str) -> dict:
        return json.loads(self.get_mapper()[key])

    def getitems(self, keys: Sequence[str]) -> dict:
        mapper = self.get_mapper()
        all_meta_raw = mapper.getitems(keys)
        return {k: json.loads(raw_bytes) for k, raw_bytes in all_meta_raw.items()}


@contextmanager
def file_opener(
    fname: str,
    cache: Optional[CacheFSSpecTarget] = None,
    copy_to_local: bool = False,
    **open_kwargs,
) -> Iterator[Union[OpenFileType, str]]:
    """
    Context manager for opening files.

    :param fname: The filename / url to open. Fsspec will inspect the protocol
        (e.g. http, ftp) and determine the appropriate filesystem type to use.
    :param cache: A target where the file may have been cached. If none, the file
        will be opened directly.
    :param copy_to_local: If True, always copy the file to a local temporary file
        before opening. In this case, function yields a path name rather than an open file.
    """
    if cache is not None:
        logger.info(f"Opening '{fname}' from cache")
        opener = cache.open(fname, mode="rb")
    else:
        logger.info(f"Opening '{fname}' directly.")
        opener = fsspec.open(fname, mode="rb", **open_kwargs)
    if copy_to_local:
        _, suffix = os.path.splitext(fname)
        ntf = tempfile.NamedTemporaryFile(suffix=suffix)
        tmp_name = ntf.name
        logger.info(f"Copying '{fname}' to local file '{tmp_name}'")
        target_opener = open(tmp_name, mode="wb")
        _copy_btw_filesystems(opener, target_opener)
        yield tmp_name
        ntf.close()  # cleans up the temporary file
    else:
        logger.debug(f"file_opener entering first context for {opener}")
        with opener as fp:
            logger.debug(f"file_opener entering second context for {fp}")
            yield fp
            logger.debug("file_opener yielded")
    logger.debug("opener done")


def _slugify(value: str) -> str:
    # Adopted from
    # https://github.com/django/django/blob/master/django/utils/text.py
    # https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
    value = str(value)
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^.\w\s-]+", "_", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")
