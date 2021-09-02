import hashlib
import json
import logging
import os
import re
import tempfile
import time
import unicodedata
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Optional, Sequence, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import fsspec
from fsspec.implementations.http import BlockSizeError

logger = logging.getLogger(__name__)

# fsspec doesn't provide type hints, so I'm not sure what the write type is for open files
OpenFileType = Any


def _get_url_size(fname, secrets, **open_kwargs):
    with _get_opener(fname, secrets, **open_kwargs) as of:
        size = of.size
    return size


def _copy_btw_filesystems(input_opener, output_opener, BLOCK_SIZE=10_000_000):
    with input_opener as source:
        with output_opener as target:
            start = time.time()
            interval = 5  # seconds
            bytes_read = log_count = 0
            while True:
                try:
                    data = source.read(BLOCK_SIZE)
                except BlockSizeError as e:
                    raise ValueError(
                        "Server does not permit random access to this file via Range requests. "
                        'Try re-instantiating recipe with `fsspec_open_kwargs={"block_size": 0}`'
                    ) from e
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

    def cache_file(self, fname: str, secrets: Optional[dict], **open_kwargs) -> None:
        # check and see if the file already exists in the cache
        logger.info(f"Caching file '{fname}'")
        if self.exists(fname):
            cached_size = self.size(fname)
            remote_size = _get_url_size(fname, secrets, **open_kwargs)
            if cached_size == remote_size:
                # TODO: add checksumming here
                logger.info(f"File '{fname}' is already cached")
                return

        input_opener = _get_opener(fname, secrets, **open_kwargs)
        target_opener = self.open(fname, mode="wb")
        logger.info(f"Copying remote file '{fname}' to cache")
        _copy_btw_filesystems(input_opener, target_opener)


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
    bypass_open: bool = False,
    secrets: Optional[dict] = None,
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
    :param bypass_open: If True, skip trying to open the file at all and just
        return the filename back directly. (A fancy way of doing nothing!)
    """

    if bypass_open:
        if cache or copy_to_local:
            raise ValueError("Can't bypass open with cache or copy_to_local.")
        logger.debug(f"Bypassing open for '{fname}'")
        yield fname
        return

    if cache is not None:
        logger.info(f"Opening '{fname}' from cache")
        opener = cache.open(fname, mode="rb")
    else:
        logger.info(f"Opening '{fname}' directly.")
        opener = _get_opener(fname, secrets, **open_kwargs)
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


def _add_query_string_secrets(fname: str, secrets: dict) -> str:
    parsed = urlparse(fname)
    query = parse_qs(parsed.query)
    for k, v in secrets.items():
        query.update({k: v})
    parsed = parsed._replace(query=urlencode(query, doseq=True))
    return urlunparse(parsed)


def _get_opener(fname: str, secrets: Optional[dict], **open_kwargs):
    fname = fname if not secrets else _add_query_string_secrets(fname, secrets)
    return fsspec.open(fname, mode="rb", **open_kwargs)
