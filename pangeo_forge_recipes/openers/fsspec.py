import logging
import os
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import fsspec
from fsspec.implementations.http import BlockSizeError

from ..storage import CacheFSSpecTarget
from .base import BaseOpener

logger = logging.getLogger(__name__)


def _add_query_string_secrets(fname: str, secrets: Optional[dict]) -> str:
    if secrets is None:
        return fname
    parsed = urlparse(fname)
    query = parse_qs(parsed.query)
    for k, v in secrets.items():
        query.update({k: v})
    parsed = parsed._replace(query=urlencode(query, doseq=True))
    return urlunparse(parsed)


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
                        'Try re-instantiating Opener with `fsspec_fsspec_open_kwargs={"block_size": 0}`'  # noqa
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


@dataclass(frozen=True)
class FsspecOpener(BaseOpener[str, fsspec.core.OpenFile]):
    """
    Opener that takes a string and opens it with fsspec.open.

    :param cache: A target where the file may have been cached. If none, the file
        will be opened directly.
    :param secrets: Dictionary of secrets to encode into the query string.
    :param fsspec_open_kwargs: Keyword arguments to pass to fsspec.open
    """

    cache: Optional[CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    fsspec_open_kwargs: dict = field(default_factory=dict)

    def _get_opener(self, fname: str):
        fname = _add_query_string_secrets(fname, self.secrets)
        return fsspec.open(fname, mode="rb", **self.fsspec_open_kwargs)

    def _get_url_size(self, fname: str) -> int:
        with self._get_opener(fname) as of:
            size = of.size
        return size

    @contextmanager
    def open(self, input: str) -> Generator[fsspec.core.OpenFile, None, None]:
        """
        Open a file. Yields an fsspec.core.OpenFile object.

        :param input: The filename / url to open. Fsspec will inspect the protocol
            (e.g. http, ftp) and determine the appropriate filesystem type to use.
        """

        if self.cache is not None:
            logger.info(f"Opening '{input}' from cache")
            opener = self.cache.open(input, mode="rb")
        else:
            logger.info(f"Opening '{input}' directly.")
            opener = self._get_opener(input)

        logger.debug(f"file_opener entering first context for {opener}")
        with opener as fp:
            logger.debug(f"file_opener entering second context for {fp}")
            yield fp
            logger.debug("file_opener yielded")
        logger.debug("opener done")

    def cache_file(self, fname: str) -> None:
        # check and see if the file already exists in the cache
        if self.cache is None:
            raise ValueError("Cannot cache file; no cache object provided.")
        logger.info(f"Caching file '{fname}'")
        if self.cache.exists(fname):
            cached_size = self.cache.size(fname)
            remote_size = self._get_url_size(fname)
            if cached_size == remote_size:
                # TODO: add checksumming here
                logger.info(f"File '{fname}' is already cached")
                return

        input_opener = self._get_opener(fname)
        target_opener = self.cache.open(fname, mode="wb")
        logger.info(f"Copying remote file '{fname}' to cache")
        _copy_btw_filesystems(input_opener, target_opener)


@dataclass(frozen=True)
class FsspecLocalCopyOpener(FsspecOpener, BaseOpener[str, str]):
    @contextmanager
    def open(self, fname: str) -> Generator[str, None, None]:
        """
        Copy the file to a local path and yield the path.

        :param fname: The filename / url to open. Fsspec will inspect the protocol
            (e.g. http, ftp) and determine the appropriate filesystem type to use.
        """

        if self.cache is not None:
            logger.info(f"Opening '{fname}' from cache")
            opener = self.cache.open(fname, mode="rb")
        else:
            logger.info(f"Opening '{fname}' directly.")
            opener = self._get_opener(fname)

        _, suffix = os.path.splitext(fname)
        ntf = tempfile.NamedTemporaryFile(suffix=suffix)
        tmp_name = ntf.name
        logger.info(f"Copying '{fname}' to local file '{tmp_name}'")
        target_opener = open(tmp_name, mode="wb")
        _copy_btw_filesystems(opener, target_opener)
        yield tmp_name
        ntf.close()  # cleans up the temporary file
