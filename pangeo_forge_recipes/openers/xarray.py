import hashlib
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, Optional

import fsspec
import xarray as xr

from ..reference import create_hdf5_reference, unstrip_protocol
from ..storage import CacheFSSpecTarget, MetadataTarget
from .base import BaseOpener
from .fsspec import FsspecLocalCopyOpener, FsspecOpener

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class XarrayOpener(BaseOpener[str, xr.Dataset]):
    """Open an input as an Xarray Dataset directly from a url path."""

    xarray_open_kwargs: dict = field(default_factory=dict)
    load: bool = False

    @contextmanager
    def open(self, input: str) -> Generator[xr.Dataset, None, None]:
        kw = self.xarray_open_kwargs.copy()
        if "engine" not in kw:
            kw["engine"] = "h5netcdf"
        with xr.open_dataset(input, **kw) as ds:
            if self.load:
                ds.load()
            yield ds


@dataclass(frozen=True)
class XarrayFsspecOpener(XarrayOpener, FsspecOpener, BaseOpener[str, xr.Dataset]):
    """Open an input as an Xarray Dataset via fsspec."""

    @contextmanager
    def open(
        self, input: str, cache: Optional[CacheFSSpecTarget] = None
    ) -> Generator[xr.Dataset, None, None]:
        # could not get super() to work right here
        with FsspecOpener.open(self, input, cache) as f_obj:
            with XarrayOpener.open(self, f_obj) as ds:
                yield ds


@dataclass(frozen=True)
class XarrayFsspecLocalCopyOpener(XarrayOpener, FsspecLocalCopyOpener, BaseOpener[str, xr.Dataset]):
    """Open an input as an xarray Dataset via fsspec,
    first copying the input to a local temporary file.
    """

    @contextmanager
    def open(
        self, input: str, cache: Optional[CacheFSSpecTarget] = None
    ) -> Generator[xr.Dataset, None, None]:
        with FsspecLocalCopyOpener.open(self, input, cache) as path:
            with XarrayOpener.open(self, path) as ds:
                yield ds


def _input_reference_fname(input: str) -> str:
    hash = hashlib.md5(input.encode()).hexdigest()
    return f"input-reference-{hash}.json"


# not sure if frozen is a good choice--we might want to dynamically set metadata_cache
@dataclass(frozen=True)
class XarrayKerchunkOpener(XarrayOpener, FsspecOpener, BaseOpener[str, xr.Dataset]):
    """Open an input as an Xarray Dataset via Kerchunk + Zarr."""

    def cache_input_metadata(
        self, input: str, metadata_cache: MetadataTarget, cache: CacheFSSpecTarget
    ) -> None:
        if cache is None:
            protocol = fsspec.utils.get_protocol(input)
            url = unstrip_protocol(input, protocol)
        else:
            url = unstrip_protocol(cache._full_path(input), cache.fs.protocol)
        with FsspecOpener.open(self, input) as fp:
            ref_data = create_hdf5_reference(fp, url, input)
        ref_fname = _input_reference_fname(input)
        metadata_cache[ref_fname] = ref_data

    # pangeo_forge_recipes/openers/xarray.py:89: error: Signature of "open" incompatible
    #   with supertype "BaseOpener"  [override]
    # Looks like mypy doesn't like having another non-keyword argument
    @contextmanager
    def open(  # type: ignore
        self, input: str, metadata_cache: MetadataTarget
    ) -> Generator[xr.Dataset, None, None]:
        from fsspec.implementations.reference import ReferenceFileSystem

        reference_data = metadata_cache[_input_reference_fname(input)]

        # TODO: figure out how to set this for the cache target
        remote_protocol = fsspec.utils.get_protocol(input)
        ref_fs = ReferenceFileSystem(
            reference_data, remote_protocol=remote_protocol, skip_instance_cache=True
        )
        mapper = ref_fs.get_mapper("/")

        # Doesn't really need to be a context manager, but that's how this function works
        with xr.open_dataset(
            mapper, engine="zarr", chunks={}, consolidated=False, **self.xarray_open_kwargs
        ) as ds:
            yield ds
