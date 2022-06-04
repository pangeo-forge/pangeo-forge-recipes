from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

import apache_beam as beam
import xarray as xr

from .patterns import Index
from .storage import CacheFSSpecTarget, OpenFileType, get_opener

logger = logging.getLogger(__name__)

# can we use a generic, e.g Indexed[xr.Dataset]?
# Indexed[int] -> Tuple[Index, int]


# This has side effects if using a cache
@dataclass
class OpenWithFSSpec(beam.PTransform):
    """Open indexed items from a FilePattern with FSSpec, optionally caching along the way."""

    cache: Optional[CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None

    def _open_with_fsspec(self, element: Tuple[Index, Any]) -> Tuple[Index, OpenFileType]:
        key, fname = element
        open_kwargs = self.open_kwargs or {}
        if self.cache is not None:
            logger.info(f"Opening '{fname}' from cache")
            # this has side effects
            self.cache.cache_file(fname, self.secrets, **open_kwargs)
            open_file = self.cache.open_file(fname, mode="rb")
        else:
            logger.info(f"Opening '{fname}' directly.")
            open_file = get_opener(fname, self.secrets, **open_kwargs)
        open_file
        return key, open_file

    def expand(self, pcoll):
        return pcoll | "Open with fsspec" >> beam.Map(self._open_with_fsspec)


@dataclass
class OpenWithXarray(beam.PTransform):

    xarray_open_kwargs: Optional[dict] = field(default_factory=dict)
    load: bool = False

    def _open_with_xarray(self, element: Tuple[Index, Any]) -> Tuple[Index, xr.Dataset]:
        key, open_file = element
        logger.debug(f"_open_with_xarray({key}, {open_file})")
        # workaround fsspec inconsistencies
        if hasattr(open_file, "open"):
            open_file = open_file.open()
        ds = xr.open_dataset(open_file, cache=False, lock=False, **self.xarray_open_kwargs)
        if self.load:
            ds.load()
        return key, ds

    def expand(self, pcoll):
        return pcoll | "Open with Xarray" >> beam.Map(self._open_with_xarray)


# @beam.typehints.with_input_types(Tuple[Index, xr.Dataset])
# @beam.typehints.with_output_types(Tuple[Index, Dict])
# @dataclass
# class GetXarraySchema(beam.PTransform):
#     def expand(self, pcoll):
#         pass
#
#
# @beam.typehints.with_input_types(Dict)
# @beam.typehints.with_output_types(zarr.Group)
# @dataclass
# class CreateZarrFromSchema(beam.PTransform):
#     def expand(self, pcoll):
#         pass
