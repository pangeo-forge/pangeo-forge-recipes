from __future__ import annotations

from functools import wraps
import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

import apache_beam as beam
import xarray as xr

from .patterns import Index, FileType
from .openers import open_with_xarray
from .storage import CacheFSSpecTarget, OpenFileType, get_opener

logger = logging.getLogger(__name__)

# From https://beam.apache.org/contribute/ptransform-style-guide/
#
# Do:
# - Expose every major data-parallel task accomplished by your library as a
#   composite PTransform. This allows the structure of the transform to evolve
#   transparently to the code that uses it: e.g. something that started as a
#   ParDo can become a more complex transform over time.
# - Expose large, non-trivial, reusable sequential bits of the transform’s code,
#   which others might want to reuse in ways you haven’t anticipated, as a regular
#   function or class library. The transform should simply wire this logic together.
#   As a side benefit, you can unit-test those functions and classes independently.
#   Example: when developing a transform that parses files in a custom data format,
#   expose the format parser as a library; likewise for a transform that implements
#   a complex machine learning algorithm, etc.
# - In some cases, this may include Beam-specific classes, such as CombineFn,
#   or nontrivial DoFns (those that are more than just a single @ProcessElement
#   method). As a rule of thumb: expose these if you anticipate that the full
#   packaged PTransform may be insufficient for a user’s needs and the user may want
#   to reuse the lower-level primitive.
#
# Do not:
# - Do not expose the exact way the transform is internally structured.
#   E.g.: the public API surface of your library usually (with exception of the
#   last bullet above) should not expose DoFn, concrete Source or Sink classes,
#   etc., in order to avoid presenting users with a confusing choice between
#   applying the PTransform or using the DoFn/Source/Sink.

# In the spirit of "[t]he transform should simply wire this logic together",
# the goal is to put _as little logic as possible_ in this module.
# Ideally each PTransform should be a simple Map or DoFn calling out to function
# from other modules


# can we use a generic, e.g Indexed[xr.Dataset]?
# Indexed[int] -> Tuple[Index, int]

def _add_keys(func):
    """Convenience decorator to remove and re-add keys to items in a Map"""
    @wraps(func)
    def wrapper(arg, **kwargs):
        key, item = arg
        result = func(item, **kwargs)
        return key, result
    return wrapper

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

    file_type: FileType = FileType.unknown
    load: bool = False
    xarray_open_kwargs: Optional[dict] = field(default_factory=dict)

    def expand(self, pcoll):
        return pcoll | "Open with Xarray" >> beam.Map(
            _add_keys(open_with_xarray),
            file_type=self.file_type,
            load=self.load,
            xarray_open_kwargs=self.xarray_open_kwargs
        )


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
