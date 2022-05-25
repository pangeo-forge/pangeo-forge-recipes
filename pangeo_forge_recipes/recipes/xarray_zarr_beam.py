from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import apache_beam as beam

from ..patterns import Index
from ..storage import CacheFSSpecTarget, OpenFileType, get_opener

logger = logging.getLogger(__name__)


IndexedElement = Tuple[Index, Any]
# can we use a generic, e.g Indexed[xr.Dataset]?


# This has side effects if using a cache
@dataclass
class OpenWithFSSpec(beam.PTransform):
    """Write keyed chunks to a Zarr store in parallel."""

    cache: Optional[CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None

    def _open_with_fsspec(self, element: IndexedElement) -> Tuple[Index, OpenFileType]:
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
