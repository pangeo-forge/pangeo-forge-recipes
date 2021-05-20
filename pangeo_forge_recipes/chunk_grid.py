"""
Abstract representation of ND chunked arrays
"""

from dataclasses import dataclass
from typing import Tuple

import numpy as np

# Most of this is probably already in Dask and Zarr!
# However, it's useful to write up our own little model that does just what we need.

ChunkIndex = Tuple[int, ...]


@dataclass
class ChunkGrid:
    shape: Tuple[int, ...]

    # we will always be dealing with named dims
    dims: Tuple[str, ...]

    # chunks are stored explicitly (don't assume uniform length)
    chunks: Tuple[Tuple[int, ...], ...]

    @property
    def ndim(self):
        return len(self.shape)

    def __post_init__(self):
        if len(self.dims) != self.ndim:  # pragma: no cover
            raise ValueError("dims must have the same len as shape")
        if len(self.chunks) != self.ndim:  # pragma: no cover
            raise ValueError("chunks must have the same len as shape")

        self._chunk_axes = [
            ChunkAxis(dim_shape, dim_chunks)
            for dim_shape, dim_chunks in zip(self.shape, self.chunks)
        ]

    def chunk_index_to_slice(self, chunk_index: ChunkIndex) -> Tuple[slice, ...]:
        return tuple(
            [axis.chunk_index_to_slice(idx) for axis, idx in zip(self._chunk_axes, chunk_index)]
        )

    def slice_to_chunk_indexes(self, slices: Tuple[slice]) -> ChunkIndex:
        return tuple([axis.slice_to_chunk_index(sl) for axis, sl in zip(self._chunk_axes, slices)])


@dataclass
class ChunkAxis:
    len: int
    chunks: Tuple[int, ...]

    def __post_init__(self):
        if sum(self.chunks) != self.len:  # pragma: no cover
            raise ValueError("chunks are incompatible with shape")

        self._chunk_bounds = np.hstack([0, np.cumsum(self.chunks)])

    def chunk_index_to_slice(self, index: int) -> slice:
        return slice(self._chunk_bounds[index], self._chunk_bounds[index + 1])

    def slice_to_chunk_index(self, sl: slice) -> int:
        assert sl.step is None
        # TODO: actuall write this logic
        return 0
