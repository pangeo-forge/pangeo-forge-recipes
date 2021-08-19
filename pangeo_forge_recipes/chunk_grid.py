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
    """A ChunkAxis has two index spaces.

    Array index space is a regular python index of an array / list.
    Chunk index space describes chunk positions.

    A ChunkAxis helps translate between these two spaces.

    """

    chunks: Tuple[int, ...]

    def __post_init__(self):
        self._chunk_bounds = np.hstack([0, np.cumsum(self.chunks)])

    def __len__(self):
        return self._chunk_bounds[-1].item()

    @property
    def nchunks(self):
        return len(self.chunks)

    def chunk_index_to_array_slice(self, chunk_index: int) -> slice:
        return slice(self._chunk_bounds[chunk_index], self._chunk_bounds[chunk_index + 1])

    def array_index_to_chunk_index(self, array_index: int) -> int:
        if array_index < 0 or array_index >= len(self):
            raise IndexError("Index out of range")
        return self._chunk_bounds.searchsorted(array_index, side="right") - 1

    def array_slice_to_chunk_slice(self, sl: slice) -> slice:
        """Find all chunks that intersect with a given slice."""
        if sl.step != 1 and sl.step is not None:
            raise IndexError("Only works with step=1 or None")
        if sl.start < 0:
            raise IndexError("Slice start must be > 0")
        if sl.stop <= sl.start:
            raise IndexError("Stop must be greater than start")
        if sl.stop > len(self):
            raise IndexError(f"Stop must be <= than {len(self)}")
        first = self._chunk_bounds.searchsorted(sl.start, side="right") - 1
        last = self._chunk_bounds.searchsorted(sl.stop, side="left")
        return slice(first, last)
