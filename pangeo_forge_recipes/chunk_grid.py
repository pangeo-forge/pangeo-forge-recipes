"""
Abstract representation of ND chunked arrays
"""

from typing import Dict, FrozenSet, Tuple

import numpy as np

# Most of this is probably already in Dask and Zarr!
# However, it's useful to write up our own little model that does just what we need.

ChunkIndex = Tuple[int, ...]


class ChunkGrid:
    """A ChunkGrid contains several named ChunkAxis.
    The order of the axes does not matter.

    :param chunks: Dictionary mapping dimension names to chunks in each dimension.
    """

    def __init__(self, chunks: Dict[str, Tuple[int, ...]]):
        self._chunk_axes = {name: ChunkAxis(axis_chunks) for name, axis_chunks in chunks.items()}

    def __eq__(self, other):
        if self.dims != other.dims:
            return False
        for name in self._chunk_axes:
            if self._chunk_axes[name] != other._chunk_axes[name]:
                return False
        return True

    @classmethod
    def from_uniform_grid(cls, chunksize_and_dimsize: Dict[str, Tuple[int, int]]):
        all_chunks = {}
        for name, (chunksize, dimsize) in chunksize_and_dimsize.items():
            assert dimsize > 0
            assert chunksize > 0 and chunksize <= dimsize
            chunks = (dimsize // chunksize) * (chunksize,)
            if dimsize % chunksize > 0:
                chunks = chunks + (dimsize % chunksize,)
            all_chunks[name] = chunks
        return cls(all_chunks)

    @property
    def dims(self) -> FrozenSet[str]:
        return frozenset(self._chunk_axes)

    @property
    def shape(self) -> Dict[str, int]:
        return {name: len(ca) for name, ca in self._chunk_axes.items()}

    @property
    def nchunks(self) -> Dict[str, int]:
        return {name: ca.nchunks for name, ca in self._chunk_axes.items()}

    @property
    def ndim(self):
        return len(self._chunk_axes)

    def chunk_index_to_array_slice(self, chunk_indexes: Dict[str, int]) -> Dict[str, slice]:
        """Convert a single index from chunk space to a slice in array space
        for each specified dimension."""

        return {
            name: self._chunk_axes[name].chunk_index_to_array_slice(chunk_index)
            for name, chunk_index in chunk_indexes.items()
        }

    def array_index_to_chunk_index(self, array_indexes: Dict[str, int]) -> Dict[str, int]:
        """Figure out which chunk a single array-space index comes from
        for each specified dimension."""
        return {
            name: self._chunk_axes[name].array_index_to_chunk_index(array_index)
            for name, array_index in array_indexes.items()
        }

    def array_slice_to_chunk_slice(self, array_slices: Dict[str, slice]) -> Dict[str, slice]:
        """Find all chunks that intersect with a given array-space slice
        for each specified dimension."""
        return {
            name: self._chunk_axes[name].array_slice_to_chunk_slice(array_slice)
            for name, array_slice in array_slices.items()
        }


class ChunkAxis:
    """A ChunkAxis has two index spaces.

    Array index space is a regular python index of an array / list.
    Chunk index space describes chunk positions.

    A ChunkAxis helps translate between these two spaces.

    :param chunks: The explicit size of each chunk
    """

    def __init__(self, chunks: Tuple[int, ...]):
        self.chunks = chunks
        self._chunk_bounds = np.hstack([0, np.cumsum(self.chunks)])

    def __eq__(self, other):
        return self.chunks == other.chunks

    def __len__(self):
        return self._chunk_bounds[-1].item()

    @property
    def nchunks(self):
        return len(self.chunks)

    def chunk_index_to_array_slice(self, chunk_index: int) -> slice:
        """Convert a single index from chunk space to a slice in array space."""

        if chunk_index < 0 or chunk_index >= self.nchunks:
            raise IndexError("chunk_index out of range")
        return slice(self._chunk_bounds[chunk_index], self._chunk_bounds[chunk_index + 1])

    def array_index_to_chunk_index(self, array_index: int) -> int:
        """Figure out which chunk a single array-space index comes from."""

        if array_index < 0 or array_index >= len(self):
            raise IndexError("Index out of range")
        return self._chunk_bounds.searchsorted(array_index, side="right") - 1

    def array_slice_to_chunk_slice(self, sl: slice) -> slice:
        """Find all chunks that intersect with a given array-space slice."""

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
