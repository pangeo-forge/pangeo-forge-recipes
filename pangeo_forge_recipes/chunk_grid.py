"""
Abstract representation of ND chunked arrays
"""
from __future__ import annotations

from itertools import chain, groupby
from typing import Dict, FrozenSet, Set, Tuple

import numpy as np

from .utils import calc_subsets

# Most of this is probably already in Dask and Zarr!
# However, it's useful to write up our own little model that does just what we need.


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
        """Create a ChunkGrid with uniform chunk sizes (except possibly the last chunk).

        :param chunksize_and_dimsize: Dictionary whose keys are dimension names and
          whose values are a tuple of `chunk_size, total_dim_size`
        """
        all_chunks = {}
        for name, (chunksize, dimsize) in chunksize_and_dimsize.items():
            assert dimsize > 0
            assert (
                chunksize > 0 and chunksize <= dimsize
            ), f"invalid chunksize {chunksize} and dimsize {dimsize}"
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

    def consolidate(self, factors: Dict[str, int]) -> ChunkGrid:
        """Return a new ChunkGrid with chunks consolidated by a given factor
        along specifed dimensions."""

        # doesn't seem like the kosher way to do this but /shrug
        new = self.__class__({})
        new._chunk_axes = {
            name: ca.consolidate(factors[name]) if name in factors else ca
            for name, ca in self._chunk_axes.items()
        }
        return new

    def subset(self, factors: Dict[str, int]) -> ChunkGrid:
        """Return a new ChunkGrid with chunks decimated by a given subset factor
        along specifed dimensions."""

        # doesn't seem like the kosher way to do this but /shrug
        new = self.__class__({})
        new._chunk_axes = {
            name: ca.subset(factors[name]) if name in factors else ca
            for name, ca in self._chunk_axes.items()
        }
        return new

    def chunk_index_to_array_slice(self, chunk_index: Dict[str, int]) -> Dict[str, slice]:
        """Convert a single index from chunk space to a slice in array space
        for each specified dimension."""

        return {
            name: self._chunk_axes[name].chunk_index_to_array_slice(idx)
            for name, idx in chunk_index.items()
        }

    def array_index_to_chunk_index(self, array_index: Dict[str, int]) -> Dict[str, int]:
        """Figure out which chunk a single array-space index comes from
        for each specified dimension."""
        return {
            name: self._chunk_axes[name].array_index_to_chunk_index(idx)
            for name, idx in array_index.items()
        }

    def array_slice_to_chunk_slice(self, array_slices: Dict[str, slice]) -> Dict[str, slice]:
        """Find all chunks that intersect with a given array-space slice
        for each specified dimension."""
        return {
            name: self._chunk_axes[name].array_slice_to_chunk_slice(array_slice)
            for name, array_slice in array_slices.items()
        }

    def chunk_conflicts(self, chunk_index: Dict[str, int], other: ChunkGrid) -> Dict[str, Set[int]]:
        """Figure out which chunks from the other ChunkGrid might potentially
        be written by other chunks from this array.
        Returns a set of chunk indexes from the _other_ ChunkGrid that would
        need to be locked for a safe write.

        :param chunk_index: The index of the chunk we want to write
        :param other: The other ChunkAxis
        """

        return {
            name: self._chunk_axes[name].chunk_conflicts(idx, other._chunk_axes[name])
            for name, idx in chunk_index.items()
        }


class ChunkAxis:
    """A ChunkAxis has two index spaces.

    Array index space is a regular python index of an array / list.
    Chunk index space describes chunk positions.

    A ChunkAxis helps translate between these two spaces.

    :param chunks: The explicit size of each chunk
    """

    def __init__(self, chunks: Tuple[int, ...]):
        self.chunks = tuple(chunks)  # want this immutable
        self._chunk_bounds = np.hstack([0, np.cumsum(self.chunks)])

    def __eq__(self, other):
        return self.chunks == other.chunks

    def __len__(self):
        return self._chunk_bounds[-1].item()

    def subset(self, factor: int) -> ChunkAxis:
        """Return a copy with chunks decimated by a subset factor."""

        new_chunks = tuple(chain(*(calc_subsets(c, factor) for c in self.chunks)))
        return self.__class__(new_chunks)

    def consolidate(self, factor: int) -> ChunkAxis:
        """Return a copy with chunks consolidated by a subset factor."""

        new_chunks = []

        def grouper(val):
            return val[0] // factor

        for _, gobj in groupby(enumerate(self.chunks), grouper):
            new_chunks.append(sum(f[1] for f in gobj))
        return self.__class__(tuple(new_chunks))

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

    def chunk_conflicts(self, chunk_index: int, other: ChunkAxis) -> Set[int]:
        """Figure out which chunks from the other ChunkAxis might potentially
        be written by other chunks from this array.
        Returns a set of chunk indexes from the _other_ ChunkAxis that would
        need to be locked for a safe write.
        If there are no potential conflicts, return an empty set.
        There are at most two other-axis chunks with conflicts;
        one each edge of this chunk.

        :param chunk_index: The index of the chunk we want to write
        :param other: The other ChunkAxis
        """

        if len(other) != len(self):
            raise ValueError("Can't compute conflict for ChunkAxes of different size.")

        other_chunk_conflicts = set()

        array_slice = self.chunk_index_to_array_slice(chunk_index)
        # The chunks from the other axis that we need to touch:
        other_chunk_indexes = other.array_slice_to_chunk_slice(array_slice)
        # Which other chunks from this array might also have to touch those chunks?
        # To answer this, we need to know if those chunks overlap any of the
        # other chunks from this array.
        # Since the slice is contiguous, we only have to worry about the first and last chunks.

        other_chunk_left = other_chunk_indexes.start
        array_slice_left = other.chunk_index_to_array_slice(other_chunk_left)
        chunk_slice_left = self.array_slice_to_chunk_slice(array_slice_left)
        if chunk_slice_left.start < chunk_index:
            other_chunk_conflicts.add(other_chunk_left)

        other_chunk_right = other_chunk_indexes.stop - 1
        array_slice_right = other.chunk_index_to_array_slice(other_chunk_right)
        chunk_slice_right = self.array_slice_to_chunk_slice(array_slice_right)
        if chunk_slice_right.stop > chunk_index + 1:
            other_chunk_conflicts.add(other_chunk_right)

        return other_chunk_conflicts
