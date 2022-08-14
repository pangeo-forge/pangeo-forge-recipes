import itertools
from typing import Dict, Tuple

import xarray as xr

from .chunk_grid import ChunkGrid
from .patterns import CombineOp, DimKey, DimVal, Index

ChunkDimDict = Dict[str, Tuple[int, int]]

# group keys are a tuple of tuples like (("lon", 1), ("time", 0))
# the ints are chunk indexes
# code should aways sort the key before emitting it
GroupKey = Tuple[Tuple[str, int], ...]


def split_fragment(fragment: Tuple[Index, xr.Dataset], target_chunks_and_dims: ChunkDimDict):
    index, ds = fragment
    chunk_grid = ChunkGrid.from_uniform_grid(target_chunks_and_dims)

    fragment_slices = {}  # type: Dict[str, slice]
    for dim in target_chunks_and_dims:
        concat_dim_key = index.find_concat_dim(dim)
        if concat_dim_key:
            # this dimension is present in the fragment as a concat dim
            concat_dim_val = index[concat_dim_key]
            dim_slice = slice(concat_dim_val.start, concat_dim_val.stop)
        else:
            # If there is a target_chunk that is NOT present as a concat_dim in the fragment,
            # then we can assume that the entire span of that dimension is present in the dataset
            # This would arise e.g. when decimating a contiguous dimension
            dim_slice = slice(0, ds.dims[dim])
        fragment_slices[dim] = dim_slice

    target_chunk_slices = chunk_grid.array_slice_to_chunk_slice(fragment_slices)

    all_chunks = itertools.product(
        *(
            [(dim, n) for n in range(chunk_slice.start, chunk_slice.stop)]
            for dim, chunk_slice in target_chunk_slices.items()
        )
    )

    for target_chunk_group in all_chunks:
        # now we need to figure out which piece of the fragment belongs in which chunk
        chunk_array_slices = chunk_grid.chunk_index_to_array_slice(dict(target_chunk_group))
        sub_fragment_indexer = {}  # passed to ds.isel
        sub_fragment_index = Index()
        for dim, chunk_slice in chunk_array_slices.items():
            fragment_slice = fragment_slices[dim]
            start = max(chunk_slice.start, fragment_slice.start)
            stop = min(chunk_slice.stop, fragment_slice.stop)
            sub_fragment_indexer[dim] = slice(
                start - fragment_slice.start, stop - fragment_slice.start
            )
            dim_key = DimKey(dim, CombineOp.CONCAT)
            # I am getting the original "position" value from the original index
            # Not sure if this makes sense. There is no way to know the actual position here
            # without knowing all the previous subfragments
            original_position = getattr(index.get(dim_key), "position", 0)
            sub_fragment_index[dim_key] = DimVal(original_position, start, stop)
        sub_fragment_ds = ds.isel(**sub_fragment_indexer)
        yield tuple(sorted(target_chunk_group)), (sub_fragment_index, sub_fragment_ds)
