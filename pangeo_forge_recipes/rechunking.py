import itertools
from typing import Dict, List, Tuple

import numpy as np
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
    keys_to_skip = []  # type: list[DimKey]
    for dim in target_chunks_and_dims:
        concat_dim_key = index.find_concat_dim(dim)
        if concat_dim_key:
            # this dimension is present in the fragment as a concat dim
            concat_dim_val = index[concat_dim_key]
            dim_slice = slice(concat_dim_val.start, concat_dim_val.stop)
            keys_to_skip.append(concat_dim_key)
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
        # initialize the new index with the items we want to keep from the original index
        # TODO: think about whether we want to always rechunk concat dims
        sub_fragment_index = Index({k: v for k, v in index.items() if k not in keys_to_skip})
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


def _sort_index_key(item):
    index = item[0]
    return tuple(index.items())


def combine_fragments(fragments: List[Tuple[Index, xr.Dataset]]) -> Tuple[Index, xr.Dataset]:
    # we are combining over all the concat dims found in the indexes
    # first check indexes for consistency
    fragments.sort(key=_sort_index_key)  # this should sort by index
    all_indexes = [item[0] for item in fragments]
    first_index = all_indexes[0]
    dim_keys = tuple(first_index)
    if not all([tuple(index) == dim_keys for index in all_indexes]):
        raise ValueError(
            f"Cannot combine fragments for elements with different combine dims: {all_indexes}"
        )
    concat_dims = [dim_key for dim_key in dim_keys if dim_key.operation == CombineOp.CONCAT]
    other_dims = [dim_key for dim_key in dim_keys if dim_key.operation != CombineOp.CONCAT]
    # initialize new index with non-concat dims
    index_combined = Index({dim: first_index[dim] for dim in other_dims})
    dim_names_and_vals = {
        dim_key.name: [index[dim_key] for index in all_indexes] for dim_key in concat_dims
    }
    for dim, dim_vals in dim_names_and_vals.items():
        for dim_val in dim_vals:
            if dim_val.start is None or dim_val.stop is None:
                raise ValueError("Can only comined indexed fragments.")
        # check for contiguity
        starts = [dim_val.start for dim_val in dim_vals][1:]
        stops = [dim_val.stop for dim_val in dim_vals][:-1]
        if not starts == stops:
            raise ValueError(
                f"Index starts and stops are not consistent for concat_dim {dim}: {dim_vals}"
            )
        # Position is unneeded at this point, but we still have to provide it
        # This API probably needs to change
        combined_dim_val = DimVal(dim_vals[0].position, dim_vals[0].start, dim_vals[-1].stop)
        index_combined[DimKey(dim, CombineOp.CONCAT)] = combined_dim_val
    # now create the nested dataset structure we need
    shape = tuple(len(dim_vals) for dim_vals in dim_names_and_vals.values())
    expected_dims = {
        dim_name: (dim_vals[-1].stop - dim_vals[0].start)  # type: ignore
        for dim_name, dim_vals in dim_names_and_vals.items()
    }
    # some tricky workarounds to put xarray datasets into a nested list
    all_datasets = np.empty(shape, dtype="O").ravel()
    for n, fragment in enumerate(fragments):
        all_datasets[n] = fragment[1]
    dsets_to_concat = all_datasets.reshape(shape).tolist()
    ds_combined = xr.combine_nested(dsets_to_concat, concat_dim=list(dim_names_and_vals))
    actual_dims = {dim: ds_combined.dims[dim] for dim in expected_dims}
    if actual_dims != expected_dims:
        raise ValueError(
            f"Combined dataset dims {actual_dims} not the same as those expected"
            f"from the index {expected_dims}"
        )
    return index_combined, ds_combined
