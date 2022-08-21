import itertools
from typing import Dict, List, Tuple

import numpy as np
import xarray as xr

from .chunk_grid import ChunkGrid
from .types import CombineOp, Dimension, Index, IndexedPosition

ChunkDimDict = Dict[str, Tuple[int, int]]

# group keys are a tuple of tuples like (("lon", 1), ("time", 0))
# the ints are chunk indexes
# code should aways sort the key before emitting it
GroupKey = Tuple[Tuple[str, int], ...]


def split_fragment(fragment: Tuple[Index, xr.Dataset], target_chunks_and_dims: ChunkDimDict):
    """Split a single indexed dataset fragment into sub-fragments, according to the
    specified target chunks

    :param fragment: the indexed fragment.
    :param target_chunks_and_dims: mapping from dimension name to a tuple of (chunksize, dimsize)
    """

    index, ds = fragment
    chunk_grid = ChunkGrid.from_uniform_grid(target_chunks_and_dims)

    # fragment_slices tells us where this fragement lies within the global dataset
    fragment_slices = {}  # type: Dict[str, slice]
    # keys_to_skip is used to track dimensions that are present in both
    # concat dims and target chunks
    keys_to_skip = []  # type: list[Dimension]
    for dim in target_chunks_and_dims:
        concat_dimension = index.find_concat_dim(dim)
        if concat_dimension:
            # this dimension is present in the fragment as a concat dim
            concat_position = index[concat_dimension]
            start = concat_position.value
            stop = start + ds.dims[dim]
            dim_slice = slice(start, stop)
            keys_to_skip.append(concat_dimension)
        else:
            # If there is a target_chunk that is NOT present as a concat_dim in the fragment,
            # then we can assume that the entire span of that dimension is present in the dataset
            # This would arise e.g. when decimating a contiguous dimension
            dim_slice = slice(0, ds.dims[dim])
        fragment_slices[dim] = dim_slice

    target_chunk_slices = chunk_grid.array_slice_to_chunk_slice(fragment_slices)

    # each chunk we are going to yield is indexed by a "target chunk group",
    # a tuple of tuples of the form (("lat", 1), ("time", 0))
    all_chunks = itertools.product(
        *(
            [(dim, n) for n in range(chunk_slice.start, chunk_slice.stop)]
            for dim, chunk_slice in target_chunk_slices.items()
        )
    )

    # this iteration yields new fragments, indexed by their target chunk group
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
            dimension = Dimension(dim, CombineOp.CONCAT)
            sub_fragment_index[dimension] = IndexedPosition(start)
        sub_fragment_ds = ds.isel(**sub_fragment_indexer)
        yield tuple(sorted(target_chunk_group)), (sub_fragment_index, sub_fragment_ds)


def _sort_index_key(item):
    index = item[0]
    return tuple((dimension, position.value) for dimension, position in index.items())


def combine_fragments(fragments: List[Tuple[Index, xr.Dataset]]) -> Tuple[Index, xr.Dataset]:
    """Combine multiple dataset fragments into a single fragment.

    Only combines concat dims; merge dims are not combined.

    :param fragments: indexed dataset fragments
    """

    # we are combining over all the concat dims found in the indexes
    # first check indexes for consistency
    fragments.sort(key=_sort_index_key)  # this should sort by index

    all_indexes = [item[0] for item in fragments]
    all_dsets = [item[1] for item in fragments]
    first_index = all_indexes[0]
    dimensions = tuple(first_index)
    if not all([tuple(index) == dimensions for index in all_indexes]):
        raise ValueError(
            f"Cannot combine fragments for elements with different combine dims: {all_indexes}"
        )
    concat_dims = [dimension for dimension in dimensions if dimension.operation == CombineOp.CONCAT]
    other_dims = [dimension for dimension in dimensions if dimension.operation != CombineOp.CONCAT]

    # initialize new index with non-concat dims
    index_combined = Index({dim: first_index[dim] for dim in other_dims})

    # now we need to unstack the 1D concat dims into an ND nested data structure
    # first step is figuring out the shape
    dims_starts_sizes = [
        (
            dim.name,
            [index[dim].value for index in all_indexes],
            [ds.dims[dim.name] for ds in all_dsets]
        )
        for dim in concat_dims
    ]

    def _sort_by_speed_of_varying(item):
        indexes = item[1]
        return np.diff(np.array(indexes)).tolist()
    dims_starts_sizes.sort(key=_sort_by_speed_of_varying)

    shape = [len(np.unique(item[1])) for item in dims_starts_sizes]
    starts_rectangles = [np.array(item[1]).reshape(shape) for item in dims_starts_sizes]
    # some tricky workarounds to put xarray datasets into a nested list
    all_datasets = np.empty(shape, dtype="O").ravel()
    for n, fragment in enumerate(fragments):
        all_datasets[n] = fragment[1]
    dsets_to_concat = all_datasets.reshape(shape).tolist()
    concat_dims_sorted = [item[0] for item in dims_starts_sizes]
    ds_combined = xr.combine_nested(dsets_to_concat, concat_dim=concat_dims_sorted)

    return first_index, ds_combined
    # TODO: make sure these rectangles are aligned correctly and verify sizes
    

    # this will look something like
    # [[0, 0, 0, 1, 1, 1], [0, 1, 2, 0, 1, 2]]
    # now we need to sort this into fastest varying to slowest varying dimension
    #
    # sizes = [[ds.dims[dimension.name]] for index in all_indexes] for dimension in concat_dims]
    #
    # dims_positions = {
    #     dimension.name: [index[dimension] for index in all_indexes] for dimension in concat_dims
    # }
    # dims_sizes = {
    #     dimension.name: [ds.dims[dimension.name] for ds in all_dsets] for dimension in concat_dims
    # }
    # for dim, positions in dims_positions.items():
    #     if not all(position.indexed for position in positions):
    #         raise ValueError("Positions are not indexed; cannot combine.")
    #     # check for contiguity
    #     sizes = np.array(dims_sizes[dim])
    #     starts = np.array([position.value for position in positions])
    #     expected_sizes = np.diff(starts)
    #     if not all(np.equal(sizes[:-1], expected_sizes)):
    #         raise ValueError(
    #             f"Dataset {sizes} and index starts {starts} are not consistent for concat_dim {dim}"
    #         )
    #     combined_position = IndexedPosition(positions[0].value)
    #     index_combined[Dimension(dim, CombineOp.CONCAT)] = combined_position
    #
    # # now create the nested dataset structure we need
    # shape = tuple(len(positions) for positions in dims_positions.values())
    # # some tricky workarounds to put xarray datasets into a nested list
    # all_datasets = np.empty(shape, dtype="O").ravel()
    # for n, fragment in enumerate(fragments):
    #     all_datasets[n] = fragment[1]
    # dsets_to_concat = all_datasets.reshape(shape).tolist()
    # ds_combined = xr.combine_nested(dsets_to_concat, concat_dim=list(dims_positions))
    #
    # return index_combined, ds_combined
