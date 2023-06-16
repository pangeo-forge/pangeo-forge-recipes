import functools
import itertools
import operator
from typing import Dict, Iterator, List, Tuple

import numpy as np
import xarray as xr

from .aggregation import XarraySchema, determine_target_chunks
from .chunk_grid import ChunkGrid
from .types import CombineOp, Dimension, Index, IndexedPosition, Optional

# group keys are a tuple of tuples like (("lon", 1), ("time", 0))
# the ints are chunk indexes
# code should aways sort the key before emitting it
GroupKey = Tuple[Tuple[str, int], ...]


def split_fragment(
    fragment: Tuple[Index, xr.Dataset],
    target_chunks: Optional[Dict[str, int]] = None,
    schema: Optional[XarraySchema] = None,
) -> Iterator[Tuple[GroupKey, Tuple[Index, xr.Dataset]]]:
    """Split a single indexed dataset fragment into sub-fragments, according to the
    specified target chunks

    :param fragment: the indexed fragment.
    :param target_chunks_and_dims: mapping from dimension name to a tuple of (chunksize, dimsize)
    """

    if target_chunks is None and schema is None:
        raise ValueError("Must specify either target_chunks or schema (or both).")
    if schema is not None:
        # we don't want to include the dims that are not getting rechunked
        target_chunks = determine_target_chunks(schema, target_chunks, include_all_dims=False)
    else:
        assert target_chunks is not None

    index, ds = fragment

    # target_chunks_and_dims contains both the chunk size and global dataset dimension size
    target_chunks_and_dims = {}  # type: Dict[str, Tuple[int, int]]
    # fragment_slices tells us where this fragement lies within the global dataset
    fragment_slices = {}  # type: Dict[str, slice]
    # rechunked_concat_dims is used to track dimensions that are present in both
    # concat dims and target chunks
    rechunked_concat_dims = []  # type: List[Dimension]
    for dim_name, chunk in target_chunks.items():
        concat_dim = Dimension(dim_name, CombineOp.CONCAT)
        if concat_dim in index:
            dimsize = getattr(index[concat_dim], "dimsize", 0)
            concat_position = index[concat_dim]
            start = concat_position.value
            stop = start + ds.dims[dim_name]
            dim_slice = slice(start, stop)
            rechunked_concat_dims.append(concat_dim)
        else:
            # If there is a target_chunk that is NOT present as a concat_dim
            # in the fragment index, then we can assume that the entire span of
            # that dimension is present in the dataset.
            # This would arise e.g. when decimating a contiguous dimension
            dimsize = ds.dims[dim_name]
            dim_slice = slice(0, dimsize)

        target_chunks_and_dims[dim_name] = (chunk, dimsize)
        fragment_slices[dim_name] = dim_slice

    if any(item[1] == 0 for item in target_chunks_and_dims.values()):
        raise ValueError("A dimsize of 0 means that this fragment has not been properly indexed.")

    # all index fragments will have this as a base
    common_index = {k: v for k, v in index.items() if k not in rechunked_concat_dims}

    chunk_grid = ChunkGrid.from_uniform_grid(target_chunks_and_dims)
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
        sub_fragment_index = Index(common_index.copy())
        for dim, chunk_slice in chunk_array_slices.items():
            fragment_slice = fragment_slices[dim]
            start = max(chunk_slice.start, fragment_slice.start)
            stop = min(chunk_slice.stop, fragment_slice.stop)
            sub_fragment_indexer[dim] = slice(
                start - fragment_slice.start, stop - fragment_slice.start
            )
            dimension = Dimension(dim, CombineOp.CONCAT)
            sub_fragment_index[dimension] = IndexedPosition(
                start, dimsize=target_chunks_and_dims[dim][1]
            )
        sub_fragment_ds = ds.isel(**sub_fragment_indexer)

        yield tuple(sorted(target_chunk_group)), (sub_fragment_index, sub_fragment_ds)


def _sort_index_key(item):
    index = item[0]
    return tuple((dimension, position.value) for dimension, position in index.items())


def _invert_meshgrid(*arrays):
    """Inverts the numpy.meshgrid function."""

    ndim = len(arrays)
    shape = arrays[0].shape
    assert all(a.shape == shape for a in arrays)
    selectors = [ndim * [0] for n in range(ndim)]
    for n in range(ndim):
        selectors[n][ndim - n - 1] = slice(None)
        selectors[n] = tuple(selectors[n])
    xi = [a[s] for a, s in zip(arrays, selectors)]
    assert all(
        np.equal(actual, expected).all() for actual, expected in zip(arrays, np.meshgrid(*xi))
    )
    return xi


def merge_fragments(
    concat_dims,
    fragments,
    all_indexes,
) -> List[Tuple[Index, xr.Dataset]]:

    indexes_to_merge: set[Index] = set(
        [Index({dim: index[dim]}) for index in all_indexes for dim in concat_dims]
    )
    merge_groups: dict[Index, list[Tuple[Index, xr.Dataset]]] = {
        idx: [f for f in fragments if f[0][dim] == indexed_position]
        for idx in indexes_to_merge
        for dim, indexed_position in idx.items()
    }
    merged_fragments = []
    for idx, fments in merge_groups.items():
        dim, indexed_position = next(iter(idx.items()))
        assert all([f[0][dim] == indexed_position for f in fments])
        dsets = [f[1] for f in fments]
        merged_ds = xr.merge(dsets)
        merged_fragments.append((idx, merged_ds))
    merged_fragments.sort(key=_sort_index_key)  # this should sort by index
    return merged_fragments


# TODO: figure out a type hint that beam likes
def combine_fragments(
    group: GroupKey, fragments: List[Tuple[Index, xr.Dataset]]
) -> Tuple[Index, xr.Dataset]:
    """Combine multiple dataset fragments into a single fragment.

    :param group: the group key; not actually used in combining
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
    merge_dims = [dimension for dimension in dimensions if dimension.operation == CombineOp.MERGE]

    if merge_dims:
        merged_fragments = merge_fragments(concat_dims, fragments, all_indexes)
        merged_indexes = [item[0] for item in merged_fragments]
        merged_dsets = [item[1] for item in merged_fragments]
    else:
        merged_fragments, merged_indexes, merged_dsets = fragments, all_indexes, all_dsets

    if not all(all(index[dim].indexed for index in merged_indexes) for dim in concat_dims):
        raise ValueError(
            "All concat dimension positions must be indexed in order to combine fragments."
        )

    # now we need to unstack the 1D concat dims into an ND nested data structure
    # first step is figuring out the shape
    dims_starts_sizes = [
        (
            dim.name,
            [index[dim].value for index in merged_indexes],
            [ds.dims[dim.name] for ds in merged_dsets],
        )
        for dim in concat_dims
    ]

    def _sort_by_speed_of_varying(item):
        indexes = item[1]
        return np.diff(np.array(indexes)).tolist()

    dims_starts_sizes.sort(key=_sort_by_speed_of_varying)

    shape = [len(np.unique(item[1])) for item in dims_starts_sizes]

    total_size = functools.reduce(operator.mul, shape)
    if len(merged_fragments) != total_size:
        # this error path is currently untested
        raise ValueError(
            "Cannot combine fragments. "
            f"Expected a hypercube of shape {shape} but got {len(merged_fragments)} fragments."
        )

    starts_cube = [np.array(item[1]).reshape(shape) for item in dims_starts_sizes]
    sizes_cube = [np.array(item[2]).reshape(shape) for item in dims_starts_sizes]
    try:
        # reversing order is necessary here because _sort_by_speed_of_varying puts the
        # arrays into the opposite order as wanted by np.meshgrid
        starts = _invert_meshgrid(*starts_cube[::-1])[::-1]
        sizes = _invert_meshgrid(*sizes_cube[::-1])[::-1]
    except AssertionError:
        raise ValueError("Cannot combine fragments because they do not form a regular hypercube.")

    expected_sizes = [np.diff(s) for s in starts]
    if not all(np.equal(s[:-1], es).all() for s, es in zip(sizes, expected_sizes)):
        raise ValueError(f"Dataset {sizes} and index starts {starts} are not consistent.")

    # some tricky workarounds to put xarray datasets into a nested list
    all_datasets = np.empty(shape, dtype="O").ravel()
    for n, fragment in enumerate(merged_fragments):
        all_datasets[n] = fragment[1]

    dsets_to_concat = all_datasets.reshape(shape).tolist()
    concat_dims_sorted = [item[0] for item in dims_starts_sizes]
    ds_combined = xr.combine_nested(dsets_to_concat, concat_dim=concat_dims_sorted)

    return first_index, ds_combined
