import itertools
import os
import random
from collections import namedtuple
from tempfile import TemporaryDirectory

import numpy as np
import pytest
import xarray as xr
import zarr

from pangeo_forge_recipes.rechunking import (
    GroupKey,
    combine_fragments,
    consolidate_dimension_coordinates,
    split_fragment,
)
from pangeo_forge_recipes.types import CombineOp, Dimension, Index, IndexedPosition, Position

from .conftest import split_up_files_by_variable_and_day
from .data_generation import make_ds


@pytest.mark.parametrize(
    "nt_dayparam",
    [(5, "1D"), (10, "2D")],
)
@pytest.mark.parametrize("time_chunks", [1, 2, 5])
@pytest.mark.parametrize("other_chunks", [{}, {"lat": 5}, {"lat": 5, "lon": 5}])
def test_split_and_combine_fragments_with_merge_dim(nt_dayparam, time_chunks, other_chunks):
    """Test if sub-fragments split from datasets with merge dims can be combined with each other."""

    target_chunks = {"time": time_chunks, **other_chunks}
    nt, dayparam = nt_dayparam
    ds = make_ds(nt=nt)
    dsets, _, _ = split_up_files_by_variable_and_day(ds, dayparam)

    # replicates indexes created by IndexItems transform.
    time_positions = {t: i for i, t in enumerate(ds.time.values)}
    merge_dim = Dimension("variable", CombineOp.MERGE)
    concat_dim = Dimension("time", CombineOp.CONCAT)
    indexes = [
        Index(
            {
                merge_dim: Position((0 if "bar" in ds.data_vars else 1)),
                concat_dim: IndexedPosition(time_positions[ds.time[0].values], dimsize=nt),
            }
        )
        for ds in dsets
    ]

    # split the (mock indexed) datasets into sub-fragments.
    # the splits list are nested tuples which are a bit confusing for humans to think about.
    # create a namedtuple to help remember the structure of these tuples and cast the
    # elements of splits list to this more descriptive type.
    splits = [
        list(split_fragment((index, ds), target_chunks=target_chunks))
        for index, ds in zip(indexes, dsets)
    ]
    Subfragment = namedtuple("Subfragment", "groupkey, content")
    subfragments = list(itertools.chain(*[[Subfragment(*s) for s in split] for split in splits]))

    # combine subfragments, starting by grouping subfragments by groupkey.
    # replicates behavior of `... | beam.GroupByKey() | beam.MapTuple(combine_fragments)`
    # in the `Rechunk` transform.
    groupkeys = set([sf.groupkey for sf in subfragments])
    grouped_subfragments: dict[GroupKey, list[Subfragment]] = {g: [] for g in groupkeys}
    for sf in subfragments:
        grouped_subfragments[sf.groupkey].append(sf)

    for g in sorted(groupkeys):
        # just confirms that grouping logic within this test is correct
        assert all([sf.groupkey == g for sf in grouped_subfragments[g]])
        # for the merge dimension of each subfragment in the current group, assert that there
        # is only one positional value present. this verifies that `split_fragments` has not
        # grouped distinct merge dimension positional values together under the same groupkey.
        merge_position_vals = [sf.content[0][merge_dim].value for sf in grouped_subfragments[g]]
        assert all([v == merge_position_vals[0] for v in merge_position_vals])
        # now actually try to combine the fragments
        _, ds_combined = combine_fragments(
            g,
            [sf.content for sf in grouped_subfragments[g]],
        )
        # ensure vars are *not* combined (we only want to concat, not merge)
        assert len([k for k in ds_combined.data_vars.keys()]) == 1
        # check that time chunking is correct
        if nt % time_chunks == 0:
            assert len(ds_combined.time) == time_chunks
        else:
            # if `nt` is not evenly divisible by `time_chunks`, all chunks will be of
            # `len(time_chunks)` except the last one, which will be the lenth of the remainder
            assert len(ds_combined.time) in [time_chunks, nt % time_chunks]


@pytest.mark.parametrize("offset", [0, 5])  # hypothetical offset of this fragment
@pytest.mark.parametrize("time_chunks", [1, 3, 5, 10, 11])
def test_split_fragment(time_chunks, offset):
    """A thorough test of 1D splitting logic that should cover all major edge cases."""

    nt_total = 20  # the total size of the hypothetical dataset
    target_chunks = {"time": time_chunks}

    nt = 10
    ds = make_ds(nt=nt)  # this represents a single dataset fragment
    dimension = Dimension("time", CombineOp.CONCAT)

    extra_indexes = [
        (Dimension("foo", CombineOp.CONCAT), Position(0)),
        (Dimension("bar", CombineOp.MERGE), Position(1)),
    ]

    index = Index([(dimension, IndexedPosition(offset, dimsize=nt_total))] + extra_indexes)

    all_splits = list(split_fragment((index, ds), target_chunks=target_chunks))

    group_keys = [item[0] for item in all_splits]
    new_indexes = [item[1][0] for item in all_splits]
    new_datasets = [item[1][1] for item in all_splits]

    for n in range(len(all_splits)):
        chunk_number = offset // time_chunks + n
        assert group_keys[n] == (("time", chunk_number), ("bar", 1))
        chunk_start = time_chunks * chunk_number
        chunk_stop = min(time_chunks * (chunk_number + 1), nt_total)
        fragment_start = max(chunk_start, offset)
        fragment_stop = min(chunk_stop, fragment_start + time_chunks, offset + nt)
        # other dimensions in the index should be passed through unchanged
        assert new_indexes[n] == Index(
            [(dimension, IndexedPosition(fragment_start, dimsize=nt_total))] + extra_indexes
        )
        start, stop = fragment_start - offset, fragment_stop - offset
        xr.testing.assert_equal(new_datasets[n], ds.isel(time=slice(start, stop)))

    # make sure we got the whole dataset back
    ds_concat = xr.concat(new_datasets, "time")
    xr.testing.assert_equal(ds, ds_concat)


def test_split_multidim():
    """A simple test that checks whether splitting logic is applied correctly
    for multiple dimensions."""

    nt = 2
    ds = make_ds(nt=nt)
    nlat = ds.dims["lat"]
    dimension = Dimension("time", CombineOp.CONCAT)
    index = Index({dimension: IndexedPosition(0, dimsize=nt)})

    time_chunks = 1
    lat_chunks = nlat // 2
    target_chunks = {"time": time_chunks, "lat": lat_chunks}

    all_splits = list(split_fragment((index, ds), target_chunks=target_chunks))

    group_keys = [item[0] for item in all_splits]

    assert group_keys == [
        (("lat", 0), ("time", 0)),
        (("lat", 1), ("time", 0)),
        (("lat", 0), ("time", 1)),
        (("lat", 1), ("time", 1)),
    ]

    for group_key, (fragment_index, fragment_ds) in all_splits:
        n_lat_chunk = group_key[0][1]
        n_time_chunk = group_key[1][1]
        time_start, time_stop = n_time_chunk * time_chunks, (n_time_chunk + 1) * time_chunks
        lat_start, lat_stop = n_lat_chunk * lat_chunks, (n_lat_chunk + 1) * lat_chunks
        expected_index = Index(
            {
                Dimension("time", CombineOp.CONCAT): IndexedPosition(time_start, dimsize=nt),
                Dimension("lat", CombineOp.CONCAT): IndexedPosition(lat_start, dimsize=nlat),
            }
        )
        assert fragment_index == expected_index
        xr.testing.assert_equal(
            fragment_ds, ds.isel(time=slice(time_start, time_stop), lat=slice(lat_start, lat_stop))
        )


@pytest.mark.parametrize("time_chunk", [1, 2, 3, 5, 10])
def test_combine_fragments(time_chunk):
    """The function applied after GroupBy to combine fragments into a single chunk.
    All concat dims that appear more than once are combined.
    """

    nt = 10
    ds = make_ds(nt=nt)

    fragments = []
    time_dim = Dimension("time", CombineOp.CONCAT)
    for nfrag, start in enumerate(range(0, nt, time_chunk)):
        stop = min(start + time_chunk, nt)
        index_frag = Index({time_dim: IndexedPosition(start)})
        ds_frag = ds.isel(time=slice(start, stop))
        fragments.append((index_frag, ds_frag))

    group = (("time", 0),)  # not actually used
    index, ds_comb = combine_fragments(group, fragments)

    assert index == Index({time_dim: IndexedPosition(0)})
    xr.testing.assert_equal(ds, ds_comb)


@pytest.mark.parametrize("time_chunk", [1, 2, 3, 5, 10])
@pytest.mark.parametrize("lat_chunk", [8, 9, 17, 18])
def test_combine_fragments_multidim(time_chunk, lat_chunk):
    """The function applied after GroupBy to combine fragments into a single chunk.
    All concat dims that appear more than once are combined.
    """

    nt = 10
    ds = make_ds(nt=nt)
    ny = ds.dims["lat"]

    fragments = []
    time_dim = Dimension("time", CombineOp.CONCAT)
    lat_dim = Dimension("lat", CombineOp.CONCAT)
    for start_t in range(0, nt, time_chunk):
        stop_t = min(start_t + time_chunk, nt)
        for start_y in range(0, ny, lat_chunk):
            stop_y = min(start_y + lat_chunk, ny)
            ds_frag = ds.isel(time=slice(start_t, stop_t), lat=slice(start_y, stop_y))
            index_frag = Index(
                {time_dim: IndexedPosition(start_t), lat_dim: IndexedPosition(start_y)}
            )
            fragments.append((index_frag, ds_frag))

    # fragments will arrive in a random order
    random.shuffle(fragments)
    group = (("time", 0), ("lat", 0))  # not actually used
    index, ds_comb = combine_fragments(group, fragments)

    assert index == Index({time_dim: IndexedPosition(0), lat_dim: IndexedPosition(0)})
    xr.testing.assert_equal(ds, ds_comb)


def test_combine_fragments_errors():

    ds = make_ds(nt=1)
    group = (("time", 0),)  # not actually used

    # check for inconsistent indexes
    index0 = Index({Dimension("time", CombineOp.CONCAT): IndexedPosition(0)})
    bad_indexes = [
        Index(
            {
                Dimension("timestep", CombineOp.CONCAT): IndexedPosition(1),
            }
        ),
        Index(
            {
                Dimension("time", CombineOp.CONCAT): IndexedPosition(1),
                Dimension("variable", CombineOp.MERGE): Position(0),
            }
        ),
    ]
    for index1 in bad_indexes:
        with pytest.raises(ValueError, match="different combine dims"):
            _ = combine_fragments(group, [(index0, ds), (index1, ds)])

    # check for missing start stop
    index1 = Index({Dimension("time", CombineOp.CONCAT): Position(1)})
    with pytest.raises(ValueError, match="positions must be indexed"):
        _ = combine_fragments(group, [(index0, ds), (index1, ds)])

    # check for non-contiguous indexes
    index1 = Index({Dimension("time", CombineOp.CONCAT): IndexedPosition(2)})
    with pytest.raises(ValueError, match="are not consistent"):
        _ = combine_fragments(group, [(index0, ds), (index1, ds)])


def test_consolidate_dimension_coordinates():
    td = TemporaryDirectory()
    store_path = os.path.join(td.name + "tmp.zarr")
    group = zarr.group(store=store_path, overwrite=True)
    group.create(name="data", shape=100, chunks=10, dtype="i4")
    group.create(name="time", shape=100, chunks=10, dtype="i4")
    group.data[:] = np.random.randn(*group.data.shape)
    group.time[:] = np.arange(100)

    # If you don't provide these attrs,
    # consolidate_dimension_coordinates does not
    # raise an error, while Xarray does
    group.data.attrs["_ARRAY_DIMENSIONS"] = ["time"]
    group.time.attrs["_ARRAY_DIMENSIONS"] = ["time"]

    consolidated_zarr = consolidate_dimension_coordinates(zarr.storage.FSStore(store_path))
    store = zarr.open(consolidated_zarr)
    assert store.time.chunks[0] == 100
    assert store.data.chunks[0] == 10
