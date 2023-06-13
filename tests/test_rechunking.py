import random
from collections import namedtuple

import pytest
import xarray as xr

from pangeo_forge_recipes.rechunking import combine_fragments, split_fragment
from pangeo_forge_recipes.types import CombineOp, Dimension, Index, IndexedPosition, Position

from .data_generation import make_ds


@pytest.mark.parametrize("nt", [2])
@pytest.mark.parametrize("nvars", [2])
def test_split_and_combine_fragments_with_merge_dim(nt, nvars):
    """Test if sub-fragments split from datasets with merge dims can be combined with each other."""

    t_offset = 0
    target_chunks = {"time": 1}

    # create two datasets, each with with `nt` time steps, and one variable
    ds0 = make_ds(nt=nt).drop_vars("foo")
    assert "foo" not in ds0.data_vars
    ds1 = make_ds(nt=nt).drop_vars("bar")
    assert "bar" not in ds1.data_vars

    # replicates indexes created by IndexItems transform.
    index0, index1 = [
        Index(
            {
                Dimension("variable", CombineOp.MERGE): Position(i),
                Dimension("time", CombineOp.CONCAT): IndexedPosition(t_offset, dimsize=nt),
            }
        )
        for i in range(2)
    ]
    # split the two (mock indexed) datasets into sub-fragments. once split according to
    # `target_chunks = {"time": 1}`, each list of splits should contain 2 sub-fragments
    ds0_splits, ds1_splits = [
        list(split_fragment((index, ds), target_chunks=target_chunks))
        for index, ds in zip([index0, index1], [ds0, ds1])
    ]
    assert len(ds0_splits) == len(ds1_splits) == 2

    # the splits list are nested tuples which are a bit confusing for humans to think about.
    # create a namedtuple to help remember the structure of these tuples and cast the
    # elements of splits list to this more descriptive type.
    Subfragment = namedtuple("Subfragment", "groupkey, subfragment")
    ds0_subfragments, ds1_subfragments = [
        [Subfragment(*s) for s in split] for split in (ds0_splits, ds1_splits)
    ]

    # attempt to combine subfragments. because the initial groupkey of each list is the same,
    # it doesn't matter which one we pass as the first argument of `combine_fragments`. this
    # replicates the behavior of `... | beam.GroupByKey() | beam.MapTuple(combine_fragments)`
    # in the `Rechunk` transform.
    for i in range(len(ds0_subfragments)):
        assert ds0_subfragments[i].groupkey == ds1_subfragments[i].groupkey
        _, ds_combined = combine_fragments(
            ds0_subfragments[i].groupkey,
            [ds0_subfragments[i].subfragment, ds1_subfragments[i].subfragment],
        )
        assert "foo" in ds_combined.data_vars
        assert "bar" in ds_combined.data_vars


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
        assert group_keys[n] == (("time", chunk_number),)
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

    #
