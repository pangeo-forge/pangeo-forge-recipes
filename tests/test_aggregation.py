import pytest

from pangeo_forge_recipes.aggregation import (
    DatasetCombineError,
    XarrayCombineAccumulator,
    dataset_to_schema,
    schema_to_template_ds,
)

from .data_generation import make_ds


def _expected_chunks(shape, chunk):
    if chunk is None:
        return (shape,)
    base = (shape // chunk) * (chunk,)
    extra = ((shape % chunk),) if (shape % chunk) else tuple()
    return base + extra


@pytest.mark.parametrize("specified_chunks", [{}, {"time": 1}, {"time": 2}, {"time": 2, "lon": 9}])
def test_schema_to_template_ds(specified_chunks):
    nt = 3
    ds = make_ds(nt=nt)
    schema = dataset_to_schema(ds)
    dst = schema_to_template_ds(schema, specified_chunks=specified_chunks)
    for v in dst:
        var = dst[v]
        for dim in var.dims:
            size = var.sizes[dim]
            chunksize = var.chunksizes[dim]
            expected_chunksize = _expected_chunks(size, specified_chunks.get(dim, None))
            assert chunksize == expected_chunksize
    schema2 = dataset_to_schema(dst)
    assert schema == schema2


def test_concat_accumulator():
    ds = make_ds(nt=3)
    s = dataset_to_schema(ds)  # expected

    aca = XarrayCombineAccumulator(concat_dim="time")
    aca.add_input(s, 0)
    s1 = s.copy()
    s1["chunks"] = {"time": {0: 3}}
    assert aca.schema == s1

    aca.add_input(s, 1)
    s2 = dataset_to_schema(make_ds(nt=6))
    s2["chunks"] = {"time": {0: 3, 1: 3}}
    assert aca.schema == s2

    aca2 = XarrayCombineAccumulator(concat_dim="time")
    aca2.add_input(s, 2)
    aca_sum = aca + aca2
    s3 = dataset_to_schema(make_ds(nt=9))
    s3["chunks"] = {"time": {0: 3, 1: 3, 2: 3}}
    assert aca_sum.schema == s3

    # now modify attrs and see that fields get dropped correctly
    ds2 = make_ds(nt=4)
    ds2.attrs["conventions"] = "wack conventions"
    ds2.bar.attrs["long_name"] = "nonsense name"
    aca_sum.add_input(dataset_to_schema(ds2), 3)
    ds_expected = make_ds(nt=13)
    del ds_expected.attrs["conventions"]
    del ds_expected.bar.attrs["long_name"]
    s4 = dataset_to_schema(ds_expected)
    s4["chunks"] = {"time": {0: 3, 1: 3, 2: 3, 3: 4}}
    assert aca_sum.schema == s4

    # make sure we can add in different order
    aca_sum.add_input(dataset_to_schema(make_ds(nt=1)), 5)
    aca_sum.add_input(dataset_to_schema(make_ds(nt=2)), 4)
    time_chunks = {0: 3, 1: 3, 2: 3, 3: 4, 4: 2, 5: 1}
    assert aca_sum.schema["chunks"]["time"] == time_chunks

    # now start checking errors
    ds3 = make_ds(nt=1).isel(lon=slice(1, None))
    with pytest.raises(DatasetCombineError, match="different sizes"):
        aca_sum.add_input(dataset_to_schema(ds3), 6)

    with pytest.raises(DatasetCombineError, match="overlapping keys"):
        aca_sum.add_input(s, 5)

    # now pretend we are concatenating along a new dimension
    s_time_concat = aca_sum.schema.copy()
    aca2 = XarrayCombineAccumulator(concat_dim="lon")
    aca2.add_input(s_time_concat, 0)
    aca2.add_input(s_time_concat, 1)
    assert aca2.schema["chunks"]["time"] == time_chunks
    assert aca2.schema["chunks"]["lon"] == {0: 36, 1: 36}
    assert aca2.schema["dims"] == {"time": 16, "lat": 18, "lon": 72}

    # check error if we try to concat something with incompatible chunks
    bad_schema = s_time_concat.copy()
    bad_schema["chunks"] = s_time_concat["chunks"].copy()
    bad_schema["chunks"]["time"] = {0: 3, 1: 3, 2: 3, 3: 4, 4: 1, 5: 2}
    with pytest.raises(DatasetCombineError, match="Non concat_dim chunks must be the same"):
        aca2.add_input(bad_schema, 2)

    # finally merge; make copy of data with upper case data var names
    modified_schema = s_time_concat.copy()
    modified_schema["data_vars"] = s_time_concat["data_vars"].copy()
    for dvar in s_time_concat["data_vars"]:
        del modified_schema["data_vars"][dvar]
        modified_schema["data_vars"][dvar.upper()] = s_time_concat["data_vars"][dvar]

    merge_accumulator = XarrayCombineAccumulator()
    merge_accumulator.add_input(s_time_concat, 0)
    merge_accumulator.add_input(modified_schema, 0)
    assert (
        merge_accumulator.schema["data_vars"]["foo"] == merge_accumulator.schema["data_vars"]["FOO"]
    )
    assert (
        merge_accumulator.schema["data_vars"]["bar"] == merge_accumulator.schema["data_vars"]["BAR"]
    )
