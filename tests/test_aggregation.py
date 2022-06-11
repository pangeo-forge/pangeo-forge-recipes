import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pangeo_forge_recipes.aggregation import DatasetCombineError, XarrayCombineAccumulator


def make_ds(nt=10):
    """Return a synthetic random xarray dataset."""
    np.random.seed(2)
    # TODO: change nt to 11 in order to catch the edge case where
    # items_per_input does not evenly divide the length of the sequence dimension
    ny, nx = 18, 36
    time = pd.date_range(start="2010-01-01", periods=nt, freq="D")
    lon = (np.arange(nx) + 0.5) * 360 / nx
    lon_attrs = {"units": "degrees_east", "long_name": "longitude"}
    lat = (np.arange(ny) + 0.5) * 180 / ny
    lat_attrs = {"units": "degrees_north", "long_name": "latitude"}
    foo = np.random.rand(nt, ny, nx)
    foo_attrs = {"long_name": "Fantastic Foo"}
    # make sure things work with heterogenous data types
    bar = np.random.randint(0, 10, size=(nt, ny, nx))
    bar_attrs = {"long_name": "Beautiful Bar"}
    dims = ("time", "lat", "lon")
    ds = xr.Dataset(
        {"bar": (dims, bar, bar_attrs), "foo": (dims, foo, foo_attrs)},
        coords={
            "time": ("time", time),
            "lat": ("lat", lat, lat_attrs),
            "lon": ("lon", lon, lon_attrs),
        },
        attrs={"conventions": "CF 1.6"},
    )
    return ds


def test_concat_accumulator():
    ds = make_ds(nt=3)
    s = ds.to_dict(data=False)  # expected

    aca = XarrayCombineAccumulator(concat_dim="time")
    aca.add_input(s, 0)
    s1 = s.copy()
    s1["chunks"] = {"time": {0: 3}}
    assert aca.schema == s1

    assert "chunks" not in s
    aca.add_input(s, 1)
    s2 = make_ds(nt=6).to_dict(data=False)
    s2["chunks"] = {"time": {0: 3, 1: 3}}
    assert aca.schema == s2

    aca2 = XarrayCombineAccumulator(concat_dim="time")
    aca2.add_input(s, 2)
    aca_sum = aca + aca2
    s3 = make_ds(nt=9).to_dict(data=False)
    s3["chunks"] = {"time": {0: 3, 1: 3, 2: 3}}
    assert aca_sum.schema == s3

    # now modify attrs and see that fields get dropped correctly
    ds2 = make_ds(nt=4)
    ds2.attrs["conventions"] = "wack conventions"
    ds2.bar.attrs["long_name"] = "nonsense name"
    aca_sum.add_input(ds2.to_dict(data=False), 3)
    ds_expected = make_ds(nt=13)
    del ds_expected.attrs["conventions"]
    del ds_expected.bar.attrs["long_name"]
    s4 = ds_expected.to_dict(data=False)
    s4["chunks"] = {"time": {0: 3, 1: 3, 2: 3, 3: 4}}
    assert aca_sum.schema == s4

    # make sure we can add in different order
    aca_sum.add_input(make_ds(nt=1).to_dict(data=False), 5)
    aca_sum.add_input(make_ds(nt=2).to_dict(data=False), 4)
    time_chunks = {0: 3, 1: 3, 2: 3, 3: 4, 4: 2, 5: 1}
    assert aca_sum.schema["chunks"]["time"] == time_chunks

    # now start checking errors
    ds3 = make_ds(nt=1).isel(lon=slice(1, None))
    with pytest.raises(DatasetCombineError, match="different sizes"):
        aca_sum.add_input(ds3.to_dict(data=False), 6)

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
