import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pangeo_forge_recipes.aggregation import DatasetMergeError, XarrayConcatAccumulator


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
    s1 = ds.to_dict(data=False)

    aca = XarrayConcatAccumulator(concat_dim="time")
    aca.add_input(s1, 0)
    assert aca.schema == s1
    assert aca.chunk_lens == (3,)
    aca.add_input(s1, 1)
    assert aca.schema == make_ds(nt=6).to_dict(data=False)
    assert aca.chunk_lens == (3, 3)

    aca2 = XarrayConcatAccumulator(concat_dim="time")
    aca2.add_input(s1, 2)
    aca_sum = aca + aca2
    assert aca_sum.schema == make_ds(nt=9).to_dict(data=False)
    assert aca_sum.chunk_lens == (3, 3, 3)

    # now modify attrs and see that fields get dropped correctly
    ds2 = make_ds(nt=4)
    ds2.attrs["conventions"] = "wack conventions"
    ds2.bar.attrs["long_name"] = "nonsense name"
    aca_sum.add_input(ds2.to_dict(data=False), 3)
    ds_expected = make_ds(nt=13)
    del ds_expected.attrs["conventions"]
    del ds_expected.bar.attrs["long_name"]
    assert aca_sum.schema == ds_expected.to_dict(data=False)
    assert aca_sum.chunk_lens == (3, 3, 3, 4)

    # make sure we can add in different order
    aca_sum.add_input(make_ds(nt=1).to_dict(data=False), 5)
    aca_sum.add_input(make_ds(nt=2).to_dict(data=False), 4)
    assert aca_sum.chunk_lens == (3, 3, 3, 4, 2, 1)

    # now start checking errors
    ds3 = make_ds(nt=1).isel(lon=slice(1, None))
    with pytest.raises(DatasetMergeError, match="different sizes"):
        aca_sum.add_input(ds3.to_dict(data=False), 6)

    aca_sum.add_input(s1, 10)
    with pytest.raises(DatasetMergeError, match="expected chunk keys"):
        _ = aca_sum.chunk_lens
