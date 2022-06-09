import numpy as np
import pandas as pd
import xarray as xr

from pangeo_forge_recipes.aggregation import XarrayConcatAccumulator


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
    print(aca.schema["coords"]["time"])
    assert aca.schema == make_ds(nt=6).to_dict(data=False)
