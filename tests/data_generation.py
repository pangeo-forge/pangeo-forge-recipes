import numpy as np
import pandas as pd
import xarray as xr


def make_ds(nt=10, non_dim_coords=False, start="2010-01-01"):
    """Return a synthetic random xarray dataset."""
    np.random.seed(2)
    # TODO: change nt to 11 in order to catch the edge case where
    # items_per_input does not evenly divide the length of the sequence dimension
    ny, nx = 18, 36
    time = pd.date_range(start=start, periods=nt, freq="D")
    lon = (np.arange(nx) + 0.5) * 360 / nx
    lon_attrs = {"units": "degrees_east", "long_name": "longitude"}
    lat = ((np.arange(ny) + 0.5) * 180 / ny) - 90
    lat_attrs = {"units": "degrees_north", "long_name": "latitude"}
    foo = np.random.rand(nt, ny, nx)
    foo_attrs = {"long_name": "Fantastic Foo"}
    # make sure things work with heterogenous data types
    bar = np.random.randint(0, 10, size=(nt, ny, nx))
    bar_attrs = {"long_name": "Beautiful Bar"}
    dims = ("time", "lat", "lon")
    coords = {
        "time": ("time", time),
        "lat": ("lat", lat, lat_attrs),
        "lon": ("lon", lon, lon_attrs),
    }
    if non_dim_coords:
        coords["timestep"] = ("time", np.arange(nt))
        coords["baz"] = (("lat", "lon"), np.random.rand(ny, nx))

    ds = xr.Dataset(
        {"bar": (dims, bar, bar_attrs), "foo": (dims, foo, foo_attrs)},
        coords=coords,
        attrs={"conventions": "CF 1.6"},
    )

    # Add time coord encoding
    # Remove "%H:%M:%s" as it will be dropped when time is 0:0:0
    ds.time.encoding = {
        "units": f"days since {time[0].strftime('%Y-%m-%d')}",
        "calendar": "proleptic_gregorian",
    }

    return ds


def make_pyramid(levels: int):
    import rioxarray  # noqa
    from ndpyramid import pyramid_reproject

    ds = make_ds(nt=10)
    ds = ds.rename({"lon": "longitude", "lat": "latitude"})
    ds = ds.rio.write_crs("EPSG:4326")

    # other_chunks added to e2e pass of pyramid b/c target_chunks invert_meshgrid error
    return pyramid_reproject(ds, levels=levels, other_chunks={"time": 1})
