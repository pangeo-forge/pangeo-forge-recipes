import subprocess
import time

import fsspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from pangeo_forge import recipe
from pangeo_forge.storage import FlatFSSpecTarget, FSSpecTarget

# where to run the http server
_PORT = "8080"
_ADDRESS = "127.0.0.1"


@pytest.fixture(scope="session")
def daily_xarray_dataset():
    """Return a synthetic random xarray dataset."""
    np.random.seed(1)
    nt, ny, nx = 10, 18, 36
    time = pd.date_range(start="2010-01-01", periods=nt, freq="D")
    lon = (np.arange(nx) + 0.5) * 360 / nx
    lon_attrs = {"units": "degrees_east", "long_name": "longitude"}
    lat = (np.arange(ny) + 0.5) * 180 / ny
    lat_attrs = {"units": "degrees_north", "long_name": "latitude"}
    foo = np.random.rand(nt, ny, nx)
    foo_attrs = {"long_name": "Fantastic Foo"}
    bar = np.random.rand(nt, ny, nx)
    bar_attrs = {"long_name": "Beautiful Bar"}
    dims = ("time", "lat", "lon")
    ds = xr.Dataset(
        {"foo": (dims, foo, foo_attrs), "bar": (dims, bar, bar_attrs)},
        coords={
            "time": ("time", time),
            "lat": ("lat", lat, lat_attrs),
            "lon": ("lon", lon, lon_attrs),
        },
        attrs={"conventions": "CF 1.6"},
    )
    return ds


@pytest.fixture(scope="session", params=["D", "2D"])
def netcdf_local_paths(daily_xarray_dataset, tmpdir_factory, request):
    """Return a list of paths pointing to netcdf files."""
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    items_per_file = {"D": 1, "2D": 2}
    daily_xarray_dataset.attrs["items_per_file"] = items_per_file[request.param]
    gb = daily_xarray_dataset.resample(time=request.param)
    _, datasets = zip(*gb)
    fnames = [f"{n:03d}.nc" for n in range(len(datasets))]
    paths = [tmp_path.join(fname) for fname in fnames]
    print(len(paths))
    xr.save_mfdataset(datasets, [str(path) for path in paths])
    return paths


@pytest.fixture(scope="session")
def netcdf_http_server(netcdf_local_paths):
    first_path = netcdf_local_paths[0]
    # assume that all files are in the same directory
    basedir = first_path.dirpath()
    print(basedir)
    fnames = [path.basename for path in netcdf_local_paths]

    # this feels very hacky
    command_list = ["python", "-m", "http.server", _PORT, "--bind", _ADDRESS]
    p = subprocess.Popen(command_list, cwd=basedir)
    url = f"http://{_ADDRESS}:{_PORT}"
    time.sleep(0.1)  # let the server start up
    yield url, fnames
    p.kill()


@pytest.fixture()
def tmp_target(tmpdir_factory):
    import fsspec

    fs = fsspec.get_filesystem_class("file")()
    path = str(tmpdir_factory.mktemp("target"))
    return FSSpecTarget(fs, path)


@pytest.fixture()
def tmp_cache(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("cache"))
    fs = fsspec.get_filesystem_class("file")()
    cache = FlatFSSpecTarget(fs, path)
    return cache


@pytest.fixture
def netCDFtoZarr_sequential_recipe(daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache):
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=netcdf_local_paths,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=daily_xarray_dataset.attrs["items_per_file"],
        target=tmp_target,
        input_cache=tmp_cache,
    )
    return r, daily_xarray_dataset, tmp_target
