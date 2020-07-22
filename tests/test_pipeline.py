"""Integration testing of pipelines."""

import subprocess

import pytest
import numpy as np
import xarray as xr
import pandas as pd
import fsspec


@pytest.fixture(scope="session")
def daily_xarray_dataset():
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


@pytest.fixture(scope="session")
def netcdf_local_paths(daily_xarray_dataset, tmpdir_factory):
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    _, datasets = zip(*daily_xarray_dataset.groupby("time"))
    fnames = [f"{n:03d}.nc" for n in range(len(datasets))]
    paths = [tmp_path.join(fname) for fname in fnames]
    print(paths)
    xr.save_mfdataset(datasets, [str(path) for path in paths])
    return paths


@pytest.fixture(scope="session")
def netcdf_http_server(netcdf_local_paths):
    first_path = netcdf_local_paths[0]
    # assume that all files are in the same directory
    basedir = first_path.dirpath()
    fnames = [path.basename for path in netcdf_local_paths]

    # this feels very hacky
    PORT = "8080"
    ADDRESS = "127.0.0.1"
    command_list = ["python", "-m", "http.server", PORT, "--bind", ADDRESS]
    p = subprocess.Popen(command_list, cwd=basedir)
    url = f"http://{ADDRESS}:{PORT}"
    yield url, fnames
    p.kill()


# tests that our fixtures work


def test_fixture_local_files(daily_xarray_dataset, netcdf_local_paths):
    paths = [str(path) for path in netcdf_local_paths]
    ds = xr.open_mfdataset(paths, combine="nested", concat_dim="time")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_server):
    url, paths = netcdf_http_server
    urls = ["/".join([url, str(path)]) for path in paths]
    open_files = [fsspec.open(url).open() for url in urls]
    ds = xr.open_mfdataset(open_files, combine="nested", concat_dim="time")
    assert ds.identical(daily_xarray_dataset)
