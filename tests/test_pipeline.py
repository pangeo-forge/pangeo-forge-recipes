"""Integration testing of pipelines."""

import subprocess
import time

import fsspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr

# classes tested here
from pangeo_forge.pipelines.base import AbstractPipeline
from pangeo_forge.pipelines.http_xarray_zarr import HttpXarrayZarrMixin

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


# a pipeline to load that data


class MyPipeline(HttpXarrayZarrMixin, AbstractPipeline):
    repo = "pangeo-forge/pangeo-forge"

    def __init__(
        self,
        name,
        cache_path,
        target_path,
        concat_dim,
        append_dim,
        files_per_chunk,
        url_base,
        nfiles,
    ):
        self.name = name
        self.cache_location = f"{cache_path}/{name}-cache/"
        self.target_location = f"{target_path}/{name}.zarr"
        self.append_dim = append_dim
        self.concat_dim = concat_dim
        self.files_per_chunk = files_per_chunk

        # needed to build up sources
        self._url_base = url_base
        self._nfiles = nfiles

    @property
    def sources(self):
        keys = range(self._nfiles)
        source_url_pattern = self._url_base + "/{n:03d}.nc"
        source_urls = [source_url_pattern.format(n=key) for key in keys]
        return source_urls

    @property
    def targets(self):
        return [self.target_location]


# a basic pipeline test
def test_pipeline(daily_xarray_dataset, netcdf_http_server, tmpdir):
    name = "TEST_DATASET"
    cache_dir = tmpdir.mkdir("cache")
    target_dir = tmpdir.mkdir("target")
    concat_dim = "time"
    append_dim = "time"
    files_per_chunk = 5

    url_base, paths = netcdf_http_server
    nfiles = len(paths)

    pipeline = MyPipeline(
        name, cache_dir, target_dir, concat_dim, append_dim, files_per_chunk, url_base, nfiles
    )
    pipeline.flow.run()

    ds_test = xr.open_zarr(pipeline.targets[0])
    assert ds_test.identical(daily_xarray_dataset)
