import logging
import os
import socket
import subprocess
import time

import fsspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from dask.distributed import Client, LocalCluster
from prefect.executors import DaskExecutor

from pangeo_forge_recipes import recipes
from pangeo_forge_recipes.executors import (
    DaskPipelineExecutor,
    PrefectPipelineExecutor,
    PythonPipelineExecutor,
)
from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget


# to use this feature, e.g.
# $ pytest --redirect-dask-worker-logs-to-stdout=DEBUG
def pytest_addoption(parser):
    parser.addoption(
        "--redirect-dask-worker-logs-to-stdout", action="store", default="NOTSET",
    )


def get_open_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = str(s.getsockname()[1])
    s.close()
    return port


@pytest.fixture(scope="session")
def daily_xarray_dataset():
    """Return a synthetic random xarray dataset."""
    np.random.seed(1)
    # TODO: change nt to 11 in order to catch the edge case where
    # items_per_input does not evenly divide the length of the sequence dimension
    nt, ny, nx = 10, 18, 36
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


def _split_up_files_by_day(ds, day_param):
    gb = ds.resample(time=day_param)
    _, datasets = zip(*gb)
    fnames = [f"{n:03d}.nc" for n in range(len(datasets))]
    return datasets, fnames


def _split_up_files_by_variable_and_day(ds, day_param):
    all_dsets = []
    all_fnames = []
    fnames_by_variable = {}
    for varname in ds.data_vars:
        var_dsets, fnames = _split_up_files_by_day(ds[[varname]], day_param)
        fnames = [f"{varname}_{fname}" for fname in fnames]
        all_dsets += var_dsets
        all_fnames += fnames
        fnames_by_variable[varname] = fnames
    return all_dsets, all_fnames, fnames_by_variable


@pytest.fixture(scope="session", params=["D", "2D"])
def items_per_file(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_local_paths(daily_xarray_dataset, tmpdir_factory, items_per_file):
    """Return a list of paths pointing to netcdf files."""
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    # copy needed to avoid polluting metadata across multiple tests
    datasets, fnames = _split_up_files_by_day(daily_xarray_dataset.copy(), items_per_file)
    full_paths = [tmp_path.join(fname) for fname in fnames]
    xr.save_mfdataset(datasets, [str(path) for path in full_paths])
    items_per_file = {"D": 1, "2D": 2}[items_per_file]
    return full_paths, items_per_file


# TODO: this is quite repetetive of the fixture above. Replace with parametrization.
@pytest.fixture(scope="session")
def netcdf_local_paths_by_variable(daily_xarray_dataset, tmpdir_factory, items_per_file):
    """Return a list of paths pointing to netcdf files."""
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    datasets, fnames, fnames_by_variable = _split_up_files_by_variable_and_day(
        daily_xarray_dataset.copy(), items_per_file
    )
    full_paths = [tmp_path.join(fname) for fname in fnames]
    xr.save_mfdataset(datasets, [str(path) for path in full_paths])
    items_per_file = {"D": 1, "2D": 2}[items_per_file]
    path_format = str(tmp_path) + "/{variable}_{time:03d}.nc"
    return full_paths, items_per_file, fnames_by_variable, path_format


# TODO: refactor to allow netcdf_local_paths_by_variable to be passed without
# duplicating the whole test.
@pytest.fixture(scope="session")
def netcdf_http_paths(netcdf_local_paths, request):
    paths, items_per_file = netcdf_local_paths

    username = ""
    password = ""

    first_path = paths[0]
    # assume that all files are in the same directory
    basedir = first_path.dirpath()
    fnames = [path.basename for path in paths]

    this_dir = os.path.dirname(os.path.abspath(__file__))
    port = get_open_port()
    command_list = [
        "python",
        os.path.join(this_dir, "http_auth_server.py"),
        f"--port={port}",
        "--address=127.0.0.1",
    ]
    if username:
        command_list += [f"--username={username}", f"--password={password}"]
    p = subprocess.Popen(command_list, cwd=basedir)
    url = f"http://127.0.0.1:{port}"
    time.sleep(2)  # let the server start up

    def teardown():
        p.kill()

    request.addfinalizer(teardown)

    all_urls = ["/".join([url, str(fname)]) for fname in fnames]
    return all_urls, items_per_file


@pytest.fixture(scope="session")
def netcdf_http_paths_with_secrets(netcdf_http_paths):
    all_urls, items_per_file = netcdf_http_paths
    all_urls = [url + "?token=bar" for url in all_urls]
    return all_urls, items_per_file


@pytest.fixture(scope="session")
def netcdf_http_paths_with_multiparam_secrets(netcdf_http_paths):
    all_urls, items_per_file = netcdf_http_paths
    all_urls = [url + "?filename=foo.nc&token=bar" for url in all_urls]
    return all_urls, items_per_file


@pytest.fixture()
def tmp_target(tmpdir_factory):
    fs = fsspec.get_filesystem_class("file")()
    path = str(tmpdir_factory.mktemp("target"))
    return FSSpecTarget(fs, path)


@pytest.fixture()
def tmp_cache(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("cache"))
    fs = fsspec.get_filesystem_class("file")()
    cache = CacheFSSpecTarget(fs, path)
    return cache


@pytest.fixture()
def tmp_metadata_target(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("cache"))
    fs = fsspec.get_filesystem_class("file")()
    cache = MetadataTarget(fs, path)
    return cache


@pytest.fixture
def netCDFtoZarr_sequential_recipe(
    daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache, tmp_metadata_target
):
    paths, items_per_file = netcdf_local_paths
    file_pattern = pattern_from_file_sequence([str(path) for path in paths], "time", items_per_file)
    kwargs = dict(
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return recipes.XarrayZarrRecipe, file_pattern, kwargs, daily_xarray_dataset, tmp_target


@pytest.fixture
def netCDFtoZarr_sequential_subset_recipe(
    daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache, tmp_metadata_target
):
    paths, items_per_file = netcdf_local_paths
    if items_per_file != 2:
        pytest.skip("This recipe only makes sense with items_per_file == 2.")
    file_pattern = pattern_from_file_sequence([str(path) for path in paths], "time", items_per_file)
    kwargs = dict(
        subset_inputs={"time": 2},
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return recipes.XarrayZarrRecipe, file_pattern, kwargs, daily_xarray_dataset, tmp_target


@pytest.fixture
def netCDFtoZarr_sequential_multi_variable_recipe(
    daily_xarray_dataset, netcdf_local_paths_by_variable, tmp_target, tmp_cache, tmp_metadata_target
):
    paths, items_per_file, fnames_by_variable, path_format = netcdf_local_paths_by_variable
    time_index = list(range(len(paths) // 2))

    def format_function(variable, time):
        return path_format.format(variable=variable, time=time)

    file_pattern = FilePattern(
        format_function,
        ConcatDim("time", time_index, items_per_file),
        MergeDim("variable", ["foo", "bar"]),
    )
    kwargs = dict(
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return recipes.XarrayZarrRecipe, file_pattern, kwargs, daily_xarray_dataset, tmp_target


@pytest.fixture(scope="session")
def dask_cluster(request):
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, silence_logs=False)

    client = Client(cluster)

    # cluster setup

    def set_blosc_threads():
        from numcodecs import blosc

        blosc.use_threads = False

    log_level_name = request.config.getoption("--redirect-dask-worker-logs-to-stdout")
    level = logging.getLevelName(log_level_name)

    def redirect_logs():
        import logging

        for log in ["pangeo_forge_recipes", "fsspec"]:
            logger = logging.getLogger(log)
            formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            handler.setLevel(level)
            logger.setLevel(level)
            logger.addHandler(handler)

    client.run(set_blosc_threads)
    client.run(redirect_logs)
    client.close()
    del client

    yield cluster

    cluster.close()


_executors = {
    "python": PythonPipelineExecutor,
    "dask": DaskPipelineExecutor,
    "prefect": PrefectPipelineExecutor,
    "prefect-dask": PrefectPipelineExecutor,
}


@pytest.fixture(params=["manual", "python", "dask", "prefect", "prefect-dask"])
def execute_recipe(request, dask_cluster):
    if request.param == "manual":

        def execute(r):
            if r.cache_inputs:
                for input_key in r.iter_inputs():
                    r.cache_input(input_key)
            r.prepare_target()
            for chunk_key in r.iter_chunks():
                r.store_chunk(chunk_key)
            r.finalize_target()

    elif request.param == "python":

        def execute(recipe):
            return recipe.to_function()()

    elif request.param == "dask":

        def execute(recipe):
            with Client(dask_cluster):
                return recipe.to_dask().compute()

    elif request.param == "prefect":

        def execute(recipe):
            state = recipe.to_prefect().run()
            if state.is_failed():
                raise ValueError(f"Prefect flow run failed with message {state.message}")

    else:
        assert request.param == "prefect-dask"

        def execute(recipe):
            flow = recipe.to_prefect()
            executor = DaskExecutor(address=dask_cluster.scheduler_address)
            state = flow.run(executor=executor)
            if state.is_failed():
                raise ValueError(f"Prefect flow run failed with message {state.message}")

    execute.param = request.param
    return execute
