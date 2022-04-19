"""
Objects in this module belong to the following groups, delimited by inline comments:
    - Helper functions:
        Functions used in the creation of fixtures
    - Dataset + path fixtures:
        Create a dataset or a path to dataset(s)
    - FilePattern fixtures:
        Create `pangeo_forge_recipes.patterns.FilePattern` instances
    - Storage fixtures:
        Create temporary storage locations for caching, writing, etc.
    - Execution fixtures:
        Create infrastructure or define steps for executing recipes
Note:
   Recipe fixtures are defined in their respective test modules, e.g. `test_recipes.py`
"""
import logging
import os
import socket
import subprocess
import time

import aiohttp
import fsspec
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from dask.distributed import Client, LocalCluster
from prefect.executors import DaskExecutor

# need to import this way (rather than use pytest.lazy_fixture) to make it work with dask
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget

# Helper functions --------------------------------------------------------------------------------


# to use this feature, e.g.
# $ pytest --redirect-dask-worker-logs-to-stdout=DEBUG
def pytest_addoption(parser):
    parser.addoption(
        "--redirect-dask-worker-logs-to-stdout",
        action="store",
        default="NOTSET",
    )


def split_up_files_by_day(ds, day_param):
    gb = ds.resample(time=day_param)
    _, datasets = zip(*gb)
    fnames = [f"{n:03d}.nc" for n in range(len(datasets))]
    return datasets, fnames


def split_up_files_by_variable_and_day(ds, day_param):
    all_dsets = []
    all_fnames = []
    fnames_by_variable = {}
    for varname in ds.data_vars:
        var_dsets, fnames = split_up_files_by_day(ds[[varname]], day_param)
        fnames = [f"{varname}_{fname}" for fname in fnames]
        all_dsets += var_dsets
        all_fnames += fnames
        fnames_by_variable[varname] = fnames
    return all_dsets, all_fnames, fnames_by_variable


def make_file_pattern(path_fixture):
    """Creates a filepattern from a `path_fixture`

    Parameters
    ----------
    path_fixture : callable
        `netcdf_local_paths`, `netcdf_http_paths`, or similar
    """
    paths, items_per_file, fnames_by_variable, path_format, kwargs = path_fixture

    if not fnames_by_variable:
        file_pattern = pattern_from_file_sequence(
            [str(path) for path in paths], "time", items_per_file, **kwargs
        )
    else:
        time_index = list(range(len(paths) // 2))

        def format_function(variable, time):
            return path_format.format(variable=variable, time=time)

        file_pattern = FilePattern(
            format_function,
            ConcatDim("time", time_index, items_per_file),
            MergeDim("variable", ["foo", "bar"]),
            **kwargs,
        )

    return file_pattern


def make_netcdf_local_paths(daily_xarray_dataset, tmpdir_factory, items_per_file, file_splitter):
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    file_splitter_tuple = file_splitter(daily_xarray_dataset.copy(), items_per_file)

    datasets, fnames = file_splitter_tuple[:2]
    full_paths = [tmp_path.join(fname) for fname in fnames]
    xr.save_mfdataset(datasets, [str(path) for path in full_paths])
    items_per_file = {"D": 1, "2D": 2}[items_per_file]

    fnames_by_variable = file_splitter_tuple[-1] if len(file_splitter_tuple) == 3 else None
    path_format = str(tmp_path) + "/{variable}_{time:03d}.nc" if fnames_by_variable else None

    kwargs = dict(fsspec_open_kwargs={}, query_string_secrets={})

    return full_paths, items_per_file, fnames_by_variable, path_format, kwargs


def get_open_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = str(s.getsockname()[1])
    s.close()
    return port


def start_http_server(paths, request, username=None, password=None, required_query_string=None):

    first_path = paths[0]
    # assume that all files are in the same directory
    basedir = first_path.dirpath()

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
    if required_query_string:
        command_list += [f"--required-query-string={required_query_string}"]
    p = subprocess.Popen(command_list, cwd=basedir)
    url = f"http://127.0.0.1:{port}"
    time.sleep(2)  # let the server start up

    def teardown():
        p.kill()

    request.addfinalizer(teardown)

    return url


def make_netcdf_http_paths(netcdf_local_paths, request):
    paths, items_per_file, fnames_by_variable, _, kwargs = netcdf_local_paths

    url = start_http_server(paths, request, **request.param)
    path_format = url + "/{variable}_{time:03d}.nc" if fnames_by_variable else None

    fnames = [path.basename for path in paths]
    all_urls = ["/".join([url, str(fname)]) for fname in fnames]

    if "username" in request.param.keys():
        kwargs.update(dict(fsspec_open_kwargs={"auth": aiohttp.BasicAuth("foo", "bar")}))
    if "required_query_string" in request.param.keys():
        kwargs.update(dict(query_string_secrets={"foo": "foo", "bar": "bar"}))

    return all_urls, items_per_file, fnames_by_variable, path_format, kwargs


# Dataset + path fixtures -------------------------------------------------------------------------


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


@pytest.fixture(scope="session")
def daily_xarray_dataset_with_coordinateless_dimension(daily_xarray_dataset):
    """
    A Dataset with a coordinateless dimension.

    Reproduces https://github.com/pangeo-forge/pangeo-forge-recipes/issues/214
    """
    ds = daily_xarray_dataset.copy()
    del ds["lon"]
    return ds


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_1d(daily_xarray_dataset, tmpdir_factory):
    return make_netcdf_local_paths(daily_xarray_dataset, tmpdir_factory, "D", split_up_files_by_day)


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_2d(daily_xarray_dataset, tmpdir_factory):
    return make_netcdf_local_paths(
        daily_xarray_dataset,
        tmpdir_factory,
        "2D",
        split_up_files_by_day,
    )


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_1d"),
        lazy_fixture("netcdf_local_paths_sequential_2d"),
    ],
)
def netcdf_local_paths_sequential(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_multivariable_1d(daily_xarray_dataset, tmpdir_factory):
    return make_netcdf_local_paths(
        daily_xarray_dataset,
        tmpdir_factory,
        "D",
        split_up_files_by_variable_and_day,
    )


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_multivariable_2d(daily_xarray_dataset, tmpdir_factory):
    return make_netcdf_local_paths(
        daily_xarray_dataset,
        tmpdir_factory,
        "2D",
        split_up_files_by_variable_and_day,
    )


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_multivariable_1d"),
        lazy_fixture("netcdf_local_paths_sequential_multivariable_2d"),
    ],
)
def netcdf_local_paths_sequential_multivariable(request):
    return request.param


@pytest.fixture(
    scope="session",
)
def netcdf_local_paths_sequential_multivariable_with_coordinateless_dimension(
    daily_xarray_dataset_with_coordinateless_dimension, tmpdir_factory
):
    return make_netcdf_local_paths(
        daily_xarray_dataset_with_coordinateless_dimension,
        tmpdir_factory,
        "D",
        split_up_files_by_variable_and_day,
    )


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_local_paths_sequential"),
        lazy_fixture("netcdf_local_paths_sequential_multivariable"),
    ],
)
def netcdf_local_paths(request):
    return request.param


http_auth_params = [
    dict(username="foo", password="bar"),
    dict(required_query_string="foo=foo&bar=bar"),
]


@pytest.fixture(scope="session", params=[dict()])
def netcdf_public_http_paths_sequential_1d(netcdf_local_paths_sequential_1d, request):
    return make_netcdf_http_paths(netcdf_local_paths_sequential_1d, request)


@pytest.fixture(scope="session", params=[*http_auth_params])
def netcdf_private_http_paths_sequential_1d(netcdf_local_paths_sequential_1d, request):
    return make_netcdf_http_paths(netcdf_local_paths_sequential_1d, request)


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_public_http_paths_sequential_1d"),
        lazy_fixture("netcdf_private_http_paths_sequential_1d"),
    ],
)
def netcdf_http_paths_sequential_1d(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_with_coordinateless_dimension(
    daily_xarray_dataset_with_coordinateless_dimension, tmpdir_factory
):
    return make_netcdf_local_paths(
        daily_xarray_dataset_with_coordinateless_dimension,
        tmpdir_factory,
        "D",
        split_up_files_by_day,
    )


# FilePattern fixtures ----------------------------------------------------------------------------


@pytest.fixture(scope="session")
def netcdf_local_file_pattern_sequential(netcdf_local_paths_sequential):
    return make_file_pattern(netcdf_local_paths_sequential)


@pytest.fixture(scope="session")
def netcdf_local_file_pattern_sequential_multivariable(
    netcdf_local_paths_sequential_multivariable,
):
    return make_file_pattern(netcdf_local_paths_sequential_multivariable)


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
        lazy_fixture("netcdf_local_file_pattern_sequential_multivariable"),
    ],
)
def netcdf_local_file_pattern(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_http_file_pattern_sequential_1d(netcdf_http_paths_sequential_1d):
    return make_file_pattern(netcdf_http_paths_sequential_1d)


@pytest.fixture(scope="session")
def netcdf_local_file_pattern_sequential_with_coordinateless_dimension(
    netcdf_local_paths_sequential_with_coordinateless_dimension,
):
    """
    Filepattern where one of the dimensions doesn't have a coordinate.
    """
    return make_file_pattern(netcdf_local_paths_sequential_with_coordinateless_dimension)


# Storage fixtures --------------------------------------------------------------------------------


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


# Execution fixtures ------------------------------------------------------------------------------


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


# The fixtures below use the following pattern to only run when the marks are activated
# Based on https://github.com/pytest-dev/pytest/issues/1368#issuecomment-466339463


@pytest.fixture(
    scope="session",
    params=[
        pytest.param("FunctionPipelineExecutor", marks=pytest.mark.executor_function),
        pytest.param("GeneratorPipelineExecutor", marks=pytest.mark.executor_generator),
        pytest.param("DaskPipelineExecutor", marks=pytest.mark.executor_dask),
        pytest.param("PrefectPipelineExecutor", marks=pytest.mark.executor_prefect),
        pytest.param("BeamPipelineExecutor", marks=pytest.mark.executor_beam),
    ],
)
def Executor(request):
    try:
        import pangeo_forge_recipes.executors as exec_module

        return getattr(exec_module, request.param)
    except AttributeError:
        pytest.skip(f"Couldn't import {request.param}")


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_function)])
def execute_recipe_function():
    def execute(recipe):
        return recipe.to_function()()

    return execute


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_generator)])
def execute_recipe_generator():
    def execute(recipe):
        for f, args, kwargs in recipe.to_generator():
            f(*args, **kwargs)

    return execute


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_dask)])
def execute_recipe_dask(dask_cluster):
    def execute(recipe):
        with Client(dask_cluster):
            return recipe.to_dask().compute()

    return execute


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_prefect)])
def execute_recipe_prefect():
    def execute(recipe):
        state = recipe.to_prefect().run()
        if state.is_failed():
            raise ValueError(f"Prefect flow run failed with message {state.message}")

    return execute


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_prefect_dask)])
def execute_recipe_prefect_dask(dask_cluster):
    def execute(recipe):
        flow = recipe.to_prefect()
        executor = DaskExecutor(address=dask_cluster.scheduler_address)
        state = flow.run(executor=executor)
        if state.is_failed():
            raise ValueError(f"Prefect flow run failed with message {state.message}")

    return execute


@pytest.fixture(params=[pytest.param(0, marks=pytest.mark.executor_beam)])
def execute_recipe_beam():
    beam = pytest.importorskip("apache_beam")

    def execute(recipe):
        pcoll = recipe.to_beam()
        with beam.Pipeline() as p:
            p | pcoll

    return execute


# now mark all other tests with "no_executor"
# https://stackoverflow.com/questions/39846230/how-to-run-only-unmarked-tests-in-pytest
def pytest_collection_modifyitems(items, config):
    for item in items:
        executor_markers = [
            marker for marker in item.iter_markers() if marker.name.startswith("executor_")
        ]
        if len(executor_markers) == 0:
            item.add_marker("no_executor")


@pytest.fixture(
    params=[
        lazy_fixture("execute_recipe_function"),
        lazy_fixture("execute_recipe_generator"),
        lazy_fixture("execute_recipe_dask"),
        lazy_fixture("execute_recipe_beam"),
    ],
)
def execute_recipe_no_prefect(request):
    return request.param


@pytest.fixture(
    params=[lazy_fixture("execute_recipe_prefect"), lazy_fixture("execute_recipe_prefect_dask")],
)
def execute_recipe_with_prefect(request):
    return request.param


@pytest.fixture(
    params=[lazy_fixture("execute_recipe_function"), lazy_fixture("execute_recipe_prefect")],
)
def execute_recipe_no_dask(request):
    return request.param


@pytest.fixture(
    params=[lazy_fixture("execute_recipe_dask"), lazy_fixture("execute_recipe_prefect_dask")],
)
def execute_recipe_with_dask(request):
    return request.param


@pytest.fixture(
    params=[
        lazy_fixture("execute_recipe_no_prefect"),
        lazy_fixture("execute_recipe_with_prefect"),
    ],
)
def execute_recipe(request):
    return request.param
