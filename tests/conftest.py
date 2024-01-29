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
import os
import socket
import subprocess
import time

import aiohttp
import apache_beam as beam
import fsspec
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# need to import this way (rather than use pytest.lazy_fixture) to make it work with dask
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec

from .data_generation import make_ds, make_pyramid


# To use this feature, e.g. `$ pytest --run-integration`
# https://jwodder.github.io/kbits/posts/pytest-mark-off/
def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests. Skipped by default because they are slow-running.",
    )


# Helper functions --------------------------------------------------------------------------------


def split_up_files_by_day(ds, day_param):
    gb = ds.resample(time=day_param)
    _, datasets = zip(*gb)
    fnames = [f"{n:03d}" for n in range(len(datasets))]
    return datasets, fnames


def split_up_files_by_variable_and_day(
    ds: xr.Dataset,
    day_param: str,
) -> tuple[list[xr.Dataset], list[str], dict]:
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
    paths, items_per_file, fnames_by_variable, path_format, kwargs, file_type = path_fixture

    if not fnames_by_variable:
        file_pattern = pattern_from_file_sequence(
            [str(path) for path in paths], "time", items_per_file, file_type=file_type, **kwargs
        )
    else:
        time_index = list(range(len(paths) // 2))

        def format_function(variable, time):
            return path_format.format(variable=variable, time=time)

        file_pattern = FilePattern(
            format_function,
            ConcatDim("time", time_index, items_per_file),
            MergeDim("variable", ["foo", "bar"]),
            file_type=file_type,
            **kwargs,
        )

    return file_pattern


def make_local_paths(
    daily_xarray_dataset, tmpdir_factory, items_per_file, file_splitter, file_type="netcdf4"
):
    tmp_path = tmpdir_factory.mktemp("netcdf_data")
    file_splitter_tuple = file_splitter(daily_xarray_dataset.copy(), items_per_file)

    if file_type == "netcdf4":
        method, suffix, kwargs = "to_netcdf", "nc", {"engine": "netcdf4", "format": "NETCDF4"}
    elif file_type == "netcdf3":
        method, suffix, kwargs = "to_netcdf", "nc", {"engine": "scipy", "format": "NETCDF3_CLASSIC"}
    elif file_type == "zarr":
        method, suffix, kwargs = "to_zarr", "zarr", {}
    else:
        assert False

    datasets, fnames = file_splitter_tuple[:2]
    full_paths = [str(tmp_path.join(fname)) + f".{suffix}" for fname in fnames]

    for ds, path in zip(datasets, full_paths):
        save_method = getattr(ds, method)
        save_method(path, **kwargs)

    # xr.save_mfdataset(datasets, [str(path) for path in full_paths])
    items_per_file = {"D": 1, "2D": 2}[items_per_file]

    fnames_by_variable = file_splitter_tuple[-1] if len(file_splitter_tuple) == 3 else None
    path_format = (
        str(tmp_path) + "/{variable}_{time:03d}" + f".{suffix}" if fnames_by_variable else None
    )

    kwargs = {}

    return full_paths, items_per_file, fnames_by_variable, path_format, kwargs, file_type


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
    basedir = os.path.dirname(first_path)

    this_dir = os.path.dirname(os.path.abspath(__file__))
    port = get_open_port()
    command_list = [
        "python3",
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


def make_http_paths(netcdf_local_paths, request):
    paths, items_per_file, fnames_by_variable, _, _, file_type = netcdf_local_paths

    param = getattr(request, "param", {})
    url = start_http_server(paths, request, **param)
    suf = "zarr" if file_type == "zarr" else "nc"
    # what is this variable for? 🤔
    path_format = url + "/{variable}_{time:03d}" + f".{suf}" if fnames_by_variable else None

    fnames = [os.path.basename(path) for path in paths]
    all_urls = ["/".join([url, str(fname)]) for fname in fnames]

    kwargs = {}
    if "username" in param.keys():
        kwargs = {"fsspec_open_kwargs": {"auth": aiohttp.BasicAuth("foo", "bar")}}
    if "required_query_string" in param.keys():
        kwargs.update({"query_string_secrets": {"foo": "foo", "bar": "bar"}})

    return all_urls, items_per_file, fnames_by_variable, path_format, kwargs, file_type


# Dataset + path fixtures -------------------------------------------------------------------------


@pytest.fixture(params=[True, False], ids=["with_cache", "no_cache"])
def cache_url(tmp_cache_url, request):
    if request.param:
        return tmp_cache_url
    else:
        return None


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
        lazy_fixture("netcdf_http_file_pattern_sequential_1d"),
    ],
    ids=["local", "http"],
)
def pattern(request):
    return request.param


@pytest.fixture
def pipeline(scope="session"):
    # TODO: make this True and fix the weird ensuing type check errors
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


@pytest.fixture(
    params=[True, False],
    ids=["concurrency_limit", "no_concurrency_limit"],
)
def max_concurrency(request):
    if request.param:
        return 2
    else:
        return None


@pytest.fixture
def pcoll_opened_files(pattern, cache_url, max_concurrency):
    input = beam.Create(pattern.items())
    output = input | OpenURLWithFSSpec(
        cache=cache_url,
        secrets=pattern.query_string_secrets,
        open_kwargs=pattern.fsspec_open_kwargs,
        max_concurrency=max_concurrency,
    )
    return output, pattern, cache_url


@pytest.fixture(scope="session")
def daily_xarray_dataset():
    return make_ds(nt=10)


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
    return make_local_paths(
        daily_xarray_dataset, tmpdir_factory, "D", split_up_files_by_day, file_type="netcdf4"
    )


@pytest.fixture(scope="session")
def netcdf3_local_paths_sequential_1d(daily_xarray_dataset, tmpdir_factory):
    return make_local_paths(
        daily_xarray_dataset, tmpdir_factory, "D", split_up_files_by_day, file_type="netcdf3"
    )


@pytest.fixture(scope="session")
def zarr_local_paths_sequential_1d(daily_xarray_dataset, tmpdir_factory):
    return make_local_paths(
        daily_xarray_dataset, tmpdir_factory, "D", split_up_files_by_day, file_type="zarr"
    )


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_2d(daily_xarray_dataset, tmpdir_factory):
    return make_local_paths(
        daily_xarray_dataset, tmpdir_factory, "2D", split_up_files_by_day, file_type="netcdf4"
    )


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_1d"),
        lazy_fixture("netcdf3_local_paths_sequential_1d"),
        lazy_fixture("netcdf_local_paths_sequential_2d"),
    ],
)
def netcdf_local_paths_sequential(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_multivariable_1d(daily_xarray_dataset, tmpdir_factory):
    return make_local_paths(
        daily_xarray_dataset,
        tmpdir_factory,
        "D",
        split_up_files_by_variable_and_day,
        file_type="netcdf4",
    )


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_multivariable_2d(daily_xarray_dataset, tmpdir_factory):
    return make_local_paths(
        daily_xarray_dataset,
        tmpdir_factory,
        "2D",
        split_up_files_by_variable_and_day,
        file_type="netcdf4",
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
    return make_local_paths(
        daily_xarray_dataset_with_coordinateless_dimension,
        tmpdir_factory,
        "D",
        split_up_files_by_variable_and_day,
        file_type="netcdf4",
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


@pytest.fixture(scope="session")
def netcdf_public_http_paths_sequential_1d(netcdf_local_paths_sequential_1d, request):
    return make_http_paths(netcdf_local_paths_sequential_1d, request)


@pytest.fixture(scope="session")
def netcdf3_public_http_paths_sequential_1d(netcdf3_local_paths_sequential_1d, request):
    return make_http_paths(netcdf3_local_paths_sequential_1d, request)


@pytest.fixture(scope="session")
def zarr_public_http_paths_sequential_1d(zarr_local_paths_sequential_1d, request):
    return make_http_paths(zarr_local_paths_sequential_1d, request)


@pytest.fixture(
    scope="session",
    params=[
        dict(username="foo", password="bar"),
        dict(required_query_string="foo=foo&bar=bar"),
    ],
    ids=["http-auth", "query-string-auth"],
)
def netcdf_private_http_paths_sequential_1d(netcdf_local_paths_sequential_1d, request):
    return make_http_paths(netcdf_local_paths_sequential_1d, request)


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("netcdf_public_http_paths_sequential_1d"),
        lazy_fixture("netcdf3_public_http_paths_sequential_1d"),
        lazy_fixture("netcdf_private_http_paths_sequential_1d"),
    ],
)
def netcdf_http_paths_sequential_1d(request):
    return request.param


@pytest.fixture(scope="session")
def netcdf_local_paths_sequential_with_coordinateless_dimension(
    daily_xarray_dataset_with_coordinateless_dimension, tmpdir_factory
):
    return make_local_paths(
        daily_xarray_dataset_with_coordinateless_dimension,
        tmpdir_factory,
        "D",
        split_up_files_by_day,
        file_type="netcdf4",
    )


@pytest.fixture(scope="session")
def pyramid_datatree(n_levels: int = 2):
    return make_pyramid(n_levels=n_levels)


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
def zarr_http_file_pattern_sequential_1d(zarr_public_http_paths_sequential_1d):
    return make_file_pattern(zarr_public_http_paths_sequential_1d)


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
def tmp_target_url(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("target.zarr"))
    return path


@pytest.fixture()
def tmp_cache(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("cache"))
    fs = fsspec.get_filesystem_class("file")()
    cache = CacheFSSpecTarget(fs, path)
    return cache


@pytest.fixture()
def tmp_cache_url(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("cache"))
    return path
