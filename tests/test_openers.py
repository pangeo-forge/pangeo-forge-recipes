from pickle import dumps, loads

import numpy as np
import pytest
import xarray as xr
from apache_beam.testing.util import assert_that
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.openers import open_url, open_with_xarray
from pangeo_forge_recipes.patterns import FileType
from pangeo_forge_recipes.transforms import OpenWithKerchunk
from pangeo_forge_recipes.types import Index


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_1d"),
        lazy_fixture("netcdf_public_http_paths_sequential_1d"),
        lazy_fixture("netcdf3_public_http_paths_sequential_1d"),
        lazy_fixture("netcdf_private_http_paths_sequential_1d"),
    ],
    ids=[
        "netcdf4_local",
        "netcdf4_http_public",
        "netcdf3_http_public",
        "netcdf4_http_private",
    ],
)
def url_and_type(request):
    all_urls, _, _, _, extra_kwargs, type_str = request.param
    kwargs = {
        "secrets": extra_kwargs.get("query_string_secrets", None),
        "open_kwargs": extra_kwargs.get("fsspec_open_kwargs", None),
    }
    file_type = FileType(type_str)
    return all_urls[0], kwargs, file_type


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_paths_sequential_1d"),
        lazy_fixture("zarr_public_http_paths_sequential_1d"),
    ],
    ids=["netcdf_local", "zarr_http"],
)
def public_url_and_type(request):
    all_urls, _, _, _, _, type_str = request.param
    file_type = FileType(type_str)
    return all_urls[0], file_type


@pytest.fixture(params=[True, False], ids=["with_cache", "no_cache"])
def cache(tmp_cache, request):
    if request.param:
        return tmp_cache
    else:
        return None


def test_open_url(url_and_type, cache):
    url, kwargs, file_type = url_and_type
    if cache:
        assert not cache.exists(url)
    open_file = open_url(url, cache=cache, **kwargs)
    open_file2 = loads(dumps(open_file))
    with open_file as f1:
        data = f1.read()
    with open_file2 as f2:
        data2 = f2.read()
    assert data == data2
    if cache:
        assert cache.exists(url)
        with cache.open(url, mode="rb") as f3:
            data3 = f3.read()
        assert data3 == data


@pytest.fixture(params=[False, True], ids=["lazy", "eager"])
def load(request):
    return request.param


@pytest.fixture(params=[False, True], ids=["dont_copy", "copy_to_local"])
def copy_to_local(request):
    return request.param


def _time_is_datetime(ds):
    assert ds.time.dtype == np.dtype("<M8[ns]")


def _time_is_int(ds):
    # netcdf3 and netcdf4 behave differently here
    assert ds.time.dtype in [np.dtype("i4"), np.dtype("i8")]


@pytest.fixture(
    params=[({}, _time_is_datetime), ({"decode_times": False}, _time_is_int)],
    ids=["no_xr_kwargs", "decode_times_False"],
)
def xarray_open_kwargs(request):
    kwargs, validate_fn = request.param
    return kwargs, validate_fn


def is_valid_dataset(ds, in_memory=False):
    ds = loads(dumps(ds))  # make sure it serializes
    assert isinstance(ds, xr.Dataset)
    offending_vars = [
        vname for vname in ds.data_vars if ds[vname].variable._in_memory != in_memory
    ]
    if offending_vars:
        msg = "were NOT in memory" if in_memory else "were in memory"
        raise AssertionError(f"The following vars {msg}: {offending_vars}")


def validate_open_file_with_xarray(
    url_and_type, cache, load, copy_to_local, xarray_open_kwargs
):
    # open fsspec OpenFile objects
    url, kwargs, file_type = url_and_type
    open_file = open_url(url, cache=cache, **kwargs)
    xr_kwargs, validate_fn = xarray_open_kwargs
    ds = open_with_xarray(
        open_file,
        file_type=file_type,
        load=load,
        copy_to_local=copy_to_local,
        xarray_open_kwargs=xr_kwargs,
    )
    validate_fn(ds)
    is_valid_dataset(ds, in_memory=load)


def test_open_file_with_xarray(
    url_and_type, cache, load, copy_to_local, xarray_open_kwargs
):
    validate_open_file_with_xarray(
        url_and_type=url_and_type,
        cache=cache,
        load=load,
        copy_to_local=copy_to_local,
        xarray_open_kwargs=xarray_open_kwargs,
    )


def test_open_file_with_xarray_unknown_filetype(
    url_and_type, cache, load, copy_to_local, xarray_open_kwargs
):
    # Ignore the specified file_type
    url, kwargs, _ = url_and_type
    # Specifying unknown file_type should ensure xarray automatically
    # selects the backend engine
    validate_open_file_with_xarray(
        url_and_type=(url, kwargs, FileType.unknown),
        cache=cache,
        load=load,
        copy_to_local=copy_to_local,
        xarray_open_kwargs=xarray_open_kwargs,
    )


def test_direct_open_with_xarray(public_url_and_type, load, xarray_open_kwargs):
    # open string URLs
    url, file_type = public_url_and_type
    xr_kwargs, validate_fn = xarray_open_kwargs
    ds = open_with_xarray(
        url, file_type=file_type, load=load, xarray_open_kwargs=xr_kwargs
    )
    validate_fn(ds)
    is_valid_dataset(ds, in_memory=load)


def is_valid_inline_threshold():
    def _is_valid_inline_threshold(indexed_references):
        assert isinstance(indexed_references[0][0], Index)
        assert isinstance(indexed_references[0][1][0]["refs"]["lat/0"], list)

    return _is_valid_inline_threshold


def test_inline_threshold(pcoll_opened_files, pipeline):
    input, pattern, cache_url = pcoll_opened_files

    with pipeline as p:
        output = p | input | OpenWithKerchunk(pattern.file_type, inline_threshold=1)
        assert_that(output, is_valid_inline_threshold())
