import os

import pytest
import xarray as xr
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.openers.fsspec import FsspecLocalCopyOpener, FsspecOpener
from pangeo_forge_recipes.openers.xarray import XarrayOpener

EXPECTED_FILE_SIZE = {1: 19000, 2: 29376}  # hard coded expected value


def _make_path_expected_size_and_kwargs(paths_fixture):
    """Take a full set of inputs and return just a single path, plus
    the other stuff the opener needs to know.
    """
    all_paths, items_per_file, _, _, kwargs = paths_fixture
    expected_size = EXPECTED_FILE_SIZE[items_per_file]
    path = str(all_paths[0])
    return path, expected_size, kwargs


@pytest.fixture
def netcdf_local_path_expected_size_and_kwargs(netcdf_local_paths_sequential):
    return _make_path_expected_size_and_kwargs(netcdf_local_paths_sequential)


@pytest.fixture
def netcdf_http_path_expected_size_and_kwargs(netcdf_http_paths_sequential_1d):
    return _make_path_expected_size_and_kwargs(netcdf_http_paths_sequential_1d)


@pytest.fixture(
    params=[
        lazy_fixture("netcdf_local_path_expected_size_and_kwargs"),
        lazy_fixture("netcdf_http_path_expected_size_and_kwargs"),
    ],
)
def netcdf_path_expected_size_and_kwargs(request):
    return request.param


def _cache_checker(opener, cache, path):
    """Check that the cache on an opener is working."""

    opener.cache_file(path)
    assert cache.exists(path)
    details = cache.fs.ls(cache.root_path, detail=True)
    opener.cache_file(path)
    # check that nothing happened
    assert cache.fs.ls(cache.root_path, detail=True) == details


def _passthrough(opener, cache, path):
    pass


@pytest.fixture(params=[(True, _cache_checker), (False, _passthrough)])
def cache_and_cache_checker(request, tmp_cache):
    use_cache, checker = request.param
    cache = tmp_cache if use_cache else None
    return cache, checker


def test_fsspec_opener(netcdf_path_expected_size_and_kwargs, cache_and_cache_checker):
    path, expected_size, kwargs = netcdf_path_expected_size_and_kwargs
    cache, cache_checker = cache_and_cache_checker
    opener = FsspecOpener(
        cache=cache,
        secrets=kwargs["query_string_secrets"],
        fsspec_open_kwargs=kwargs["fsspec_open_kwargs"],
    )
    cache_checker(opener, cache, path)

    with opener.open(path) as fp:
        data = fp.read()
        assert hasattr(fp, "fs")  # should be true for fsspec.OpenFile objects
    assert len(data) == expected_size


def test_fsspec_local_copy_opener(netcdf_path_expected_size_and_kwargs, cache_and_cache_checker):
    path, expected_size, kwargs = netcdf_path_expected_size_and_kwargs
    cache, cache_checker = cache_and_cache_checker
    opener = FsspecLocalCopyOpener(
        cache=cache,
        secrets=kwargs["query_string_secrets"],
        fsspec_open_kwargs=kwargs["fsspec_open_kwargs"],
    )
    cache_checker(opener, cache, path)

    with opener.open(path) as fname:
        assert isinstance(fname, str)  # shouldn't be necessary with proper type hints
        with open(fname, mode="rb") as fp2:
            data = fp2.read()
    assert not os.path.exists(fname)  # make sure file got cleaned up
    assert len(data) == expected_size


def test_xarray_opener(netcdf_local_path_expected_size_and_kwargs):
    path, _, _ = netcdf_local_path_expected_size_and_kwargs
    opener = XarrayOpener()
    print(path, type(path))
    with opener.open(path) as ds:
        assert isinstance(ds, xr.Dataset)


#     if use_cache and not cache_first:
#         with pytest.raises(FileNotFoundError):
#             with opener.open(path) as fp:
#                 pass
#         return
