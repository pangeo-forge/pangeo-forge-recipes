import pytest
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.openers.fsspec import FsspecLocalCopyOpener, FsspecOpener

EXPECTED_FILE_SIZE = {1: 19000, 2: 29376}  # hard coded expected value


@pytest.fixture(
    params=[
        lazy_fixture("netcdf_local_paths_sequential"),
        lazy_fixture("netcdf_http_paths_sequential_1d"),
    ],
)
def path_expected_len_and_kwargs(request):
    """Take a full set of inputs and return just a single path, plus
    the other stuff the opener needs to know.
    """

    all_paths, items_per_file, _, _, kwargs = request.param
    expected_len = EXPECTED_FILE_SIZE[items_per_file]
    path = str(all_paths[0])
    return path, expected_len, kwargs


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


def test_fsspec_opener(path_expected_len_and_kwargs, cache_and_cache_checker):
    path, expected_len, kwargs = path_expected_len_and_kwargs
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
    assert len(data) == expected_len


def test_fsspec_local_copy_opener(path_expected_len_and_kwargs, cache_and_cache_checker):
    path, expected_len, kwargs = path_expected_len_and_kwargs
    cache, cache_checker = cache_and_cache_checker
    opener = FsspecLocalCopyOpener(
        cache=cache,
        secrets=kwargs["query_string_secrets"],
        fsspec_open_kwargs=kwargs["fsspec_open_kwargs"],
    )
    cache_checker(opener, cache, path)

    with opener.open(path) as fp:
        assert isinstance(fp, str)
        with open(fp, mode="rb") as fp2:
            data = fp2.read()
    assert len(data) == expected_len


#     if use_cache and not cache_first:
#         with pytest.raises(FileNotFoundError):
#             with opener.open(path) as fp:
#                 pass
#         return
