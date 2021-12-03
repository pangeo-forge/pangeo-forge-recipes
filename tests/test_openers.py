import pytest
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.openers.fsspec import FsspecLocalCopyOpener, FsspecOpener


@pytest.mark.parametrize(
    "file_paths",
    [
        lazy_fixture("netcdf_local_paths_sequential"),
        lazy_fixture("netcdf_http_paths_sequential_1d"),
    ],
)
@pytest.mark.parametrize("copy_to_local", [False, True])
@pytest.mark.parametrize("use_cache, cache_first", [(False, False), (True, False), (True, True)])
@pytest.mark.parametrize("use_xarray", [True, False])
def test_file_opener(
    file_paths, tmp_cache, copy_to_local, use_cache, cache_first, use_xarray,
):
    all_paths, items_per_file, _, _, kwargs = file_paths
    path = str(all_paths[0])
    fsspec_open_kwargs = kwargs["fsspec_open_kwargs"]
    secrets = kwargs["query_string_secrets"]

    cache = tmp_cache if use_cache else None

    if copy_to_local:
        Opener = FsspecLocalCopyOpener
    else:
        Opener = FsspecOpener
    opener = Opener(cache=cache, secrets=secrets, fsspec_open_kwargs=fsspec_open_kwargs)

    if cache_first:
        opener.cache_file(path)
        assert cache.exists(path)
        details = cache.fs.ls(cache.root_path, detail=True)
        opener.cache_file(path)
        # check that nothing happened
        assert cache.fs.ls(cache.root_path, detail=True) == details

    if use_cache and not cache_first:
        with pytest.raises(FileNotFoundError):
            with opener.open(path) as fp:
                pass
        return

    with opener.open(path) as fp:
        if copy_to_local:
            assert isinstance(fp, str)
            with open(fp, mode="rb") as fp2:
                data = fp2.read()
        else:
            data = fp.read()
            assert hasattr(fp, "fs")  # should be true for fsspec.OpenFile objects

    expected_length = {1: 19000, 2: 29376}  # hard coded expected value
    assert len(data) == expected_length[items_per_file]
