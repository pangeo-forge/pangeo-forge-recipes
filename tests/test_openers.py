import pytest
import xarray as xr
from dask import delayed
from dask.distributed import Client
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
@pytest.mark.parametrize("use_dask", [True, False])
@pytest.mark.parametrize("use_xarray", [True, False])
def test_file_opener(
    file_paths,
    tmp_cache,
    copy_to_local,
    use_cache,
    cache_first,
    dask_cluster,
    use_dask,
    use_xarray,
):
    all_paths = file_paths[0]
    path = str(all_paths[0])
    open_kwargs = file_paths[-1]["fsspec_open_kwargs"]
    secrets = file_paths[-1]["query_string_secrets"]
    cache = tmp_cache if use_cache else None

    def do_actual_test():
        if copy_to_local:
            Opener = FsspecLocalCopyOpener
        else:
            Opener = FsspecOpener
        opener = Opener(cache=cache, secrets=secrets, open_kwargs=open_kwargs)

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
                    if use_xarray:
                        ds = xr.open_dataset(fp2, engine="h5netcdf")
                        ds.load()
                    else:
                        _ = fp2.read()
            else:
                if use_xarray:
                    ds = xr.open_dataset(fp, engine="h5netcdf")
                    ds.load()
                else:
                    _ = fp.read()
                assert hasattr(fp, "fs")  # should be true for fsspec.OpenFile objects

    if use_dask:
        with Client(dask_cluster) as _:
            to_actual_test_delayed = delayed(do_actual_test)()
            to_actual_test_delayed.compute()
    else:
        do_actual_test()
