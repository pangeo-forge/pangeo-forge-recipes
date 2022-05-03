import hashlib
import os
import re

import pytest
import xarray as xr
from dask import delayed
from dask.distributed import Client
from fsspec.implementations.local import LocalFileSystem
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, file_opener


def test_target(tmp_target):
    mapper = tmp_target.get_mapper()
    mapper["foo"] = b"bar"
    with open(tmp_target.root_path + "/foo") as f:
        res = f.read()
    assert res == "bar"
    with pytest.raises(FileNotFoundError):
        tmp_target.rm("baz")
    with pytest.raises(FileNotFoundError):
        with tmp_target.open("baz"):
            pass


def test_cache(tmp_cache):
    assert not tmp_cache.exists("foo")
    with tmp_cache.open("foo", mode="w") as f:
        f.write("bar")
    assert tmp_cache.exists("foo")
    assert tmp_cache.size("foo") == 3
    with tmp_cache.open("foo", mode="r") as f:
        assert f.read() == "bar"
    tmp_cache.rm("foo")
    assert not tmp_cache.exists("foo")


def test_metadata_target(tmp_metadata_target):
    data = {"foo": 1, "bar": "baz"}
    tmp_metadata_target["key1"] = data
    assert "key1" in tmp_metadata_target
    assert "key2" not in tmp_metadata_target
    assert tmp_metadata_target["key1"] == data
    assert tmp_metadata_target.getitems(["key1"]) == {"key1": data}


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
        if cache_first:
            cache.cache_file(path, secrets, **open_kwargs)
            assert cache.exists(path)
            details = cache.fs.ls(cache.root_path, detail=True)
            cache.cache_file(path, secrets, **open_kwargs)
            # check that nothing happened
            assert cache.fs.ls(cache.root_path, detail=True) == details

        opener = file_opener(
            path, cache, copy_to_local=copy_to_local, secrets=secrets, **open_kwargs
        )
        if use_cache and not cache_first:
            with pytest.raises(FileNotFoundError):
                with opener as fp:
                    pass
        else:
            with opener as fp:
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
        with Client(dask_cluster) as client:
            to_actual_test_delayed = delayed(do_actual_test)()
            to_actual_test_delayed.compute()
            client.close()
    else:
        do_actual_test()


def test_caching_local_fname_length_not_greater_than_255_bytes(tmpdir_factory):
    POSIX_MAX_FNAME_LENGTH = 255
    extension = ".nc"
    tmp_path = tmpdir_factory.mktemp("fname-length-tempdir")

    fname = "".join(["a" for i in range(POSIX_MAX_FNAME_LENGTH + 1 - len(extension))]) + extension
    assert len(fname) > POSIX_MAX_FNAME_LENGTH

    obj_without_fname_len_control = FSSpecTarget(LocalFileSystem(), tmp_path)
    _, uncontrolled_fname = os.path.split(obj_without_fname_len_control._full_path(fname))
    assert len(uncontrolled_fname) > POSIX_MAX_FNAME_LENGTH
    expected_error_msg = (
        f"[Errno 63] File name too long: '{obj_without_fname_len_control._full_path(fname)}'"
    )
    with pytest.raises(OSError, match=re.escape(expected_error_msg)):
        with obj_without_fname_len_control.open(fname, mode="w"):
            pass

    cache_with_fname_len_control = CacheFSSpecTarget(LocalFileSystem(), tmp_path)
    _, controlled_fname = os.path.split(cache_with_fname_len_control._full_path(fname))
    # assert len(controlled_fname) == POSIX_MAX_FNAME_LENGTH
    _, actual_extension = os.path.splitext(cache_with_fname_len_control._full_path(fname))
    assert actual_extension == extension
    expected_prefix = hashlib.md5(fname.encode()).hexdigest()
    assert controlled_fname.startswith(expected_prefix)
    with cache_with_fname_len_control.open(fname, mode="w"):
        pass
