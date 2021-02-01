import fsspec
import pytest
import xarray as xr

from pangeo_forge.storage import UninitializedTargetError
from pangeo_forge.utils import fix_scalar_attr_encoding


def test_fixture_local_files(daily_xarray_dataset, netcdf_local_paths):
    paths, items_per_file = netcdf_local_paths
    paths = [str(path) for path in paths]
    ds = xr.open_mfdataset(paths, combine="nested", concat_dim="time")
    assert ds.identical(daily_xarray_dataset)


# TODO: this is quite repetetive of the test above. Replace with parametrization.
def test_fixture_local_files_by_variable(daily_xarray_dataset, netcdf_local_paths_by_variable):
    paths = [str(path) for path in netcdf_local_paths_by_variable]
    ds = xr.open_mfdataset(paths, combine="by_coords", concat_dim="time")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_server):
    url, paths, items_per_file = netcdf_http_server()
    urls = ["/".join([url, str(path)]) for path in paths]
    open_files = [fsspec.open(url).open() for url in urls]
    ds = xr.open_mfdataset(open_files, combine="nested", concat_dim="time").load()
    ds = fix_scalar_attr_encoding(ds)
    assert ds.identical(daily_xarray_dataset)


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


def test_uninitialized_target(uninitialized_target):
    target = uninitialized_target
    with pytest.raises(UninitializedTargetError):
        target.get_mapper()
    with pytest.raises(UninitializedTargetError):
        target.exists("foo")
    with pytest.raises(UninitializedTargetError):
        target.rm("foo")
    with pytest.raises(UninitializedTargetError):
        with target.open("foo"):
            pass


def test_cache(tmp_cache):
    assert not tmp_cache.exists("foo")
    with tmp_cache.open("foo", mode="w") as f:
        f.write("bar")
    assert tmp_cache.exists("foo")
    with tmp_cache.open("foo", mode="r") as f:
        assert f.read() == "bar"
    tmp_cache.rm("foo")
    assert not tmp_cache.exists("foo")
