import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.utils import fix_scalar_attr_encoding


def test_fixture_local_files(daily_xarray_dataset, netcdf_local_paths):
    paths = netcdf_local_paths[0]
    paths = [str(path) for path in paths]
    ds = xr.open_mfdataset(paths, combine="by_coords", concat_dim="time", engine="h5netcdf")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_paths):
    urls = netcdf_http_paths[0]
    if (
        "auth" in netcdf_http_paths[-1]["fsspec_open_kwargs"].keys()
        or netcdf_http_paths[-1]["query_string_secrets"].keys()
    ):
        pytest.skip("Authentication and required query strings are tested in test_storage.py")
    open_files = [fsspec.open(url).open() for url in urls]
    ds = xr.open_mfdataset(
        open_files, combine="by_coords", concat_dim="time", engine="h5netcdf"
    ).load()
    ds = fix_scalar_attr_encoding(ds)  # necessary?
    assert ds.identical(daily_xarray_dataset)
