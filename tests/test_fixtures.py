import fsspec
import xarray as xr

from pangeo_forge_recipes.storage import _add_query_string_secrets
from pangeo_forge_recipes.utils import fix_scalar_attr_encoding


def test_fixture_local_files(daily_xarray_dataset, netcdf_local_paths):
    paths = netcdf_local_paths[0]
    paths = [str(path) for path in paths]
    ds = xr.open_mfdataset(paths, combine="by_coords", concat_dim="time", engine="h5netcdf")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_paths):
    urls = netcdf_http_paths[0]
    open_kwargs = netcdf_http_paths[-1]["fsspec_open_kwargs"]
    secrets = netcdf_http_paths[-1]["query_string_secrets"]
    if secrets:
        urls = [_add_query_string_secrets(url, secrets) for url in urls]
    open_files = [fsspec.open(url, **open_kwargs).open() for url in urls]
    ds = xr.open_mfdataset(
        open_files, combine="by_coords", concat_dim="time", engine="h5netcdf"
    ).load()
    ds = fix_scalar_attr_encoding(ds)  # necessary?
    assert ds.identical(daily_xarray_dataset)
