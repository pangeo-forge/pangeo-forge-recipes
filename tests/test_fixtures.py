import fsspec
import xarray as xr

from pangeo_forge_recipes.utils import fix_scalar_attr_encoding


def test_fixture_local_files(daily_xarray_dataset, netcdf_paths):
    paths = netcdf_paths[0]
    paths = [str(path) for path in paths]
    combine = "nested" if len(netcdf_paths) == 2 else "by_coords"
    ds = xr.open_mfdataset(paths, combine=combine, concat_dim="time", engine="h5netcdf")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_paths):
    urls = netcdf_http_paths[0]
    open_files = [fsspec.open(url).open() for url in urls]
    combine = "nested" if len(netcdf_http_paths) == 2 else "by_coords"
    ds = xr.open_mfdataset(open_files, combine=combine, concat_dim="time", engine="h5netcdf").load()
    ds = fix_scalar_attr_encoding(ds)  # necessary?
    assert ds.identical(daily_xarray_dataset)
