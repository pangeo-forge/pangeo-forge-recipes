import fsspec
import xarray as xr

from pangeo_forge_recipes.utils import fix_scalar_attr_encoding


def test_fixture_local_files(daily_xarray_dataset, netcdf_local_paths):
    paths, items_per_file = netcdf_local_paths
    paths = [str(path) for path in paths]
    ds = xr.open_mfdataset(paths, combine="nested", concat_dim="time",
                           engine="h5netcdf")
    assert ds.identical(daily_xarray_dataset)


# TODO: this is quite repetetive of the test above. Replace with parametrization.
def test_fixture_local_files_by_variable(daily_xarray_dataset, netcdf_local_paths_by_variable):
    paths, items_per_file, fnames_by_variable, path_format = netcdf_local_paths_by_variable
    paths = [str(path) for path in paths]
    ds = xr.open_mfdataset(paths, combine="by_coords", concat_dim="time",
                           engine="h5netcdf")
    assert ds.identical(daily_xarray_dataset)


def test_fixture_http_files(daily_xarray_dataset, netcdf_http_paths):
    urls, items_per_file = netcdf_http_paths
    open_files = [fsspec.open(url).open() for url in urls]
    ds = xr.open_mfdataset(open_files, combine="nested", concat_dim="time",
                           engine="h5netcdf").load()
    ds = fix_scalar_attr_encoding(ds)
    assert ds.identical(daily_xarray_dataset)
