import xarray as xr


def test_recipe_w_executor(netCDFtoZarr_sequential_recipe, execute_recipe):
    Recipe, kwargs, ds_expected, target = netCDFtoZarr_sequential_recipe
    rec = Recipe(**kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    assert ds_actual.identical(ds_expected)
