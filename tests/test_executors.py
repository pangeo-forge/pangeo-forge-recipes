import pytest
import xarray as xr


@pytest.mark.parametrize(
    "recipe_fixture",
    [
        pytest.lazy_fixture("netCDFtoZarr_sequential_recipe"),
        pytest.lazy_fixture("netCDFtoZarr_sequential_multi_variable_recipe"),
    ],
)
def test_recipe_w_executor(recipe_fixture, execute_recipe):
    RecipeClass, kwargs, ds_expected, target = recipe_fixture
    rec = RecipeClass(**kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    assert ds_actual.identical(ds_expected)
