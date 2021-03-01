import pytest
import xarray as xr

# need to import this way (rather than use pytest.lazy_fixture) to make it work with dask
from pytest_lazyfixture import lazy_fixture

all_recipes = [
    lazy_fixture("netCDFtoZarr_sequential_recipe"),
    lazy_fixture("netCDFtoZarr_sequential_multi_variable_recipe"),
]


@pytest.mark.parametrize("recipe_fixture", all_recipes)
def test_recipe(recipe_fixture, execute_recipe):
    """The basic recipe test. Use this as a template for other tests."""

    RecipeClass, kwargs, ds_expected, target = recipe_fixture
    rec = RecipeClass(**kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    xr.testing.assert_identical(ds_actual, ds_expected)


# function passed to preprocessing
def incr_date(ds, filename=""):
    # add one day
    t = [d + int(24 * 3600e9) for d in ds.time.values]
    ds = ds.assign_coords(time=t)
    return ds


@pytest.mark.parametrize(
    "process_input, process_chunk",
    [(None, None), (incr_date, None), (None, incr_date), (incr_date, incr_date)],
)
@pytest.mark.parametrize("recipe_fixture", all_recipes)
def test_process(recipe_fixture, execute_recipe, process_input, process_chunk):
    """Check that the process_chunk and process_input arguments work as expected."""

    RecipeClass, kwargs, ds_expected, target = recipe_fixture
    kwargs["process_input"] = process_input
    kwargs["process_chunk"] = process_chunk
    rec = RecipeClass(**kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()

    if process_input and process_chunk:
        assert not ds_actual.identical(ds_expected)
        ds_expected = incr_date(incr_date(ds_expected))
    elif process_input or process_chunk:
        assert not ds_actual.identical(ds_expected)
        ds_expected = incr_date(ds_expected)

    xr.testing.assert_identical(ds_actual, ds_expected)
