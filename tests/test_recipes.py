from contextlib import nullcontext as does_not_raise

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


@pytest.mark.parametrize("inputs_per_chunk", [1, 2])
@pytest.mark.parametrize(
    "target_chunks,specify_nitems_per_input,chunk_expectation",
    [
        ({}, True, does_not_raise()),
        ({"lon": 12}, True, does_not_raise()),
        ({"lon": 12, "time": 1}, True, does_not_raise()),
        ({"lon": 12, "time": 3}, True, does_not_raise()),
        ({"time": 100}, True, does_not_raise()),  # only one big chunk
        ({"lon": 12, "time": 1}, False, does_not_raise()),
        ({"lon": 12, "time": 3}, False, does_not_raise()),
        # can't determine target chunks for the next two because 'time' missing from target_chunks
        ({}, False, pytest.raises(ValueError)),
        ({"lon": 12}, False, pytest.raises(ValueError)),
    ],
)
@pytest.mark.parametrize("recipe_fixture", all_recipes)
def test_chunks(
    recipe_fixture,
    execute_recipe,
    inputs_per_chunk,
    target_chunks,
    chunk_expectation,
    specify_nitems_per_input,
):
    """Check that chunking of datasets works as expected."""

    RecipeClass, kwargs, ds_expected, target = recipe_fixture

    kwargs["target_chunks"] = target_chunks
    kwargs["inputs_per_chunk"] = inputs_per_chunk
    if specify_nitems_per_input:
        kwargs["nitems_per_input"] = kwargs["nitems_per_input"]  # it's already there in kwargs
        kwargs["metadata_cache"] = None
    else:
        # file will be scanned and metadata cached
        kwargs["nitems_per_input"] = None
        kwargs["metadata_cache"] = kwargs["input_cache"]

    with chunk_expectation as excinfo:
        rec = RecipeClass(**kwargs)
    if excinfo:
        # don't continue if we got an exception
        return

    execute_recipe(rec)

    # chunk validation
    ds_actual = xr.open_zarr(target.get_mapper(), consolidated=True)
    sequence_chunks = ds_actual.chunks["time"]
    seq_chunk_len = target_chunks.get("time", None) or (
        kwargs["nitems_per_input"] * inputs_per_chunk
    )
    # we expect all chunks but the last to have the expected size
    assert all([item == seq_chunk_len for item in sequence_chunks[:-1]])
    for other_dim, chunk_len in target_chunks.items():
        if other_dim == "time":
            continue
        assert all([item == chunk_len for item in ds_actual.chunks[other_dim][:-1]])

    ds_actual.load()
    print(ds_actual)
    xr.testing.assert_identical(ds_actual, ds_expected)
