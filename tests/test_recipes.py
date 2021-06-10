from contextlib import nullcontext as does_not_raise
from unittest.mock import patch

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

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture
    rec = RecipeClass(file_pattern, **kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    xr.testing.assert_identical(ds_actual, ds_expected)


@pytest.mark.parametrize("recipe_fixture", all_recipes)
@pytest.mark.parametrize("nkeep", [1, 2])
def test_prune_recipe(recipe_fixture, execute_recipe, nkeep):
    """The basic recipe test. Use this as a template for other tests."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture
    rec = RecipeClass(file_pattern, **kwargs)
    rec_pruned = rec.copy_pruned(nkeep=nkeep)
    assert len(list(rec.iter_inputs())) > len(list(rec_pruned.iter_inputs()))
    execute_recipe(rec_pruned)
    ds_pruned = xr.open_zarr(target.get_mapper()).load()
    nitems_per_input = list(file_pattern.nitems_per_input.values())[0]
    assert ds_pruned.dims["time"] == nkeep * nitems_per_input


@pytest.mark.parametrize("cache_inputs", [True, False])
@pytest.mark.parametrize("copy_input_to_local_file", [True, False])
def test_recipe_caching_copying(
    netCDFtoZarr_sequential_recipe, execute_recipe, cache_inputs, copy_input_to_local_file
):
    """The basic recipe test. Use this as a template for other tests."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_sequential_recipe
    if not cache_inputs:
        kwargs.pop("input_cache")  # make sure recipe doesn't require input_cache
    rec = RecipeClass(
        file_pattern,
        **kwargs,
        cache_inputs=cache_inputs,
        copy_input_to_local_file=copy_input_to_local_file
    )
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

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture
    kwargs["process_input"] = process_input
    kwargs["process_chunk"] = process_chunk
    rec = RecipeClass(file_pattern, **kwargs)
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

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture

    kwargs["target_chunks"] = target_chunks
    kwargs["inputs_per_chunk"] = inputs_per_chunk
    if specify_nitems_per_input:
        kwargs["metadata_cache"] = None
    else:
        # modify file_pattern in place to remove nitems_per_file; a bit hacky
        for cdim in file_pattern.combine_dims:
            if hasattr(cdim, "nitems_per_file"):
                cdim.nitems_per_file = None

    with chunk_expectation as excinfo:
        rec = RecipeClass(file_pattern, **kwargs)
    if excinfo:
        # don't continue if we got an exception
        return

    execute_recipe(rec)

    # chunk validation
    ds_actual = xr.open_zarr(target.get_mapper(), consolidated=True)
    sequence_chunks = ds_actual.chunks["time"]
    nitems_per_input = list(file_pattern.nitems_per_input.values())[0]
    seq_chunk_len = target_chunks.get("time", None) or (nitems_per_input * inputs_per_chunk)
    # we expect all chunks but the last to have the expected size
    assert all([item == seq_chunk_len for item in sequence_chunks[:-1]])
    for other_dim, chunk_len in target_chunks.items():
        if other_dim == "time":
            continue
        assert all([item == chunk_len for item in ds_actual.chunks[other_dim][:-1]])

    ds_actual.load()
    print(ds_actual)
    xr.testing.assert_identical(ds_actual, ds_expected)


def test_lock_timeout(netCDFtoZarr_sequential_recipe, execute_recipe):
    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_sequential_recipe
    recipe = RecipeClass(file_pattern=file_pattern, lock_timeout=1, **kwargs)

    with patch("pangeo_forge_recipes.recipes.xarray_zarr.lock_for_conflicts") as p:
        execute_recipe(recipe)

    # We can only check that the mock object is called with the right parameters if
    # the function is called in the same processes as our mock object. We can't
    # observe anything that happens when the function is executed in subprocess, i.e.
    # if we're using a Dask executor.
    if execute_recipe.param in {"manual", "python", "prefect"}:
        assert p.call_args[1]["timeout"] == 1
