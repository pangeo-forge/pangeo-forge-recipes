from contextlib import nullcontext as does_not_raise
from dataclasses import replace
from unittest.mock import patch

import aiohttp
import pytest
import requests
import xarray as xr

# need to import this way (rather than use pytest.lazy_fixture) to make it work with dask
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.recipes.base import BaseRecipe
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe


@pytest.fixture
def netCDFtoZarr_recipe(
    daily_xarray_dataset, netcdf_local_file_pattern, tmp_target, tmp_cache, tmp_metadata_target
):
    kwargs = dict(
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return XarrayZarrRecipe, netcdf_local_file_pattern, kwargs, daily_xarray_dataset, tmp_target


@pytest.fixture
def netCDFtoZarr_http_recipe(
    daily_xarray_dataset, netcdf_http_file_pattern, tmp_target, tmp_cache, tmp_metadata_target
):
    kwargs = dict(
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return XarrayZarrRecipe, netcdf_http_file_pattern, kwargs, daily_xarray_dataset, tmp_target


@pytest.fixture
def netCDFtoZarr_subset_recipe(
    daily_xarray_dataset, netcdf_local_file_pattern, tmp_target, tmp_cache, tmp_metadata_target
):
    items_per_file = netcdf_local_file_pattern.nitems_per_input.get("time", None)
    if items_per_file != 2:
        pytest.skip("This recipe only makes sense with items_per_file == 2.")

    kwargs = dict(
        subset_inputs={"time": 2},
        inputs_per_chunk=1,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=tmp_metadata_target,
    )
    return XarrayZarrRecipe, netcdf_local_file_pattern, kwargs, daily_xarray_dataset, tmp_target


all_recipes = [
    lazy_fixture("netCDFtoZarr_recipe"),
    lazy_fixture("netCDFtoZarr_subset_recipe"),
]

recipes_no_subset = [
    lazy_fixture("netCDFtoZarr_recipe"),
]


def test_to_pipelines_warns(netCDFtoZarr_recipe):
    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_recipe

    rec = RecipeClass(file_pattern, **kwargs)
    with pytest.warns(FutureWarning):
        rec.to_pipelines()


@pytest.mark.parametrize("recipe_fixture", all_recipes)
def test_recipe(recipe_fixture, execute_recipe):
    """The basic recipe test. Use this as a template for other tests."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture
    rec = RecipeClass(file_pattern, **kwargs)
    execute_recipe(rec)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    xr.testing.assert_identical(ds_actual, ds_expected)

    with rec.open_input(next(rec.iter_inputs())):
        pass

    with rec.open_chunk(next(rec.iter_chunks())):
        pass


@pytest.mark.parametrize("recipe_fixture", all_recipes)
@pytest.mark.parametrize("nkeep", [1, 2])
def test_prune_recipe(recipe_fixture, execute_recipe, nkeep):
    """Check that recipe.copy_pruned works as expected."""

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
    netCDFtoZarr_recipe, execute_recipe, cache_inputs, copy_input_to_local_file
):
    """Test that caching and copying to local file work."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_recipe

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


@pytest.mark.parametrize("cache_inputs", [True, False])
@pytest.mark.parametrize("copy_input_to_local_file", [True, False])
def test_recipe_http_caching_copying(
    netCDFtoZarr_http_recipe, execute_recipe, cache_inputs, copy_input_to_local_file
):
    """Test that caching and copying from http to local file work."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_http_recipe

    first_url = list(file_pattern.items())[0][1]
    r = requests.get(first_url)
    if r.status_code == 401:
        fsspec_open_kwargs = dict(auth=aiohttp.BasicAuth("foo", "bar"))
        kwargs.update({"fsspec_open_kwargs": fsspec_open_kwargs})
    elif r.status_code == 400:
        query_string_secrets = {"foo": "foo", "bar": "bar"}
        kwargs.update({"query_string_secrets": query_string_secrets})

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


@pytest.mark.parametrize("inputs_per_chunk,subset_inputs", [(1, {}), (1, {"time": 2}), (2, {})])
@pytest.mark.parametrize(
    "target_chunks,specify_nitems_per_input,error_expectation",
    [
        ({}, True, does_not_raise()),
        ({"lon": 12}, True, does_not_raise()),
        ({"lon": 12, "time": 1}, True, does_not_raise()),
        ({"lon": 12, "time": 3}, True, does_not_raise()),
        ({"time": 10}, True, does_not_raise()),  # only one big chunk
        ({"lon": 12, "time": 1}, False, does_not_raise()),
        ({"lon": 12, "time": 3}, False, does_not_raise()),
        # can't determine target chunks for the next two because 'time' missing from target_chunks
        ({}, False, pytest.raises(ValueError)),
        ({"lon": 12}, False, pytest.raises(ValueError)),
    ],
)
@pytest.mark.parametrize("recipe_fixture", recipes_no_subset)
def test_chunks(
    recipe_fixture,
    execute_recipe,
    inputs_per_chunk,
    target_chunks,
    error_expectation,
    subset_inputs,
    specify_nitems_per_input,
):
    """Check that chunking of datasets works as expected."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture

    for cdim in file_pattern.combine_dims:
        if hasattr(cdim, "nitems_per_file"):
            nitems_per_file = cdim.nitems_per_file

    kwargs["target_chunks"] = target_chunks
    kwargs["inputs_per_chunk"] = inputs_per_chunk
    kwargs["subset_inputs"] = subset_inputs
    if specify_nitems_per_input:
        kwargs["metadata_cache"] = None
    else:
        # modify file_pattern in place to remove nitems_per_file; a bit hacky
        new_combine_dims = []
        for cdim in file_pattern.combine_dims:
            if hasattr(cdim, "nitems_per_file"):
                new_combine_dims.append(replace(cdim, nitems_per_file=None))
            else:
                new_combine_dims.append(cdim)
            file_pattern = FilePattern(file_pattern.format_function, *new_combine_dims)

    with error_expectation as excinfo:
        rec = RecipeClass(file_pattern, **kwargs)
    if excinfo:
        # don't continue if we got an exception
        return

    # we should get a runtime error if we try to subset by a factor of 2
    # when the file is only 1 item long
    subset_factor = kwargs.get("subset_inputs", {}).get("time", 1)
    if nitems_per_file == 1 and subset_factor > 1:
        subset_error_expectation = pytest.raises(ValueError)
    else:
        subset_error_expectation = does_not_raise()
    with subset_error_expectation as excinfo:
        # error is raised at execution stage because we don't generally know a priori how
        # many items in each file
        execute_recipe(rec)
    if excinfo:
        # don't continue if we got an exception
        return

    # chunk validation
    ds_actual = xr.open_zarr(target.get_mapper(), consolidated=True)
    sequence_chunks = ds_actual.chunks["time"]
    nitems_per_input = list(file_pattern.nitems_per_input.values())[0]
    seq_chunk_len = target_chunks.get("time", None) or (
        nitems_per_input * inputs_per_chunk // subset_factor
    )
    # we expect all chunks but the last to have the expected size
    assert all([item == seq_chunk_len for item in sequence_chunks[:-1]])
    for other_dim, chunk_len in target_chunks.items():
        if other_dim == "time":
            continue
        assert all([item == chunk_len for item in ds_actual.chunks[other_dim][:-1]])

    ds_actual.load()
    xr.testing.assert_identical(ds_actual, ds_expected)


def test_lock_timeout(netCDFtoZarr_recipe, execute_recipe):
    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_recipe

    # `netCDFtoZarr_recipe` fixture is parametrized. We don't need to run this test more than once.
    if len(file_pattern.merge_dims) != 0:
        pytest.skip("It's redundant to run this test more than once.")

    recipe = RecipeClass(file_pattern=file_pattern, lock_timeout=1, **kwargs)

    with patch("pangeo_forge_recipes.recipes.xarray_zarr.lock_for_conflicts") as p:
        execute_recipe(recipe)

    # We can only check that the mock object is called with the right parameters if
    # the function is called in the same processes as our mock object. We can't
    # observe anything that happens when the function is executed in subprocess, i.e.
    # if we're using a Dask executor.
    if execute_recipe.param in {"manual", "python", "prefect"}:
        assert p.call_args[1]["timeout"] == 1


class MyRecipe(BaseRecipe):
    def __init__(self) -> None:
        super().__init__()
        self.cache = {}
        self.target = None
        self.finalized = False
        self.cache_inputs = True

    @property
    def prepare_target(self):
        def _():
            self.target = {}

        return _

    @property
    def cache_input(self):
        def _(input_key):
            self.cache[input_key] = input_key

        return _

    @property
    def store_chunk(self):
        def _(chunk_key):
            self.target[chunk_key] = self.cache[chunk_key]

        return _

    @property
    def finalize_target(self):
        def _():
            self.finalized = True

        return _

    def iter_inputs(self):
        return iter(range(4))

    def iter_chunks(self):
        return iter(range(4))


def test_base_recipe():
    recipe = MyRecipe()
    recipe.to_function()()
    assert recipe.finalized
    assert recipe.target == {i: i for i in range(4)}

    import dask

    dask.config.set(scheduler="single-threaded")
    recipe = MyRecipe()
    recipe.to_dask().compute()
    assert recipe.finalized
    assert recipe.target == {i: i for i in range(4)}

    recipe = MyRecipe()
    recipe.to_prefect().run()
    assert recipe.finalized
    assert recipe.target == {i: i for i in range(4)}
