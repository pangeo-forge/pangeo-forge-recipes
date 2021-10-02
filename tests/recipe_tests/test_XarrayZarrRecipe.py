from contextlib import nullcontext as does_not_raise
from dataclasses import replace
from unittest.mock import patch

import dask.core
import pytest
import xarray as xr
import zarr

# need to import this way (rather than use pytest.lazy_fixture) to make it work with dask
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.base import BaseRecipe
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe


def make_netCDFtoZarr_recipe(
    file_pattern, xarray_dataset, target, cache, metadata_target, extra_kwargs=None
):
    kwargs = dict(
        inputs_per_chunk=1, target=target, input_cache=cache, metadata_cache=metadata_target,
    )
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    return XarrayZarrRecipe, file_pattern, kwargs, xarray_dataset, target


@pytest.fixture
def netCDFtoZarr_recipe_sequential_only(
    netcdf_local_file_pattern_sequential,
    daily_xarray_dataset,
    tmp_target,
    tmp_cache,
    tmp_metadata_target,
):
    return make_netCDFtoZarr_recipe(
        netcdf_local_file_pattern_sequential,
        daily_xarray_dataset,
        tmp_target,
        tmp_cache,
        tmp_metadata_target,
    )


@pytest.fixture
def netCDFtoZarr_recipe(
    netcdf_local_file_pattern, daily_xarray_dataset, tmp_target, tmp_cache, tmp_metadata_target
):
    return make_netCDFtoZarr_recipe(
        netcdf_local_file_pattern, daily_xarray_dataset, tmp_target, tmp_cache, tmp_metadata_target
    )


@pytest.fixture
def netCDFtoZarr_http_recipe(
    netcdf_http_file_pattern, daily_xarray_dataset, tmp_target, tmp_cache, tmp_metadata_target
):
    return make_netCDFtoZarr_recipe(
        netcdf_http_file_pattern, daily_xarray_dataset, tmp_target, tmp_cache, tmp_metadata_target
    )


@pytest.fixture
def netCDFtoZarr_http_recipe_sequential_1d(
    netcdf_http_file_pattern_sequential_1d,
    daily_xarray_dataset,
    tmp_target,
    tmp_cache,
    tmp_metadata_target,
):
    return make_netCDFtoZarr_recipe(
        netcdf_http_file_pattern_sequential_1d,
        daily_xarray_dataset,
        tmp_target,
        tmp_cache,
        tmp_metadata_target,
    )


@pytest.fixture
def netCDFtoZarr_subset_recipe(
    netcdf_local_file_pattern, daily_xarray_dataset, tmp_target, tmp_cache, tmp_metadata_target
):
    items_per_file = netcdf_local_file_pattern.nitems_per_input.get("time", None)
    if items_per_file != 2:
        pytest.skip("This recipe only makes sense with items_per_file == 2.")

    extra_kwargs = dict(subset_inputs={"time": 2})

    return make_netCDFtoZarr_recipe(
        netcdf_local_file_pattern,
        daily_xarray_dataset,
        tmp_target,
        tmp_cache,
        tmp_metadata_target,
        extra_kwargs,
    )


all_recipes = [
    lazy_fixture("netCDFtoZarr_recipe"),
    lazy_fixture("netCDFtoZarr_subset_recipe"),
]

recipes_no_subset = [
    lazy_fixture("netCDFtoZarr_recipe"),
]


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
@pytest.mark.parametrize(
    "recipe",
    [
        lazy_fixture("netCDFtoZarr_recipe_sequential_only"),
        lazy_fixture("netCDFtoZarr_http_recipe_sequential_1d"),
    ],
)
def test_recipe_caching_copying(recipe, execute_recipe, cache_inputs, copy_input_to_local_file):
    """Test that caching and copying to local file work."""

    RecipeClass, file_pattern, kwargs, ds_expected, target = recipe

    if not cache_inputs:
        kwargs.pop("input_cache")  # make sure recipe doesn't require input_cache
    rec = RecipeClass(
        file_pattern,
        **kwargs,
        cache_inputs=cache_inputs,
        copy_input_to_local_file=copy_input_to_local_file,
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


def do_actual_chunks_test(
    recipe_fixture,
    executor,
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
        executor(rec)
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
    store = zarr.open_consolidated(target.get_mapper())
    for dim in ds_actual.dims:
        assert store[dim].chunks == ds_actual[dim].shape

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
    execute_recipe_python,
    inputs_per_chunk,
    target_chunks,
    error_expectation,
    subset_inputs,
    specify_nitems_per_input,
):
    do_actual_chunks_test(
        recipe_fixture,
        execute_recipe_python,
        inputs_per_chunk,
        target_chunks,
        error_expectation,
        subset_inputs,
        specify_nitems_per_input,
    )


@pytest.mark.parametrize("inputs_per_chunk,subset_inputs", [(1, {}), (1, {"time": 2}), (2, {})])
@pytest.mark.parametrize(
    "target_chunks,specify_nitems_per_input,error_expectation",
    [({"lon": 12, "time": 3}, False, does_not_raise())],
)
@pytest.mark.parametrize("recipe_fixture", recipes_no_subset)
def test_chunks_distributed_locking(
    recipe_fixture,
    execute_recipe_with_dask,
    inputs_per_chunk,
    target_chunks,
    error_expectation,
    subset_inputs,
    specify_nitems_per_input,
):
    do_actual_chunks_test(
        recipe_fixture,
        execute_recipe_with_dask,
        inputs_per_chunk,
        target_chunks,
        error_expectation,
        subset_inputs,
        specify_nitems_per_input,
    )


def test_no_consolidate_dimension_coordinates(netCDFtoZarr_recipe):
    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_recipe

    rec = RecipeClass(file_pattern, **kwargs)
    rec.consolidate_dimension_coordinates = False
    rec.to_function()()
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    xr.testing.assert_identical(ds_actual, ds_expected)

    store = zarr.open_consolidated(target.get_mapper())
    assert store["time"].chunks == (file_pattern.nitems_per_input["time"],)


def test_consolidate_dimension_coordinates_with_coordinateless_dimension(
    netcdf_local_file_pattern_sequential_with_coordinateless_dimension,
    daily_xarray_dataset_with_coordinateless_dimension,
    tmp_target,
    tmp_cache,
    tmp_metadata_target,
):
    RecipeClass, file_pattern, kwargs, ds_expected, target = make_netCDFtoZarr_recipe(
        netcdf_local_file_pattern_sequential_with_coordinateless_dimension,
        daily_xarray_dataset_with_coordinateless_dimension,
        tmp_target,
        tmp_cache,
        tmp_metadata_target,
    )
    rec = RecipeClass(file_pattern, **kwargs)
    rec.to_function()()
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    xr.testing.assert_identical(ds_actual, ds_expected)


def test_lock_timeout(netCDFtoZarr_recipe_sequential_only, execute_recipe_no_dask):
    RecipeClass, file_pattern, kwargs, ds_expected, target = netCDFtoZarr_recipe_sequential_only

    recipe = RecipeClass(file_pattern=file_pattern, lock_timeout=1, **kwargs)

    with patch("pangeo_forge_recipes.recipes.xarray_zarr.lock_for_conflicts") as p:
        # We can only check that the mock object is called with the right parameters if
        # the function is called in the same processes as our mock object. We can't
        # observe anything that happens when the function is executed in subprocess, i.e.
        # if we're using a Dask executor.
        execute_recipe_no_dask(recipe)

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


def _make_filename_for_memory_usage_test(time):
    import pandas as pd

    input_url_pattern = (
        "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata/{yyyy}/{mm}/{dd}/"
        "imerg/3B-HHR.MS.MRG.3IMERG.{yyyymmdd}-S{sh}{sm}00-E{eh}{em}59.{MMMM}.V06B.HDF5"
    ).format(
        yyyy=time.strftime("%Y"),
        mm=time.strftime("%m"),
        dd=time.strftime("%d"),
        yyyymmdd=time.strftime("%Y%m%d"),
        sh=time.strftime("%H"),
        sm=time.strftime("%M"),
        eh=time.strftime("%H"),
        em=(time + pd.Timedelta("29 min")).strftime("%M"),
        MMMM=f"{(time.hour*60 + time.minute):04}",
    )
    return input_url_pattern


def _simple_func(*args, **kwargs):
    return None


@pytest.mark.timeout(90)
@pytest.mark.filterwarnings("ignore:Large object")
def test_memory_usage():
    # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/151
    # Requires >4 GiB of memory to run.
    pd = pytest.importorskip("pandas")
    distributed = pytest.importorskip("distributed")

    dates = pd.date_range("2020-05-31T00:00:00", "2021-05-31T23:59:59", freq="30min")
    time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
    pattern = FilePattern(_make_filename_for_memory_usage_test, time_concat_dim,)

    recipe = XarrayZarrRecipe(
        pattern, xarray_open_kwargs={"group": "Grid", "decode_coords": "all"}, inputs_per_chunk=1,
    )

    obj = recipe.to_dask()
    dsk = obj.dask

    for k, v in dsk.items():
        if dask.core.istask(v):
            _, *args = v
            dsk[k] = (_simple_func,) + tuple(args)

    with dask.config.set(
        **{"distributed.worker.memory.pause": 0.95, "distributed.worker.memory.terminate": 0.9}
    ):
        with distributed.Client(n_workers=1, threads_per_worker=1, memory_limit="4G") as client:
            print("submitting")
            client.get(dsk, [obj.key])
