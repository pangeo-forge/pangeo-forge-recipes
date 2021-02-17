from contextlib import nullcontext as does_not_raise

import aiohttp
import pytest
import xarray as xr

from pangeo_forge import recipe
from pangeo_forge.patterns import VariableSequencePattern
from pangeo_forge.storage import UninitializedTargetError

dummy_fnames = ["a.nc", "b.nc", "c.nc"]


def incr_date(ds, filename=""):
    # add one day
    t = [d + int(24 * 3600e9) for d in ds.time.values]
    ds = ds.assign_coords(time=t)
    return ds


def _manually_execute_recipe(r):
    for input_key in r.iter_inputs():
        r.cache_input(input_key)
    r.prepare_target()
    for chunk_key in r.iter_chunks():
        r.store_chunk(chunk_key)
    r.finalize_target()


@pytest.mark.parametrize(
    "username, password", [("foo", "bar"), ("foo", "wrong"),],  # noqa: E231
)
def test_NetCDFtoZarrSequentialRecipeHttpAuth(
    daily_xarray_dataset, netcdf_http_server, tmp_target, tmp_cache, username, password
):

    url, fnames, items_per_file = netcdf_http_server("foo", "bar")
    urls = [f"{url}/{fname}" for fname in fnames]
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=urls,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=items_per_file,
        target=tmp_target,
        input_cache=tmp_cache,
        fsspec_open_kwargs={"client_kwargs": {"auth": aiohttp.BasicAuth(username, password)}},
    )

    if password == "wrong":
        with pytest.raises(aiohttp.client_exceptions.ClientResponseError):
            r.cache_input(next(r.iter_inputs()))
    else:
        _manually_execute_recipe(r)

        ds_target = xr.open_zarr(tmp_target.get_mapper(), consolidated=True).load()
        ds_expected = daily_xarray_dataset.compute()
        assert ds_target.identical(ds_expected)


@pytest.mark.parametrize(
    "process_input, process_chunk",
    [(None, None), (incr_date, None), (None, incr_date), (incr_date, incr_date)],
)
@pytest.mark.parametrize("inputs_per_chunk", [1, 2])
@pytest.mark.parametrize(
    "target_chunks,specify_nitems_per_input,chunk_expectation",
    [
        ({}, True, does_not_raise()),
        ({"lon": 12}, True, does_not_raise()),
        ({"lon": 12, "time": 1}, True, does_not_raise()),
        (
            {"lon": 12, "time": 3},
            True,
            does_not_raise(),
        ),  # currently failing because we don't handle last chunk properly
        ({}, False, pytest.raises(ValueError)),  # can't determine target chunks
        ({"lon": 12}, False, pytest.raises(ValueError)),  # can't determine target_chunks
        ({"lon": 12, "time": 1}, False, does_not_raise()),
        (
            {"lon": 12, "time": 3},
            False,
            does_not_raise(),
        ),  # currently failing because we don't handle last chunk properly
    ],
)
def test_NetCDFtoZarrSequentialRecipe_options(
    daily_xarray_dataset,
    netcdf_local_paths,
    tmp_target,
    tmp_cache,
    process_input,
    process_chunk,
    inputs_per_chunk,
    target_chunks,
    chunk_expectation,
    specify_nitems_per_input,
):

    # the same recipe is created as a fixture in conftest.py
    # I left it here explicitly because it makes the test easier to read.
    paths, items_per_file = netcdf_local_paths
    if specify_nitems_per_input:
        nitems_per_input = items_per_file
        metadata_cache = None
    else:
        # file will be scanned and metadata cached
        nitems_per_input = None
        metadata_cache = tmp_cache
    with chunk_expectation as excinfo:
        r = recipe.NetCDFtoZarrSequentialRecipe(
            input_urls=paths,
            sequence_dim="time",
            inputs_per_chunk=inputs_per_chunk,
            nitems_per_input=nitems_per_input,
            target=tmp_target,
            input_cache=tmp_cache,
            metadata_cache=metadata_cache,
            process_input=process_input,
            process_chunk=process_chunk,
            target_chunks=target_chunks,
        )
    if excinfo:
        # don't continue if we got an exception
        return

    _manually_execute_recipe(r)

    ds_target = xr.open_zarr(tmp_target.get_mapper(), consolidated=True)

    # chunk validation
    sequence_chunks = ds_target.chunks["time"]
    seq_chunk_len = target_chunks.get("time", None) or (items_per_file * inputs_per_chunk)
    # we expect all chunks but the last to have the expected size
    assert all([item == seq_chunk_len for item in sequence_chunks[:-1]])
    for other_dim, chunk_len in target_chunks.items():
        if other_dim == "time":
            continue
        assert all([item == chunk_len for item in ds_target.chunks[other_dim][:-1]])

    ds_target.load()
    ds_expected = daily_xarray_dataset.compute()

    if process_input is not None:
        # check that the process_input hook made some changes
        assert not ds_target.identical(ds_expected)
        # apply these changes to the expected dataset
        ds_expected = process_input(ds_expected)
    if process_chunk is not None:
        # check that the process_chunk hook made some changes
        assert not ds_target.identical(ds_expected)
        # apply these changes to the expected dataset
        ds_expected = process_chunk(ds_expected)

    assert ds_target.identical(ds_expected)


def test_NetCDFtoZarrSequentialRecipeNoTarget(
    daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache
):

    paths, items_per_file = netcdf_local_paths
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=paths, sequence_dim="time", inputs_per_chunk=1, nitems_per_input=items_per_file,
    )

    with pytest.raises(UninitializedTargetError):
        r.cache_input(next(r.iter_inputs()))


@pytest.mark.parametrize("specify_nitems_per_input", [True, False])
def test_NetCDFtoZarrMultiVarSequentialRecipe(
    daily_xarray_dataset,
    netcdf_local_paths_by_variable,
    tmp_target,
    tmp_cache,
    specify_nitems_per_input,
):
    paths, items_per_file, fnames_by_variable, path_format = netcdf_local_paths_by_variable
    if specify_nitems_per_input:
        nitems_per_input = items_per_file
        metadata_cache = None
    else:
        # file will be scanned and metadata cached
        nitems_per_input = None
        metadata_cache = tmp_cache
    pattern = VariableSequencePattern(
        path_format, keys={"variable": ["foo", "bar"], "n": list(range(len(paths) // 2))}
    )
    r = recipe.NetCDFtoZarrMultiVarSequentialRecipe(
        input_pattern=pattern,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=nitems_per_input,
        target=tmp_target,
        input_cache=tmp_cache,
        metadata_cache=metadata_cache,
    )
    _manually_execute_recipe(r)

    ds_target = xr.open_zarr(tmp_target.get_mapper(), consolidated=True).compute()
    assert ds_target.identical(daily_xarray_dataset)


# WIP
# _recipes = {"test_NetCDFtoZarrSequentialRecipe": {}, "NetCDFtoZarrMultiVarSequentialRecipe": {}}
#
#
# @pytest.fixture(params=[list(_recipes)])
# def recipe_class_and_kwargs(request):
#     Recipe = getattr(pangeo_forge.recipe, request.param)
#     recipe_kwargs = _recipes[request.param]
#     if request.param ==
#     return Recipe, recipe_kwargs
#
#
# def test_recipe_options(recipe_class_and_kwargs):
#     Recipe, recipe_kwargs = recipe_class_and_kwargs
#     r = Recipe(**recipe_kwargs)
