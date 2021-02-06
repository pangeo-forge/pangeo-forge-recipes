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
def test_NetCDFtoZarrSequentialRecipe(
    daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache, process_input, process_chunk
):

    # the same recipe is created as a fixture in conftest.py
    # I left it here explicitly because it makes the test easier to read.
    paths, items_per_file = netcdf_local_paths
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=paths,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=items_per_file,
        target=tmp_target,
        input_cache=tmp_cache,
        process_input=process_input,
        process_chunk=process_chunk,
    )

    _manually_execute_recipe(r)

    ds_target = xr.open_zarr(tmp_target.get_mapper(), consolidated=True).load()
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

    print(ds_target)
    print(ds_expected)

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


@pytest.fixture
def test_NetCDFtoZarrMultiVarSequentialRecipe(
    daily_xarray_dataset, netcdf_local_paths_by_variable, tmp_target, tmp_cache
):
    paths, items_per_file, fnames_by_variable, path_format = netcdf_local_paths_by_variable
    pattern = VariableSequencePattern(
        path_format, keys={"variable": ["foo", "bar"], "n": list(range(len(paths) / 2))}
    )
    _ = recipe.NetCDFtoZarrMultiVarSequentialRecipe(
        input_pattern=pattern,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=items_per_file,
        target=tmp_target,
        input_cache=tmp_cache,
    )
