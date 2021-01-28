import aiohttp
import pytest
import xarray as xr

from pangeo_forge import recipe
from pangeo_forge.storage import UninitializedTargetError

dummy_fnames = ["a.nc", "b.nc", "c.nc"]


def incr_date(ds, filename=""):
    # add one day
    t = [d + int(24 * 3600e9) for d in ds.time.values]
    ds = ds.assign_coords(time=t)
    return ds


@pytest.mark.skip(reason="Removed this class for now")
@pytest.mark.parametrize(
    "file_urls, files_per_chunk, expected_keys, expected_filenames",
    [
        (dummy_fnames, 1, [0, 1, 2], [("a.nc",), ("b.nc",), ("c.nc",)]),
        (dummy_fnames, 2, [0, 1], [("a.nc", "b.nc",), ("c.nc",),],),  # noqa: E231
    ],
)
def test_sequence_recipe(file_urls, files_per_chunk, expected_keys, expected_filenames, tmp_target):

    r = recipe.SequenceRecipe(
        input_urls=file_urls,
        sequence_dim="time",
        inputs_per_chunk=files_per_chunk,
        target=tmp_target,
        chunk_preprocess_funcs=[],
    )

    assert r.sequence_len() == len(file_urls)

    chunk_keys = list(r.iter_chunks())
    assert chunk_keys == expected_keys

    for k, expected in zip(r.iter_chunks(), expected_filenames):
        fnames = r.inputs_for_chunk(k)
        assert fnames == expected


@pytest.mark.parametrize(
    "username, password", [("foo", "bar"), ("foo", "wrong"),],  # noqa: E231
)
def test_NetCDFtoZarrSequentialRecipeHttpAuth(
    daily_xarray_dataset, netcdf_http_server, tmp_target, tmp_cache, username, password
):

    url, fnames = netcdf_http_server("foo", "bar")
    urls = [f"{url}/{fname}" for fname in fnames]
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=urls,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=daily_xarray_dataset.attrs["items_per_file"],
        target=tmp_target,
        input_cache=tmp_cache,
        fsspec_open_kwargs={"client_kwargs": {"auth": aiohttp.BasicAuth(username, password)}},
    )

    if password == "wrong":
        with pytest.raises(aiohttp.client_exceptions.ClientResponseError):
            r.cache_input(next(r.iter_inputs()))
    else:
        # this is the cannonical way to manually execute a recipe
        for input_key in r.iter_inputs():
            r.cache_input(input_key)
        r.prepare_target()
        for chunk_key in r.iter_chunks():
            r.store_chunk(chunk_key)
        r.finalize_target()

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
    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=netcdf_local_paths,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=daily_xarray_dataset.attrs["items_per_file"],
        target=tmp_target,
        input_cache=tmp_cache,
        process_input=process_input,
        process_chunk=process_chunk,
    )

    # this is the cannonical way to manually execute a recipe
    for input_key in r.iter_inputs():
        r.cache_input(input_key)
    r.prepare_target()
    for chunk_key in r.iter_chunks():
        r.store_chunk(chunk_key)
    r.finalize_target()

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

    assert ds_target.identical(ds_expected)


def test_NetCDFtoZarrSequentialRecipeNoTarget(
    daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache
):

    r = recipe.NetCDFtoZarrSequentialRecipe(
        input_urls=netcdf_local_paths,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=daily_xarray_dataset.attrs["items_per_file"],
    )

    # this is the cannonical way to manually execute a recipe
    with pytest.raises(UninitializedTargetError):
        r.cache_input(next(r.iter_inputs()))
