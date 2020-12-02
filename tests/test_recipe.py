import pytest
import xarray as xr
import zarr

from pangeo_forge.recipe import recipe

from .fixtures import daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache

dummy_fnames = ["a.nc", "b.nc", "c.nc"]
@pytest.mark.parametrize(
    "file_urls, files_per_chunk, expected_keys, expected_filenames",
    [
        (dummy_fnames, 1, [0, 1, 2], [("a.nc",), ("b.nc",), ("c.nc",)]),
        (dummy_fnames, 2, [0, 1], [("a.nc", "b.nc",), ("c.nc",)])
    ]
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


def test_full_recipe(daily_xarray_dataset, netcdf_local_paths, tmp_target, tmp_cache):

    r = recipe.StandardSequentialRecipe(
        consolidate_zarr=True,
        xarray_open_kwargs={},
        xarray_concat_kwargs={},
        require_cache=False,
        input_cache=tmp_cache,
        target=tmp_target,
        chunk_preprocess_funcs=[],
        input_urls=netcdf_local_paths,
        sequence_dim="time",
        inputs_per_chunk=1,
        nitems_per_input=daily_xarray_dataset.attrs['items_per_file']
    )

    # this is the cannonical way to manually execute a recipe
    r.prepare()
    for input_key in r.iter_inputs():
        r.cache_input(input_key)
    for chunk_key in r.iter_chunks():
        r.store_chunk(chunk_key)
    r.finalize()

    ds_target = xr.open_zarr(tmp_target.get_mapper(), consolidated=True).load()
    ds_expected = daily_xarray_dataset.compute()
    assert ds_target.identical(ds_expected)
