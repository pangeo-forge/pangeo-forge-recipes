import pytest

from pangeo_forge.recipe import recipe

from .fixtures import daily_xarray_dataset, netcdf_local_paths, tmp_target

dummy_fnames = ["a.nc", "b.nc", "c.nc"]
@pytest.mark.parametrize(
    "file_urls, files_per_chunk, expected_keys, expected_filenames",
    [
        (dummy_fnames, 1, [0, 1, 2], [("a.nc",), ("b.nc",), ("c.nc",)]),
        (dummy_fnames, 2, [0, 1], [("a.nc", "b.nc",), ("c.nc",)])
    ]
)
def test_file_sequence_recipe(file_urls, files_per_chunk, expected_keys, expected_filenames, tmp_target):

    r = recipe.FileSequenceRecipe(
        file_urls=file_urls,
        sequence_dim="time",
        files_per_chunk=files_per_chunk,
        target=tmp_target
    )

    chunk_keys = list(r.iter_chunks())
    assert chunk_keys == expected_keys

    for k, expected in zip(r.iter_chunks(), expected_filenames):
        fnames = r.filenames_for_chunk(k)
        assert fnames == expected


def test_full_recipe(daily_xarray_dataset, netcdf_local_paths, tmp_target):

    r = StandardSequentialRecipe(
        file_urls=netcdf_local_paths,
        sequence_dim='time',
    )

    r.set_target(tmp_dir)
    # manual execution of recipe
    r.prepare()
    for input_key in r.iter_inputs():
        r.cache_input(input_key)
    for chunk_key in r.iter_chunks():
        r.store_chunk(chunk_key)
    r.finalize()

    ds_target = xr.open_dataset(tmp_dir)
    assert ds_target.identical(daily_xarray_dataset)
