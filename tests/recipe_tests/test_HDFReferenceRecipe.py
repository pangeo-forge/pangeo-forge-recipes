import tempfile

import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget

pytest.importorskip("fsspec_reference_maker")
intake = pytest.importorskip("intake")
reference_hdf_zarr = pytest.importorskip("pangeo_forge_recipes.recipes.reference_hdf_zarr")
HDFReferenceRecipe = reference_hdf_zarr.HDFReferenceRecipe


@pytest.mark.parametrize("with_intake", [True, False])
def test_single(netcdf_local_file_pattern_sequential, tmpdir, with_intake):
    file_pattern = netcdf_local_file_pattern_sequential
    path = list(file_pattern.items())[0][1]
    expected = xr.open_dataset(path, engine="h5netcdf")
    recipe = HDFReferenceRecipe(file_pattern)

    # make sure assigning storage later works
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
    recipe.target = out_target
    recipe.metadata_cache = metadata_cache

    recipe.to_dask().compute(scheduler="sync")

    assert out_target.exists("reference.json")
    assert out_target.exists("reference.yaml")

    if with_intake:
        cat = intake.open_catalog(out_target._full_path("reference.yaml"))
        ds = cat.data.read()
    else:
        fs = fsspec.filesystem(
            "reference", fo=out_target._full_path("reference.json"), remote_protocol="file"
        )
        m = fs.get_mapper("")
        ds = xr.open_dataset(m, engine="zarr", chunks={}, consolidated=False)
    assert (ds.foo == expected.foo).all()


@pytest.mark.parametrize("with_intake", [True, False])
def test_multi(netcdf_local_file_pattern_sequential, tmpdir, with_intake):
    file_pattern = netcdf_local_file_pattern_sequential
    paths = [f for _, f in list(file_pattern.items())]
    expected = xr.open_mfdataset(paths, engine="h5netcdf")
    recipe = HDFReferenceRecipe(file_pattern)

    # make sure assigning storage later works
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
    recipe.target = out_target
    recipe.metadata_cache = metadata_cache

    recipe.to_dask().compute(scheduler="sync")

    assert out_target.exists("reference.json")
    assert out_target.exists("reference.yaml")

    if with_intake:
        cat = intake.open_catalog(out_target._full_path("reference.yaml"))
        ds = cat.data.read()
    else:
        m = fsspec.get_mapper(
            "reference://",
            fo=out_target._full_path("reference.json"),
            target_protocol="file",
            remote_protocol="file",
            skip_instance_cache=True,
        )
        ds = xr.open_dataset(m, engine="zarr", chunks={}, consolidated=False)
    assert (ds.foo == expected.foo).all()


def _drop_lon(ds):
    return ds.drop_vars(["lon"])


@pytest.mark.parametrize("process", [None, _drop_lon])
@pytest.mark.parametrize("with_intake", [True, False])
def test_process(netcdf_local_file_pattern_sequential, tmpdir, with_intake, process):
    file_pattern = netcdf_local_file_pattern_sequential
    paths = [f for _, f in list(file_pattern.items())]
    expected = xr.open_mfdataset(paths, engine="h5netcdf")
    if process:
        expected = _drop_lon(expected)

    recipe = HDFReferenceRecipe(file_pattern, process_input=process)

    # make sure assigning storage later works
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
    recipe.target = out_target
    recipe.metadata_cache = metadata_cache

    recipe.to_dask().compute(scheduler="sync")

    assert out_target.exists("reference.json")
    assert out_target.exists("reference.yaml")

    if with_intake:
        cat = intake.open_catalog(out_target._full_path("reference.yaml"))
        ds = cat.data.read()
    else:
        m = fsspec.get_mapper(
            "reference://",
            fo=out_target._full_path("reference.json"),
            target_protocol="file",
            remote_protocol="file",
            skip_instance_cache=True,
        )
        ds = xr.open_dataset(m, engine="zarr", chunks={}, consolidated=False)
    assert (ds.foo == expected.foo).all()


# # function passed to preprocessing
# def incr_date(ds, filename=""):
#     # add one day
#     t = [d + int(24 * 3600e9) for d in ds.time.values]
#     ds = ds.assign_coords(time=t)
#     return ds


# @pytest.mark.parametrize(
#     "process_input, process_chunk",
#     [(None, None), (incr_date, None), (None, incr_date), (incr_date, incr_date)],
# )
# @pytest.mark.parametrize("recipe_fixture", all_recipes)
# def test_process(recipe_fixture, execute_recipe, process_input, process_chunk):
#     """Check that the process_chunk and process_input arguments work as expected."""

#     RecipeClass, file_pattern, kwargs, ds_expected, target = recipe_fixture
#     kwargs["process_input"] = process_input
#     kwargs["process_chunk"] = process_chunk
#     rec = RecipeClass(file_pattern, **kwargs)
#     execute_recipe(rec)
#     ds_actual = xr.open_zarr(target.get_mapper()).load()

#     if process_input and process_chunk:
#         assert not ds_actual.identical(ds_expected)
#         ds_expected = incr_date(incr_date(ds_expected))
#     elif process_input or process_chunk:
#         assert not ds_actual.identical(ds_expected)
#         ds_expected = incr_date(ds_expected)

#     xr.testing.assert_identical(ds_actual, ds_expected)
