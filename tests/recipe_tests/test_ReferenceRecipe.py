import tempfile

import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget

pytest.importorskip("kerchunk")
intake = pytest.importorskip("intake")
reference_zarr = pytest.importorskip("pangeo_forge_recipes.recipes.reference_zarr")
ReferenceRecipe = reference_zarr.ReferenceRecipe


@pytest.mark.xfail  # This needs to be updated with filetypes
@pytest.mark.parametrize("default_storage", [True, False])
@pytest.mark.parametrize("with_intake", [False, True])
def test_single_grib(
    grib_local_file_pattern_sequential,
    tmpdir,
    with_intake,
    default_storage,
    execute_recipe,
):
    file_pattern = grib_local_file_pattern_sequential
    path = list(file_pattern.items())[0][1]
    expected = xr.open_dataset(path, engine="cfgrib")
    recipe = ReferenceRecipe(file_pattern)
    recipe = recipe.copy_pruned(nkeep=1)
    recipe.coo_map = {"time": "cf:time"}  # ensure cftime encoding end-to-end

    if default_storage:
        out_target = recipe.storage_config.target
    else:
        # make sure assigning storage later works
        out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
        metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
        recipe.storage_config.target = out_target
        recipe.storage_config.metadata = metadata_cache

    execute_recipe(recipe)

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
        ds = xr.open_dataset(m, engine="zarr", chunks={}, backend_kwargs=dict(consolidated=False))

    assert (expected.foo.values == ds.foo.values).all()


@pytest.mark.parametrize("default_storage", [True, False])
@pytest.mark.parametrize("with_intake", [False, True])
def test_single_netcdf(
    netcdf_local_file_pattern_sequential,
    tmpdir,
    with_intake,
    default_storage,
    execute_recipe,
):
    file_pattern = netcdf_local_file_pattern_sequential
    path = list(file_pattern.items())[0][1]
    expected = xr.open_dataset(path, engine="h5netcdf")
    recipe = ReferenceRecipe(file_pattern)
    recipe = recipe.copy_pruned(nkeep=1)
    recipe.coo_map = {"time": "cf:time"}  # ensure cftime encoding end-to-end

    if default_storage:
        out_target = recipe.storage_config.target
    else:
        # make sure assigning storage later works
        out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
        metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
        recipe.storage_config.target = out_target
        recipe.storage_config.metadata = metadata_cache

    execute_recipe(recipe)

    assert recipe.dataset_type == "kerchunk"

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
        ds = xr.open_dataset(m, engine="zarr", chunks={}, backend_kwargs=dict(consolidated=False))

    assert (expected.foo.values == ds.foo.values).all()


@pytest.mark.parametrize("with_intake", [True, False])
def test_multi_netcdf(netcdf_local_file_pattern_sequential, tmpdir, with_intake, execute_recipe):
    file_pattern = netcdf_local_file_pattern_sequential
    paths = [f for _, f in list(file_pattern.items())]
    expected = xr.open_mfdataset(paths, engine="h5netcdf")
    recipe = ReferenceRecipe(file_pattern)
    recipe.coo_map = {"time": "cf:time"}  # ensure cftime encoding end-to-end

    # make sure assigning storage later works
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    metadata_cache = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())
    recipe.storage_config.target = out_target
    recipe.storage_config.metadata = metadata_cache

    execute_recipe(recipe)

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
