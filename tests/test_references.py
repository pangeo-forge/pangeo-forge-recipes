import tempfile

import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget

pytest.importorskip("fsspec_reference_maker")
intake = pytest.importorskip("intake")
reference_hdf_zarr = pytest.importorskip("pangeo_forge_recipes.recipes.reference_hdf_zarr")
HDFReferenceRecipe = reference_hdf_zarr.HDFReferenceRecipe


@pytest.mark.parametrize("with_intake", [True, False])
def test_single(netcdf_local_paths, tmpdir, with_intake):
    full_paths, items_per_file, fnames_by_variable = netcdf_local_paths[:3]
    if fnames_by_variable:
        pytest.skip("This does not test merging operations.")
    path = str(full_paths[0])
    expected = xr.open_dataset(path, engine="h5netcdf")

    file_pattern = pattern_from_file_sequence([path], "time")
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
def test_multi(netcdf_local_paths, tmpdir, with_intake):
    full_paths, items_per_file, fnames_by_variable = netcdf_local_paths[:3]
    if fnames_by_variable:
        pytest.skip("This does not test merging operations.")
    paths = [str(f) for f in full_paths]
    expected = xr.open_mfdataset(paths, engine="h5netcdf")

    file_pattern = pattern_from_file_sequence(paths, "time")
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
