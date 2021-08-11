import tempfile

import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget

try:
    import intake
except ImportError:
    intake = False

pytest.importorskip("fsspec_reference_maker")
reference_hdf_zarr = pytest.importorskip("pangeo_forge_recipes.recipes.reference_hdf_zarr")
ReferenceHDFRecipe = reference_hdf_zarr.ReferenceHDFRecipe


@pytest.mark.parametrize("with_intake", [True, False])
def test_single(netcdf_local_paths, tmpdir, with_intake):
    if with_intake and intake is False:
        pytest.skip("intake not installed")
    full_paths, items_per_file = netcdf_local_paths
    path = str(full_paths[0])
    expected = xr.open_dataset(path, engine="h5netcdf")

    out = "out.json"
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    work_dir = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())

    recipe = ReferenceHDFRecipe(
        netcdf_url=path, output_url=out, output_target=out_target, _work_dir=work_dir
    )

    recipe.to_dask().compute(scheduler="sync")

    assert out_target.exists(out)
    assert out_target.exists(out + ".yaml")

    if with_intake:
        cat = intake.open_catalog(out_target._full_path(out + ".yaml"))
        ds = cat.data.read()
    else:
        fs = fsspec.filesystem("reference", fo=out_target._full_path(out), remote_protocol="file")
        m = fs.get_mapper("")
        ds = xr.open_dataset(m, engine="zarr", chunks={}, consolidated=False)
    assert (ds.foo == expected.foo).all()


@pytest.mark.parametrize("with_intake", [True, False])
def test_multi(netcdf_local_paths, tmpdir, with_intake):
    if with_intake and intake is False:
        pytest.skip("intake not installed")
    full_paths, items_per_file = netcdf_local_paths
    paths = [str(f) for f in full_paths]
    expected = xr.open_mfdataset(paths, engine="h5netcdf")

    # repeated code could be fixture
    out = "out.json"
    out_target = FSSpecTarget(fs=fsspec.filesystem("file"), root_path=str(tmpdir))
    work_dir = MetadataTarget(fs=fsspec.filesystem("file"), root_path=tempfile.mkdtemp())

    concat_kwargs = {"dim": "time"}
    recipe = ReferenceHDFRecipe(
        netcdf_url=paths,
        output_url=out,
        output_target=out_target,
        _work_dir=work_dir,
        xarray_concat_args=concat_kwargs,
    )

    recipe.to_dask().compute(scheduler="sync")

    assert out_target.exists(out)
    assert out_target.exists(out + ".yaml")

    if with_intake:
        cat = intake.open_catalog(out_target._full_path(out + ".yaml"))
        ds = cat.data.read()
    else:
        m = fsspec.get_mapper(
            "reference://",
            fo=out_target._full_path(out),
            target_protocol="file",
            remote_protocol="file",
            skip_instance_cache=True,
        )
        ds = xr.open_dataset(m, engine="zarr", chunks={}, consolidated=False)
    assert (ds.foo == expected.foo).all()
