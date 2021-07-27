import os.path

import fsspec
import pytest
import xarray as xr

from pangeo_forge_recipes.recipes.reference_hdf_zarr import ReferenceHDFRecipe

try:
    import intake
except ImportError:
    intake = False


@pytest.mark.parametrize("with_intake", [True, False])
def test_single(netcdf_local_paths, tmpdir, with_intake):
    if with_intake and intake is False:
        pytest.skip("intake not installed")
    full_paths, items_per_file = netcdf_local_paths
    path = str(full_paths[0])
    expected = xr.open_dataset(path, engine="h5netcdf")

    out = os.path.join(tmpdir, "out.json")

    recipe = ReferenceHDFRecipe(netcdf_url=path, output_url=out)

    recipe.to_dask().compute(scheduler="sync")

    assert os.path.isfile(out)
    assert os.path.isfile(out + ".yaml")

    if with_intake:
        cat = intake.open_catalog(out + ".yaml")
        ds = cat.data.read()
    else:
        fs = fsspec.filesystem("reference", fo=out, remote_protocol="file")
        m = fs.get_mapper("")
        ds = xr.open_dataset(m, engine="zarr")
    assert (ds.foo == expected.foo).all()
