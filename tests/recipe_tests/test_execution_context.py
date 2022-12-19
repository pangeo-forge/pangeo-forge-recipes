import pytest
import xarray as xr
import zarr
from packaging import version

from pangeo_forge_recipes.recipes import XarrayZarrRecipe


@pytest.mark.parametrize("recipe_cls", [XarrayZarrRecipe])
def test_execution_context(recipe_cls, netcdf_local_file_pattern_sequential):

    recipe = recipe_cls(netcdf_local_file_pattern_sequential)
    ec = recipe.get_execution_context()

    ec_version = version.parse(ec["version"])
    assert ec_version.is_devrelease  # should be True for editable installs used in tests
    assert isinstance(ec_version.major, int) and 0 <= ec_version.major <= 1
    assert isinstance(ec_version.minor, int) and 0 <= ec_version.major <= 99

    assert isinstance(ec["recipe_hash"], str) and len(ec["recipe_hash"]) == 64
    assert isinstance(ec["inputs_hash"], str) and len(ec["inputs_hash"]) == 64

    recipe.to_function()()
    zgroup = zarr.open_group(recipe.target_mapper)
    ds = xr.open_zarr(recipe.target_mapper, consolidated=True)

    for k, v in ec.items():
        assert zgroup.attrs[f"pangeo-forge:{k}"] == v
        assert ds.attrs[f"pangeo-forge:{k}"] == v
