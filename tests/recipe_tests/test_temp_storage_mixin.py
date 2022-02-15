import pytest

from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe
from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget

from .test_HDFReferenceRecipe import HDFReferenceRecipe


@pytest.fixture(params=[XarrayZarrRecipe, HDFReferenceRecipe])
def recipe_with_unset_targets(netcdf_local_file_pattern_sequential, request):
    """Fixture for `test_assign_temp_storage`."""

    RecipeClass = request.param
    rec = RecipeClass(netcdf_local_file_pattern_sequential)

    targets = {"target": FSSpecTarget, "metadata_cache": MetadataTarget}
    if isinstance(rec, XarrayZarrRecipe):
        targets.update({"input_cache": CacheFSSpecTarget})

    return rec, targets


def test_assign_temp_storage(recipe_with_unset_targets):
    """Test `assign_temporary_storage` method inherited from `TemporaryStorageMixin`."""

    rec, targets = recipe_with_unset_targets

    for t in targets:
        assert getattr(rec, t) is None

    rec.assign_temporary_storage()

    for t, storage_cls in targets.items():
        assert isinstance(getattr(rec, t), storage_cls)
