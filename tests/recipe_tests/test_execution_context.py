import re

import pandas as pd
import pytest

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import HDFReferenceRecipe, XarrayZarrRecipe


@pytest.fixture
def pattern_for_execution_context():
    dates = pd.date_range("1981-09-01", "1981-09-04", freq="D")

    def make_url(time):
        return f"https://data-provider.org/{time}.nc"

    time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
    return FilePattern(make_url, time_concat_dim)


@pytest.mark.parametrize("recipe_cls", [XarrayZarrRecipe, HDFReferenceRecipe])
def test_execution_context(recipe_cls, pattern_for_execution_context):

    recipe = recipe_cls(pattern_for_execution_context)
    ec = recipe.get_execution_context()

    assert re.match(r"^([0-9]+)\.([0-9]+)\.([0-9]+)$", ec["version"].split(".dev")[0])
    assert isinstance(ec["recipe_hash"], str) and len(ec["recipe_hash"]) == 64
    assert isinstance(ec["inputs_hash"], str) and len(ec["inputs_hash"]) == 64
