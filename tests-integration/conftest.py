from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import pytest
import xarray as xr

recipes = Path(__file__).parent / "recipes"


@dataclass
class RecipeIntegrationTests(ABC):

    ds: xr.Dataset

    @abstractmethod
    def test_ds(self):
        pass


class gpcp_from_gcs(RecipeIntegrationTests):
    def test_ds(self):
        assert self.ds.title == (
            "Global Precipitation Climatatology Project (GPCP) "
            "Climate Data Record (CDR), Daily V1.3"
        )


@pytest.fixture(
    params=[
        (recipes / "gpcp_from_gcs.py", gpcp_from_gcs),
    ],
    ids=[
        "gpcp_from_gcs",
    ],
)
def recipe_modules_with_test_cls(request):
    return request.param
