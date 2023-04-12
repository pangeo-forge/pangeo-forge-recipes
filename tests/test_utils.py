import pytest

from pangeo_forge_recipes.utils import calc_subsets


def test_calc_subsets():
    with pytest.raises(ValueError):
        _ = calc_subsets(4, 5)
    assert calc_subsets(5, 5) == [1, 1, 1, 1, 1]
    assert calc_subsets(6, 5) == [1, 1, 1, 1, 2]
