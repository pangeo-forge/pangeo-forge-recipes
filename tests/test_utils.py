import pytest

from pangeo_forge_recipes.utils import calc_subsets, chunk_bounds_and_conflicts


def test_chunk_conflicts():
    zchunks = 10
    assert chunk_bounds_and_conflicts([10, 10], zchunks) == ([0, 10, 20], [(), ()])
    assert chunk_bounds_and_conflicts([9, 10], zchunks) == ([0, 9, 19], [(0,), (0,)])
    assert chunk_bounds_and_conflicts([10, 9, 11, 10], zchunks) == (
        [0, 10, 19, 30, 40],
        [(), (1,), (1,), ()],
    )
    assert chunk_bounds_and_conflicts([9, 12, 5], zchunks) == ([0, 9, 21, 26], [(0,), (0, 2), (2,)])


def test_calc_subsets():
    with pytest.raises(ValueError):
        _ = calc_subsets(4, 5)
    assert calc_subsets(5, 5) == [1, 1, 1, 1, 1]
    assert calc_subsets(6, 5) == [1, 1, 1, 1, 2]
