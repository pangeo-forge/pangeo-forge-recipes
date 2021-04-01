import pytest

from pangeo_forge.utils import chunk_bounds_and_conflicts, chunked_iterable


@pytest.mark.parametrize(
    "iterable, size, expected",
    [
        ([1, 2, 3], 1, [(1,), (2,), (3,)]),
        ([1, 2, 3], 2, [(1, 2), (3,)]),
        ([1, 2, 3], 3, [(1, 2, 3,)],),
        ([1, 2, 3], 4, [(1, 2, 3,)],),
    ],
)
def test_chunked_iterable(iterable, size, expected):
    actual = list(chunked_iterable(iterable, size))
    assert actual == expected


def test_chunk_conflicts():
    zchunks = 10
    assert chunk_bounds_and_conflicts([10, 10], zchunks) == ([0, 10, 20], [(), ()])
    assert chunk_bounds_and_conflicts([9, 10], zchunks) == ([0, 9, 19], [(0,), (0,)])
    assert chunk_bounds_and_conflicts([10, 9, 11, 10], zchunks) == (
        [0, 10, 19, 30, 40],
        [(), (1,), (1,), ()],
    )
    assert chunk_bounds_and_conflicts([9, 12, 5], zchunks) == ([0, 9, 21, 26], [(0,), (0, 2), (2,)])
