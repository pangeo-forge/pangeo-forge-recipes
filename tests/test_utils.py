import pytest

from pangeo_forge import utils


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
    actual = list(utils.chunked_iterable(iterable, size))
    assert actual == expected


def test_chunk_conflicts():
    zchunks = 10
    assert utils.calc_chunk_conflicts([10, 10], zchunks) == [(), ()]
    assert utils.calc_chunk_conflicts([9, 10], zchunks) == [(0,), (0,)]
    assert utils.calc_chunk_conflicts([10, 9, 11, 10], zchunks) == [(), (1,), (1,), ()]
    assert utils.calc_chunk_conflicts([9, 12, 5], zchunks) == [(0,), (0, 2), (2,)]
