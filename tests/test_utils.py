import pytest

import pangeo_forge.utils


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
    actual = list(pangeo_forge.utils.chunked_iterable(iterable, size))
    assert actual == expected
