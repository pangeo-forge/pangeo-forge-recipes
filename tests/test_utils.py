import pangeo_forge.utils


def test_chunk():
    result = pangeo_forge.utils.chunk.run([1, 2, 3], 2)
    assert result == [(1, 2), (3,)]
