import pytest

from pangeo_forge_recipes.chunk_grid import ChunkAxis


def test_chunk_axis():
    ca = ChunkAxis(chunks=(2, 4, 3))
    assert len(ca) == 9
    assert ca.nchunks == 3

    # yes we could parameterize this but writing it out helps understanding
    with pytest.raises(IndexError):
        _ = ca.array_index_to_chunk_index(-1)
    assert ca.array_index_to_chunk_index(0) == 0
    assert ca.array_index_to_chunk_index(1) == 0
    assert ca.array_index_to_chunk_index(2) == 1
    assert ca.array_index_to_chunk_index(3) == 1
    assert ca.array_index_to_chunk_index(4) == 1
    assert ca.array_index_to_chunk_index(5) == 1
    assert ca.array_index_to_chunk_index(6) == 2
    assert ca.array_index_to_chunk_index(7) == 2
    assert ca.array_index_to_chunk_index(8) == 2
    with pytest.raises(IndexError):
        _ = ca.array_index_to_chunk_index(9)

    bad_array_slices = slice(0, 5, 2), slice(-1, 5), slice(5, 4), slice(5, 10)
    for sl in bad_array_slices:
        with pytest.raises(IndexError):
            _ = ca.array_slice_to_chunk_slice(sl)

    assert ca.array_slice_to_chunk_slice(slice(0, 9)) == slice(0, 3)
    assert ca.array_slice_to_chunk_slice(slice(1, 9)) == slice(0, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 9)) == slice(1, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 8)) == slice(1, 3)
    assert ca.array_slice_to_chunk_slice(slice(2, 6)) == slice(1, 2)
    assert ca.array_slice_to_chunk_slice(slice(2, 5)) == slice(1, 2)
    assert ca.array_slice_to_chunk_slice(slice(6, 7)) == slice(2, 3)
