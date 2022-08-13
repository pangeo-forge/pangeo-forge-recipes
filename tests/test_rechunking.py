import math

import pytest
import xarray as xr

from pangeo_forge_recipes.patterns import CombineOp, DimKey, DimVal, Index
from pangeo_forge_recipes.rechunking import split_fragment

from .data_generation import make_ds


@pytest.mark.parametrize("offset", [0, 5])  # hypothetical offset of this fragment
@pytest.mark.parametrize("time_chunks", [1, 3, 5, 10, 11])
def test_split_fragment(time_chunks, offset):

    nt_total = 20  # the total size of the hypothetical dataset
    target_chunks_and_dims = {"time": (time_chunks, nt_total)}

    nt = 10
    ds = make_ds(nt=nt)  # this represents a single dataset fragment
    dim_key = DimKey("time", CombineOp.CONCAT)
    index = Index({dim_key: DimVal(0, offset, offset + nt)})

    all_splits = list(split_fragment((index, ds), target_chunks_and_dims=target_chunks_and_dims))

    expected_chunks = math.ceil(nt / time_chunks)
    assert len(all_splits) == expected_chunks

    group_keys = [item[0] for item in all_splits]
    new_indexes = [item[1][0] for item in all_splits]
    new_datasets = [item[1][1] for item in all_splits]

    for n in range(expected_chunks):
        chunk_number = offset // time_chunks + n
        assert group_keys[n] == (("time", chunk_number),)
        chunk_start = time_chunks * chunk_number
        chunk_stop = min(time_chunks * (chunk_number + 1), nt_total)
        fragment_start = max(chunk_start, offset)
        fragment_stop = min(chunk_stop, fragment_start + time_chunks)
        assert new_indexes[n] == Index({dim_key: DimVal(0, fragment_start, fragment_stop)})
        start, stop = fragment_start - offset, fragment_stop - offset
        xr.testing.assert_equal(new_datasets[n], ds.isel(time=slice(start, stop)))
