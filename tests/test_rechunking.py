import math

import pytest
import xarray as xr

from pangeo_forge_recipes.patterns import CombineOp, DimKey, DimVal, Index
from pangeo_forge_recipes.rechunking import split_fragment

from .data_generation import make_ds


@pytest.mark.parametrize("time_chunks", [1, 3, 5, 10, 11])
def test_split_fragment(time_chunks):

    nt = 10
    ds = make_ds(nt=nt)  # this represents a single dataset fragment

    # in this case we have a single fragment which overlaps two
    # target chunks; it needs to be split into two pieces
    target_chunks_and_dims = {"time": (time_chunks, nt)}
    ds_fragment = ds.isel(time=slice(0, nt))  # the whole thing
    dim_key = DimKey("time", CombineOp.CONCAT)
    index = Index({dim_key: DimVal(0, 0, nt)})

    all_splits = list(
        split_fragment((index, ds_fragment), target_chunks_and_dims=target_chunks_and_dims)
    )

    expected_chunks = math.ceil(nt / time_chunks)
    assert len(all_splits) == expected_chunks

    group_keys = [item[0] for item in all_splits]
    new_indexes = [item[1][0] for item in all_splits]
    new_datasets = [item[1][1] for item in all_splits]

    assert group_keys == [(("time", n),) for n in range(expected_chunks)]

    for n in range(expected_chunks):
        start, stop = time_chunks * n, min(time_chunks * (n + 1), nt)
        assert new_indexes[n] == Index({dim_key: DimVal(0, start, stop)})
        xr.testing.assert_equal(new_datasets[n], ds.isel(time=slice(start, stop)))
