"""
Tests for our custom array writing locking stuff.
"""

import logging
import sys
from time import sleep

import dask
import numpy as np
import pytest
import zarr
from dask.distributed import Client

from pangeo_forge_recipes.utils import chunk_bounds_and_conflicts, lock_for_conflicts


@pytest.mark.parametrize("n_tasks, conflicts", [(2, (0,)), (2, (1, 2)), (4, (0,))])
def test_locks(n_tasks, conflicts, tmp_target, dask_cluster):

    this_client = Client(dask_cluster)

    @dask.delayed
    def do_stuff(n):
        logger = logging.getLogger("pangeo_forge_recipes")
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        with lock_for_conflicts(conflicts):
            # test for concurrency by writing an object and asserting there are
            # no other objects present
            mapper = tmp_target.get_mapper()
            key = f"foo_{n}"
            mapper[key] = b"bar"
            assert list(mapper) == [key]
            sleep(0.25)
            del mapper[key]
            assert list(mapper) == []

    dask.compute([do_stuff(n) for n in range(n_tasks)])

    this_client.close()
    del this_client


# chunking is always over first dimension
@pytest.mark.parametrize(
    "shape, zarr_chunks, write_chunks",
    [
        ((100,), (10,), (15,)),  # just a few conflicts
        ((100,), (100,), (10,)),  # lots of conflicts!
    ],
)
def test_locked_array_writing(shape, zarr_chunks, write_chunks, tmp_target, dask_cluster):

    sequence_lens = (shape[0] // write_chunks[0]) * [write_chunks[0]]
    remainder = shape[0] % write_chunks[0]
    if remainder > 0:
        sequence_lens.append(remainder)

    chunk_bounds, chunk_conflicts = chunk_bounds_and_conflicts(sequence_lens, zarr_chunks[0])

    data = np.random.rand(*shape)

    mapper = tmp_target.get_mapper()
    zarr_array = zarr.open(mapper, mode="w", shape=shape, chunks=zarr_chunks, dtype=data.dtype)

    @dask.delayed
    def write_block(n):
        write_region = slice(chunk_bounds[n], chunk_bounds[n + 1])
        with lock_for_conflicts(chunk_conflicts[n]):
            zarr_array[write_region] = data[write_region]

    this_client = Client(dask_cluster)
    dask.compute([write_block(n) for n in range(len(sequence_lens))])
    np.testing.assert_equal(data, zarr_array)

    this_client.close()
    del this_client


def test_lock_timeout(dask_cluster):
    with Client(dask_cluster, set_as_default=True):
        with lock_for_conflicts(["key"]):
            with Client(dask_cluster, set_as_default=True):
                with pytest.raises(ValueError, match="Failed to acquire"):
                    with lock_for_conflicts(["key"], timeout=1):
                        pass
