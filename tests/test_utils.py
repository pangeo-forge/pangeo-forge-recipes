import dask
import pytest
from dask import delayed
from dask.distributed import Client, LocalCluster

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


@pytest.mark.parametrize("conflicts", [{}, {0}, {0, 1}])
def test_locks(conflicts):
    # TOOD: move this into a fixture
    with LocalCluster(n_workers=1, processes=False, threads_per_worker=1,) as cluster, Client(
        cluster
    ):

        @delayed
        def do_stuff():
            with utils.lock_for_conflicts(conflicts):
                # todo; how to actually test for concurrency! hard!
                pass

        dask.compute([do_stuff() for n in range(3)])
