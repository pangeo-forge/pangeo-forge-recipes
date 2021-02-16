import itertools
from contextlib import contextmanager
from typing import Iterable, List, Tuple

import numpy as np
from dask.distributed import Lock


# https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


# only needed because of
# https://github.com/pydata/xarray/issues/4631
def fix_scalar_attr_encoding(ds):
    def _fixed_attrs(d):
        fixed = {}
        for k, v in d.items():
            if isinstance(v, np.ndarray) and len(v) == 1:
                fixed[k] = v[0]
        return fixed

    ds = ds.copy()
    ds.attrs.update(_fixed_attrs(ds.attrs))
    ds.encoding.update(_fixed_attrs(ds.encoding))
    for v in ds.variables:
        ds[v].attrs.update(_fixed_attrs(ds[v].attrs))
        ds[v].encoding.update(_fixed_attrs(ds[v].encoding))
    return ds


def calc_chunk_conflicts(chunks: Iterable[int], zchunks: int) -> List[Tuple[int]]:
    n_chunks = len(chunks)

    chunk_bounds = np.hstack([0, np.cumsum(chunks)])
    chunk_overlap = []
    for start, stop in zip(chunk_bounds[:-1], chunk_bounds[1:]):
        chunk_start = start // zchunks
        chunk_stop = (stop - 1) // zchunks
        chunk_overlap.append((chunk_start, chunk_stop))

    chunk_conflicts = []
    for n, chunk_pair in enumerate(chunk_overlap):
        conflicts = set()
        if n > 0:
            prev_pair = chunk_overlap[n - 1]
            if prev_pair[1] == chunk_pair[0]:
                conflicts.add(chunk_pair[0])
        if n < (n_chunks - 1):
            next_pair = chunk_overlap[n + 1]
            if next_pair[0] == chunk_pair[1]:
                conflicts.add(chunk_pair[1])
        chunk_conflicts.append(tuple(conflicts))

    return chunk_conflicts


@contextmanager
def lock_for_conflicts(conflicts, base_name="pangeo-forge"):
    locks = [Lock(f"{base_name}-{c}") for c in conflicts]
    for lock in locks:
        lock.acquire()
    try:
        yield
    finally:
        for lock in locks:
            lock.release()
