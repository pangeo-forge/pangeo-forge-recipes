import logging
from contextlib import contextmanager
from typing import List, Sequence, Tuple

import numpy as np
from dask.distributed import Lock, get_client

logger = logging.getLogger(__name__)


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


def calc_subsets(sequence_len: int, n_segments: int) -> List[int]:
    """Given a sequence length, return a list that divides it into n_segments
    (possibly uneven) integer segments."""
    if n_segments > sequence_len:
        raise ValueError(f"Can't split len {sequence_len} into {n_segments} segments")
    step = sequence_len // n_segments
    remainder = sequence_len % n_segments
    return (n_segments - 1) * [step] + [step + remainder]


def chunk_bounds_and_conflicts(
    chunks: Sequence[int], zchunks: int
) -> Tuple[List[int], List[Tuple[int, ...]]]:
    """
    Calculate the boundaries of contiguous put possibly uneven blocks over
    a regularly chunked array

    Parameters
    ----------
    chunks : A list of chunk lengths. Len of array is the sum of each length.
    zchunks : A constant on-disk chunk

    Returns
    -------
    chunk_bounds : the boundaries of the regions to write (1 longer than chunks)
    conflicts: a list of conflicts for each chunk
    """
    n_chunks = len(chunks)

    # coerce numpy array to list for mypy
    chunk_bounds = list([int(item) for item in np.hstack([0, np.cumsum(chunks)])])
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

    return chunk_bounds, chunk_conflicts


@contextmanager
# TODO: use a recipe-specific base_name to handle multiple recipes potentially
# running at the same time
def lock_for_conflicts(conflicts, base_name="pangeo-forge", timeout=None):
    """
    Parameters
    ----------
    timeout : int, optional
        The time to wait *for each lock*.
    """

    try:
        global_client = get_client()
        is_distributed = True
    except ValueError:
        # Don't bother with locks if we are not in a distributed context
        # NOTE! This means we HAVE to use dask.distributed as our parallel execution enviroment
        # This should be compatible with Prefect.
        is_distributed = False
    if is_distributed:
        locks = [Lock(f"{base_name}-{c}", global_client) for c in conflicts]
        for lock in locks:
            logger.debug(f"Acquiring lock {lock.name}...")
            acquired = lock.acquire(timeout=timeout)
            if not acquired:
                logger.warning("Failed to acquire lock %s before timeout %s", lock.name, timeout)
                raise ValueError(f"Failed to acquire lock {lock.name} before timeout {timeout}")
            logger.debug(f"Acquired lock {lock.name}")
    elif conflicts:
        logger.debug(f"Asked to lock {conflicts} but no Dask client found.")
    try:
        yield
    finally:
        if is_distributed:
            for lock in locks:
                lock.release()
                logger.debug(f"Released lock {lock.name}")
