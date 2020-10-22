import itertools
from typing import Any, List, Tuple

from prefect import task


# https://alexwlchan.net/2018/12/iterating-in-fixed-size-chunks/
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk


@task
def chunk(sources: List[Any], size: int) -> List[Tuple[Any, ...]]:
    """
    Prefect task to chunk a list of sources into batches.

    Examples
    --------
    >>> import pangeo_forge.utils
    >>> pangeo_forge.utils.chunk.run([1, 2, 3, 4, 5], size=2)
    [(1, 2), (3, 4), (5,)]
    """
    return list(chunked_iterable(sources, size))
