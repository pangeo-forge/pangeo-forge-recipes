from typing import List, Optional

import fsspec
import zarr
from prefect import task


@task
def consolidate_metadata(target, writes: Optional[List[str]] = None) -> None:
    """
    Consolidate the metadata the Zarr group at `target`.

    Parameters
    ----------
    target : str
        The URL for the (combined) Zarr group.
    writes : list of strings, optional
        The URLs the combined stores were written to. This is only a
        parameter to introduce a dependency in the pipeline execution graph.
        The actual value isn't used.
    """
    mapper = fsspec.get_mapper(target)
    zarr.consolidate_metadata(mapper)
