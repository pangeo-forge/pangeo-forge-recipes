import fsspec
import zarr
from prefect import task


@task
def consolidate_metadata(target):
    """
    Consolidate Zarr metadata

    Parameters
    ----------
    target : str
         Path or url of the Zarr store.
    """
    mapper = fsspec.get_mapper(target)
    zarr.consolidate_metadata(mapper)
