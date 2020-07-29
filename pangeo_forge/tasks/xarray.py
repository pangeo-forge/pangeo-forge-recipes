import fsspec
import xarray as xr

from prefect import task

@task
def combine_and_write(sources, target, append_dim, concat_dim, first=True):
    '''
    Combine one or more source datasets into a single `Xarray.Dataset`, then
    write them to a Zarr store.
    
    Parameters
    ----------
    sources : list of str
        Path or url to the source files.
    target : str
        Path or url to the target location of the Zarr store.
    append_dim : str
        Name of the dimension of which datasets should be appended during write.
    concat_dim : str
        Name of the dimension of which datasets should be concatenated during read.
    first : bool
        Flag to indicate if the target dataset should be expected to already exist.
    
    Returns
    -------
    target_url : str
        Path or url in the form of `{cache_location}/hash({source_url})`.
    '''

    double_open_files = [fsspec.open(url).open() for url in sources]
    ds = xr.open_mfdataset(double_open_files, combine="nested", concat_dim=concat_dim)
    
    # by definition, this should be a contiguous chunk
    ds = ds.chunk({append_dim: len(sources)})

    if first:
        kwargs = dict(mode="w")
    else:
        kwargs = dict(mode="a", append_dim=append_dim)

    mapper = fsspec.get_mapper(target)
    ds.to_zarr(mapper, **kwargs)
