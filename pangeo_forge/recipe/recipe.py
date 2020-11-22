"""
A Pangeo Forge Recipe
"""

import xarray as xr
import fsspec


class Recipe:
    datasets = []
    keys = []

    def filenames_for_chunk(**chunk_keys):
        raise NotImplementedError

    def open_file(self, fname):
        # todo: caching
        return fsspec.open(fname)

    def open_chunk(self, filenames):
        files_to_open = [self.open_file(f) for f in filenames]
        ds_chunk = xr.open_mfdataset(filenames, **self.open_chunk_kwargs)
        return ds_chunk

    def store_chunk(self, store_target, **chunk_keys):
        filenames = self.filenames_for_chunk(**chunk_keys)
        ds_chunk = self.open_chunk(filenames)
        ds_chunk.to_zarr(store_target, mode='a')
