import os
from dataclasses import dataclass
from typing import Tuple, Union

import fsspec
import numpy as np
import xarray as xr
import zarr
from fsspec.implementations.reference import LazyReferenceMapper
from kerchunk.combine import MultiZarrToZarr

from .patterns import CombineOp, Index
from .storage import FSSpecTarget


def _region_for(var: xr.Variable, index: Index) -> Tuple[slice, ...]:
    region_slice = []
    for dim, dimsize in var.sizes.items():
        concat_dimension = index.find_concat_dim(dim)
        if concat_dimension:
            # we are concatenating over this dimension
            position = index[concat_dimension]
            assert position.indexed
            start = position.value
            stop = start + dimsize
            region_slice.append(slice(start, stop))
        else:
            # we are writing the entire dimension
            region_slice.append(slice(None))
    return tuple(region_slice)


def _store_data(vname: str, var: xr.Variable, index: Index, zgroup: zarr.Group) -> None:
    zarr_array = zgroup[vname]
    # get encoding for variable from zarr attributes
    var_coded = var.copy()  # copy needed for test suit to avoid modifying inputs in-place
    var_coded.encoding.update(zarr_array.attrs)
    var_coded.attrs = {}
    var = xr.backends.zarr.encode_zarr_variable(var_coded)
    data = np.asarray(var.data)
    region = _region_for(var, index)
    # check that the region evenly overlaps the zarr chunks
    for dimsize, chunksize, region_slice in zip(zarr_array.shape, zarr_array.chunks, region):
        if region_slice.start is None:
            continue
        try:
            assert region_slice.start % chunksize == 0
            assert (region_slice.stop % chunksize == 0) or (region_slice.stop == dimsize)
        except AssertionError:
            raise ValueError(
                f"Region {region} does not align with Zarr chunks {zarr_array.chunks}."
            )
    zarr_array[region] = data


def _is_first_item(index):
    for _, v in index.items():
        if v.value > 0:
            return False
    return True


def _is_first_in_merge_dim(index):
    for k, v in index.items():
        if k.operation == CombineOp.MERGE:
            if v.value > 0:
                return False
    return True


def store_dataset_fragment(
    item: Tuple[Index, xr.Dataset], target_store: zarr.storage.FSStore
) -> zarr.storage.FSStore:
    """Store a piece of a dataset in a Zarr store.

    :param item: The index and dataset to be stored
    :param target_store: The destination to store in
    """

    index, ds = item
    zgroup = zarr.open_group(target_store)

    # TODO: check that the dataset and the index are compatible

    # only store coords if this is the first item in a merge dim
    if _is_first_in_merge_dim(index):
        for vname, da in ds.coords.items():
            # if this variable contains a concat dim, we always store it
            possible_concat_dims = [index.find_concat_dim(dim) for dim in da.dims]
            if any(possible_concat_dims) or _is_first_item(index):
                _store_data(vname, da.variable, index, zgroup)
    for vname, da in ds.data_vars.items():
        _store_data(vname, da.variable, index, zgroup)

    return target_store


def write_combined_reference(
    reference: MultiZarrToZarr,
    full_target: FSSpecTarget,
    output_file_name: str,
    output_file_type: str,
):
    """Write a kerchunk combined references object to file."""

    import ujson  # type: ignore

    outpath = os.path.join(full_target.root_path, output_file_name, ".", output_file_type)

    if output_file_type == "json":
        multi_kerchunk = reference.translate()
        with full_target.fs.open(outpath, "wb") as f:
            f.write(ujson.dumps(multi_kerchunk).encode())
    elif output_file_type == "parquet":

        fs = fsspec.filesystem("file")
        out = LazyReferenceMapper.create(1000, outpath, fs)
        os.makedirs(outpath)
        out = LazyReferenceMapper.create(1000, "combined.parq", fs)

        mzz = MultiZarrToZarr(reference, remote_protocol="memory").translate()
        out.flush()

    else:
        # TODO: implement parquet writer
        raise NotImplementedError(f"{output_file_type = } not supported.")


@dataclass
class ZarrWriterMixin:
    """Defines common attributes and methods for storing zarr datasets, which can be either actual
    zarr stores or virtual (i.e. kerchunked) stores. This class should not be directly instantiated.
    Instead, PTransforms in the `.transforms` module which write consolidated zarr stores should
    inherit from this mixin, so that they share a common interface for target store naming.

    :param target_root: Location the Zarr store will be created inside.
    :param store_name: Name for the Zarr store. It will be created with this name
                       under `target_root`.
    """

    target_root: Union[str, FSSpecTarget]
    store_name: str

    def get_full_target(self) -> FSSpecTarget:
        if isinstance(self.target_root, str):
            target_root = FSSpecTarget.from_url(self.target_root)
        else:
            target_root = self.target_root
        return target_root / self.store_name
