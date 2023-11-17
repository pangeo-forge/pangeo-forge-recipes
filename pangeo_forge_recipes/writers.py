import os
from typing import List, Protocol, Tuple, Union

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


def _select_single_protocol(full_target: FSSpecTarget) -> str:
    # Grabs first protocol if there are multiple options: Based off of logic in fsspec:
    # https://github.com/fsspec/filesystem_spec/blob/b8aeb13361e89f22f323bbc93c8308ff2ffede19/fsspec/spec.py#L1410-L1414
    return (
        full_target.fs.protocol[0]
        if isinstance(full_target.fs.protocol, (tuple, list))
        else full_target.fs.protocol
    )


def write_combined_reference(
    reference: dict,
    full_target: FSSpecTarget,
    concat_dims: List[str],
    output_file_name: str,
    refs_per_component: int = 1000,
) -> FSSpecTarget:
    """Write a kerchunk combined references object to file."""

    import ujson  # type: ignore

    file_ext = os.path.splitext(output_file_name)[-1]

    outpath = full_target._full_path(output_file_name)

    if file_ext == ".json":
        multi_kerchunk = reference
        with full_target.fs.open(outpath, "wb") as f:
            f.write(ujson.dumps(multi_kerchunk).encode())

    elif file_ext == ".parquet":

        # Creates empty parquet store to be written to
        if full_target.exists(output_file_name):
            full_target.rm(output_file_name, recursive=True)
        full_target.makedir(output_file_name)

        remote_protocol = _select_single_protocol(full_target)

        out = LazyReferenceMapper.create(refs_per_component, outpath, full_target.fs)

        # Calls MultiZarrToZarr on a MultiZarrToZarr object and adds kwargs to write to parquet.
        MultiZarrToZarr(
            [reference],
            concat_dims=concat_dims,
            remote_protocol=remote_protocol,
            out=out,
        ).translate()

        # call to write reference to empty parquet store
        out.flush()

    else:
        raise NotImplementedError(f"{file_ext = } not supported.")

    return full_target


class ZarrWriterProtocol(Protocol):
    """Protocol for mixin typing, following best practices described in:
    https://mypy.readthedocs.io/en/stable/more_types.html#mixin-classes.
    When used as a type hint for the `self` argument on mixin classes, this protocol just tells type
    checkers that the given method is expected to be called in the context of a class which defines
    the attributes declared here. This satisfies type checkers without the need to define these
    attributes more than once in an inheritance heirarchy.
    """

    store_name: str
    target_root: Union[str, FSSpecTarget]


class ZarrWriterMixin:
    """Defines common methods relevant to storing zarr datasets, which can be either actual zarr
    stores or virtual (i.e. kerchunked) stores. This class should not be directly instantiated.
    Instead, PTransforms in the `.transforms` module which write zarr stores should inherit from
    this mixin, so that they share a common interface for target store naming.
    """

    def get_full_target(self: ZarrWriterProtocol) -> FSSpecTarget:
        if isinstance(self.target_root, str):
            target_root = FSSpecTarget.from_url(self.target_root)
        else:
            target_root = self.target_root
        return target_root / self.store_name
