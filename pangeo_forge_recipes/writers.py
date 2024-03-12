import os
from typing import Dict, List, MutableMapping, Optional, Protocol, Tuple, Union

import fsspec
import numpy as np
import xarray as xr
import zarr
from fsspec.implementations.reference import LazyReferenceMapper, ReferenceFileSystem
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


def consolidate_metadata(store: MutableMapping) -> MutableMapping:
    """Consolidate metadata for a Zarr store

    :param store: Input Store for Zarr
    :type store: MutableMapping
    :return: Output Store
    :rtype: zarr.storage.FSStore
    """

    import zarr

    if isinstance(store, fsspec.FSMap) and isinstance(store.fs, ReferenceFileSystem):
        raise ValueError(
            """Creating consolidated metadata for Kerchunk references should not
            yield a performance benefit so consolidating metadata is not supported."""
        )
    if isinstance(store, zarr.storage.FSStore):
        zarr.convenience.consolidate_metadata(store)

    return store


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
    reference: MutableMapping,
    full_target: FSSpecTarget,
    concat_dims: List[str],
    output_file_name: str,
    refs_per_component: int = 10000,
    mzz_kwargs: Optional[Dict] = None,
) -> zarr.storage.FSStore:
    """Write a kerchunk combined references object to file."""
    file_ext = os.path.splitext(output_file_name)[-1]
    outpath = full_target._full_path(output_file_name)

    import ujson  # type: ignore

    # unpack fsspec options that will be used below for call sites without dep injection
    storage_options = full_target.fsspec_kwargs  # type: ignore[union-attr]
    remote_protocol = full_target.get_fsspec_remote_protocol()  # type: ignore[union-attr]

    if file_ext == ".parquet":
        # Creates empty parquet store to be written to
        if full_target.exists(output_file_name):
            full_target.rm(output_file_name, recursive=True)
        full_target.makedir(output_file_name)

        out = LazyReferenceMapper.create(
            root=outpath, fs=full_target.fs, record_size=refs_per_component
        )

        # Calls MultiZarrToZarr on a MultiZarrToZarr object and adds kwargs to write to parquet.
        MultiZarrToZarr(
            [reference],
            concat_dims=concat_dims,
            target_options=storage_options,
            remote_options=storage_options,
            remote_protocol=remote_protocol,
            out=out,
            **mzz_kwargs,
        ).translate()

        # call to write reference to empty parquet store
        out.flush()

    # If reference is a ReferenceFileSystem, write to json
    elif isinstance(reference, fsspec.FSMap) and isinstance(reference.fs, ReferenceFileSystem):
        # context manager reuses dep injected auth credentials without passing storage options
        with full_target.fs.open(outpath, "wb") as f:
            f.write(ujson.dumps(reference.fs.references).encode())

    else:
        raise NotImplementedError(f"{file_ext = } not supported.")
    return ReferenceFileSystem(
        outpath,
        target_options=storage_options,
        # NOTE: `target_protocol` is required here b/c
        # fsspec classes are inconsistent about deriving
        # protocols if they are not passed. In this case ReferenceFileSystem
        # decides how to read a reference based on `target_protocol` before
        # it is automagically derived unfortunately
        # https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/reference.py#L650-L663
        target_protocol=remote_protocol,
        remote_options=storage_options,
        remote_protocol=remote_protocol,
        lazy=True,
    ).get_mapper()


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


def create_pyramid(
    item: Tuple[Index, str],
    level: int,
    epsg_code: Optional[str] = None,
    rename_spatial_dims: Optional[dict] = None,
    pyramid_kwargs: Optional[dict] = {},
    fsspec_kwargs: Optional[dict] = {},
) -> zarr.storage.FSStore:
    index, url = item
    from ndpyramid.reproject import level_reproject
    from ndpyramid.utils import set_zarr_encoding
    import xarray
    import fsspec

    with fsspec.open(url, mode='rb', **fsspec_kwargs) as open_file:
        ds = xarray.open_dataset(open_file, engine='h5netcdf')
        if epsg_code:
            import rioxarray  # noqa

            ds = ds.rio.write_crs(f"EPSG:{epsg_code}")

        # Ideally we can use ds = ds.anom.rio.set_spatial_dims(x_dim='lon',y_dim='lat')
        # But rioxarray.set_spatial_dims seems to only operate on the dataarray level
        # For now, we can use ds.rename
        if rename_spatial_dims:
            ds = ds.rename(rename_spatial_dims)

        level_ds = level_reproject(ds, level=level, **pyramid_kwargs)

        level_ds = set_zarr_encoding(level_ds, float_dtype="float32", int_dtype="int32")
        return index, level_ds
