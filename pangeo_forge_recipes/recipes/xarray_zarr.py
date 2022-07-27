"""
A Pangeo Forge Recipe
"""
from __future__ import annotations

import itertools
import logging
import os
import warnings
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from itertools import chain, product
from math import ceil
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import dask
import fsspec
import numpy as np
import xarray as xr
import zarr

from ..chunk_grid import ChunkGrid
from ..executors.base import Pipeline, Stage
from ..patterns import CombineOp, DimIndex, FilePattern, FileType, Index
from ..reference import create_kerchunk_reference, unstrip_protocol
from ..storage import FSSpecTarget, MetadataTarget, file_opener
from ..utils import calc_subsets, fix_scalar_attr_encoding, lock_for_conflicts
from .base import BaseRecipe, FilePatternMixin, StorageMixin

# use this filename to store global recipe metadata in the metadata_cache
# it will be written once (by prepare_target) and read many times (by store_chunk)
_GLOBAL_METADATA_KEY = "pangeo-forge-recipe-metadata.json"
_ARRAY_DIMENSIONS = "_ARRAY_DIMENSIONS"
MAX_MEMORY = (
    int(os.getenv("PANGEO_FORGE_MAX_MEMORY"))  # type: ignore
    if os.getenv("PANGEO_FORGE_MAX_MEMORY")
    else 500_000_000
)
OPENER_MAP = {
    FileType.netcdf3: dict(engine="scipy"),
    FileType.netcdf4: dict(engine="h5netcdf"),
}

logger = logging.getLogger(__name__)

# Some types that help us keep things organized
InputKey = Index  # input keys are the same as the file pattern keys
ChunkKey = Index

SubsetSpec = Dict[str, int]
# SubsetSpec is a dictionary mapping dimension names to the number of subsets along that dimension
# (e.g. {'time': 5, 'depth': 2})


def _input_metadata_fname(input_key: InputKey) -> str:
    key_str = "-".join([f"{k.name}_{k.index}" for k in sorted(input_key)])
    return "input-meta-" + key_str + ".json"


def _input_reference_fname(input_key: InputKey) -> str:
    key_str = "-".join([f"{k.name}_{k.index}" for k in sorted(input_key)])
    return "input-reference-" + key_str + ".json"


def inputs_for_chunk(
    chunk_key: ChunkKey, inputs_per_chunk: int, ninputs: int
) -> Sequence[InputKey]:
    """For a chunk key, figure out which inputs belong to it.
    Returns at least one InputKey."""

    merge_dims = [dim_idx for dim_idx in chunk_key if dim_idx.operation == CombineOp.MERGE]
    concat_dims = [dim_idx for dim_idx in chunk_key if dim_idx.operation == CombineOp.CONCAT]
    # Ignore subset dims, we don't need them here
    # subset_dims = [dim_idx for dim_idx in chunk_key if dim_idx.operation == CombineOp.SUBSET]
    assert len(merge_dims) <= 1
    if len(merge_dims) == 1:
        merge_dim = merge_dims[0]  # type: Optional[DimIndex]
    else:
        merge_dim = None
    assert len(concat_dims) == 1
    concat_dim = concat_dims[0]

    input_keys = []

    for n in range(inputs_per_chunk):
        input_index = (inputs_per_chunk * concat_dim.index) + n
        if input_index >= ninputs:
            break
        input_concat_dim = DimIndex(concat_dim.name, input_index, ninputs, CombineOp.CONCAT)
        input_key = [input_concat_dim]
        if merge_dim is not None:
            input_key.append(merge_dim)
        input_keys.append(Index(input_key))

    return input_keys


def expand_target_dim(target: FSSpecTarget, concat_dim: Optional[str], dimsize: int) -> None:
    target_mapper = target.get_mapper()
    zgroup = zarr.open_group(target_mapper)
    ds = open_target(target)
    sequence_axes = {
        v: ds[v].get_axis_num(concat_dim) for v in ds.variables if concat_dim in ds[v].dims
    }

    for v, axis in sequence_axes.items():
        arr = zgroup[v]
        shape = list(arr.shape)
        shape[axis] = dimsize
        logger.debug(f"resizing array {v} to shape {shape}")
        arr.resize(shape)


def open_target(target: FSSpecTarget) -> xr.Dataset:
    return xr.open_zarr(target.get_mapper())


def input_position(input_key: InputKey) -> int:
    """Return the position of the input within the input sequence."""
    for dim_idx in input_key:
        # assumes there is one and only one concat dim
        if dim_idx.operation == CombineOp.CONCAT:
            return dim_idx.index
    return -1  # make mypy happy


def chunk_position(chunk_key: ChunkKey) -> int:
    """Return the position of the input within the input sequence."""
    concat_idx = -1
    for dim_idx in chunk_key:
        # assumes there is one and only one concat dim
        if dim_idx.operation == CombineOp.CONCAT:
            concat_idx = dim_idx.index
            concat_dim = dim_idx.name
    if concat_idx == -1:
        raise ValueError("Couldn't find concat_dim")
    subset_idx = 0
    subset_factor = 1
    for dim_idx in chunk_key:
        if dim_idx.operation == CombineOp.SUBSET:
            if dim_idx.name == concat_dim:
                subset_idx = dim_idx.index
                subset_factor = dim_idx.sequence_len
    return subset_factor * concat_idx + subset_idx


def cache_input(input_key: InputKey, *, config: XarrayZarrRecipe) -> None:
    if config.cache_inputs:
        if config.file_pattern.file_type == FileType.opendap:
            raise ValueError("Can't cache opendap inputs")
        if config.storage_config.cache is None:
            raise ValueError("input_cache is not set.")
        logger.info(f"Caching input '{input_key!s}'")
        fname = config.file_pattern[input_key]
        config.storage_config.cache.cache_file(
            fname,
            config.file_pattern.query_string_secrets,
            **config.file_pattern.fsspec_open_kwargs,
        )

    if config.open_input_with_kerchunk:
        if config.file_pattern.file_type == FileType.opendap:
            raise ValueError("Can't make references for opendap inputs")
        if config.storage_config.metadata is None:
            raise ValueError("Can't make references; no metadata cache assigned")
        fname = config.file_pattern[input_key]

        ref_fname = _input_reference_fname(input_key)

        if ref_fname in config.storage_config.metadata:
            logger.info("Metadata is already cached with kerchunk.")
            return

        if config.storage_config.cache is None:
            protocol = fsspec.utils.get_protocol(fname)
            url = unstrip_protocol(fname, protocol)
        else:
            url = unstrip_protocol(
                config.storage_config.cache._full_path(fname),
                config.storage_config.cache.fs.protocol,
            )
        with file_opener(
            fname,
            cache=config.storage_config.cache,
            copy_to_local=config.copy_input_to_local_file,
            bypass_open=False,
            secrets=config.file_pattern.query_string_secrets,
            **config.file_pattern.fsspec_open_kwargs,
        ) as fp:
            logger.info(f"creating kerchunk referernce for {url}")
            ref_data = create_kerchunk_reference(fp, url, config.file_pattern.file_type)
        config.storage_config.metadata[ref_fname] = ref_data

    if config.cache_metadata:
        if config.storage_config.metadata is None:
            raise ValueError("metadata_cache is not set.")

        if not _input_metadata_fname(input_key) in config.storage_config.metadata:
            with open_input(input_key, config=config) as ds:
                logger.info(f"Caching metadata for input '{input_key!s}'")
                input_metadata = ds.to_dict(data=False)
                config.storage_config.metadata[_input_metadata_fname(input_key)] = input_metadata
        else:
            logger.info(f"Metadata already cached for input '{input_key!s}'")


def region_and_conflicts_for_chunk(
    config: XarrayZarrRecipe, chunk_key: ChunkKey
) -> Tuple[Dict[str, slice], Dict[str, Set[int]]]:
    # return a dict suitable to pass to xr.to_zarr(region=...)
    # specifies where in the overall array to put this chunk's data
    # also return the conflicts with other chunks

    if config.nitems_per_input:
        input_sequence_lens = (config.nitems_per_input,) * config.file_pattern.dims[
            config.concat_dim
        ]  # type: ignore
    else:
        assert config.storage_config.metadata is not None  # for mypy
        global_metadata = config.storage_config.metadata[_GLOBAL_METADATA_KEY]
        input_sequence_lens = global_metadata["input_sequence_lens"]
    total_len = sum(input_sequence_lens)

    # for now this will just have one key since we only allow one concat_dim
    # but it could expand to accomodate multiple concat dims
    chunk_index = {config.concat_dim: chunk_position(chunk_key)}

    input_chunk_grid = ChunkGrid({config.concat_dim: input_sequence_lens})
    if config.subset_inputs and config.concat_dim in config.subset_inputs:
        assert (
            config.inputs_per_chunk == 1
        ), "Doesn't make sense to have multiple inputs per chunk plus subsetting"
        chunk_grid = input_chunk_grid.subset(config.subset_inputs)
    elif config.inputs_per_chunk > 1:
        chunk_grid = input_chunk_grid.consolidate({config.concat_dim: config.inputs_per_chunk})
    else:
        chunk_grid = input_chunk_grid
    assert chunk_grid.shape[config.concat_dim] == total_len

    region = chunk_grid.chunk_index_to_array_slice(chunk_index)

    assert config.concat_dim_chunks is not None
    target_grid = ChunkGrid.from_uniform_grid(
        {config.concat_dim: (config.concat_dim_chunks, total_len)}
    )
    conflicts = chunk_grid.chunk_conflicts(chunk_index, target_grid)

    return region, conflicts


@contextmanager
def open_input(input_key: InputKey, *, config: XarrayZarrRecipe) -> xr.Dataset:
    fname = config.file_pattern[input_key]
    logger.info(f"Opening input with Xarray {input_key!s}: '{fname}'")

    if config.file_pattern.file_type == FileType.opendap:
        if config.cache_inputs:
            raise ValueError("Can't cache opendap inputs")
        if config.copy_input_to_local_file:
            raise ValueError("Can't copy opendap inputs to local file")
        if config.open_input_with_kerchunk:
            raise ValueError("Can't open opendap inputs with kerchunk")

    if config.open_input_with_kerchunk:
        if config.storage_config.metadata is None:
            raise ValueError("metadata_cache is not set.")
        from fsspec.implementations.reference import ReferenceFileSystem

        reference_data = config.storage_config.metadata[_input_reference_fname(input_key)]
        if config.cache_inputs and config.storage_config.cache is not None:
            remote_protocol = config.storage_config.cache.fs.protocol
        else:
            remote_protocol = fsspec.utils.get_protocol(next(config.file_pattern.items())[1])
        ref_fs = ReferenceFileSystem(
            reference_data, remote_protocol=remote_protocol, skip_instance_cache=True
        )
        mapper = ref_fs.get_mapper("/")
        # Doesn't really need to be a context manager, but that's how this function works
        with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
            with xr.open_dataset(mapper, engine="zarr", chunks={}, consolidated=False) as ds:
                logger.debug("successfully opened reference dataset with zarr")

                if config.delete_input_encoding:
                    for var in ds.variables:
                        ds[var].encoding = {}

                if config.process_input is not None:
                    ds = config.process_input(ds, str(fname))

                logger.debug(f"{ds}")
                yield ds

    else:

        cache = config.storage_config.cache if config.cache_inputs else None
        bypass_open = True if config.file_pattern.file_type == FileType.opendap else False

        with file_opener(
            fname,
            cache=cache,
            copy_to_local=config.copy_input_to_local_file,
            bypass_open=bypass_open,
            secrets=config.file_pattern.query_string_secrets,
            **config.file_pattern.fsspec_open_kwargs,
        ) as f:
            with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
                kw = config.xarray_open_kwargs.copy()
                file_type = config.file_pattern.file_type
                if file_type in OPENER_MAP:
                    if "engine" in kw:
                        engine_message_base = (
                            "pangeo-forge-recipes will automatically set the xarray backend for "
                            f"files of type '{file_type.value}' to '{OPENER_MAP[file_type]}', "
                        )
                        warn_matching_msg = engine_message_base + (
                            "which is the same value you have passed via `xarray_open_kwargs`. "
                            f"If this input file is actually of type '{file_type.value}', you can "
                            f"remove `{{'engine': '{kw['engine']}'}}` from `xarray_open_kwargs`. "
                        )
                        error_mismatched_msg = engine_message_base + (
                            f"which is different from the value you have passed via "
                            "`xarray_open_kwargs`. If this input file is actually of type "
                            f"'{file_type.value}', please remove `{{'engine': '{kw['engine']}'}}` "
                            "from `xarray_open_kwargs`. "
                        )
                        engine_message_tail = (
                            f"If this input file is not of type '{file_type.value}', please update"
                            " this recipe by passing a different value to `FilePattern.file_type`."
                        )
                        warn_matching_msg += engine_message_tail
                        error_mismatched_msg += engine_message_tail

                        if kw["engine"] == OPENER_MAP[file_type]["engine"]:
                            warnings.warn(warn_matching_msg)
                        elif kw["engine"] != OPENER_MAP[file_type]["engine"]:
                            raise ValueError(error_mismatched_msg)
                    else:
                        kw.update(OPENER_MAP[file_type])
                logger.debug(f"about to enter xr.open_dataset context on {f}")
                try:
                    with xr.open_dataset(f, **kw) as ds:
                        logger.debug("successfully opened dataset")
                        ds = fix_scalar_attr_encoding(ds)

                        if config.delete_input_encoding:
                            for var in ds.variables:
                                ds[var].encoding = {}

                        if config.process_input is not None:
                            ds = config.process_input(ds, str(fname))

                        logger.debug(f"{ds}")
                        yield ds
                except OSError as e:
                    if "unable to open file (file signature not found)" in str(e).lower():
                        oserror_message_base = (
                            f"Unable to open file {f.path} "  # type: ignore
                            f"with `{{engine: {kw.get('engine')}}}`, "
                        )
                        if "engine" in config.xarray_open_kwargs:
                            oserror_message = oserror_message_base + (
                                "which was set explicitly via `xarray_open_kwargs`. Please remove "
                                f"`{{engine: {kw.get('engine')}}}` from `xarray_open_kwargs`."
                            )
                        elif file_type == FileType.netcdf4:
                            oserror_message = oserror_message_base + (
                                "which was set automatically based on the fact that "
                                "`FilePattern.file_type` is using the default value of 'netcdf4'. "
                                "It seems likely that this input file is in NetCDF3 format. If "
                                "that is the case, please re-instantiate your `FilePattern` with "
                                '`FilePattern(..., file_type="netcdf3")`.'
                            )
                        raise OSError(oserror_message) from e
                    else:
                        raise e


def subset_dataset(ds: xr.Dataset, subset_spec: DimIndex) -> xr.Dataset:
    assert subset_spec.operation == CombineOp.SUBSET
    dim = subset_spec.name
    dim_len = ds.dims[dim]
    subset_lens = calc_subsets(dim_len, subset_spec.sequence_len)
    start = sum(subset_lens[: subset_spec.index])
    stop = sum(subset_lens[: (subset_spec.index + 1)])
    subset_slice = slice(start, stop)
    indexer = {dim: subset_slice}
    logger.debug(f"Subsetting dataset with indexer {indexer}")
    return ds.isel(**indexer)


@contextmanager
def open_chunk(chunk_key: ChunkKey, *, config: XarrayZarrRecipe) -> xr.Dataset:
    logger.info(f"Opening inputs for chunk {chunk_key!s}")
    ninputs = config.file_pattern.dims[config.file_pattern.concat_dims[0]]
    inputs = inputs_for_chunk(chunk_key, config.inputs_per_chunk, ninputs)

    # need to open an unknown number of contexts at the same time
    with ExitStack() as stack:
        dsets = [stack.enter_context(open_input(input_key, config=config)) for input_key in inputs]

        # subset before chunking; hopefully lazy
        subset_dims = [dim_idx for dim_idx in chunk_key if dim_idx.operation == CombineOp.SUBSET]

        for subset_dim in subset_dims:
            logger.info(f"Subsetting input according to {subset_dim}")
            dsets = [subset_dataset(ds, subset_dim) for ds in dsets]

        # explicitly chunking prevents eager evaluation during concat
        dsets = [ds.chunk() for ds in dsets]

        logger.info(f"Combining inputs for chunk '{chunk_key!s}'")
        if len(dsets) > 1:
            # During concat, attributes and encoding are taken from the first dataset
            # https://github.com/pydata/xarray/issues/1614
            with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
                ds = xr.concat(dsets, config.concat_dim, **config.xarray_concat_kwargs)
        elif len(dsets) == 1:
            ds = dsets[0]
        else:  # pragma: no cover
            assert False, "Should never happen"

        if config.process_chunk is not None:
            with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
                ds = config.process_chunk(ds)

        with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
            logger.debug(f"{ds}")

        if config.target_chunks:
            # The input may be too large to process in memory at once, so
            # rechunk it to the target chunks.
            ds = ds.chunk(config.target_chunks)
        yield ds


def get_input_meta(
    metadata_cache: Optional[MetadataTarget],
    file_pattern: FilePattern,
) -> Dict:
    # getitems should be async; much faster than serial calls
    if metadata_cache is None:
        raise ValueError("metadata_cache is not set.")
    return metadata_cache.getitems([_input_metadata_fname(k) for k in file_pattern])


def _get_fname_from_error_pos(pos: Tuple[int, int], file_pattern: FilePattern) -> str:
    kwargs = {}
    for idx, p in enumerate(pos):
        dim = file_pattern.combine_dims[idx]
        kwargs.update({dim.name: dim.keys[p]})

    return file_pattern.format_function(**kwargs)


def calculate_sequence_lens(
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    metadata_cache: Optional[MetadataTarget],
) -> List[int]:

    assert len(file_pattern.concat_dims) == 1
    concat_dim = file_pattern.concat_dims[0]

    if nitems_per_input:
        concat_dim = file_pattern.concat_dims[0]
        return list((nitems_per_input,) * file_pattern.dims[concat_dim])

    # read per-input metadata; this is distinct from global metadata
    # get the sequence length of every file
    input_meta = get_input_meta(metadata_cache, file_pattern)
    # use a numpy array to allow reshaping
    all_lens = np.array([m["dims"][concat_dim] for m in input_meta.values()])
    all_lens.shape = list(file_pattern.dims.values())

    if len(all_lens.shape) == 1:
        all_lens = np.expand_dims(all_lens, 1)

    # check that all lens are the same along the concat dim
    unique_vec, unique_idx, unique_counts = np.unique(
        all_lens, axis=1, return_index=True, return_counts=True
    )

    if len(unique_idx) > 1:
        correct_col = all_lens[:, unique_idx[np.argmax(unique_counts)]]
        err_idx = np.where(np.expand_dims(correct_col, 1) != all_lens)
        err_pos = list(zip(*err_idx))
        # present a list of problematic files to the user, given the file pattern.
        file_list = "\n".join(
            [f"- {_get_fname_from_error_pos(epos, file_pattern)!r}" for epos in err_pos]
        )
        raise ValueError(
            f"Inconsistent sequence lengths between indices {unique_idx} of the concat dim."
            f"\nValue(s) {all_lens[err_idx]} at position(s) {err_pos} are different from the rest."
            f"\nPlease check the following file(s) for data errors:"
            f"\n{file_list}"
            f"\n{all_lens}"
        )
    return np.atleast_1d(unique_vec.squeeze()).tolist()


def prepare_target(*, config: XarrayZarrRecipe) -> None:
    if config.storage_config.target is None:
        raise ValueError("Cannot proceed without a target")
    try:
        ds = open_target(config.storage_config.target)
        logger.info("Found an existing dataset in target")
        logger.debug(f"{ds}")

        if config.target_chunks:
            # TODO: check that target_chunks id compatibile with the
            # existing chunks
            pass
    except (FileNotFoundError, IOError, zarr.errors.GroupNotFoundError):
        logger.info("Creating a new dataset in target")

        # need to rewrite this as an append loop

        def filter_init_chunks(chunk_key):
            for dim_idx in chunk_key:
                if (dim_idx.operation != CombineOp.MERGE) and (dim_idx.index > 0):
                    return False
            return True

        init_chunks = list(filter(filter_init_chunks, config.iter_chunks()))

        for chunk_key in init_chunks:
            with open_chunk(chunk_key, config=config) as ds:
                # ds is already chunked

                # https://github.com/pydata/xarray/blob/5287c7b2546fc8848f539bb5ee66bb8d91d8496f/xarray/core/variable.py#L1069
                for v in ds.variables:
                    if config.target_chunks:
                        this_var = ds[v]
                        chunks = {
                            this_var.get_axis_num(dim): chunk
                            for dim, chunk in config.target_chunks.items()
                            if dim in this_var.dims
                        }
                        encoding_chunks = tuple(
                            chunks.get(n, s) for n, s in enumerate(this_var.shape)
                        )
                    else:
                        encoding_chunks = ds[v].shape
                    logger.debug(f"Setting variable {v} encoding chunks to {encoding_chunks}")
                    ds[v].encoding["chunks"] = encoding_chunks

                # load all variables that don't have the sequence dim in them
                # these are usually coordinates.
                # Variables that are loaded will be written even with compute=False
                # TODO: make this behavior customizable
                for v in ds.variables:
                    if config.concat_dim not in ds[v].dims:
                        ds[v].load()

                target_mapper = config.storage_config.target.get_mapper()
                logger.info(f"Storing dataset in {config.storage_config.target.root_path}")
                logger.debug(f"{ds}")
                with warnings.catch_warnings():
                    warnings.simplefilter(
                        "ignore"
                    )  # suppress the warning that comes with safe_chunks
                    ds.to_zarr(target_mapper, mode="a", compute=False, safe_chunks=False)

    # Regardless of whether there is an existing dataset or we are creating a new one,
    # we need to expand the concat_dim to hold the entire expected size of the data
    input_sequence_lens = calculate_sequence_lens(
        config.nitems_per_input,
        config.file_pattern,
        config.storage_config.metadata,
    )
    n_sequence = sum(input_sequence_lens)
    logger.info(f"Expanding target concat dim '{config.concat_dim}' to size {n_sequence}")
    expand_target_dim(config.storage_config.target, config.concat_dim, n_sequence)
    # TODO: handle possible subsetting
    # The init chunks might not cover the whole dataset along multiple dimensions!

    if config.cache_metadata:
        # if nitems_per_input is not constant, we need to cache this info
        assert config.storage_config.metadata is not None  # for mypy
        recipe_meta = {"input_sequence_lens": input_sequence_lens}
        config.storage_config.metadata[_GLOBAL_METADATA_KEY] = recipe_meta

    zgroup = zarr.open_group(config.target_mapper)
    for k, v in config.get_execution_context().items():
        zgroup.attrs[f"pangeo-forge:{k}"] = v


def store_chunk(chunk_key: ChunkKey, *, config: XarrayZarrRecipe) -> None:
    if config.storage_config.target is None:
        raise ValueError("target has not been set.")

    with open_chunk(chunk_key, config=config) as ds_chunk:
        # writing a region means that all the variables MUST have concat_dim
        to_drop = [v for v in ds_chunk.variables if config.concat_dim not in ds_chunk[v].dims]
        ds_chunk = ds_chunk.drop_vars(to_drop)

        target_mapper = config.storage_config.target.get_mapper()
        write_region, conflicts = region_and_conflicts_for_chunk(config, chunk_key)

        zgroup = zarr.open_group(target_mapper)
        for vname, var_coded in ds_chunk.variables.items():
            zarr_array = zgroup[vname]
            # get encoding for variable from zarr attributes
            # could this backfire some way?
            var_coded.encoding.update(zarr_array.attrs)
            # just delete all attributes from the var;
            # they are not used anyway, and there can be conflicts
            # related to xarray.coding.variables.safe_setitem
            var_coded.attrs = {}
            with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
                var = xr.backends.zarr.encode_zarr_variable(var_coded)
                logger.debug(
                    f"Converting variable {vname} of {var.data.nbytes} bytes to `numpy.ndarray`"
                )
                if var.data.nbytes > MAX_MEMORY:
                    factor = round((var.data.nbytes / MAX_MEMORY), 2)
                    cdim = config.concat_dim
                    logger.warning(
                        f"Variable {vname} of {var.data.nbytes} bytes is {factor} times larger "
                        f"than specified maximum variable array size of {MAX_MEMORY} bytes. "
                        f'Consider re-instantiating recipe with `subset_inputs = {{"{cdim}": '
                        f'{ceil(factor)}}}`. If `len(ds["{cdim}"])` < {ceil(factor)}, '
                        f'substitute "{cdim}" for any name in ds["{vname}"].dims with length '
                        f">= {ceil(factor)} or consider subsetting along multiple dimensions."
                        " Setting PANGEO_FORGE_MAX_MEMORY env variable changes the variable array"
                        " size which will trigger this warning."
                    )
                data = np.asarray(
                    var.data
                )  # TODO: can we buffer large data rather than loading it all?
            zarr_region = tuple(write_region.get(dim, slice(None)) for dim in var.dims)
            lock_keys = [
                f"{vname}-{dim}-{c}"
                for dim, dim_conflicts in conflicts.items()
                for c in dim_conflicts
            ]
            logger.debug(f"Acquiring locks {lock_keys}")
            with lock_for_conflicts(lock_keys, timeout=config.lock_timeout):
                logger.info(
                    f"Storing variable {vname} chunk {chunk_key!s} " f"to Zarr region {zarr_region}"
                )
                zarr_array[zarr_region] = data


def _gather_coordinate_dimensions(group: zarr.Group) -> List[str]:
    return list(
        set(itertools.chain(*(group[var].attrs.get(_ARRAY_DIMENSIONS, []) for var in group)))
    )


def finalize_target(*, config: XarrayZarrRecipe) -> None:
    if config.storage_config.target is None:
        raise ValueError("target has not been set.")

    if config.consolidate_dimension_coordinates:
        logger.info("Consolidating dimension coordinate arrays")
        target_mapper = config.storage_config.target.get_mapper()
        group = zarr.open(target_mapper, mode="a")
        # https://github.com/pangeo-forge/pangeo-forge-recipes/issues/214
        # filter out the dims from the array metadata not in the Zarr group
        # to handle coordinateless dimensions.
        dims = (dim for dim in _gather_coordinate_dimensions(group) if dim in group)
        for dim in dims:
            arr = group[dim]
            attrs = dict(arr.attrs)
            new = group.array(
                dim,
                arr[:],
                chunks=arr.shape,
                dtype=arr.dtype,
                compressor=arr.compressor,
                fill_value=arr.fill_value,
                order=arr.order,
                filters=arr.filters,
                overwrite=True,
            )
            new.attrs.update(attrs)

    if config.consolidate_zarr:
        logger.info("Consolidating Zarr metadata")
        target_mapper = config.storage_config.target.get_mapper()
        zarr.consolidate_metadata(target_mapper)


def xarray_zarr_recipe_compiler(recipe: XarrayZarrRecipe) -> Pipeline:
    stages = [
        Stage(name="cache_input", function=cache_input, mappable=list(recipe.iter_inputs())),
        Stage(name="prepare_target", function=prepare_target),
        Stage(name="store_chunk", function=store_chunk, mappable=list(recipe.iter_chunks())),
        Stage(name="finalize_target", function=finalize_target),
    ]
    return Pipeline(stages=stages, config=recipe)


# Notes about dataclasses:
# - https://www.python.org/dev/peps/pep-0557/#inheritance
# - https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses


_deprecation_message = (
    "This method will be deprecated in v0.8.0. "
    "Please call the equivalent function directly from the xarray_zarr module."
)


@dataclass
class XarrayZarrRecipe(BaseRecipe, StorageMixin, FilePatternMixin):
    """This configuration represents a dataset composed of many individual NetCDF files.
    This class uses Xarray to read and write data and writes its output to Zarr.
    The organization of the source files is described by the ``file_pattern``.
    Currently this recipe supports at most one ``MergeDim`` and one ``ConcatDim``
    in the File Pattern.

    :param file_pattern: An object which describes the organization of the input files.
    :param inputs_per_chunk: The number of inputs to use in each chunk along the concat dim.
       Must be an integer >= 1.
    :param target_chunks: Desired chunk structure for the targret dataset. This is a dictionary
       mapping dimension names to chunk size. When using a :class:`patterns.FilePattern` with
       a :class:`patterns.ConcatDim` that specifies ``n_items_per_file``, then you don't need
       to include the concat dim in ``target_chunks``.
    :param storage_config: Defines locations for writing the output dataset, caching temporary data,
      and for caching metadata for inputs and chunks. All three locations default to
      ``tempdir.TemporaryDirectory``; this default config can be used for testing and debugging the
      recipe. In an actual execution context, the default config is re-assigned to point to the
      destination(s) of choice, which can be any combination of ``fsspec``-compatible storage
      backends.
    :param cache_inputs: If ``True``, inputs are copied to ``input_cache`` before
      opening. If ``False``, try to open inputs directly from their source location.
    :param copy_input_to_local_file: Whether to copy the inputs to a temporary
      local file. In this case, a path (rather than file object) is passed to
      ``xr.open_dataset``. This is required for engines that can't open
      file-like objects (e.g. pynio).
    :param consolidate_zarr: Whether to consolidate the resulting Zarr dataset.
    :param consolidate_dimension_coordinates: Whether to rewrite coordinate variables as a
        single chunk. We recommend consolidating coordinate variables to avoid
        many small read requests to get the coordinates in xarray.
    :param xarray_open_kwargs: Extra options for opening the inputs with Xarray.
    :param xarray_concat_kwargs: Extra options to pass to Xarray when concatenating
      the inputs to form a chunk.
    :param delete_input_encoding: Whether to remove Xarray encoding from variables
      in the input dataset
    :param process_input: Function to call on each opened input, with signature
      `(ds: xr.Dataset, filename: str) -> ds: xr.Dataset`.
    :param process_chunk: Function to call on each concatenated chunk, with signature
      `(ds: xr.Dataset) -> ds: xr.Dataset`.
    :param lock_timeout: The default timeout for acquiring a chunk lock.
    :param subset_inputs: If set, break each input file up into multiple chunks
      along dimension according to the specified mapping. For example,
      ``{'time': 5}`` would split each input file into 5 chunks along the
      time dimension. Multiple dimensions are allowed.
    :param open_input_with_kerchunk: If True, use kerchunk
      to generate a reference filesystem for each input, to be used when opening
      the file with Xarray as a virtual Zarr dataset.
    """

    _compiler = xarray_zarr_recipe_compiler

    inputs_per_chunk: int = 1
    target_chunks: Dict[str, int] = field(default_factory=dict)
    cache_inputs: Optional[bool] = None
    copy_input_to_local_file: bool = False
    consolidate_zarr: bool = True
    consolidate_dimension_coordinates: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_kwargs: dict = field(default_factory=dict)
    delete_input_encoding: bool = True
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]] = None
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]] = None
    lock_timeout: Optional[int] = None
    subset_inputs: SubsetSpec = field(default_factory=dict)
    open_input_with_kerchunk: bool = False

    # internal attributes not meant to be seen or accessed by user
    concat_dim: str = field(default_factory=str, repr=False, init=False)
    """The concatenation dimension name."""

    concat_dim_chunks: Optional[int] = field(default=None, repr=False, init=False)
    """The desired chunking along the sequence dimension."""

    init_chunks: List[ChunkKey] = field(default_factory=list, repr=False, init=False)
    """List of chunks needed to initialize the recipe."""

    cache_metadata: bool = field(default=False, repr=False, init=False)
    """Whether metadata caching is needed."""

    nitems_per_input: Optional[int] = field(default=None, repr=False, init=False)
    """How many items per input along concat_dim."""

    def __post_init__(self):
        self._validate_file_pattern()

        # from here on we know there is at most one merge dim and one concat dim
        self.concat_dim = self.file_pattern.concat_dims[0]
        self.cache_metadata = any(
            [v is None for v in self.file_pattern.concat_sequence_lens.values()]
        )
        self.nitems_per_input = self.file_pattern.nitems_per_input[self.concat_dim]

        if self.file_pattern.file_type == FileType.opendap:
            if self.cache_inputs:
                raise ValueError("Can't cache opendap inputs.")
            else:
                self.cache_inputs = False
            if self.open_input_with_kerchunk:
                raise ValueError("Can't generate references on opendap inputs")
            if "engine" in self.xarray_open_kwargs:
                if self.xarray_open_kwargs["engine"] != "netcdf4":
                    raise ValueError(
                        "Opendap inputs only work with `xarray_open_kwargs['engine'] == 'netcdf4'`"
                    )
            else:
                new_kw = self.xarray_open_kwargs.copy()
                new_kw["engine"] = "netcdf4"
                self.xarray_open_kwargs = new_kw
        elif self.cache_inputs is None:
            self.cache_inputs = True  # old defult

        self._validate_input_and_chunk_keys()

        # set concat_dim_chunks
        target_concat_dim_chunks = self.target_chunks.get(self.concat_dim)
        if (self.nitems_per_input is None) and (target_concat_dim_chunks is None):
            raise ValueError(
                "Unable to determine target chunks. Please specify either "
                "`target_chunks` or `nitems_per_input`"
            )
        elif target_concat_dim_chunks:
            self.concat_dim_chunks = target_concat_dim_chunks
        else:
            self.concat_dim_chunks = self.nitems_per_input * self.inputs_per_chunk

    def _validate_file_pattern(self):
        if len(self.file_pattern.merge_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one merge dim.")
        if len(self.file_pattern.concat_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one concat dim.")

    def _validate_input_and_chunk_keys(self):
        all_input_keys = set(self.iter_inputs())
        ninputs = self.file_pattern.dims[self.file_pattern.concat_dims[0]]
        all_inputs_for_chunks = set(
            list(
                chain(
                    *(
                        inputs_for_chunk(chunk_key, self.inputs_per_chunk, ninputs)
                        for chunk_key in self.iter_chunks()
                    )
                )
            )
        )
        if all_input_keys != all_inputs_for_chunks:
            chunk_key = next(iter(self.iter_chunks()))
            print("First chunk", chunk_key)
            print("Inputs_for_chunk", inputs_for_chunk(chunk_key, self.inputs_per_chunk, ninputs))
            raise ValueError("Inputs and chunks are inconsistent")

    def iter_inputs(self) -> Iterator[InputKey]:
        yield from self.file_pattern

    def iter_chunks(self) -> Iterator[ChunkKey]:
        for input_key in self.iter_inputs():
            concat_dims = [
                dim_idx for dim_idx in input_key if dim_idx.operation == CombineOp.CONCAT
            ]
            assert len(concat_dims) == 1
            concat_dim = concat_dims[0]
            input_concat_index = concat_dim.index
            if input_concat_index % self.inputs_per_chunk > 0:
                continue  # don't emit a chunk
            chunk_concat_index = input_concat_index // self.inputs_per_chunk
            chunk_sequence_len = ceil(concat_dim.sequence_len / self.inputs_per_chunk)
            chunk_concat_dim = replace(
                concat_dim, index=chunk_concat_index, sequence_len=chunk_sequence_len
            )
            chunk_key_base = [
                chunk_concat_dim if dim_idx.operation == CombineOp.CONCAT else dim_idx
                for dim_idx in input_key
            ]
            if len(self.subset_inputs) == 0:
                yield Index(chunk_key_base)
                # no subsets
                continue
            subset_iterators = [range(v) for k, v in self.subset_inputs.items()]
            for i in product(*subset_iterators):
                # TODO: remove redundant name
                subset_dims = [
                    DimIndex(*args, CombineOp.SUBSET)
                    for args in zip(self.subset_inputs.keys(), i, self.subset_inputs.values())
                ]
                yield Index((chunk_key_base + subset_dims))
