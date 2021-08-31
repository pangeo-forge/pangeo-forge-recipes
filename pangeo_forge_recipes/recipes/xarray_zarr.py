"""
A Pangeo Forge Recipe
"""

import functools
import logging
import os
import warnings
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from itertools import chain, product
from math import ceil
from typing import Callable, Dict, Hashable, Iterator, List, Optional, Sequence, Set, Tuple

import dask
import numpy as np
import xarray as xr
import zarr

from ..chunk_grid import ChunkGrid
from ..patterns import CombineOp, DimIndex, FilePattern, Index, prune_pattern
from ..storage import AbstractTarget, CacheFSSpecTarget, MetadataTarget, file_opener
from ..utils import calc_subsets, fix_scalar_attr_encoding, lock_for_conflicts
from .base import BaseRecipe

# use this filename to store global recipe metadata in the metadata_cache
# it will be written once (by prepare_target) and read many times (by store_chunk)
_GLOBAL_METADATA_KEY = "pangeo-forge-recipe-metadata.json"
MAX_MEMORY = (
    int(os.getenv("PANGEO_FORGE_MAX_MEMORY"))  # type: ignore
    if os.getenv("PANGEO_FORGE_MAX_MEMORY")
    else 500_000_000
)

logger = logging.getLogger(__name__)

# Some types that help us keep things organized
InputKey = Index  # input keys are the same as the file pattern keys
ChunkKey = Index

SubsetSpec = Dict[str, int]
# SubsetSpec is a dictionary mapping dimension names to the number of subsets along that dimension
# (e.g. {'time': 5, 'depth': 2})


def _input_metadata_fname(input_key):
    key_str = "-".join([f"{k.name}_{k.index}" for k in input_key])
    return "input-meta-" + key_str + ".json"


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


def expand_target_dim(target: CacheFSSpecTarget, concat_dim: Optional[str], dimsize: int) -> None:
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

    # now explicity write the sequence coordinate to avoid missing data
    # when reopening
    if concat_dim in zgroup:
        zgroup[concat_dim][:] = 0


def open_target(target: CacheFSSpecTarget) -> xr.Dataset:
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


def cache_input_metadata(
    input_key: InputKey,
    metadata_cache: Optional[MetadataTarget],
    file_pattern: FilePattern,
    input_cache: Optional[CacheFSSpecTarget],
    cache_inputs: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
) -> None:
    if metadata_cache is None:
        raise ValueError("metadata_cache is not set.")
    logger.info(f"Caching metadata for input '{input_key!s}'")
    with open_input(
        input_key,
        file_pattern=file_pattern,
        input_cache=input_cache,
        cache_inputs=cache_inputs,
        copy_input_to_local_file=copy_input_to_local_file,
        xarray_open_kwargs=xarray_open_kwargs,
        delete_input_encoding=delete_input_encoding,
        process_input=process_input,
    ) as ds:
        input_metadata = ds.to_dict(data=False)
        metadata_cache[_input_metadata_fname(input_key)] = input_metadata


def cache_input(
    input_key: InputKey,
    cache_inputs: bool,
    input_cache: Optional[CacheFSSpecTarget],
    file_pattern: FilePattern,
    cache_metadata: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
    metadata_cache: Optional[MetadataTarget],
) -> None:
    if cache_inputs:
        if file_pattern.is_opendap:
            raise ValueError("Can't cache opendap inputs")
        if input_cache is None:
            raise ValueError("input_cache is not set.")
        logger.info(f"Caching input '{input_key!s}'")
        fname = file_pattern[input_key]
        input_cache.cache_file(
            fname, file_pattern.query_string_secrets, **file_pattern.fsspec_open_kwargs
        )

    if cache_metadata:
        return cache_input_metadata(
            input_key,
            file_pattern=file_pattern,
            input_cache=input_cache,
            cache_inputs=cache_inputs,
            copy_input_to_local_file=copy_input_to_local_file,
            xarray_open_kwargs=xarray_open_kwargs,
            delete_input_encoding=delete_input_encoding,
            process_input=process_input,
            metadata_cache=metadata_cache,
        )


def region_and_conflicts_for_chunk(
    chunk_key: ChunkKey,
    inputs_per_chunk: int,
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    concat_dim_chunks: Optional[int],
    concat_dim: str,
    metadata_cache: Optional[MetadataTarget],
    subset_inputs: Optional[SubsetSpec],
) -> Tuple[Dict[str, slice], Dict[str, Set[int]]]:
    # return a dict suitable to pass to xr.to_zarr(region=...)
    # specifies where in the overall array to put this chunk's data
    # also return the conflicts with other chunks

    if nitems_per_input:
        input_sequence_lens = (nitems_per_input,) * file_pattern.dims[concat_dim]  # type: ignore
    else:
        assert metadata_cache is not None  # for mypy
        global_metadata = metadata_cache[_GLOBAL_METADATA_KEY]
        input_sequence_lens = global_metadata["input_sequence_lens"]
    total_len = sum(input_sequence_lens)

    # for now this will just have one key since we only allow one concat_dim
    # but it could expand to accomodate multiple concat dims
    chunk_index = {concat_dim: chunk_position(chunk_key)}

    input_chunk_grid = ChunkGrid({concat_dim: input_sequence_lens})
    if subset_inputs and concat_dim in subset_inputs:
        assert (
            inputs_per_chunk == 1
        ), "Doesn't make sense to have multiple inputs per chunk plus subsetting"
        chunk_grid = input_chunk_grid.subset(subset_inputs)
    elif inputs_per_chunk > 1:
        chunk_grid = input_chunk_grid.consolidate({concat_dim: inputs_per_chunk})
    else:
        chunk_grid = input_chunk_grid
    assert chunk_grid.shape[concat_dim] == total_len

    region = chunk_grid.chunk_index_to_array_slice(chunk_index)

    assert concat_dim_chunks is not None
    target_grid = ChunkGrid.from_uniform_grid({concat_dim: (concat_dim_chunks, total_len)})
    conflicts = chunk_grid.chunk_conflicts(chunk_index, target_grid)

    return region, conflicts


@contextmanager
def open_input(
    input_key: InputKey,
    file_pattern: FilePattern,
    input_cache: Optional[CacheFSSpecTarget],
    cache_inputs: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
) -> xr.Dataset:
    fname = file_pattern[input_key]
    logger.info(f"Opening input with Xarray {input_key!s}: '{fname}'")

    if file_pattern.is_opendap:
        if input_cache:
            raise ValueError("Can't cache opendap inputs")
        if copy_input_to_local_file:
            raise ValueError("Can't copy opendap inputs to local file")

    cache = input_cache if cache_inputs else None

    with file_opener(
        fname,
        cache=cache,
        copy_to_local=copy_input_to_local_file,
        bypass_open=file_pattern.is_opendap,
        secrets=file_pattern.query_string_secrets,
        **file_pattern.fsspec_open_kwargs,
    ) as f:
        with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
            kw = xarray_open_kwargs.copy()
            if "engine" not in kw:
                kw["engine"] = "h5netcdf"
            logger.debug(f"about to enter xr.open_dataset context on {f}")
            with xr.open_dataset(f, **kw) as ds:
                logger.debug("successfully opened dataset")
                ds = fix_scalar_attr_encoding(ds)

                if delete_input_encoding:
                    for var in ds.variables:
                        ds[var].encoding = {}

                if process_input is not None:
                    ds = process_input(ds, str(fname))

                logger.debug(f"{ds}")
                yield ds


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
def open_chunk(
    chunk_key: ChunkKey,
    inputs_per_chunk: int,
    concat_dim: Optional[str],
    xarray_concat_kwargs: dict,
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]],
    target_chunks: Dict[str, int],
    file_pattern: FilePattern,
    input_cache: Optional[CacheFSSpecTarget],
    cache_inputs: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
) -> xr.Dataset:
    logger.info(f"Opening inputs for chunk {chunk_key!s}")
    ninputs = file_pattern.dims[file_pattern.concat_dims[0]]
    inputs = inputs_for_chunk(chunk_key, inputs_per_chunk, ninputs)

    # need to open an unknown number of contexts at the same time
    with ExitStack() as stack:
        dsets = [
            stack.enter_context(
                open_input(
                    i,
                    file_pattern=file_pattern,
                    input_cache=input_cache,
                    cache_inputs=cache_inputs,
                    copy_input_to_local_file=copy_input_to_local_file,
                    xarray_open_kwargs=xarray_open_kwargs,
                    delete_input_encoding=delete_input_encoding,
                    process_input=process_input,
                )
            )
            for i in inputs
        ]

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
                ds = xr.concat(dsets, concat_dim, **xarray_concat_kwargs)
        elif len(dsets) == 1:
            ds = dsets[0]
        else:  # pragma: no cover
            assert False, "Should never happen"

        if process_chunk is not None:
            with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
                ds = process_chunk(ds)

        with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
            logger.debug(f"{ds}")

        if target_chunks:
            # The input may be too large to process in memory at once, so
            # rechunk it to the target chunks.
            ds = ds.chunk(target_chunks)
        yield ds


def get_input_meta(metadata_cache: Optional[MetadataTarget], *input_keys: InputKey,) -> Dict:
    # getitems should be async; much faster than serial calls
    if metadata_cache is None:
        raise ValueError("metadata_cache is not set.")
    return metadata_cache.getitems([_input_metadata_fname(k) for k in input_keys])


def calculate_sequence_lens(
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    metadata_cache: Optional[MetadataTarget],
) -> List[int]:

    assert len(file_pattern.concat_dims) == 1
    concat_dim = file_pattern.concat_dims[0]

    if nitems_per_input:
        return list((nitems_per_input,) * file_pattern.dims[concat_dim])

    # read per-input metadata; this is distinct from global metadata
    # get the sequence length of every file
    # this line could become problematic for large (> 10_000) lists of files
    input_meta = get_input_meta(metadata_cache, *file_pattern)
    # use a numpy array to allow reshaping
    all_lens = np.array([m["dims"][concat_dim] for m in input_meta.values()])
    all_lens.shape = list(file_pattern.dims.values())

    # check that all lens are the same along the concat dim
    concat_dim_axis = list(file_pattern.dims).index(concat_dim)
    selector = [slice(0, 1)] * len(file_pattern.dims)
    selector[concat_dim_axis] = slice(None)  # this should broadcast correctly agains all_lens
    sequence_lens = all_lens[tuple(selector)]
    if not (all_lens == sequence_lens).all():
        raise ValueError(f"Inconsistent sequence lengths found: f{all_lens}")
    return np.atleast_1d(sequence_lens.squeeze()).tolist()


def prepare_target(
    target: CacheFSSpecTarget,
    target_chunks: Dict[str, int],
    init_chunks: List[ChunkKey],
    concat_dim: Optional[str],
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    inputs_per_chunk: int,
    cache_metadata: bool,
    xarray_concat_kwargs: dict,
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]],
    input_cache: Optional[CacheFSSpecTarget],
    cache_inputs: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
    metadata_cache: Optional[MetadataTarget],
) -> None:
    try:
        ds = open_target(target)
        logger.info("Found an existing dataset in target")
        logger.debug(f"{ds}")

        if target_chunks:
            # TODO: check that target_chunks id compatibile with the
            # existing chunks
            pass
    except (FileNotFoundError, IOError, zarr.errors.GroupNotFoundError):
        logger.info("Creating a new dataset in target")

        # need to rewrite this as an append loop
        for chunk_key in init_chunks:
            with open_chunk(
                chunk_key=chunk_key,
                inputs_per_chunk=inputs_per_chunk,
                concat_dim=concat_dim,
                xarray_concat_kwargs=xarray_concat_kwargs,
                process_chunk=process_chunk,
                target_chunks=target_chunks,
                file_pattern=file_pattern,
                input_cache=input_cache,
                cache_inputs=cache_inputs,
                copy_input_to_local_file=copy_input_to_local_file,
                xarray_open_kwargs=xarray_open_kwargs,
                delete_input_encoding=delete_input_encoding,
                process_input=process_input,
            ) as ds:
                # ds is already chunked

                # https://github.com/pydata/xarray/blob/5287c7b2546fc8848f539bb5ee66bb8d91d8496f/xarray/core/variable.py#L1069
                for v in ds.variables:
                    if target_chunks:
                        this_var = ds[v]
                        chunks = {
                            this_var.get_axis_num(dim): chunk
                            for dim, chunk in target_chunks.items()
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
                    if concat_dim not in ds[v].dims:
                        ds[v].load()

                target_mapper = target.get_mapper()
                logger.info(f"Storing dataset in {target.root_path}")
                logger.debug(f"{ds}")
                with warnings.catch_warnings():
                    warnings.simplefilter(
                        "ignore"
                    )  # suppress the warning that comes with safe_chunks
                    ds.to_zarr(target_mapper, mode="a", compute=False, safe_chunks=False)

    # Regardless of whether there is an existing dataset or we are creating a new one,
    # we need to expand the concat_dim to hold the entire expected size of the data
    input_sequence_lens = calculate_sequence_lens(
        nitems_per_input, file_pattern, metadata_cache=metadata_cache,
    )
    n_sequence = sum(input_sequence_lens)
    logger.info(f"Expanding target concat dim '{concat_dim}' to size {n_sequence}")
    expand_target_dim(target, concat_dim, n_sequence)
    # TODO: handle possible subsetting
    # The init chunks might not cover the whole dataset along multiple dimensions!

    if cache_metadata:
        # if nitems_per_input is not constant, we need to cache this info
        assert metadata_cache is not None  # for mypy
        recipe_meta = {"input_sequence_lens": input_sequence_lens}
        metadata_cache[_GLOBAL_METADATA_KEY] = recipe_meta


def store_chunk(
    chunk_key: ChunkKey,
    target: CacheFSSpecTarget,
    concat_dim: str,
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    inputs_per_chunk: int,
    subset_inputs: SubsetSpec,
    concat_dim_chunks: Optional[int],
    lock_timeout: Optional[int],
    xarray_concat_kwargs: dict,
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]],
    target_chunks: Dict[str, int],
    input_cache: Optional[CacheFSSpecTarget],
    cache_inputs: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
    metadata_cache: Optional[MetadataTarget],
) -> None:
    if target is None:
        raise ValueError("target has not been set.")

    with open_chunk(
        chunk_key=chunk_key,
        inputs_per_chunk=inputs_per_chunk,
        concat_dim=concat_dim,
        xarray_concat_kwargs=xarray_concat_kwargs,
        process_chunk=process_chunk,
        target_chunks=target_chunks,
        file_pattern=file_pattern,
        input_cache=input_cache,
        cache_inputs=cache_inputs,
        copy_input_to_local_file=copy_input_to_local_file,
        xarray_open_kwargs=xarray_open_kwargs,
        delete_input_encoding=delete_input_encoding,
        process_input=process_input,
    ) as ds_chunk:
        # writing a region means that all the variables MUST have concat_dim
        to_drop = [v for v in ds_chunk.variables if concat_dim not in ds_chunk[v].dims]
        ds_chunk = ds_chunk.drop_vars(to_drop)

        target_mapper = target.get_mapper()
        write_region, conflicts = region_and_conflicts_for_chunk(
            chunk_key=chunk_key,
            inputs_per_chunk=inputs_per_chunk,
            nitems_per_input=nitems_per_input,
            file_pattern=file_pattern,
            concat_dim_chunks=concat_dim_chunks,
            concat_dim=concat_dim,
            metadata_cache=metadata_cache,
            subset_inputs=subset_inputs,
        )

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
                    logger.warning(
                        f"Variable {vname} of {var.data.nbytes} bytes is {factor} times larger "
                        f"than specified maximum variable array size of {MAX_MEMORY} bytes. "
                        f'Consider re-instantiating recipe with `subset_inputs = {{"{concat_dim}": '
                        f'{ceil(factor)}}}`. If `len(ds["{concat_dim}"])` < {ceil(factor)}, '
                        f'substitute "{concat_dim}" for any name in ds["{vname}"].dims with length '
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
            with lock_for_conflicts(lock_keys, timeout=lock_timeout):
                logger.info(
                    f"Storing variable {vname} chunk {chunk_key!s} " f"to Zarr region {zarr_region}"
                )
                zarr_array[zarr_region] = data


def finalize_target(target: CacheFSSpecTarget, consolidate_zarr: bool) -> None:
    if target is None:
        raise ValueError("target has not been set.")
    if consolidate_zarr:
        logger.info("Consolidating Zarr metadata")
        target_mapper = target.get_mapper()
        zarr.consolidate_metadata(target_mapper)


# Notes about dataclasses:
# - https://www.python.org/dev/peps/pep-0557/#inheritance
# - https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses


@dataclass
class XarrayZarrRecipe(BaseRecipe):
    """This class represents a dataset composed of many individual NetCDF files.
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
    :param target: A location in which to put the dataset. Can also be assigned at run time.
    :param input_cache: A location in which to cache temporary data.
    :param metadata_cache: A location in which to cache metadata for inputs and chunks.
      Required if ``nitems_per_file=None`` on concat dim in file pattern.
    :param cache_inputs: If ``True``, inputs are copied to ``input_cache`` before
      opening. If ``False``, try to open inputs directly from their source location.
    :param copy_input_to_local_file: Whether to copy the inputs to a temporary
      local file. In this case, a path (rather than file object) is passed to
      ``xr.open_dataset``. This is required for engines that can't open
      file-like objects (e.g. pynio).
    :param consolidate_zarr: Whether to consolidate the resulting Zarr dataset.
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
    """

    file_pattern: FilePattern
    inputs_per_chunk: int = 1
    target_chunks: Dict[str, int] = field(default_factory=dict)
    target: Optional[AbstractTarget] = None
    input_cache: Optional[CacheFSSpecTarget] = None
    metadata_cache: Optional[MetadataTarget] = None
    cache_inputs: Optional[bool] = None
    copy_input_to_local_file: bool = False
    consolidate_zarr: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_kwargs: dict = field(default_factory=dict)
    delete_input_encoding: bool = True
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]] = None
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]] = None
    lock_timeout: Optional[int] = None
    subset_inputs: SubsetSpec = field(default_factory=dict)

    # internal attributes not meant to be seen or accessed by user
    _concat_dim: str = field(default_factory=str, repr=False, init=False)
    """The concatenation dimension name."""

    _concat_dim_chunks: Optional[int] = field(default=None, repr=False, init=False)
    """The desired chunking along the sequence dimension."""

    _init_chunks: List[ChunkKey] = field(default_factory=list, repr=False, init=False)
    """List of chunks needed to initialize the recipe."""

    def __post_init__(self):
        self._validate_file_pattern()
        # from here on we know there is at most one merge dim and one concat dim
        self._concat_dim = self.file_pattern.concat_dims[0]
        self._cache_metadata = any(
            [v is None for v in self.file_pattern.concat_sequence_lens.values()]
        )
        self._nitems_per_input = self.file_pattern.nitems_per_input[self._concat_dim]

        if self.file_pattern.is_opendap:
            if self.cache_inputs:
                raise ValueError("Can't cache opendap inputs.")
            else:
                self.cache_inputs = False
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

        def filter_init_chunks(chunk_key):
            for dim_idx in chunk_key:
                if (dim_idx.operation != CombineOp.MERGE) and (dim_idx.index > 0):
                    return False
            return True

        self._init_chunks = list(filter(filter_init_chunks, self.iter_chunks()))

        self._validate_input_and_chunk_keys()
        self._set_target_chunks()

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

    def copy_pruned(self, nkeep: int = 2) -> BaseRecipe:
        """Make a copy of this recipe with a pruned file pattern.

        :param nkeep: The number of items to keep from each ConcatDim sequence.
        """

        new_pattern = prune_pattern(self.file_pattern, nkeep=nkeep)
        return replace(self, file_pattern=new_pattern)

    # below here are methods that are part of recipe execution

    def _set_target_chunks(self):
        target_concat_dim_chunks = self.target_chunks.get(self._concat_dim)
        if (self._nitems_per_input is None) and (target_concat_dim_chunks is None):
            raise ValueError(
                "Unable to determine target chunks. Please specify either "
                "`target_chunks` or `nitems_per_input`"
            )
        elif target_concat_dim_chunks:
            self._concat_dim_chunks = target_concat_dim_chunks
        else:
            self._concat_dim_chunks = self._nitems_per_input * self.inputs_per_chunk

    # Each stage of the recipe follows the same pattern:
    # 1. A top-level function, e.g. `prepare_target`, that does the actual work.
    # 2. A public property, e.g. `.prepare_target`, that calls the partially applied function
    #    with the provided arguments if any (e.g. a chunk_key)
    # This ensures that the actual function objects shipped to and executed on
    # workers do not contain any references to the `recipe` object itself, which is complicated
    # to serialize.

    @property
    def prepare_target(self) -> Callable[[], None]:
        return functools.partial(
            prepare_target,
            target=self.target,
            target_chunks=self.target_chunks,
            init_chunks=self._init_chunks,
            concat_dim=self._concat_dim,
            nitems_per_input=self._nitems_per_input,
            file_pattern=self.file_pattern,
            inputs_per_chunk=self.inputs_per_chunk,
            cache_metadata=self._cache_metadata,
            xarray_concat_kwargs=self.xarray_concat_kwargs,
            process_chunk=self.process_chunk,
            input_cache=self.input_cache,
            cache_inputs=self.cache_inputs,
            copy_input_to_local_file=self.copy_input_to_local_file,
            xarray_open_kwargs=self.xarray_open_kwargs,
            delete_input_encoding=self.delete_input_encoding,
            process_input=self.process_input,
            metadata_cache=self.metadata_cache,
        )

    @property
    def cache_input(self) -> Callable[[Hashable], None]:
        return functools.partial(
            cache_input,
            cache_inputs=self.cache_inputs,
            input_cache=self.input_cache,
            file_pattern=self.file_pattern,
            cache_metadata=self._cache_metadata,
            copy_input_to_local_file=self.copy_input_to_local_file,
            xarray_open_kwargs=self.xarray_open_kwargs,
            delete_input_encoding=self.delete_input_encoding,
            process_input=self.process_input,
            metadata_cache=self.metadata_cache,
        )

    @property
    def store_chunk(self) -> Callable[[Hashable], None]:
        return functools.partial(
            store_chunk,
            target=self.target,
            concat_dim=self._concat_dim,
            nitems_per_input=self._nitems_per_input,
            file_pattern=self.file_pattern,
            inputs_per_chunk=self.inputs_per_chunk,
            subset_inputs=self.subset_inputs,
            concat_dim_chunks=self._concat_dim_chunks,
            lock_timeout=self.lock_timeout,
            xarray_concat_kwargs=self.xarray_concat_kwargs,
            xarray_open_kwargs=self.xarray_open_kwargs,
            process_chunk=self.process_chunk,
            target_chunks=self.target_chunks,
            input_cache=self.input_cache,
            cache_inputs=self.cache_inputs,
            copy_input_to_local_file=self.copy_input_to_local_file,
            delete_input_encoding=self.delete_input_encoding,
            process_input=self.process_input,
            metadata_cache=self.metadata_cache,
        )

    @property
    def finalize_target(self) -> Callable[[], None]:
        return functools.partial(
            finalize_target, target=self.target, consolidate_zarr=self.consolidate_zarr
        )

    def iter_inputs(self) -> Iterator[InputKey]:
        for input_key in self.file_pattern:
            yield input_key

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

    def inputs_for_chunk(self, chunk_key: ChunkKey) -> Sequence[InputKey]:
        """Convenience function for users to introspect recipe."""
        ninputs = self.file_pattern.dims[self.file_pattern.concat_dims[0]]
        return inputs_for_chunk(chunk_key, self.inputs_per_chunk, ninputs)

    # ------------------------------------------------------------------------
    # Convenience methods
    @contextmanager
    def open_input(self, input_key):
        with open_input(
            input_key,
            file_pattern=self.file_pattern,
            input_cache=self.input_cache,
            cache_inputs=self.cache_inputs,
            copy_input_to_local_file=self.copy_input_to_local_file,
            xarray_open_kwargs=self.xarray_open_kwargs,
            delete_input_encoding=self.delete_input_encoding,
            process_input=self.process_input,
        ) as ds:
            yield ds

    @contextmanager
    def open_chunk(self, chunk_key):
        with open_chunk(
            chunk_key,
            inputs_per_chunk=self.inputs_per_chunk,
            concat_dim=self._concat_dim,
            xarray_concat_kwargs=self.xarray_concat_kwargs,
            process_chunk=self.process_chunk,
            target_chunks=self.target_chunks,
            file_pattern=self.file_pattern,
            input_cache=self.input_cache,
            cache_inputs=self.cache_input,
            copy_input_to_local_file=self.copy_input_to_local_file,
            xarray_open_kwargs=self.xarray_open_kwargs,
            delete_input_encoding=self.delete_input_encoding,
            process_input=self.process_input,
        ) as ds:
            yield ds
