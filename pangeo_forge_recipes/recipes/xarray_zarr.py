"""
A Pangeo Forge Recipe
"""

import functools
import logging
import warnings
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field, replace
from itertools import product
from typing import Callable, Dict, Hashable, List, Optional, Tuple

import dask
import numpy as np
import xarray as xr
import zarr

from ..patterns import FilePattern, prune_pattern
from ..storage import AbstractTarget, CacheFSSpecTarget, MetadataTarget, file_opener
from ..utils import (
    chunk_bounds_and_conflicts,
    chunked_iterable,
    fix_scalar_attr_encoding,
    lock_for_conflicts,
)
from .base import BaseRecipe

# use this filename to store global recipe metadata in the metadata_cache
# it will be written once (by prepare_target) and read many times (by store_chunk)
_GLOBAL_METADATA_KEY = "pangeo-forge-recipe-metadata.json"

logger = logging.getLogger(__name__)


def _encode_key(key) -> str:
    return "-".join([str(k) for k in key])


def _input_metadata_fname(input_key):
    return "input-meta-" + _encode_key(input_key) + ".json"


ChunkKey = Tuple[int]
InputKey = Tuple[int]


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


def input_position(input_key, file_pattern: FilePattern, concat_dim: Optional[str]):
    assert concat_dim is not None
    concat_dim_axis = list(file_pattern.dims).index(concat_dim)
    return input_key[concat_dim_axis]


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
):
    if metadata_cache is None:
        raise ValueError("metadata_cache is not set.")
    logger.info(f"Caching metadata for input '{input_key}'")
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
    fsspec_open_kwargs: dict,
    cache_metadata: bool,
    copy_input_to_local_file: bool,
    xarray_open_kwargs: dict,
    delete_input_encoding: bool,
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]],
    metadata_cache: Optional[MetadataTarget],
):
    if cache_inputs:
        if input_cache is None:
            raise ValueError("input_cache is not set.")
        logger.info(f"Caching input '{input_key}'")
        fname = file_pattern[input_key]
        input_cache.cache_file(fname, **fsspec_open_kwargs)

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
    chunks_inputs: Dict[ChunkKey, Tuple[InputKey]],
    chunk_key: ChunkKey,
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    input_sequence_lens,
    concat_dim_chunks: Optional[int],
    concat_dim: Optional[str],
    metadata_cache: Optional[MetadataTarget],
):
    # return a dict suitable to pass to xr.to_zarr(region=...)
    # specifies where in the overall array to put this chunk's data
    # also return the conflicts with other chunks

    input_keys = chunks_inputs[chunk_key]

    if nitems_per_input:
        input_sequence_lens = (nitems_per_input,) * file_pattern.dims[concat_dim]  # type: ignore
    else:
        assert metadata_cache is not None  # for mypy
        global_metadata = metadata_cache[_GLOBAL_METADATA_KEY]
        input_sequence_lens = global_metadata["input_sequence_lens"]

    assert concat_dim_chunks is not None

    chunk_bounds, all_chunk_conflicts = chunk_bounds_and_conflicts(
        chunks=input_sequence_lens, zchunks=concat_dim_chunks
    )
    input_positions = [
        input_position(input_key, file_pattern, concat_dim) for input_key in input_keys
    ]
    start = chunk_bounds[min(input_positions)]
    stop = chunk_bounds[max(input_positions) + 1]

    this_chunk_conflicts = set()
    for k in input_keys:
        # for multi-variable recipes, the confilcts will usually be the same
        # for each variable. using a set avoids duplicate locks
        for input_conflict in all_chunk_conflicts[
            input_position(input_key=k, file_pattern=file_pattern, concat_dim=concat_dim)
        ]:
            this_chunk_conflicts.add(input_conflict)
    region_slice = slice(start, stop)
    return {concat_dim: region_slice}, this_chunk_conflicts


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
    logger.info(f"Opening input with Xarray {input_key}: '{fname}'")
    cache = input_cache if cache_inputs else None
    with file_opener(fname, cache=cache, copy_to_local=copy_input_to_local_file) as f:
        with dask.config.set(scheduler="single-threaded"):  # make sure we don't use a scheduler
            logger.debug(f"about to call xr.open_dataset on {f}")
            kw = xarray_open_kwargs.copy()
            if "engine" not in kw:
                kw["engine"] = "h5netcdf"
            ds = xr.open_dataset(f, **kw)
            logger.debug("successfully opened dataset")
            ds = fix_scalar_attr_encoding(ds)

            if delete_input_encoding:
                for var in ds.variables:
                    ds[var].encoding = {}

            if process_input is not None:
                ds = process_input(ds, str(fname))

            logger.debug(f"{ds}")
        yield ds


@contextmanager
def open_chunk(
    chunk_key: ChunkKey,
    chunks_inputs: Dict[ChunkKey, Tuple[InputKey]],
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
    logger.info(f"Opening inputs for chunk {chunk_key}")
    inputs = chunks_inputs[chunk_key]

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
        # explicitly chunking prevents eager evaluation during concat
        dsets = [ds.chunk() for ds in dsets]
        logger.info(f"Combining inputs for chunk '{chunk_key}'")
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
    concat_dim: Optional[str],
    inputs_chunks: Dict[InputKey, Tuple[ChunkKey]],
    metadata_cache: Optional[MetadataTarget],
) -> List[int]:
    if nitems_per_input:
        assert concat_dim is not None
        return list((nitems_per_input,) * file_pattern.dims[concat_dim])

    # read per-input metadata; this is distinct from global metadata
    # get the sequence length of every file
    # this line could become problematic for large (> 10_000) lists of files
    input_meta = get_input_meta(metadata_cache, *inputs_chunks)
    # use a numpy array to allow reshaping
    all_lens = np.array([m["dims"][concat_dim] for m in input_meta.values()])
    all_lens.shape = list(file_pattern.dims.values())
    # check that all lens are the same along the concat dim
    assert concat_dim is not None
    concat_dim_axis = list(file_pattern.dims).index(concat_dim)
    selector = [slice(0, 1)] * len(file_pattern.dims)
    selector[concat_dim_axis] = slice(None)  # this should broadcast correctly agains all_lens
    sequence_lens = all_lens[tuple(selector)]
    if not (all_lens == sequence_lens).all():
        raise ValueError(f"Inconsistent sequence lengths found: f{all_lens}")
    return sequence_lens.squeeze().tolist()


def prepare_target(
    target: CacheFSSpecTarget,
    target_chunks: Dict[str, int],
    init_chunks: List[ChunkKey],
    concat_dim: Optional[str],
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
    inputs_chunks: Dict[InputKey, Tuple[ChunkKey]],
    cache_metadata: bool,
    chunks_inputs: Dict[ChunkKey, Tuple[InputKey]],
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
                chunks_inputs=chunks_inputs,
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
        nitems_per_input, file_pattern, concat_dim, inputs_chunks, metadata_cache=metadata_cache,
    )
    n_sequence = sum(input_sequence_lens)
    logger.info(f"Expanding target concat dim '{concat_dim}' to size {n_sequence}")
    expand_target_dim(target, concat_dim, n_sequence)

    if cache_metadata:
        # if nitems_per_input is not constant, we need to cache this info
        assert metadata_cache is not None  # for mypy
        recipe_meta = {"input_sequence_lens": input_sequence_lens}
        metadata_cache[_GLOBAL_METADATA_KEY] = recipe_meta


def store_chunk(
    chunk_key: ChunkKey,
    target: CacheFSSpecTarget,
    concat_dim: Optional[str],
    chunks_inputs: Dict[ChunkKey, Tuple[InputKey]],
    nitems_per_input: Optional[int],
    file_pattern: FilePattern,
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
    inputs_chunks: Dict[InputKey, Tuple[ChunkKey]],
    metadata_cache: Optional[MetadataTarget],
) -> None:
    if target is None:
        raise ValueError("target has not been set.")

    input_sequence_lens = calculate_sequence_lens(
        nitems_per_input=nitems_per_input,
        file_pattern=file_pattern,
        concat_dim=concat_dim,
        inputs_chunks=inputs_chunks,
        metadata_cache=metadata_cache,
    )

    with open_chunk(
        chunk_key=chunk_key,
        chunks_inputs=chunks_inputs,
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
            chunks_inputs=chunks_inputs,
            chunk_key=chunk_key,
            nitems_per_input=nitems_per_input,
            file_pattern=file_pattern,
            input_sequence_lens=input_sequence_lens,
            concat_dim_chunks=concat_dim_chunks,
            concat_dim=concat_dim,
            metadata_cache=metadata_cache,
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
                data = np.asarray(
                    var.data
                )  # TODO: can we buffer large data rather than loading it all?
            zarr_region = tuple(write_region.get(dim, slice(None)) for dim in var.dims)
            lock_keys = [f"{vname}-{c}" for c in conflicts]
            logger.debug(f"Acquiring locks {lock_keys}")
            with lock_for_conflicts(lock_keys, timeout=lock_timeout):
                logger.info(
                    f"Storing variable {vname} chunk {chunk_key} " f"to Zarr region {zarr_region}"
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
    :param target_chunks: Desired chunk structure for the targret dataset.
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
    :param fsspec_open_kwargs: Extra options for opening the inputs with fsspec.
    :param process_input: Function to call on each opened input, with signature
      `(ds: xr.Dataset, filename: str) -> ds: xr.Dataset`.
    :param process_chunk: Function to call on each concatenated chunk, with signature
      `(ds: xr.Dataset) -> ds: xr.Dataset`.
    :param lock_timeout: The default timeout for acquiring a chunk lock.
    """

    file_pattern: FilePattern
    inputs_per_chunk: Optional[int] = 1
    target_chunks: Dict[str, int] = field(default_factory=dict)
    target: Optional[AbstractTarget] = None
    input_cache: Optional[CacheFSSpecTarget] = None
    metadata_cache: Optional[MetadataTarget] = None
    cache_inputs: bool = True
    copy_input_to_local_file: bool = False
    consolidate_zarr: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_kwargs: dict = field(default_factory=dict)
    delete_input_encoding: bool = True
    fsspec_open_kwargs: dict = field(default_factory=dict)
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]] = None
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]] = None
    lock_timeout: Optional[int] = None

    # internal attributes not meant to be seen or accessed by user
    _concat_dim: Optional[str] = None
    """The concatenation dimension name."""

    _concat_dim_chunks: Optional[int] = None
    """The desired chunking along the sequence dimension."""

    # In general there may be a many-to-many relationship between input keys and chunk keys
    _inputs_chunks: Dict[InputKey, Tuple[ChunkKey]] = field(
        default_factory=dict, repr=False, init=False
    )
    """Mapping of input keys to chunk keys."""

    _chunks_inputs: Dict[ChunkKey, Tuple[InputKey]] = field(
        default_factory=dict, repr=False, init=False
    )
    """Mapping of chunk keys to input keys."""

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

        # now for the fancy bit: we have to define the mappings _inputs_chunks and _chunks_inputs
        # this is where refactoring would need to happen to support more complex file patterns
        # (e.g. multiple concat dims)
        # for now we assume 1:many chunk_keys:input_keys
        # theoretically this could handle more than one merge dimension

        # list of iterators that iterates over merge dims normally
        # but concat dims in chunks
        dimension_iterators = [
            range(v)
            if k != self._concat_dim
            else enumerate(chunked_iterable(range(v), self.inputs_per_chunk))
            for k, v in self.file_pattern.dims.items()
        ]
        for k in product(*dimension_iterators):
            # typical k would look like (0, (0, (0, 1)))
            chunk_key = tuple([v[0] if hasattr(v, "__len__") else v for v in k])
            all_as_tuples = tuple([v[1] if hasattr(v, "__len__") else (v,) for v in k])
            input_keys = tuple(v for v in product(*all_as_tuples))
            self._chunks_inputs[chunk_key] = input_keys
            for input_key in input_keys:
                self._inputs_chunks[input_key] = (chunk_key,)

        # init chunks are all elements from merge dim and first element from concat dim
        merge_dimension_iterators = [
            range(v) if k != self._concat_dim else (range(1))
            for k, v in self.file_pattern.dims.items()
        ]
        self._init_chunks = list(product(*merge_dimension_iterators))

        self._validate_input_and_chunk_keys()
        self._set_target_chunks()

    def _validate_file_pattern(self):
        if len(self.file_pattern.merge_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one merge dim.")
        if len(self.file_pattern.concat_dims) > 1:
            raise NotImplementedError("This Recipe class can't handle more than one concat dim.")

    def _validate_input_and_chunk_keys(self):
        all_input_keys = set(self._inputs_chunks.keys())
        all_chunk_keys = set(self._chunks_inputs.keys())
        if not all_input_keys == set(self.file_pattern):
            raise ValueError("_inputs_chunks and file_pattern don't have the same keys")
        if not all_input_keys == set([c for val in self._chunks_inputs.values() for c in val]):
            raise ValueError("_inputs_chunks and _chunks_inputs don't use the same input keys.")
        if not all_chunk_keys == set([c for val in self._inputs_chunks.values() for c in val]):
            raise ValueError("_inputs_chunks and _chunks_inputs don't use the same chunk keys.")

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
            inputs_chunks=self._inputs_chunks,
            cache_metadata=self._cache_metadata,
            chunks_inputs=self._chunks_inputs,
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
            fsspec_open_kwargs=self.fsspec_open_kwargs,
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
            chunks_inputs=self._chunks_inputs,
            nitems_per_input=self._nitems_per_input,
            file_pattern=self.file_pattern,
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
            inputs_chunks=self._inputs_chunks,
            metadata_cache=self.metadata_cache,
        )

    @property
    def finalize_target(self) -> Callable[[], None]:
        return functools.partial(
            finalize_target, target=self.target, consolidate_zarr=self.consolidate_zarr
        )

    def iter_inputs(self):
        for input in self._inputs_chunks:
            yield input

    def iter_chunks(self):
        for k in self._chunks_inputs:
            yield k

    def inputs_for_chunk(self, chunk_key: ChunkKey) -> Tuple[InputKey]:
        """Convenience function for users to introspect recipe."""
        return self._chunks_inputs[chunk_key]

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
            chunks_inputs=self._chunks_inputs,
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
