"""
A Pangeo Forge Recipe
"""

import json
import logging
import os
import tempfile
from abc import ABC, abstractmethod
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from itertools import product
from typing import Callable, Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

import dask
import fsspec
import numpy as np
import xarray as xr
import zarr
from rechunker.types import MultiStagePipeline, ParallelPipelines, Stage

from .patterns import FilePattern
from .storage import AbstractTarget, UninitializedTarget, UninitializedTargetError
from .utils import (
    chunk_bounds_and_conflicts,
    chunked_iterable,
    fix_scalar_attr_encoding,
    lock_for_conflicts,
)

# use this filename to store global recipe metadata in the metadata_cache
# it will be written once (by prepare_target) and read many times (by store_chunk)
_GLOBAL_METADATA_KEY = "pangeo-forge-recipe-metadata.json"

logger = logging.getLogger(__name__)

# How to manually execute a recipe: ###
#
#   t = PangeoForgeTarget()
#   r = MyRecipe(target=t, **opts) # 1
#   # manual execution of recipe
#   for input_key in r.iter_inputs():
#       r.cache_input(input_key) # 4
#   r.prepare_target() # 3
#   for chunk_key in r.iter_chunks():
#       r.store_chunk(chunk_key) # 5
#   r.finalize_target() # 6


# 1) Initialize the Recipe object
# 2) Point the Recipe at its Target
# 3) Initialize the recipe.
#    Check if the target exists; if not, create it.
# 4) cache the inputs to proximate storage (OPTIONAL)
#    Some recipes won't need this (e.g. cloud to cloud)
#    If so, iter_inputs is just an empty iterator
# 5) Load each chunk from the inputs and store it in the target
#    Might be coming from the cache or might be read directly.
# 6)


class BaseRecipe(ABC):
    """Base recipe class from which all other Recipes inherit.
    """

    @property
    @abstractmethod
    def prepare_target(self) -> Callable[[], None]:
        """Prepare the recipe for execution by initializing the target.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_inputs(self) -> Iterable[Hashable]:
        """Iterate over all inputs."""
        pass

    @property
    @abstractmethod
    def cache_input(self) -> Callable[[Hashable], None]:
        """Copy an input from its source location to the cache.
        Attribute that returns a callable function.
        """
        pass

    @abstractmethod
    def iter_chunks(self) -> Iterable[Hashable]:
        """Iterate over all target chunks."""
        pass

    @property
    @abstractmethod
    def store_chunk(self) -> Callable[[Hashable], None]:
        """Store a chunk of data in the target.
        Attribute that returns a callable function.
        """
        pass

    @property
    @abstractmethod
    def finalize_target(self) -> Callable[[], None]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        pass

    def to_pipelines(self) -> ParallelPipelines:
        """Translate recipe to pipeline for execution.
        """

        pipeline = []  # type: MultiStagePipeline
        if getattr(self, "cache_inputs", False):  # TODO: formalize this contract
            pipeline.append(Stage(self.cache_input, list(self.iter_inputs())))
        pipeline.append(Stage(self.prepare_target))
        pipeline.append(Stage(self.store_chunk, list(self.iter_chunks())))
        pipeline.append(Stage(self.finalize_target))
        pipelines = []  # type: ParallelPipelines
        pipelines.append(pipeline)
        return pipelines

    # https://stackoverflow.com/questions/59986413/achieving-multiple-inheritance-using-python-dataclasses
    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass


def _encode_key(key) -> str:
    return "-".join([str(k) for k in key])


def _input_metadata_fname(input_key):
    return "input-meta-" + _encode_key(input_key) + ".json"


def _chunk_metadata_fname(chunk_key) -> str:
    return "chunk-meta-" + _encode_key(chunk_key) + ".json"


def _copy_btw_filesystems(input_opener, output_opener, BLOCK_SIZE=10_000_000):
    with input_opener as source:
        with output_opener as target:
            while True:
                data = source.read(BLOCK_SIZE)
                if not data:
                    break
                target.write(data)


@contextmanager
def _maybe_open_or_copy_to_local(opener, copy_to_local, orig_name):
    _, suffix = os.path.splitext(orig_name)
    if copy_to_local:
        ntf = tempfile.NamedTemporaryFile(suffix=suffix)
        tmp_name = ntf.name
        logger.info(f"Copying {orig_name} to local file {tmp_name}")
        target_opener = open(tmp_name, mode="wb")
        _copy_btw_filesystems(opener, target_opener)
        yield tmp_name
        ntf.close()  # cleans up the temporary file
    else:
        with opener as fp:
            with fp as fp2:
                yield fp2


@contextmanager
def _fsspec_safe_open(fname, **kwargs):
    # workaround for inconsistent behavior of fsspec.open
    # https://github.com/intake/filesystem_spec/issues/579
    with fsspec.open(fname, **kwargs) as fp:
        with fp as fp2:
            yield fp2


# Notes about dataclasses:
# - https://www.python.org/dev/peps/pep-0557/#inheritance
# - https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses
# The main awkward thing here is that, because we are using multiple inheritance
# with dataclasses, _ALL_ fields must have default values. This makes it impossible
# to have "required" keyword arguments--everything needs some default.
# That's whay we end up with `UninitializedTarget` and `_variable_sequence_pattern_default_factory`


ChunkKey = Tuple[int]
InputKey = Tuple[int]


@dataclass
class XarrayZarrRecipe(BaseRecipe):
    """This class represents a dataset composed of many individual NetCDF files.
    This class uses Xarray to read and write data and writes its output to Zarr.
    The files are assumed to be arranged in a sequence along dimension ``concat_dim``
    (commonly "time".)

    This class should not be used on its own but rather via one of its sub-classes.

    :param concat_dim: The dimension name along which the inputs will be concatenated.
    :param inputs_per_chunk: The number of inputs to use in each chunk.
    :param nitems_per_input: The length of each input along the ``concat_dim`` dimension.
      If ``None``, each input will be scanned and its metadata cached in order to
      determine the size of the target dataset.
    :param target: A location in which to put the dataset. Can also be assigned at run time.
    :param input_cache: A location in which to cache temporary data.
    :param metadata_cache: A location in which to cache metadata for inputs and chunks.
      Required if ``nitems_per_input=None``.
    :param cache_inputs: Whether to allow opening inputs directly which have not
      yet been cached. This could lead to very unstanble behavior if the inputs
      live behind a slow network connection.
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
    :param target_chunks: Desired chunk structure for the targret dataset.
    """

    file_pattern: FilePattern
    inputs_per_chunk: Optional[int] = 1
    target: AbstractTarget = field(default_factory=UninitializedTarget)
    input_cache: AbstractTarget = field(default_factory=UninitializedTarget)
    metadata_cache: AbstractTarget = field(default_factory=UninitializedTarget)
    cache_inputs: bool = True
    copy_input_to_local_file: bool = False
    consolidate_zarr: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_kwargs: dict = field(default_factory=dict)
    delete_input_encoding: bool = True
    fsspec_open_kwargs: dict = field(default_factory=dict)
    process_input: Optional[Callable[[xr.Dataset, str], xr.Dataset]] = None
    process_chunk: Optional[Callable[[xr.Dataset], xr.Dataset]] = None
    target_chunks: Dict[str, int] = field(default_factory=dict)

    # internal attributes not meant to be seen or accessed by user
    _concat_dim: Optional[str] = None
    """The concatenation dimension name."""

    _concat_dim_chunks: Optional[int] = None
    """The desired chunking along the sequence dimension."""

    # Don't need, use filepath directly
    # _inputs_files: Dict[InputKey, str] = field(default_factory=dict, repr=False, init=False)
    """Mapping of input keys to filenames. Should be 1:1."""

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

    @property
    def prepare_target(self) -> Callable:
        def _prepare_target():

            try:
                ds = self.open_target()
                logger.info("Found an existing dataset in target")
                logger.debug(f"{ds}")

                if self.target_chunks:
                    # TODO: check that target_chunks id compatibile with the
                    # existing chunks
                    pass

            except (FileNotFoundError, IOError, zarr.errors.GroupNotFoundError):
                logger.info("Creating a new dataset in target")

                # need to rewrite this as an append loop
                for chunk_key in self._init_chunks:
                    with self.open_chunk(chunk_key) as ds:
                        # ds is already chunked

                        # https://github.com/pydata/xarray/blob/5287c7b2546fc8848f539bb5ee66bb8d91d8496f/xarray/core/variable.py#L1069
                        for v in ds.variables:
                            if self.target_chunks:
                                this_var = ds[v]
                                chunks = {
                                    this_var.get_axis_num(dim): chunk
                                    for dim, chunk in self.target_chunks.items()
                                    if dim in this_var.dims
                                }

                                chunks = tuple(
                                    chunks.get(n, s) for n, s in enumerate(this_var.shape)
                                )
                                encoding_chunks = chunks
                            else:
                                encoding_chunks = ds[v].shape
                            logger.debug(
                                f"Setting variable {v} encoding chunks to {encoding_chunks}"
                            )
                            ds[v].encoding["chunks"] = encoding_chunks

                        # load all variables that don't have the sequence dim in them
                        # these are usually coordinates.
                        # Variables that are loaded will be written even with compute=False
                        # TODO: make this behavior customizable
                        for v in ds.variables:
                            if self._concat_dim not in ds[v].dims:
                                ds[v].load()

                        target_mapper = self.target.get_mapper()
                        logger.debug(f"Storing dataset:\n {ds}")
                        ds.to_zarr(target_mapper, mode="a", compute=False, safe_chunks=False)

            # Regardless of whether there is an existing dataset or we are creating a new one,
            # we need to expand the concat_dim to hold the entire expected size of the data
            input_sequence_lens = self.calculate_sequence_lens()
            n_sequence = sum(input_sequence_lens)
            self.expand_target_dim(self._concat_dim, n_sequence)

            if self._cache_metadata:
                # if nitems_per_input is not constant, we need to cache this info
                recipe_meta = {"input_sequence_lens": input_sequence_lens}
                meta_mapper = self.metadata_cache.get_mapper()
                # we are saving a dictionary with one key (input_sequence_lens)
                meta_mapper[_GLOBAL_METADATA_KEY] = json.dumps(recipe_meta).encode("utf-8")

        return _prepare_target

    @property
    def cache_input(self) -> Callable:
        def cache_func(input_key: InputKey) -> None:
            fname = self.file_pattern[input_key]
            logger.info(f"Caching input {input_key}: {fname}")
            # TODO: check and see if the file already exists in the cache
            input_opener = _fsspec_safe_open(fname, mode="rb", **self.fsspec_open_kwargs)
            target_opener = self.input_cache.open(fname, mode="wb")
            _copy_btw_filesystems(input_opener, target_opener)
            # TODO: make it so we can cache metadata WITHOUT copying the file
            if self._cache_metadata:
                self.cache_input_metadata(input_key)

        return cache_func

    @property
    def store_chunk(self) -> Callable:
        def _store_chunk(chunk_key: ChunkKey):
            with self.open_chunk(chunk_key) as ds_chunk:
                # writing a region means that all the variables MUST have concat_dim
                to_drop = [
                    v for v in ds_chunk.variables if self._concat_dim not in ds_chunk[v].dims
                ]
                ds_chunk = ds_chunk.drop_vars(to_drop)

                target_mapper = self.target.get_mapper()
                write_region, conflicts = self.region_and_conflicts_for_chunk(chunk_key)

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
                    with dask.config.set(
                        scheduler="single-threaded"
                    ):  # make sure we don't use a scheduler
                        var = xr.backends.zarr.encode_zarr_variable(var_coded)
                        data = np.asarray(
                            var.data
                        )  # TODO: can we buffer large data rather than loading it all?
                    zarr_region = tuple(write_region.get(dim, slice(None)) for dim in var.dims)
                    lock_keys = [f"{vname}-{c}" for c in conflicts]
                    logger.debug(f"Acquiring locks {lock_keys}")
                    with lock_for_conflicts(lock_keys):
                        logger.info(
                            f"Storing variable {vname} chunk {chunk_key} "
                            f"to Zarr region {zarr_region}"
                        )
                        zarr_array[zarr_region] = data

        return _store_chunk

    @property
    def finalize_target(self) -> Callable:
        def _finalize_target():
            if self.consolidate_zarr:
                logger.info("Consolidating Zarr metadata")
                target_mapper = self.target.get_mapper()
                zarr.consolidate_metadata(target_mapper)

        return _finalize_target

    @contextmanager
    def input_opener(self, fname: str):
        try:
            logger.info(f"Opening '{fname}' from cache")
            opener = self.input_cache.open(fname, mode="rb")
            with _maybe_open_or_copy_to_local(opener, self.copy_input_to_local_file, fname) as fp:
                yield fp
        except (IOError, FileNotFoundError, UninitializedTargetError) as err:
            if self.cache_inputs:
                raise Exception(
                    f"You are trying to open input {fname}, but the file is "
                    "not cached yet. First call `cache_input` or set "
                    "`cache_inputs=False`."
                ) from err
            logger.info(f"No cache found. Opening input `{fname}` directly.")
            opener = _fsspec_safe_open(fname, mode="rb", **self.fsspec_open_kwargs)
            with _maybe_open_or_copy_to_local(opener, self.copy_input_to_local_file, fname) as fp:
                yield fp

    @contextmanager
    def open_input(self, input_key: InputKey):
        fname = self.file_pattern[input_key]
        logger.info(f"Opening input with Xarray {input_key}: '{fname}'")
        with self.input_opener(fname) as f:
            logger.info(f"f is {f}")
            ds = xr.open_dataset(f, **self.xarray_open_kwargs)
            # Explicitly load into memory;
            # if we don't do this, we get a ValueError: seek of closed file.
            # But there will be some cases where we really don't want to load.
            # how to keep around the open file object?
            # ds = ds.load()

            ds = fix_scalar_attr_encoding(ds)

            if self.delete_input_encoding:
                for var in ds.variables:
                    ds[var].encoding = {}

            if self.process_input is not None:
                ds = self.process_input(ds, str(fname))

            logger.debug(f"{ds}")
            yield ds

    def cache_input_metadata(self, input_key: InputKey):
        logger.info(f"Caching metadata for input '{input_key}'")
        with self.open_input(input_key) as ds:
            metadata = ds.to_dict(data=False)
            mapper = self.metadata_cache.get_mapper()
            mapper[_input_metadata_fname(input_key)] = json.dumps(metadata).encode("utf-8")

    @contextmanager
    def open_chunk(self, chunk_key: ChunkKey):
        logger.info(f"Concatenating inputs for chunk '{chunk_key}'")
        inputs = self._chunks_inputs[chunk_key]

        # need to open an unknown number of contexts at the same time
        with ExitStack() as stack:
            dsets = [stack.enter_context(self.open_input(i)) for i in inputs]
            # explicitly chunking prevents eager evaluation during concat
            dsets = [ds.chunk() for ds in dsets]
            if len(dsets) > 1:
                # During concat, attributes and encoding are taken from the first dataset
                # https://github.com/pydata/xarray/issues/1614
                ds = xr.concat(dsets, self._concat_dim, **self.xarray_concat_kwargs)
            elif len(dsets) == 1:
                ds = dsets[0]
            else:  # pragma: no cover
                assert False, "Should never happen"

            if self.process_chunk is not None:
                ds = self.process_chunk(ds)

            logger.debug(f"{ds}")

            # TODO: maybe do some chunking here?
            yield ds

    def open_target(self):
        target_mapper = self.target.get_mapper()
        return xr.open_zarr(target_mapper)

    def expand_target_dim(self, dim, dimsize):
        target_mapper = self.target.get_mapper()
        zgroup = zarr.open_group(target_mapper)
        ds = self.open_target()
        sequence_axes = {v: ds[v].get_axis_num(dim) for v in ds.variables if dim in ds[v].dims}

        for v, axis in sequence_axes.items():
            arr = zgroup[v]
            shape = list(arr.shape)
            shape[axis] = dimsize
            logger.debug(f"resizing array {v} to shape {shape}")
            arr.resize(shape)

        # now explicity write the sequence coordinate to avoid missing data
        # when reopening
        if dim in zgroup:
            zgroup[dim][:] = 0

    def iter_inputs(self):
        for input in self._inputs_chunks:
            yield input

    def region_and_conflicts_for_chunk(self, chunk_key: ChunkKey):
        # return a dict suitable to pass to xr.to_zarr(region=...)
        # specifies where in the overall array to put this chunk's data
        # also return the conflicts with other chunks

        input_keys = self._chunks_inputs[chunk_key]

        if self._nitems_per_input:
            input_sequence_lens = (self._nitems_per_input,) * self.file_pattern.dims[
                self._concat_dim  # type: ignore
            ]
        else:
            input_sequence_lens = json.loads(
                self.metadata_cache.get_mapper()[_GLOBAL_METADATA_KEY]
            )["input_sequence_lens"]

        chunk_bounds, all_chunk_conflicts = chunk_bounds_and_conflicts(
            input_sequence_lens, self._concat_dim_chunks  # type: ignore
        )
        input_positions = [self.input_position(input_key) for input_key in input_keys]
        start = chunk_bounds[min(input_positions)]
        stop = chunk_bounds[max(input_positions) + 1]

        this_chunk_conflicts = set()
        for k in input_keys:
            # for multi-variable recipes, the confilcts will usually be the same
            # for each variable. using a set avoids duplicate locks
            for input_conflict in all_chunk_conflicts[self.input_position(k)]:
                this_chunk_conflicts.add(input_conflict)
        region_slice = slice(start, stop)
        return {self._concat_dim: region_slice}, this_chunk_conflicts

    def iter_chunks(self):
        for k in self._chunks_inputs:
            yield k

    def get_input_meta(self, *input_keys: Sequence[InputKey]) -> Dict:
        meta_mapper = self.metadata_cache.get_mapper()
        # getitems should be async; much faster than serial calls
        all_meta_raw = meta_mapper.getitems([_input_metadata_fname(k) for k in input_keys])
        return {k: json.loads(raw_bytes) for k, raw_bytes in all_meta_raw.items()}

    def input_position(self, input_key):
        # returns the index position of an input key wrt the concat_dim
        concat_dim_axis = list(self.file_pattern.dims).index(self._concat_dim)
        return input_key[concat_dim_axis]

    def calculate_sequence_lens(self):
        if self._nitems_per_input:
            return list((self._nitems_per_input,) * self.file_pattern.dims[self._concat_dim])

        # read per-input metadata; this is distinct from global metadata
        # get the sequence length of every file
        # this line could become problematic for large (> 10_000) lists of files
        input_meta = self.get_input_meta(*self._inputs_chunks)
        # use a numpy array to allow reshaping
        all_lens = np.array([m["dims"][self._concat_dim] for m in input_meta.values()])
        all_lens.shape = list(self.file_pattern.dims.values())
        # check that all lens are the same along the concat dim
        concat_dim_axis = list(self.file_pattern.dims).index(self._concat_dim)
        selector = [slice(0, 1)] * len(self.file_pattern.dims)
        selector[concat_dim_axis] = slice(None)  # this should broadcast correctly agains all_lens
        sequence_lens = all_lens[tuple(selector)]
        if not (all_lens == sequence_lens).all():
            raise ValueError(f"Inconsistent sequence lengths found: f{all_lens}")
        return sequence_lens.squeeze().tolist()

    def inputs_for_chunk(self, chunk_key: ChunkKey) -> Tuple[InputKey]:
        """Convenience function for users to introspect recipe."""
        return self._chunks_inputs[chunk_key]
