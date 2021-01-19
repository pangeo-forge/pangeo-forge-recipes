"""
A Pangeo Forge Recipe
"""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Hashable, Iterable, NoReturn, Optional

import fsspec
import xarray as xr
import zarr
from rechunker.types import MultiStagePipeline, ParallelPipelines, Stage

from .storage import InputCache, Target
from .utils import chunked_iterable, fix_scalar_attr_encoding

logger = logging.getLogger(__name__)

# How to manually execute a recipe: ###
#
#   t = PangeoForgeTarget()
#   r = MyRecipe(target=t, **opts) # 1
#   # manual execution of recipe
#   r.prepare() # 3
#   for input_key in r.iter_inputs():
#       r.cache_input(input_key) # 4
#   for chunk_key in r.iter_chunks():
#       r.store_chunk(chunk_key) # 5
#   r.finalize() # 6


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


@contextmanager
def input_opener(fname, **kwargs):
    logger.info(f"Opening input '{fname}'")
    with fsspec.open(fname, **kwargs) as f:
        yield f


class BaseRecipe(ABC):
    """Base recipe class from which all other Recipes inherit.
    """

    @property
    @abstractmethod
    def prepare(self) -> Callable[[], NoReturn]:
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
    def cache_input(self) -> Callable[[Hashable], NoReturn]:
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
    def store_chunk(self) -> Callable[[Hashable], NoReturn]:
        """Store a chunk of data in the target.
        Attribute that returns a callable function.
        """
        pass

    @property
    @abstractmethod
    def finalize(self) -> Callable[[], NoReturn]:
        """Final step to finish the recipe after data has been written.
        Attribute that returns a callable function.
        """
        pass

    def to_pipelines(self) -> ParallelPipelines:
        """Translate recipe to pipeline for execution.
        """

        pipeline = []  # type: MultiStagePipeline
        pipeline.append(Stage(self.prepare))
        pipeline.append(Stage(self.cache_input, list(self.iter_inputs())))
        pipeline.append(Stage(self.store_chunk, list(self.iter_chunks())))
        pipeline.append(Stage(self.finalize))
        pipelines = []  # type: ParallelPipelines
        pipelines.append(pipeline)
        return pipelines


# Notes about dataclasses:
# - https://www.python.org/dev/peps/pep-0557/#inheritance
# - https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses


@dataclass
class NetCDFtoZarrSequentialRecipe(BaseRecipe):
    """There are many inputs (a.k.a. files, granules), arranged in a sequence
    along the dimension `sequence_dim`. Each file may contain multiple variables.

    :param input_urls: The inputs used to generate the dataset.
    :param sequence_dim: The dimension name along which the inputs will be concatenated.
    :param inputs_per_chunk: The number of inputs to use in each chunk.
    :param nitems_per_input: The length of each input along the `sequence_dim` dimension.
    :param target: A location in which to put the dataset. Can also be assigned at run time.
    :param input_cache: A location in which to cache temporary data.
    :param consolidate_zarr: Whether to consolidate the resulting Zarr dataset.
    :param xarray_open_kwargs: Extra options for opening the inputs with Xarray.
    :param xarray_concat_kwargs: Extra options to pass to Xarray when concatenating
       the inputs to form a chunk.
    """

    input_urls: Iterable[str]
    sequence_dim: str
    inputs_per_chunk: int = 1
    nitems_per_input: int = 1
    target: Optional[Target] = None
    input_cache: Optional[InputCache] = None
    consolidate_zarr: bool = True
    xarray_open_kwargs: dict = field(default_factory=dict)
    xarray_concat_kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        self._chunks_inputs = {
            k: v for k, v in enumerate(chunked_iterable(self.input_urls, self.inputs_per_chunk))
        }

    @property
    def prepare(self) -> Callable:
        """Prepare target for storing dataset."""

        def _prepare():

            try:
                ds = self.open_target()
                logger.info("Found an existing dataset in target")
                logger.debug(f"{ds}")
            except (IOError, zarr.errors.GroupNotFoundError):
                first_chunk_key = next(self.iter_chunks())
                for input_url in self.inputs_for_chunk(first_chunk_key):
                    self.cache_input(input_url)
                ds = self.open_chunk(first_chunk_key).chunk()

                # make sure the concat dim has a valid fill_value to avoid
                # overruns when writing chunk
                ds[self.sequence_dim].encoding = {"_FillValue": -1}
                # actually not necessary if we use decode_times=False
                self.initialize_target(ds)

            self.expand_target_dim(self.sequence_dim, self.sequence_len())

        return _prepare

    @property
    def cache_input(self) -> Callable:
        """Cache the input.

        Properties
        ----------
        url : URL pointing to the input file. Must be openable by fsspec.
        """

        def cache_func(fname: str) -> None:
            logger.info(f"Caching input '{fname}'")
            with input_opener(fname, mode="rb") as source:
                with self.input_cache.open(fname, mode="wb") as target:
                    target.write(source.read())

        return cache_func

    @property
    def store_chunk(self) -> Callable:
        """Store a chunk in the target.

        Parameters
        ----------
        chunk_key : str
            The identifier for the chunk
        """

        def _store_chunk(chunk_key):
            ds_chunk = self.open_chunk(chunk_key)

            def drop_vars(ds):
                # writing a region means that all the variables MUST have sequence_dim
                to_drop = [v for v in ds.variables if self.sequence_dim not in ds[v].dims]
                return ds.drop_vars(to_drop)

            ds_chunk = drop_vars(ds_chunk)
            target_mapper = self.target.get_mapper()
            write_region = self.region_for_chunk(chunk_key)
            logger.info(f"Storing chunk '{chunk_key}' to Zarr region {write_region}")
            ds_chunk.to_zarr(target_mapper, region=write_region)

        return _store_chunk

    @property
    def finalize(self) -> Callable:
        """Finalize writing of dataset."""

        def _finalize():
            if self.consolidate_zarr:
                logger.info("Consolidating Zarr metadata")
                target_mapper = self.target.get_mapper()
                zarr.consolidate_metadata(target_mapper)

        return _finalize

    @contextmanager
    def input_opener(self, fname: str):
        if self.input_cache is None:
            logger.info(f"No cache. Opening input `{fname}` directly.")
            # This will bypass the cache. May be slow.
            with input_opener(fname, mode="rb") as f:
                yield f
        elif self.input_cache.exists(fname):
            logger.info(f"Input '{fname}' found in cache")
            with self.input_cache.open(fname, mode="rb") as f:
                yield f
        else:
            raise ValueError(
                f"Input '{fname}' has not been cached yet. " "Call .cache_input() first."
            )

    def open_input(self, fname: str):
        with self.input_opener(fname) as f:
            logger.info(f"Opening input with Xarray '{fname}'")
            ds = xr.open_dataset(f, **self.xarray_open_kwargs)
            # explicitly load into memory
            ds = ds.load()
        ds = fix_scalar_attr_encoding(ds)
        logger.debug(f"{ds}")
        return ds

    def open_chunk(self, chunk_key):
        logger.info(f"Concatenating inputs for chunk '{chunk_key}'")
        inputs = self.inputs_for_chunk(chunk_key)
        dsets = [self.open_input(i) for i in inputs]
        # CONCAT DELETES ENCODING!!!
        ds = xr.concat(dsets, self.sequence_dim, **self.xarray_concat_kwargs)
        logger.debug(f"{ds}")

        # TODO: maybe do some chunking here?
        return ds

    def open_target(self):
        target_mapper = self.target.get_mapper()
        return xr.open_zarr(target_mapper)

    def initialize_target(self, ds, **expand_dims):
        logger.info("Creating a new dataset in target")
        target_mapper = self.target.get_mapper()
        ds.to_zarr(target_mapper, mode="w", compute=False)

    def expand_target_dim(self, dim, dimsize):
        target_mapper = self.target.get_mapper()
        zgroup = zarr.open_group(target_mapper)

        ds = self.open_target()
        sequence_axes = {v: ds[v].get_axis_num(dim) for v in ds.variables if dim in ds[v].dims}

        for v, axis in sequence_axes.items():
            arr = zgroup[v]
            shape = list(arr.shape)
            shape[axis] = dimsize
            arr.resize(shape)

    def inputs_for_chunk(self, chunk_key):
        return self._chunks_inputs[chunk_key]

    def iter_inputs(self):
        for chunk_key in self.iter_chunks():
            for input in self.inputs_for_chunk(chunk_key):
                yield input

    def nitems_for_chunk(self, chunk_key):
        return self.nitems_per_input * len(self.inputs_for_chunk(chunk_key))

    def region_for_chunk(self, chunk_key):
        # return a dict suitable to pass to xr.to_zarr(region=...)
        # specifies where in the overall array to put this chunk's data
        stride = self.nitems_per_input * self.inputs_per_chunk
        start = chunk_key * stride
        region_slice = slice(start, start + self.nitems_for_chunk(chunk_key))
        return {self.sequence_dim: region_slice}

    def sequence_len(self):
        # tells the total size of dataset along the sequence dimension
        return sum([self.nitems_for_chunk(k) for k in self.iter_chunks()])

    def sequence_chunks(self):
        # chunking
        return {self.sequence_dim: self.inputs_per_chunk * self.nitems_per_input}

    def iter_chunks(self):
        for k in self._chunks_inputs:
            yield k
