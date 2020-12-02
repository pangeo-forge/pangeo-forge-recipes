"""
A Pangeo Forge Recipe
"""

import logging
from dataclasses import dataclass, field
from contextlib import contextmanager
from typing import Optional, Iterable, Callable, Any

import numpy as np
import xarray as xr
import fsspec
import zarr

from ..utils import chunked_iterable
from .storage import Target, InputCache

#logger = logging.getLogger(__name__)
logger = logging.getLogger("recipe")

### How to manually execute a recipe: ###
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
#
###


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


@dataclass
class DatasetRecipe():
    target: Target
    chunk_preprocess_funcs: Iterable[Callable]

    @property
    def prepare(self):
        def _prepare():
            pass
        return _prepare

    def iter_inputs(self):
        return []

    # need to figure out what's going on with these methods and inheritance
    # @property
    # def cache_input(self):
    #     def _cache_input(input_key):
    #         raise NotImplementedError
    #     return _cache_input

    # this only gets run when iterating, not preparing!
    def preprocess_chunk(self, ds):
        for f in self.chunk_preprocess_funcs:
             ds = f(ds)
        return ds

    def iter_chunks(self):
        raise NotImplementedError

    # @property
    # def store_chunk(self):
    #     def _store_chunk(chunk_key):
    #         raise NotImplementedError
    #     return _store_chunk

    # @property
    # def finalize(self):
    #
    #     def _finalize():
    #         pass
    #     return _finalize

# Notes about dataclasses:
# - https://www.python.org/dev/peps/pep-0557/#inheritance
# - https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses
# This means that, for now, I can't get default arguments to work.

@dataclass
class FSSpecFileOpenerMixin:
    #input_open_kwargs: dict #= field(default_factory=dict)

    @contextmanager
    def input_opener(self, fname, **kwargs):
        logger.info(f"Opening input '{fname}'")
        with fsspec.open(fname, **kwargs) as f:
            yield f


@dataclass
class InputCachingMixin(FSSpecFileOpenerMixin):
    require_cache: bool #= False
    input_cache: InputCache

    # returns a function that takes one input, the input_key
    # this allows us to parallelize these operations
    @property
    def cache_input(self):

        opener = super().input_opener
        def cache_func(fname: str) -> None:
            logger.info(f"Caching input '{fname}'")
            with opener(fname, mode="rb") as source:
                with self.input_cache.open(fname, mode="wb") as target:
                    target.write(source.read())

        return cache_func

    @contextmanager
    def input_opener(self, fname):
        if self.input_cache.exists(fname):
            logger.info(f"Input '{fname}' found in cache")
            with self.input_cache.open(fname, mode='rb') as f:
                yield f
        elif self.require_cache:
            # this creates an error on prepare because nothing is cached
            raise IOError("Input can only be opened from cache. Call .cache_input first.")
        else:
            logger.info(f"Input '{fname}' not found in cache. Opening directly.")
            # This will bypass the cache. May be slow.
            with super().input_opener(fname, mode="rb") as f:
                yield f



@dataclass
class XarrayInputOpener:
    xarray_open_kwargs: dict

    def open_input(self, fname):
        with self.input_opener(fname) as f:
            logger.info(f"Opening input with Xarray '{fname}'")
            ds = xr.open_dataset(f, **self.xarray_open_kwargs).load()
            # do we always want to remove encoding? I think so.
        ds = _fix_scalar_attr_encoding(ds)
        logger.debug(f"{ds}")
        return ds


@dataclass
class XarrayConcatChunkOpener(XarrayInputOpener):
    xarray_concat_kwargs: dict

    def open_chunk(self, chunk_key):
        logger.info(f"Concatenating inputs for chunk '{chunk_key}'")
        inputs = self.inputs_for_chunk(chunk_key)
        dsets = [self.open_input(i) for i in inputs]
        # CONCAT DELETES ENCODING!!!
        ds = xr.concat(dsets, self.sequence_dim, **self.xarray_concat_kwargs)
        logger.debug(f"{ds}")

        # do we really want to just delete all encoding?
        #for v in ds.variables:
        #    ds[v].encoding = {}

        # TODO: maybe do some chunking here?
        return ds


@dataclass
class ZarrXarrayWriterMixin:

    @property
    def store_chunk(self) -> Callable:

        def _store_chunk(chunk_key):
            ds_chunk = self.open_chunk(chunk_key)
            ds_chunk = self.preprocess_chunk(ds_chunk)
            target_mapper = self.target.get_mapper()
            write_region = self.region_for_chunk(chunk_key)
            logger.info(f"Storing chunk '{chunk_key}' to Zarr region {write_region}")
            ds_chunk.to_zarr(target_mapper, region=write_region)

        return _store_chunk


    def open_target(self):
        target_mapper = self.target.get_mapper()
        return xr.open_zarr(target_mapper)


    def initialize_target(self, ds, **expand_dims):
        logger.info(f"Creating a new dataset in target")
        target_mapper = self.target.get_mapper()
        ds.to_zarr(target_mapper, mode='w', compute=False)


    def expand_target_dim(self, dim, dimsize):
        target_mapper = self.target.get_mapper()
        zgroup = zarr.open_group(target_mapper)

        ds = self.open_target()
        sequence_axes = {v: ds[v].get_axis_num(dim)
                         for v in ds.variables
                         if dim in ds[v].dims}

        for v, axis in sequence_axes.items():
            arr = zgroup[v]
            shape = list(arr.shape)
            shape[axis] = dimsize
            arr.resize(shape)




@dataclass
class ZarrConsolidatorMixin():
    consolidate_zarr: bool #= True

    @property
    def finalize(self):

        def _finalize():
            if self.consolidate_zarr:
                logger.info(f"Consolidating Zarr metadata")
                target_mapper = self.target.get_mapper()
                zarr.consolidate_metadata(target_mapper)

        return _finalize


@dataclass
class SequenceRecipe(DatasetRecipe):
    input_urls: Iterable[str]
    sequence_dim: str
    inputs_per_chunk: int = 1
    nitems_per_input: int = 1


    def __post_init__(self):
        self._chunks_inputs = {k: v for k, v in
                              enumerate(chunked_iterable(self.input_urls, self.inputs_per_chunk))}

        def drop_vars(ds):
            # writing a region means that all the variables MUST have sequence_dim
            to_drop = [v for v in ds.variables
                       if self.sequence_dim not in ds[v].dims]
            return ds.drop(to_drop)

        self.chunk_preprocess_funcs.append(drop_vars)


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
        return {
            self.sequence_dim:
                slice(start, start + self.nitems_for_chunk(chunk_key))
        }


    def sequence_len(self):
        # tells the total size of dataset along the sequence dimension
        return sum([self.nitems_for_chunk(k) for k in self.iter_chunks()])


    def sequence_chunks(self):
        # chunking
        return {self.sequence_dim: self.inputs_per_chunk * self.nitems_per_input}


    def iter_chunks(self):
        for k in self._chunks_inputs:
            yield k

    @property
    def prepare(self):

        def _prepare():

            target_store = self.target.get_mapper()

            try:
                ds = self.open_target()
                logger.info(f"Found an existing dataset in target")
                logger.debug(f"{ds}")
            except (IOError, zarr.errors.GroupNotFoundError):
                first_chunk_key = next(self.iter_chunks())
                ds = self.open_chunk(first_chunk_key).chunk()

                # make sure the concat dim has a valid fill_value to avoid
                # overruns when writing chunk
                ds[self.sequence_dim].encoding = {'_FillValue': -1}
                # actually not necessary if we use decode_times=False
                self.initialize_target(ds)

            self.expand_target_dim(self.sequence_dim, self.sequence_len())

        return _prepare


@dataclass
class StandardSequentialRecipe(
        SequenceRecipe,
        InputCachingMixin,
        XarrayConcatChunkOpener,
        ZarrXarrayWriterMixin,
        ZarrConsolidatorMixin
    ):
    pass


# helper utilities

# only needed because of
# https://github.com/pydata/xarray/issues/4631
def _fix_scalar_attr_encoding(ds):

    def _fixed_attrs(d):
        fixed = {}
        for k, v in d.items():
            if isinstance(v, np.ndarray) and len(v) == 1:
                fixed[k] = v[0]
        return fixed

    ds = ds.copy()
    ds.attrs.update(_fixed_attrs(ds.attrs))
    ds.encoding.update(_fixed_attrs(ds.encoding))
    for v in ds.variables:
        ds[v].attrs.update(_fixed_attrs(ds[v].attrs))
        ds[v].encoding.update(_fixed_attrs(ds[v].encoding))
    return ds
