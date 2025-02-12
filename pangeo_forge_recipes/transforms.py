from __future__ import annotations

import logging
import random
import sys
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple, TypeVar, Union

# PEP612 Concatenate & ParamSpec are useful for annotating decorators, but their import
# differs between Python versions 3.9 & 3.10. See: https://stackoverflow.com/a/71990006
if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec

import apache_beam as beam
import xarray as xr
import zarr

from .aggregation import XarraySchema, dataset_to_schema, schema_to_template_ds, schema_to_zarr
from .combiners import CombineXarraySchemas
from .openers import open_url, open_with_xarray
from .patterns import CombineOp, Dimension, FileType, Index, augment_index_with_start_stop
from .rechunking import combine_fragments, consolidate_dimension_coordinates, split_fragment
from .storage import CacheFSSpecTarget, FSSpecTarget
from .types import Indexed
from .writers import ZarrWriterMixin, consolidate_metadata, store_dataset_fragment

logger = logging.getLogger(__name__)

# From https://beam.apache.org/contribute/ptransform-style-guide/
#
# Do:
# - Expose every major data-parallel task accomplished by your library as a
#   composite PTransform. This allows the structure of the transform to evolve
#   transparently to the code that uses it: e.g. something that started as a
#   ParDo can become a more complex transform over time.
# - Expose large, non-trivial, reusable sequential bits of the transform’s code,
#   which others might want to reuse in ways you haven’t anticipated, as a regular
#   function or class library. The transform should simply wire this logic together.
#   As a side benefit, you can unit-test those functions and classes independently.
#   Example: when developing a transform that parses files in a custom data format,
#   expose the format parser as a library; likewise for a transform that implements
#   a complex machine learning algorithm, etc.
# - In some cases, this may include Beam-specific classes, such as CombineFn,
#   or nontrivial DoFns (those that are more than just a single @ProcessElement
#   method). As a rule of thumb: expose these if you anticipate that the full
#   packaged PTransform may be insufficient for a user’s needs and the user may want
#   to reuse the lower-level primitive.
#
# Do not:
# - Do not expose the exact way the transform is internally structured.
#   E.g.: the public API surface of your library usually (with exception of the
#   last bullet above) should not expose DoFn, concrete Source or Sink classes,
#   etc., in order to avoid presenting users with a confusing choice between
#   applying the PTransform or using the DoFn/Source/Sink.

# In the spirit of "[t]he transform should simply wire this logic together",
# the goal is to put _as little logic as possible_ in this module.
# Ideally each PTransform should be a simple Map or DoFn calling out to function
# from other modules

T = TypeVar("T")
IndexedArg = Indexed[T]

R = TypeVar("R")
IndexedReturn = Indexed[R]

P = ParamSpec("P")


class RequiredAtRuntimeDefault:
    """Sentinel class to use as default for transform attributes which are required to run a
    pipeline, but may not be available (or preferable) to define during recipe develoment; for
    example, the ``target_root`` kwarg of a transform that writes data to a target location. By
    using this sentinel as the default value for such an kwarg, a recipe module can define all
    required arguments on the transform (and therefore be importable, satisfy type-checkers, be
    unit-testable, etc.) before it is deployed, with the understanding that the attribute using
    this sentinel as default will be re-assigned to the desired value at deploy time.
    """

    pass


def _assign_concurrency_group(elem, max_concurrency: int):
    return (random.randint(0, max_concurrency - 1), elem)


@dataclass
class MapWithConcurrencyLimit(beam.PTransform):
    """A transform which maps calls to the provided function, optionally limiting the maximum
    number of concurrent calls. Useful for situations where the provided function requests data
    from an external service that does not support an unlimited number of concurrent requests.

    :param fn: Callable object passed to beam.Map (in the case of no concurrency limit)
      or beam.FlatMap (if max_concurrency is specified).
    :param args: Positional arguments passed to all invocations of `fn`.
    :param kwargs: Keyword arguments passed to all invocations of `fn`.
    :param max_concurrency: The maximum number of concurrent invocations of `fn`.
      If unspecified, no limit is imposed by this transform (therefore the concurrency
      limit will be set by the Beam Runner's configuration).
    """

    fn: Callable
    args: Optional[list] = field(default_factory=list)
    kwargs: Optional[dict] = field(default_factory=dict)
    max_concurrency: Optional[int] = None

    def expand(self, pcoll):
        return (
            pcoll | self.fn.__name__ >> beam.MapTuple(lambda k, v: (k, self.fn(v, *self.args, **self.kwargs)))
            if not self.max_concurrency
            else (
                pcoll
                | "Assign concurrency key" >> beam.Map(_assign_concurrency_group, self.max_concurrency)
                | "Group together by concurrency key" >> beam.GroupByKey()
                | "Drop concurrency key" >> beam.Values()
                | f"{self.fn.__name__} (max_concurrency={self.max_concurrency})"
                >> beam.FlatMap(lambda kvlist: [(kv[0], self.fn(kv[1], *self.args, **self.kwargs)) for kv in kvlist])
            )
        )


# This has side effects if using a cache
@dataclass
class OpenURLWithFSSpec(beam.PTransform):
    """Open indexed string-based URLs with fsspec.

    :param cache: If provided, data will be cached at this url path before opening.
    :param secrets: If provided these secrets will be injected into the URL as a query string.
    :param open_kwargs: Extra arguments passed to fsspec.open.
    :param max_concurrency: Max concurrency for this transform.
    :param fsspec_sync_patch: Experimental. Likely slower. When enabled, this attempts to
        replace asynchronous code with synchronous implementations to potentially address
        deadlocking issues. cf. https://github.com/h5py/h5py/issues/2019
    """

    cache: Optional[str | CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None
    max_concurrency: Optional[int] = None
    fsspec_sync_patch: bool = False

    def expand(self, pcoll):
        if isinstance(self.cache, str):
            cache = CacheFSSpecTarget.from_url(self.cache)
        else:
            cache = self.cache

        kws = dict(
            cache=cache,
            secrets=self.secrets,
            fsspec_sync_patch=self.fsspec_sync_patch,
            open_kwargs=self.open_kwargs,
        )
        return pcoll | MapWithConcurrencyLimit(
            open_url,
            kwargs=kws,
            max_concurrency=self.max_concurrency,
        )


@dataclass
class OpenWithXarray(beam.PTransform):
    """Open indexed items with Xarray. Accepts either fsspec open-file-like objects
    or string URLs that can be passed directly to Xarray.

    :param file_type: Provide this if you know what type of file it is.
    :param load: Whether to eagerly load the data into memory ofter opening.
    :param copy_to_local: Whether to copy the file-like-object to a local path
       and pass the path to Xarray. Required for some file types (e.g. Grib).
       Can only be used with file-like-objects, not URLs.
    :param xarray_open_kwargs: Extra arguments to pass to Xarray's open function.
    """

    file_type: FileType = FileType.unknown
    load: bool = False
    copy_to_local: bool = False
    xarray_open_kwargs: Optional[dict] = field(default_factory=dict)

    def expand(self, pcoll):
        return pcoll | "Open with Xarray" >> beam.MapTuple(
            lambda k, v: (
                k,
                open_with_xarray(
                    v,
                    file_type=self.file_type,
                    load=self.load,
                    copy_to_local=self.copy_to_local,
                    xarray_open_kwargs=self.xarray_open_kwargs,
                ),
            )
        )


def _nest_dim(item: Indexed[T], dimension: Dimension) -> Indexed[Indexed[T]]:
    index, value = item
    inner_index = Index({dimension: index[dimension]})
    outer_index = Index({dk: index[dk] for dk in index if dk != dimension})
    return outer_index, (inner_index, value)


@dataclass
class _NestDim(beam.PTransform):
    """Prepare a collection for grouping by transforming an Index into a nested
    Tuple of Indexes.

    :param dimension: The dimension to nest
    """

    dimension: Dimension

    def expand(self, pcoll):
        return pcoll | beam.Map(_nest_dim, dimension=self.dimension)


@dataclass
class DatasetToSchema(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(lambda k, v: (k, dataset_to_schema(v)))


@dataclass
class DetermineSchema(beam.PTransform):
    """Combine many Datasets into a single schema along multiple dimensions.
    This is a reduction that produces a singleton PCollection.

    :param combine_dims: The dimensions to combine
    """

    combine_dims: List[Dimension]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        schemas = pcoll | beam.MapTuple(lambda k, v: (k, dataset_to_schema(v)))
        cdims = self.combine_dims.copy()
        while len(cdims) > 0:
            last_dim = cdims.pop()
            if len(cdims) == 0:
                # at this point, we should have a 1D index as our key
                schemas = schemas | beam.CombineGlobally(CombineXarraySchemas(last_dim))
            else:
                schemas = (
                    schemas
                    | f"Nest {last_dim.name}" >> _NestDim(last_dim)
                    | f"Combine {last_dim.name}" >> beam.CombinePerKey(CombineXarraySchemas(last_dim))
                )
        return schemas


@dataclass
class IndexItems(beam.PTransform):
    """Augment dataset indexes with information about start and stop position."""

    schema: beam.PCollection
    append_offset: int = 0

    @staticmethod
    def index_item(item: Indexed[T], schema: XarraySchema, append_offset: int) -> Indexed[T]:
        index, ds = item
        new_index = Index()
        for dimkey, dimval in index.items():
            if dimkey.operation == CombineOp.CONCAT:
                item_len_dict = schema["chunks"][dimkey.name]
                item_lens = [item_len_dict[n] for n in range(len(item_len_dict))]
                dimval = augment_index_with_start_stop(dimval, item_lens, append_offset)
            new_index[dimkey] = dimval
        return new_index, ds

    def expand(self, pcoll: beam.PCollection):
        return pcoll | beam.Map(
            self.index_item,
            schema=beam.pvalue.AsSingleton(self.schema),
            append_offset=self.append_offset,
        )


@dataclass
class PrepareZarrTarget(beam.PTransform):
    """
    From a singleton PCollection containing a dataset schema, initialize a
    Zarr store with the correct variables, dimensions, attributes and chunking.
    Note that the dimension coordinates will be initialized with dummy values.

    :param target: Where to store the target Zarr dataset.
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the schema.
        If chunking is present in the schema for a given dimension, the length of
        the first fragment will be used. Otherwise, the dimension will not be chunked.
    :param attrs: Extra group-level attributes to inject into the dataset.
    :param encoding: Dictionary describing encoding for xarray.to_zarr()
    :param consolidated_metadata: Bool controlling if xarray.to_zarr()
                                  writes consolidated metadata. Default's to False. In StoreToZarr,
                                  always default to unconsolidated. This leaves it up to the
                                  user whether or not they want to consolidate with
                                  ConsolidateMetadata(). Also, it prevents a broken/inconsistent
                                  state that could arise from metadata being consolidated here, and
                                  then falling out of sync with coordinates if
                                  ConsolidateDimensionCoordinates() is applied to the output of
                                  StoreToZarr().
    :param append_dim: Optional name of the dimension to append to.
    """

    target: str | FSSpecTarget
    target_chunks: Dict[str, int] = field(default_factory=dict)
    attrs: Dict[str, str] = field(default_factory=dict)
    consolidated_metadata: Optional[bool] = True
    encoding: Optional[dict] = field(default_factory=dict)
    append_dim: Optional[str] = None

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        if isinstance(self.target, str):
            target = FSSpecTarget.from_url(self.target)
        else:
            target = self.target
        store = target.get_mapper()
        initialized_target = pcoll | beam.Map(
            schema_to_zarr,
            target_store=store,
            target_chunks=self.target_chunks,
            attrs=self.attrs,
            encoding=self.encoding,
            consolidated_metadata=False,
            append_dim=self.append_dim,
        )
        return initialized_target


@dataclass
class StoreDatasetFragments(beam.PTransform):
    target_store: beam.PCollection  # side input

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(store_dataset_fragment, target_store=beam.pvalue.AsSingleton(self.target_store))


@dataclass
class ConsolidateMetadata(beam.PTransform):
    """Calls Zarr Python consolidate_metadata on an existing Zarr store
    (https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata)"""

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(consolidate_metadata)


@dataclass
class Rechunk(beam.PTransform):
    target_chunks: Optional[Dict[str, int]]
    schema: beam.PCollection

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        new_fragments = (
            pcoll
            | beam.FlatMap(
                split_fragment,
                target_chunks=self.target_chunks,
                schema=beam.pvalue.AsSingleton(self.schema),
            )
            | beam.GroupByKey()  # this has major performance implication
            | beam.MapTuple(combine_fragments)
        )
        return new_fragments


class ConsolidateDimensionCoordinates(beam.PTransform):
    def expand(self, pcoll: beam.PCollection[zarr.storage.FSStore]) -> beam.PCollection[zarr.storage.FSStore]:
        return pcoll | beam.Map(consolidate_dimension_coordinates)


@dataclass
class StoreToZarr(beam.PTransform, ZarrWriterMixin):
    """Store a PCollection of Xarray datasets to Zarr.

    :param combine_dims: The dimensions to combine
    :param store_name: Name for the Zarr store. It will be created with
        this name under `target_root`.
    :param target_root: Root path the Zarr store will be created inside;
        `store_name` will be appended to this prefix to create a full path.
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
      If a dimension is a not named, the chunks will be inferred from the data.
    :param dynamic_chunking_fn: Optionally provide a function that takes an ``xarray.Dataset``
      template dataset as its first argument and returns a dynamically generated chunking dict.
      If provided, ``target_chunks`` cannot also be passed. You can use this to determine chunking
      based on the full dataset (e.g. divide along a certain dimension based on a desired chunk
      size in memory). For more advanced chunking strategies, check
      out https://github.com/jbusecke/dynamic_chunks
    :param dynamic_chunking_fn_kwargs: Optional keyword arguments for ``dynamic_chunking_fn``.
    :param attrs: Extra group-level attributes to inject into the dataset.
    :param encoding: Dictionary encoding for xarray.to_zarr().
    :param append_dim: Optional name of the dimension to append to.
    """

    # TODO: make it so we don't have to explicitly specify combine_dims
    # Could be inferred from the pattern instead
    combine_dims: List[Dimension]
    store_name: str
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(default_factory=RequiredAtRuntimeDefault)
    target_chunks: Dict[str, int] = field(default_factory=dict)
    dynamic_chunking_fn: Optional[Callable[[xr.Dataset], dict]] = None
    dynamic_chunking_fn_kwargs: Optional[dict] = field(default_factory=dict)
    attrs: Dict[str, str] = field(default_factory=dict)
    encoding: Optional[dict] = field(default_factory=dict)
    append_dim: Optional[str] = None

    def __post_init__(self):
        if self.target_chunks and self.dynamic_chunking_fn:
            raise ValueError("Passing both `target_chunks` and `dynamic_chunking_fn` not allowed.")

        self._append_offset = 0
        if self.append_dim:
            logger.warn(
                "When `append_dim` is given, StoreToZarr is NOT idempotent. Successive deployment "
                "with the same inputs will append duplicate data to the existing store."
            )
            dim = [d for d in self.combine_dims if d.name == self.append_dim]
            assert dim, f"Append dim not in {self.combine_dims=}."
            assert dim[0].operation == CombineOp.CONCAT, "Append dim operation must be CONCAT."
            existing_ds = xr.open_dataset(self.get_full_target().get_mapper(), engine="zarr")
            assert self.append_dim in existing_ds, "Append dim must be in existing dataset."
            self._append_offset = len(existing_ds[self.append_dim])

    def expand(
        self,
        datasets: beam.PCollection[Tuple[Index, xr.Dataset]],
    ) -> beam.PCollection[zarr.storage.FSStore]:
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)
        indexed_datasets = datasets | IndexItems(schema=schema, append_offset=self._append_offset)
        target_chunks = (
            self.target_chunks
            if not self.dynamic_chunking_fn
            else beam.pvalue.AsSingleton(
                schema
                | beam.Map(schema_to_template_ds)
                | beam.Map(self.dynamic_chunking_fn, **self.dynamic_chunking_fn_kwargs)
            )
        )
        rechunked_datasets = indexed_datasets | Rechunk(target_chunks=target_chunks, schema=schema)
        target_store = schema | PrepareZarrTarget(
            target=self.get_full_target(),
            target_chunks=target_chunks,
            attrs=self.attrs,
            encoding=self.encoding,
            append_dim=self.append_dim,
        )
        n_target_stores = rechunked_datasets | StoreDatasetFragments(target_store=target_store)
        singleton_target_store = (
            n_target_stores
            | beam.combiners.Sample.FixedSizeGlobally(1)
            | beam.FlatMap(lambda x: x)  # https://stackoverflow.com/a/47146582
        )

        return singleton_target_store
