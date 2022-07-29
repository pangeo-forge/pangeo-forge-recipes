from __future__ import annotations

import logging
from dataclasses import dataclass, field

# from functools import wraps
from typing import Dict, List, Optional, Tuple, TypeVar

import apache_beam as beam

from .aggregation import XarraySchema, dataset_to_schema, schema_to_zarr
from .combiners import CombineXarraySchemas
from .openers import open_url, open_with_xarray
from .patterns import CombineOp, DimKey, FileType, Index, augment_index_with_start_stop
from .storage import CacheFSSpecTarget, FSSpecTarget
from .writers import store_dataset_fragment

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
Indexed = Tuple[Index, T]


def _add_keys(func):
    """Convenience decorator to remove and re-add keys to items in a Map"""
    annotations = func.__annotations__.copy()
    arg_name, annotation = next(iter(annotations.items()))
    annotations[arg_name] = Tuple[Index, annotation]
    return_annotation = annotations["return"]
    annotations["return"] = Tuple[Index, return_annotation]

    # @wraps(func)  # doesn't work for some reason
    def wrapper(arg, **kwargs):
        key, item = arg
        result = func(item, **kwargs)
        return key, result

    wrapper.__annotations__ = annotations
    return wrapper


# This has side effects if using a cache
@dataclass
class OpenURLWithFSSpec(beam.PTransform):
    """Open indexed string-based URLs with fsspec.

    :param cache_url: If provided, data will be cached at this url path before opening.
    :param secrets: If provided these secrets will be injected into the URL as a query string.
    :param open_kwargs: Extra arguments passed to fsspec.open.
    """

    cache_url: Optional[str] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None

    def expand(self, pcoll):
        cache = CacheFSSpecTarget.from_url(self.cache_url) if self.cache_url else None
        return pcoll | "Open with fsspec" >> beam.Map(
            _add_keys(open_url),
            cache=cache,
            secrets=self.secrets,
            open_kwargs=self.open_kwargs,
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
        return pcoll | "Open with Xarray" >> beam.Map(
            _add_keys(open_with_xarray),
            file_type=self.file_type,
            load=self.load,
            copy_to_local=self.copy_to_local,
            xarray_open_kwargs=self.xarray_open_kwargs,
        )


def _nest_dim(item: Indexed[T], dim_key: DimKey) -> Indexed[Indexed[T]]:
    index, value = item
    inner_index = Index({dim_key: index[dim_key]})
    outer_index = Index({dk: index[dk] for dk in index if dk != dim_key})
    return outer_index, (inner_index, value)


@dataclass
class _NestDim(beam.PTransform):
    """Prepare a collection for grouping by transforming an Index into a nested
    Tuple of Indexes.

    :param dim_key: The dimension to nest
    """

    dim_key: DimKey

    def expand(self, pcoll):
        return pcoll | beam.Map(_nest_dim, dim_key=self.dim_key)


@dataclass
class DatasetToSchema(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(_add_keys(dataset_to_schema))


@dataclass
class DetermineSchema(beam.PTransform):
    """Combine many Dataset schemas into a single schema along multiple dimensions.
    This is a reduction that produces a singleton PCollection.

    :param combine_dims: The dimensions to combine
    """

    combine_dims: List[DimKey]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        cdims = self.combine_dims.copy()
        while len(cdims) > 0:
            last_dim = cdims.pop()
            if len(cdims) == 0:
                # at this point, we should have a 1D index as our key
                pcoll = pcoll | beam.CombineGlobally(CombineXarraySchemas(last_dim))
            else:
                pcoll = (
                    pcoll | _NestDim(last_dim) | beam.CombinePerKey(CombineXarraySchemas(last_dim))
                )
        return pcoll


@dataclass
class IndexItems(beam.PTransform):
    """Augment dataset indexes with information about start and stop position."""

    schema: beam.PCollection

    @staticmethod
    def index_item(item: Indexed[T], schema: XarraySchema) -> Indexed[T]:
        index, ds = item
        new_index = Index()
        for dimkey, dimval in index.items():
            if dimkey.operation == CombineOp.CONCAT:
                item_len_dict = schema["chunks"][dimkey.name]
                item_lens = [item_len_dict[n] for n in range(len(item_len_dict))]
                dimval = augment_index_with_start_stop(dimval, item_lens)
            new_index[dimkey] = dimval
        return new_index, ds

    def expand(self, pcoll: beam.PCollection):
        return pcoll | beam.Map(self.index_item, schema=beam.pvalue.AsSingleton(self.schema))


@dataclass
class PrepareZarrTarget(beam.PTransform):
    """From a singleton PCollection containing a dataset schema, initialize a
    Zarr store with the correct variables, dimensions, attributes and chunking.
    Note that the dimension coordinates will be initialized with dummy values.

    :param target_url: Where to store the target Zarr dataset.
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the schema.
        If chunking is present in the schema for a given dimension, the length of
        the first chunk will be used. Otherwise, the dimension will not be chunked.
    """

    target_url: str
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        target = FSSpecTarget.from_url(self.target_url)
        store = target.get_mapper()
        initialized_target = pcoll | beam.Map(
            schema_to_zarr, target_store=store, target_chunks=self.target_chunks
        )
        return initialized_target


@dataclass
class StoreDatasetFragments(beam.PTransform):

    target_store: beam.PCollection  # side input

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(
            store_dataset_fragment, target_store=beam.pvalue.AsSingleton(self.target_store)
        )


# TODO
# - consolidate coords
# - consolidate metadata


@dataclass
class StoreToZarr(beam.PTransform):

    target_url: str
    combine_dims: List[DimKey]
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, datasets: beam.PCollection) -> beam.PCollection:
        schemas = datasets | DatasetToSchema()
        schema = schemas | DetermineSchema(combine_dims=self.combine_dims)
        indexed_datasets = datasets | IndexItems(schema=schema)
        target_store = schema | PrepareZarrTarget(
            target_url=self.target_url, target_chunks=self.target_chunks
        )
        return indexed_datasets | StoreDatasetFragments(target_store=target_store)
