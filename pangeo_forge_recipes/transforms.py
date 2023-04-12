from __future__ import annotations

import logging
from dataclasses import dataclass, field

# from functools import wraps
from typing import Dict, List, Optional, Tuple, TypeVar

import apache_beam as beam

from .aggregation import XarraySchema, dataset_to_schema, schema_to_zarr
from .combiners import CombineMultiZarrToZarr, CombineXarraySchemas
from .openers import open_url, open_with_kerchunk, open_with_xarray
from .patterns import CombineOp, Dimension, FileType, Index, augment_index_with_start_stop
from .rechunking import combine_fragments, split_fragment
from .storage import CacheFSSpecTarget, FSSpecTarget
from .writers import ZarrWriterMixin, store_dataset_fragment, write_combined_reference

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


# TODO: replace with beam.MapTuple?
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


def _drop_keys(kvp):
    """Function for DropKeys Method"""
    key, item = kvp
    return item


@dataclass
class DropKeys(beam.PTransform):
    """Simple Method to remove keys for use in a Kerchunk Reference Recipe Pipeline"""

    def expand(self, pcoll):
        return pcoll | "Drop Keys" >> beam.Map(_drop_keys)


# This has side effects if using a cache
@dataclass
class OpenURLWithFSSpec(beam.PTransform):
    """Open indexed string-based URLs with fsspec.

    :param cache: If provided, data will be cached at this url path before opening.
    :param secrets: If provided these secrets will be injected into the URL as a query string.
    :param open_kwargs: Extra arguments passed to fsspec.open.
    """

    cache: Optional[str | CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None

    def expand(self, pcoll):
        if isinstance(self.cache, str):
            cache = CacheFSSpecTarget.from_url(self.cache)
        else:
            cache = self.cache
        return pcoll | "Open with fsspec" >> beam.Map(
            _add_keys(open_url),
            cache=cache,
            secrets=self.secrets,
            open_kwargs=self.open_kwargs,
        )


@dataclass
class OpenWithKerchunk(beam.PTransform):
    """Open indexed items with Kerchunk. Accepts either fsspec open-file-like objects
    or string URLs that can be passed directly to Kerchunk.

    :param file_type: Provide this if you know what type of file it is.
    :param kerchunk_open_kwargs: Extra arguments to pass to Kerchunk.
    """

    file_type: FileType = FileType.unknown
    inline_threshold: Optional[int] = 300
    netcdf3_max_chunk_size: Optional[int] = 100000000
    storage_options: Optional[Dict] = None
    grib_filters: Optional[Dict] = None

    def expand(self, pcoll):
        return pcoll | "Open with Kerchunk" >> beam.Map(
            _add_keys(open_with_kerchunk),
            file_type=self.file_type,
            inline_threshold=self.inline_threshold,
            netcdf3_max_chunk_size=self.netcdf3_max_chunk_size,
            storage_options=self.storage_options,
            grib_filters=self.grib_filters,
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
        return pcoll | beam.Map(_add_keys(dataset_to_schema))


@dataclass
class DetermineSchema(beam.PTransform):
    """Combine many Datasets into a single schema along multiple dimensions.
    This is a reduction that produces a singleton PCollection.

    :param combine_dims: The dimensions to combine
    """

    combine_dims: List[Dimension]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        schemas = pcoll | beam.Map(_add_keys(dataset_to_schema))
        cdims = self.combine_dims.copy()
        while len(cdims) > 0:
            last_dim = cdims.pop()
            if len(cdims) == 0:
                # at this point, we should have a 1D index as our key
                schemas = schemas | beam.CombineGlobally(CombineXarraySchemas(last_dim))
            else:
                schemas = (
                    schemas
                    | _NestDim(last_dim)
                    | beam.CombinePerKey(CombineXarraySchemas(last_dim))
                )
        return schemas


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

    :param target: Where to store the target Zarr dataset.
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the schema.
        If chunking is present in the schema for a given dimension, the length of
        the first fragment will be used. Otherwise, the dimension will not be chunked.
    """

    target: str | FSSpecTarget
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        if isinstance(self.target, str):
            target = FSSpecTarget.from_url(self.target)
        else:
            target = self.target
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


@dataclass
class CombineReferences(beam.PTransform):
    """Combines Kerchunk references into a single reference dataset.

    :param concat_dims: The dimensions to concatenate across
    :param identical_dims: Shared dimensions.
    :param mzz_kwargs: Additonal kwargs passed to MultiZarrToZarr
    """

    concat_dims: List[Dimension]
    identical_dims: List[Dimension]
    mzz_kwargs: dict = field(default_factory=dict)

    def expand(self, references: beam.PCollection) -> beam.PCollection:
        return references | beam.CombineGlobally(
            CombineMultiZarrToZarr(
                concat_dims=self.concat_dims,
                identical_dims=self.identical_dims,
                mzz_kwargs=self.mzz_kwargs,
            ),
        )


@dataclass
class WriteCombinedReference(beam.PTransform, ZarrWriterMixin):
    """Store a singleton PCollection consisting of a ``kerchunk.combine.MultiZarrToZarr`` object.

    :param reference_file_type: The storage target type. Currently only ``'json'`` is supported.
    """

    reference_file_type: str = "json"

    def expand(self, reference: beam.PCollection) -> beam.PCollection:
        return reference | beam.Map(
            write_combined_reference,
            full_target=self.get_full_target(),
            reference_file_type=self.reference_file_type,
        )


@dataclass
class StoreToZarr(beam.PTransform, ZarrWriterMixin):
    """Store a PCollection of Xarray datasets to Zarr.

    :param combine_dims: The dimensions to combine
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the data.
    """

    # TODO: make it so we don't have to explicitly specify combine_dims
    # Could be inferred from the pattern instead
    combine_dims: List[Dimension]
    target_chunks: Dict[str, int] = field(default_factory=dict)

    def expand(self, datasets: beam.PCollection) -> beam.PCollection:
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)
        indexed_datasets = datasets | IndexItems(schema=schema)
        rechunked_datasets = indexed_datasets | Rechunk(
            target_chunks=self.target_chunks, schema=schema
        )
        target_store = schema | PrepareZarrTarget(
            target=self.get_full_target(), target_chunks=self.target_chunks
        )
        return rechunked_datasets | StoreDatasetFragments(target_store=target_store)
