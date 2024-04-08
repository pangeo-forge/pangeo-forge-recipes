from __future__ import annotations

import logging
import math
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
import fsspec
import xarray as xr
import zarr
from kerchunk.combine import MultiZarrToZarr

from .aggregation import dataset_to_schema, schema_to_template_ds, schema_to_zarr
from .combiners import CombineXarraySchemas, MinMaxCountCombineFn
from .openers import open_url, open_with_kerchunk, open_with_xarray
from .patterns import CombineOp, Dimension, FileType, Index, augment_index_with_byte_range
from .rechunking import combine_fragments, consolidate_dimension_coordinates, split_fragment
from .storage import CacheFSSpecTarget, FSSpecTarget
from .types import Indexed
from .writers import (
    ZarrWriterMixin,
    consolidate_metadata,
    store_dataset_fragment,
    write_combined_reference,
)

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
            pcoll
            | self.fn.__name__
            >> beam.MapTuple(lambda k, v: (k, self.fn(v, *self.args, **self.kwargs)))
            if not self.max_concurrency
            else (
                pcoll
                | "Assign concurrency key"
                >> beam.Map(_assign_concurrency_group, self.max_concurrency)
                | "Group together by concurrency key" >> beam.GroupByKey()
                | "Drop concurrency key" >> beam.Values()
                | f"{self.fn.__name__} (max_concurrency={self.max_concurrency})"
                >> beam.FlatMap(
                    lambda kvlist: [
                        (kv[0], self.fn(kv[1], *self.args, **self.kwargs)) for kv in kvlist
                    ]
                )
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
    """

    cache: Optional[str | CacheFSSpecTarget] = None
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None
    max_concurrency: Optional[int] = None

    def expand(self, pcoll):
        if isinstance(self.cache, str):
            cache = CacheFSSpecTarget.from_url(self.cache)
        else:
            cache = self.cache

        kws = dict(
            cache=cache,
            secrets=self.secrets,
            open_kwargs=self.open_kwargs,
        )
        return pcoll | MapWithConcurrencyLimit(
            open_url,
            kwargs=kws,
            max_concurrency=self.max_concurrency,
        )


@dataclass
class OpenWithKerchunk(beam.PTransform):
    """Open indexed items with Kerchunk. Accepts either fsspec open-file-like objects
    or string URLs that can be passed directly to Kerchunk.

    :param file_type: The type of file to be openend; e.g. "netcdf4", "netcdf3", "grib", etc.
    :param inline_threshold: Passed to kerchunk opener.
    :param storage_options: Storage options dict to pass to the kerchunk opener.
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "https", etc.
    :param kerchunk_open_kwargs: Additional kwargs to pass to kerchunk opener. Any kwargs which
      are specific to a particular input file type should be passed here;  e.g.,
      ``{"filter": ...}`` for GRIB; ``{"max_chunk_size": ...}`` for NetCDF3, etc.
    """

    # passed directly to `open_with_kerchunk`
    file_type: FileType = FileType.unknown
    inline_threshold: Optional[int] = 300
    storage_options: Optional[Dict] = field(default_factory=dict)
    remote_protocol: Optional[str] = None
    kerchunk_open_kwargs: Optional[dict] = field(default_factory=dict)

    def expand(self, pcoll):
        return pcoll | "Open with Kerchunk" >> beam.MapTuple(
            lambda k, v: (
                k,
                open_with_kerchunk(
                    v,
                    file_type=self.file_type,
                    inline_threshold=self.inline_threshold,
                    storage_options=self.storage_options,
                    remote_protocol=self.remote_protocol,
                    kerchunk_open_kwargs=self.kerchunk_open_kwargs,
                ),
            )
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


@dataclass
class DatasetToSchema(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(lambda k, v: (k, dataset_to_schema(v)))


def _nest_dim(item: Indexed[T], dimension: Dimension) -> Indexed[Indexed[T]]:
    """Nest dimensions to support multiple combine dimensions"""
    index, value = item
    inner_index = Index({dimension: index[dimension]})
    outer_index = Index({dk: index[dk] for dk in index if dk != dimension})
    return outer_index, (inner_index, value)


@dataclass
class DetermineSchema(beam.PTransform):
    """Combine many Datasets into a single schema along multiple dimensions.
    This is a reduction that produces a singleton PCollection.

    :param combine_dims: The dimensions to combine
    """

    combine_dims: List[Dimension]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        schemas = pcoll | beam.MapTuple(lambda k, v: (k, dataset_to_schema(v)))
        # Recursively combine schema definitions
        for i, dim in enumerate(self.combine_dims[::-1]):
            if i < len(self.combine_dims) - 1:
                schemas = (
                    schemas
                    | f"Nest {dim.name}" >> beam.Map(_nest_dim, dimension=dim)
                    | f"Combine {dim.name}" >> beam.CombinePerKey(CombineXarraySchemas(dim))
                )
            else:  # at this point, we should have a 1D index as our key
                schemas = schemas | beam.CombineGlobally(CombineXarraySchemas(dim))
        return schemas


class IndexWithPosition(beam.DoFn):
    """Augment dataset indexes with information about start and stop position."""

    def __init__(self, append_offset=0):
        self.append_offset = append_offset

    def process(self, element, schema):
        index, ds = element
        new_index = Index()
        for dimkey, dimval in index.items():
            if dimkey.operation == CombineOp.CONCAT:
                item_len_dict = schema["chunks"][dimkey.name]
                item_lens = [item_len_dict[n] for n in range(len(item_len_dict))]
                dimval = augment_index_with_byte_range(dimval, item_lens, self.append_offset)
            new_index[dimkey] = dimval
        yield new_index, ds


@dataclass
class PrepareZarrTarget(beam.PTransform):
    """
    From a singleton PCollection containing a dataset schema, initialize a
    Zarr store with the correct variables, dimensions, attributes and chunking.
    Note that the dimension coordinates will be initialized with dummy values.

    :param target: Where to store the target Zarr dataset.
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
    attrs: Dict[str, str] = field(default_factory=dict)
    consolidated_metadata: Optional[bool] = True
    encoding: Optional[dict] = field(default_factory=dict)
    append_dim: Optional[str] = None

    def expand(
        self, pcoll: beam.PCollection, target_chunks: beam.pvalue.AsSingleton
    ) -> beam.PCollection:
        if isinstance(self.target, str):
            target = FSSpecTarget.from_url(self.target)
        else:
            target = self.target
        store = target.get_mapper()
        initialized_target = pcoll | "initialize zarr store" >> beam.Map(
            schema_to_zarr,
            target_store=store,
            target_chunks=target_chunks,
            attrs=self.attrs,
            encoding=self.encoding,
            consolidated_metadata=False,
            append_dim=self.append_dim,
        )
        return initialized_target


@dataclass
class ConsolidateMetadata(beam.PTransform):
    """Calls Zarr Python consolidate_metadata on an existing Zarr store
    (https://zarr.readthedocs.io/en/stable/_modules/zarr/convenience.html#consolidate_metadata)"""

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(consolidate_metadata)


@dataclass
class ChunkToTarget(beam.PTransform):
    def expand(
        self,
        pcoll: beam.PCollection,
        target_chunks: beam.pvalue.AsSingleton,
        schema: beam.pvalue.AsSingleton,
    ) -> beam.PCollection:
        return (
            pcoll
            | "key to chunks following schema"
            >> beam.FlatMap(
                split_fragment,
                target_chunks=target_chunks,
                schema=schema,
            )
            | "group by write chunk key" >> beam.GroupByKey()  # group by key ensures locality
            | "per chunk dataset merge" >> beam.MapTuple(combine_fragments)
        )


class ConsolidateDimensionCoordinates(beam.PTransform):
    def expand(
        self, pcoll: beam.PCollection[zarr.storage.FSStore]
    ) -> beam.PCollection[zarr.storage.FSStore]:
        return pcoll | beam.Map(consolidate_dimension_coordinates)


@dataclass
class CombineReferences(beam.PTransform):
    """Combines Kerchunk references into a single reference dataset.

    :param concat_dims: Dimensions along which to concatenate inputs.
    :param identical_dims: Dimensions shared among all inputs.
    :param target_options: Storage options for opening target files
    :param remote_options: Storage options for opening remote files
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "gcp", "https", etc.
    :param max_refs_per_merge: Maximum number of references to combine in a single merge operation.
    :param mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    """

    concat_dims: List[str]
    identical_dims: List[str]
    target_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_options: Optional[Dict] = field(default_factory=lambda: {"anon": True})
    remote_protocol: Optional[str] = None
    max_refs_per_merge: int = 5
    mzz_kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        """Store chosen sort dimension to keep things DRY"""
        # last element chosen here to follow the xarray `pop` choice above
        self.sort_dimension = self.concat_dims[-1]

    def to_mzz(self, references):
        """Converts references into a MultiZarrToZarr object with configured parameters."""
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            target_options=self.target_options,
            remote_options=self.remote_options,
            remote_protocol=self.remote_protocol,
            **self.mzz_kwargs,
        )

    def handle_gribs(self, indexed_references: Tuple[Index, list[dict]]) -> Tuple[Index, dict]:
        """Handles the special case of GRIB format files by combining multiple references."""

        references = indexed_references[1]
        idx = indexed_references[0]
        if len(references) > 1:
            ref = self.to_mzz(references).translate()
            return (idx, ref)
        elif len(references) == 1:
            return (idx, references[0])
        else:
            raise ValueError("No references produced for {idx}. Expected at least 1.")

    def bucket_by_position(
        self,
        indexed_references: Tuple[Index, dict],
        global_position_min_max_count: Tuple[int, int, int],
    ) -> Tuple[int, dict]:
        """
        Assigns a bucket based on the index position to order data during GroupByKey.

        :param indexed_references: A tuple containing the index and the reference dictionary. The
            index is used to determine the reference's position within the global data order.
        :param global_position_min_max_count: A tuple containing the global minimum and maximum
            positions and the total count of references. These values are used to determine the
            range and distribution of buckets.
        :returns: A tuple where the first element is the bucket number (an integer) assigned to the
            reference, and the second element is the original reference dictionary.
        """
        idx = indexed_references[0]
        ref = indexed_references[1]
        global_min, global_max, global_count = global_position_min_max_count

        position = idx.find_position(self.sort_dimension)

        # Calculate the total range size based on global minimum and maximum positions
        # And asserts the distribution is contiguous/uniform or dump warning
        expected_range_size = global_max - global_min + 1  # +1 to include both ends
        if expected_range_size != global_count:
            logger.warning("The distribution of indexes is not contiguous/uniform")

        # Determine the number of buckets needed, based on the maximum references allowed per merge
        num_buckets = math.ceil(global_count / self.max_refs_per_merge)

        # Calculate the total range size based on global minimum and maximum positions.
        range_size = global_max - global_min

        # Calculate the size of each bucket by dividing the total range by the number of buckets
        bucket_size = range_size / num_buckets

        # Assign the current reference to a bucket based on its position.
        # The bucket number is determined by how far the position is from the global minimum,
        # divided by the size of each bucket.
        bucket = int((position - global_min) / bucket_size)

        return bucket, ref

    def global_combine_refs(self, refs) -> fsspec.FSMap:
        """Performs a global combination of references to produce the final dataset."""
        return fsspec.filesystem(
            "reference",
            fo=self.to_mzz(refs).translate(),
            storage_options={
                "remote_protocol": self.remote_protocol,
                "skip_instance_cache": True,
            },
        ).get_mapper()

    def expand(self, reference_lists: beam.PCollection) -> beam.PCollection:
        min_max_count_positions = (
            reference_lists
            | "Get just the positions"
            >> beam.MapTuple(lambda k, v: k.find_position(self.sort_dimension))
            | "Get minimum/maximum positions" >> beam.CombineGlobally(MinMaxCountCombineFn())
        )
        return (
            reference_lists
            | "Handle special case of gribs" >> beam.Map(self.handle_gribs)
            | "Bucket to preserve order"
            >> beam.Map(
                self.bucket_by_position,
                global_position_min_max_count=beam.pvalue.AsSingleton(min_max_count_positions),
            )
            | "Group by buckets for ordering" >> beam.GroupByKey()
            | "Distributed reduce" >> beam.MapTuple(lambda k, refs: self.to_mzz(refs).translate())
            | "Assign global key for collecting to executor" >> beam.Map(lambda ref: (None, ref))
            | "Group globally" >> beam.GroupByKey()
            | "Global reduce" >> beam.MapTuple(lambda k, refs: self.global_combine_refs(refs))
        )


@dataclass
class WriteReference(beam.PTransform, ZarrWriterMixin):
    """
    Store a singleton PCollection consisting of a ``kerchunk.combine.MultiZarrToZarr`` object.

    :param store_name: Zarr store will be created with this name under ``target_root``.
    :param concat_dims: Dimensions along which to concatenate inputs.
    :param target_root: Root path the Zarr store will be created inside; ``store_name``
                        will be appended to this prefix to create a full path.
    :param output_file_name: Name to give the output references file (``.json`` or ``.parquet``
                             suffix) over which they are accessed. e.g.: "s3", "gcp", "https", etc.
    :param mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    """

    store_name: str
    concat_dims: List[str]
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    output_file_name: str = "reference.json"
    mzz_kwargs: dict = field(default_factory=dict)

    def expand(self, references: beam.PCollection) -> beam.PCollection:
        return references | beam.Map(
            write_combined_reference,
            full_target=self.get_full_target(),
            concat_dims=self.concat_dims,
            output_file_name=self.output_file_name,
            mzz_kwargs=self.mzz_kwargs,
        )


@dataclass
class WriteCombinedReference(beam.PTransform, ZarrWriterMixin):
    """Store a singleton PCollection consisting of a ``kerchunk.combine.MultiZarrToZarr`` object.

    :param store_name: Zarr store will be created with this name under ``target_root``.
    :param concat_dims: Dimensions along which to concatenate inputs.
    :param identical_dims: Dimensions shared among all inputs.
    :param mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    :param remote_options: options to pass to ``kerchunk.combine.MultiZarrToZarr``
      to read reference inputs (can include credentials).
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "https", etc.
    :param target_root: Output root path the store will be created inside; ``store_name``
      will be appended to this prefix to create a full path.
    :param output_file_name: Name to give the output references file
      (``.json`` or ``.parquet`` suffix).
    """

    store_name: str
    concat_dims: List[str]
    identical_dims: List[str]
    mzz_kwargs: dict = field(default_factory=dict)
    remote_options: Optional[Dict] = field(default_factory=dict)
    remote_protocol: Optional[str] = None
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    output_file_name: str = "reference.json"

    def expand(self, references: beam.PCollection) -> beam.PCollection[zarr.storage.FSStore]:
        return (
            references
            | CombineReferences(
                concat_dims=self.concat_dims,
                identical_dims=self.identical_dims,
                target_options=self.remote_options,
                remote_options=self.remote_options,
                remote_protocol=self.remote_protocol,
                mzz_kwargs=self.mzz_kwargs,
            )
            | WriteReference(
                store_name=self.store_name,
                concat_dims=self.concat_dims,
                target_root=self.target_root,
                output_file_name=self.output_file_name,
            )
        )


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
    :param attrs: Extra group-level attributes to inject into the dataset.
    :param encoding: Dictionary encoding for xarray.to_zarr().
    :param append_dim: Optional name of the dimension to append to.

    Example of using a wrapper function to reduce the arity of a more complex dynamic_chunking_fn:

    Suppose there's a function `calculate_dynamic_chunks` that requires extra parameters: an
    `xarray.Dataset`, a `target_chunk_size` in bytes, and a `dim_name` along which to chunk.
    To fit the expected signature for `dynamic_chunking_fn`, we can define a wrapper function
    that presets `target_chunk_size` and `dim_name`:

        def calculate_dynamic_chunks(ds, target_chunk_size, dim_name) -> Dict[str, int]:
            ...

        def dynamic_chunking_wrapper(ds: xarray.Dataset) -> Dict[str, int]:
            target_chunk_size = 1024 * 1024 * 10
            dim_name = 'time'
            return calculate_dynamic_chunks(ds, target_chunk_size, dim_name)

        StoreToZarr(..., dynamic_chunking_fn=dynamic_chunking_wrapper, ...)
    """

    combine_dims: List[Dimension]
    store_name: str
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    target_chunks: Dict[str, int] = field(default_factory=dict)
    dynamic_chunking_fn: Optional[Callable[[xr.Dataset], dict]] = None
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
        logger.info(f"Storing Zarr with {self.target_chunks =} to {self.get_full_target()}")

        pipeline = datasets.pipeline

        # build a global xarray schema (i.e. it ranges over all input datasets)
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)

        # Index datasets according to their place within the global schema
        indexed_datasets = datasets | beam.ParDo(
            IndexWithPosition(append_offset=self._append_offset),
            schema=beam.pvalue.AsSingleton(schema),
        )

        # either use target chunks or else compute them with provided chunking function
        # Make a PColl for target chunks to match the output of mapping a dynamic chunking fn
        target_chunks_pcoll = (
            pipeline | "Create target_chunks pcoll" >> beam.Create([self.target_chunks])
            if not self.dynamic_chunking_fn
            else (
                schema
                | "make template dataset" >> beam.Map(schema_to_template_ds)
                | "generate chunks dynamically" >> beam.Map(self.dynamic_chunking_fn)
            )
        )

        # split datasets according to their write-targets (chunks of bytes)
        # then combine datasets with shared write targets
        # Note that the pipe (|) operator in beam is just sugar for passing into expand
        rechunked_datasets = ChunkToTarget().expand(
            indexed_datasets,
            target_chunks=beam.pvalue.AsSingleton(target_chunks_pcoll),
            schema=beam.pvalue.AsSingleton(schema),
        )

        target_store = PrepareZarrTarget(
            target=self.get_full_target(),
            attrs=self.attrs,
            encoding=self.encoding,
            append_dim=self.append_dim,
        ).expand(schema, beam.pvalue.AsSingleton(target_chunks_pcoll))

        # Actually attempt to write datasets to their target bytes/files
        rechunking = rechunked_datasets | "write chunks" >> beam.Map(
            store_dataset_fragment, target_store=beam.pvalue.AsSingleton(target_store)
        )

        # the last thing we need to do is extract the zarrstore target. To do this *after*
        # rechunking, we need to make the dependency on `rechunking` (and its side effects) explicit
        singleton_target_store = (
            rechunking
            | beam.combiners.Sample.FixedSizeGlobally(1)
            | beam.FlatMap(lambda x: x)  # https://stackoverflow.com/a/47146582
        )

        return singleton_target_store
