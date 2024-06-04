from __future__ import annotations

import logging
import math
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

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

from .aggregation import XarraySchema, dataset_to_schema, schema_to_template_ds, schema_to_zarr
from .combiners import CombineXarraySchemas, MinMaxCountCombineFn
from .openers import open_url, open_with_kerchunk, open_with_xarray
from .patterns import CombineOp, Dimension, FileType, Index, augment_index_with_start_stop
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


@dataclass
class TransferFilesWithConcurrency(beam.DoFn):
    """
    A DoFn for transferring files with concurrency.

    Attributes:
        transfer_target: The target directory to which files will be transferred.
        concurrency_per_executor: The number of concurrent threads per executor.
        secrets: Optional dictionary containing secrets required for accessing the transfer target.
        open_kwargs: Optional dictionary of keyword arguments to be passed when opening files.
        fsspec_sync_patch: Experimental. Likely slower. When enabled, this attempts to
            replace asynchronous code with synchronous implementations to potentially address
            deadlocking issues. cf. https://github.com/h5py/h5py/issues/2019
        max_retries: The maximum number of retries for failed file transfers.
        initial_backoff: The initial backoff time in seconds before retrying a failed transfer.
        backoff_factor: The factor by which the backoff time is multiplied after each retry.
    """

    transfer_target: CacheFSSpecTarget
    max_concurrency: int
    secrets: Optional[Dict] = None
    open_kwargs: Optional[Dict] = None
    fsspec_sync_patch: bool = False
    max_retries: int = 5
    initial_backoff: float = 1.0
    backoff_factor: float = 2.0

    def process(self, indexed_urls):
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            futures = {
                executor.submit(self.transfer_file, index, url): (index, url)
                for index, url in indexed_urls
            }
            for future in as_completed(futures):
                try:
                    index, new_url = future.result()
                    yield (index, new_url)
                except Exception as e:
                    _, url = futures[future]
                    logger.error(f"Error transferring file {url}: {e}")

    def transfer_file(self, index: int, url: str) -> Tuple[int, str]:
        retries = 0
        while retries <= self.max_retries:
            try:
                open_kwargs = self.open_kwargs or {}
                self.transfer_target.cache_file(
                    url, self.secrets, self.fsspec_sync_patch, **open_kwargs
                )
                return (index, self.transfer_target._full_path(url))
            except Exception as e:
                if retries == self.max_retries:
                    logger.error(f"Max retries reached for {url}: {e}")
                    raise e
                else:
                    backoff_time = self.initial_backoff * (self.backoff_factor**retries)
                    logger.warning(
                        f"Error transferring file {url}: {e}. Retrying in {backoff_time} seconds..."
                    )
                    time.sleep(backoff_time)
                    retries += 1

        raise RuntimeError(f"Failed to transfer file {url} after {self.max_retries} attempts.")


@dataclass
class CheckpointFileTransfer(beam.PTransform):
    """
    A Beam transform that transfers files to a cache target with concurrency and grouping by key.

    Attributes:
        transfer_target: The target to which files will be transferred. This can be a string URL
            or a CacheFSSpecTarget instance.
        secrets: Optional dictionary containing secrets required for accessing the transfer target.
        open_kwargs: Optional dictionary of keyword arguments to be passed when opening files.
        max_executors: The maximum number of executors to be used. Elements will be grouped by this
            number to limit total cluster concurrency. Default is 20.
        concurrency_per_executor (Optional[int]): The number of concurrent threads per executor.
            Default is 10.
        fsspec_sync_patch: Experimental. Likely slower. When enabled, this attempts to
            replace asynchronous code with synchronous implementations to potentially address
            deadlocking issues. cf. https://github.com/h5py/h5py/issues/2019
        max_retries: The maximum number of retries for failed file transfers.
        initial_backoff: The initial backoff time in seconds before retrying a failed transfer.
        backoff_factor: The factor by which the backoff time is multiplied after each retry.
    """

    transfer_target: Union[str, CacheFSSpecTarget]
    secrets: Optional[dict] = None
    open_kwargs: Optional[dict] = None
    max_executors: int = 20
    concurrency_per_executor: int = 10
    fsspec_sync_patch: bool = False
    max_retries: int = 5
    initial_backoff: float = 1.0
    backoff_factor: float = 2.0

    def assign_keys(self, element) -> Tuple[int, Any]:
        index, url = element
        key = random.randint(0, self.max_executors - 1)
        return (key, (index, url))

    def expand(self, pcoll):
        if isinstance(self.transfer_target, str):
            target = CacheFSSpecTarget.from_url(self.transfer_target)
        else:
            target = self.transfer_target

        return (
            pcoll
            | "Assign Executor Grouping Key" >> beam.Map(self.assign_keys)
            | "Group per-executor work" >> beam.GroupByKey()
            | "Unkey after grouping" >> beam.Values()
            | "Limited concurrency file transfer"
            >> beam.ParDo(
                TransferFilesWithConcurrency(
                    transfer_target=target,
                    secrets=self.secrets,
                    open_kwargs=self.open_kwargs,
                    max_concurrency=self.concurrency_per_executor,
                    fsspec_sync_patch=self.fsspec_sync_patch,
                    max_retries=self.max_retries,
                    initial_backoff=self.initial_backoff,
                    backoff_factor=self.backoff_factor,
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
                    | f"Combine {last_dim.name}"
                    >> beam.CombinePerKey(CombineXarraySchemas(last_dim))
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
        return pcoll | beam.Map(
            store_dataset_fragment, target_store=beam.pvalue.AsSingleton(self.target_store)
        )


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
    :param dynamic_chunking_fn_kwargs: Optional keyword arguments for ``dynamic_chunking_fn``.
    :param attrs: Extra group-level attributes to inject into the dataset.
    :param encoding: Dictionary encoding for xarray.to_zarr().
    :param append_dim: Optional name of the dimension to append to.
    """

    # TODO: make it so we don't have to explicitly specify combine_dims
    # Could be inferred from the pattern instead
    combine_dims: List[Dimension]
    store_name: str
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
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
        logger.info(f"Storing Zarr with {target_chunks=} to {self.get_full_target()}")
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
