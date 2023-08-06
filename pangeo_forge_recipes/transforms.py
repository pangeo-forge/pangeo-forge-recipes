from __future__ import annotations

import logging
from dataclasses import dataclass, field

# from functools import wraps
from typing import Dict, List, Optional, Tuple, TypeVar, Union, cast

import apache_beam as beam

from .aggregation import XarraySchema, dataset_to_schema, schema_to_zarr
from .combiners import CombineMultiZarrToZarr, CombineXarraySchemas
from .dynamic_target_chunks import dynamic_target_chunks_from_schema
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
    """Function for DropKeys transform."""
    key, item = kvp
    return item


@dataclass
class DropKeys(beam.PTransform):
    """Simple transform to remove keys for use in a Kerchunk Reference Recipe Pipeline"""

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

    :param file_type: The type of file to be openend; e.g. "netcdf4", "netcdf3", "grib", etc.
    :param inline_threshold: Passed to kerchunk opener.
    :param storage_options: Storage options dict to pass to the kerchunk opener.
    :param remote_protocol: If files are accessed over the network, provide the remote protocol
      over which they are accessed. e.g.: "s3", "https", etc.
    :param kerchunk_open_kwargs: Additional kwargs to pass to kerchunk opener. Any kwargs which
      are specific to a particular input file type should be passed here;  e.g.,
      ``{"filter": ...}`` for GRIB; ``{"max_chunk_size": ...}`` for NetCDF3, etc.
    :param drop_keys: If True, remove Pangeo Forge's FilePattern keys from the output PCollection
      before returning. This is the default behavior, which is used for cases where the output
      PCollection of references is passed to the ``CombineReferences`` transform for creation of a
      Kerchunk reference dataset as the target dataset of the pipeline. If this transform is used
      for other use cases (e.g., opening inputs for creation of another target dataset type), you
      may want to set this option to False to preserve the keys on the output PCollection.
    """

    # passed directly to `open_with_kerchunk`
    file_type: FileType = FileType.unknown
    inline_threshold: Optional[int] = 300
    storage_options: Optional[Dict] = None
    remote_protocol: Optional[str] = None
    kerchunk_open_kwargs: Optional[dict] = field(default_factory=dict)

    # not passed to `open_with_kerchunk`
    drop_keys: bool = True

    def expand(self, pcoll):
        refs = pcoll | "Open with Kerchunk" >> beam.Map(
            _add_keys(open_with_kerchunk),
            file_type=self.file_type,
            inline_threshold=self.inline_threshold,
            storage_options=self.storage_options,
            remote_protocol=self.remote_protocol,
            kerchunk_open_kwargs=self.kerchunk_open_kwargs,
        )
        return refs if not self.drop_keys else refs | DropKeys()


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

    :param concat_dims: Dimensions along which to concatenate inputs.
    :param identical_dims: Dimensions shared among all inputs.
    :mzz_kwargs: Additional kwargs to pass to ``kerchunk.combine.MultiZarrToZarr``.
    :precombine_inputs: If ``True``, precombine each input with itself, using
      ``kerchunk.combine.MultiZarrToZarr``, before adding it to the accumulator.
      Used for multi-message GRIB2 inputs, which produce > 1 reference when opened
      with kerchunk's ``scan_grib`` function, and therefore need to be consolidated
      into a single reference before adding to the accumulator. Also used for inputs
      consisting of single reference, for cases where the output dataset concatenates
      along a dimension that does not exist in the individual inputs. In this latter
      case, precombining adds the additional dimension to the input so that its
      dimensionality will match that of the accumulator.
    """

    concat_dims: List[str]
    identical_dims: List[str]
    mzz_kwargs: dict = field(default_factory=dict)
    precombine_inputs: bool = False

    def expand(self, references: beam.PCollection) -> beam.PCollection:
        return references | beam.CombineGlobally(
            CombineMultiZarrToZarr(
                concat_dims=self.concat_dims,
                identical_dims=self.identical_dims,
                mzz_kwargs=self.mzz_kwargs,
                precombine_inputs=self.precombine_inputs,
            ),
        )


@dataclass
class WriteCombinedReference(beam.PTransform, ZarrWriterMixin):
    """Store a singleton PCollection consisting of a ``kerchunk.combine.MultiZarrToZarr`` object.

    :param output_json_fname: Name to give the output references file. Must end in ``.json``.
    """

    output_json_fname: str = "reference.json"

    def expand(self, reference: beam.PCollection) -> beam.PCollection:
        return reference | beam.Map(
            write_combined_reference,
            full_target=self.get_full_target(),
            output_json_fname=self.output_json_fname,
        )


@dataclass
class StoreToZarr(beam.PTransform, ZarrWriterMixin):
    """Store a PCollection of Xarray datasets to Zarr.

    :param combine_dims: The dimensions to combine
    :param target_chunks: Dictionary mapping dimension names to chunks sizes.
        If a dimension is a not named, the chunks will be inferred from the data.
    :param target_chunk_aspect_ratio: Dictionary mapping dimension names to desired
        aspect ratio of total number of chunks along each dimension.
    :param target_chunk_size: Desired single chunks size. Can be provided as
        integer (bytes) or as a str like '100MB' etc.
    :param size_tolerance : float, optional
        Chunksize tolerance. Resulting chunk size will be within
        [target_chunk_size*(1-size_tolerance),
        target_chunk_size*(1+size_tolerance)] , by default 0.2
    """

    # TODO: make it so we don't have to explicitly specify combine_dims
    # Could be inferred from the pattern instead
    combine_dims: List[Dimension]
    target_chunks: Optional[Dict[str, int]] = field(default=None)
    target_chunks_aspect_ratio: Optional[Dict[str, int]] = field(default=None)
    target_chunk_size: Optional[Union[str, int]] = field(
        default=None
    )  # ? Should we provide a default?
    size_tolerance: float = 0.2
    default_ratio: int = -1
    allow_extra_dims: bool = False

    def __post_init__(self):
        # if none of the chunking parameters is specified, set the default behavior
        # of target_chunks={}
        if all(
            a is None
            for a in (self.target_chunks, self.target_chunk_size, self.target_chunks_aspect_ratio)
        ):
            self.target_chunks = {}

        # check that not both static and dynamic chunking are specified
        if self.target_chunks is not None and (
            self.target_chunk_size is not None or self.target_chunks_aspect_ratio is not None
        ):
            raise ValueError(
                (
                    "Cannot specify both target_chunks and "
                    "target_chunk_size or target_chunks_aspect_ratio."
                )
            )

        # if dynamic chunking is specified, make sure both target_chunk_size
        # and target_chunks_aspect_ratio are specified
        if self.target_chunks is None and not (
            self.target_chunk_size is not None and self.target_chunks_aspect_ratio is not None
        ):
            raise ValueError(
                (
                    "Must specify both target_chunk_size and "
                    "target_chunks_aspect_ratio to enable dynamic chunking."
                )
            )

    def determine_target_chunks(self, schema: XarraySchema) -> Dict[str, int]:
        # if dynamic chunking is chosen, set the objects target_chunks here
        # We need to explicitly cast the types here because we know
        # that for the combinations chosen here (dynamic vs static chunking)
        # the type of the input is never None (our __post_init__ method takes
        # care of raising errors if this is the case).
        if self.target_chunks_aspect_ratio is not None:
            target_chunks = dynamic_target_chunks_from_schema(
                schema,
                target_chunk_size=cast(Union[int, str], self.target_chunk_size),
                target_chunks_aspect_ratio=cast(Dict[str, int], self.target_chunks_aspect_ratio),
                size_tolerance=self.size_tolerance,
                default_ratio=self.default_ratio,
                allow_extra_dims=self.allow_extra_dims,
            )
            logger.info(f"Dynamically determined target_chunks: {target_chunks}")
        else:
            target_chunks = cast(Dict[str, int], self.target_chunks)
        return target_chunks

    def expand(self, datasets: beam.PCollection) -> beam.PCollection:
        schema = datasets | DetermineSchema(combine_dims=self.combine_dims)
        target_chunks = beam.pvalue.AsSingleton(schema | beam.Map(self.determine_target_chunks))
        indexed_datasets = datasets | IndexItems(schema=schema)
        rechunked_datasets = indexed_datasets | Rechunk(target_chunks=target_chunks, schema=schema)
        target_store = schema | PrepareZarrTarget(
            target=self.get_full_target(), target_chunks=target_chunks
        )
        return rechunked_datasets | StoreDatasetFragments(target_store=target_store)
