import itertools
import logging
import warnings
from typing import Dict, List, Union

import numpy as np
import xarray as xr
from dask.utils import parse_bytes

from pangeo_forge_recipes.aggregation import XarraySchema, schema_to_template_ds

logger = logging.getLogger(__name__)


def get_memory_size(ds: xr.Dataset, chunks: Dict[str, int]) -> int:
    """Returns an estimate of memory size based on input chunks.
    Currently this applies the chunks input to the dataset, then
    iterates through the variables and returns the maximum.
    """
    ds_single_chunk = ds.isel({dim: slice(0, chunk) for dim, chunk in chunks.items()})
    mem_size = max([ds_single_chunk[var].nbytes for var in ds_single_chunk.data_vars])
    return mem_size


def similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    return np.sqrt(np.sum((a - b) ** 2))


def normalize(a: np.ndarray) -> np.ndarray:
    """Convert to a unit vector"""
    return a / np.sqrt(np.sum(a**2))


def even_divisor_chunks(n: int) -> List[int]:
    """Returns values that evenly divide n"""
    divisors = []
    for i in range(1, n + 1):
        if n % i == 0:
            divisors.append(n // i)
    return divisors


def _maybe_parse_bytes(target_chunk_size: Union[str, int]) -> int:
    if isinstance(target_chunk_size, str):
        return parse_bytes(target_chunk_size)
    else:
        return target_chunk_size


def even_divisor_algo(
    ds: xr.Dataset,
    target_chunk_size: int,
    target_chunks_aspect_ratio: Dict[str, int],
    size_tolerance: float,
) -> Dict[str, int]:
    logger.info("Running primary dynamic chunking algorithm using even divisors")

    # filter out the dimensions that are unchunked
    target_chunks_aspect_ratio_chunked_only = {
        dim: ratio for dim, ratio in target_chunks_aspect_ratio.items() if ratio != -1
    }
    print(f"{target_chunks_aspect_ratio_chunked_only=}")
    unchunked_dims = [
        dim
        for dim in target_chunks_aspect_ratio.keys()
        if dim not in target_chunks_aspect_ratio_chunked_only.keys()
    ]
    print(f"{unchunked_dims=}")

    possible_chunks = []
    for dim, s in ds.dims.items():
        if dim in unchunked_dims:
            # Always keep this dimension unchunked
            possible_chunks.append([s])
        else:
            # Get a list of all the even divisors
            possible_chunks.append(even_divisor_chunks(s))

    combinations = [
        {dim: chunk for dim, chunk in zip(ds.dims.keys(), c)}
        for c in itertools.product(*possible_chunks)
    ]
    # Check the size of each combination on the dataset
    combination_sizes = [get_memory_size(ds, c) for c in combinations]

    # And select a subset with some form of tolerance based on the size requirement
    tolerance = size_tolerance * target_chunk_size
    combinations_filtered = [
        c for c, s in zip(combinations, combination_sizes) if abs(s - target_chunk_size) < tolerance
    ]

    # If there are no matches in the range, the user has to increase the tolerance for this to work.
    if len(combinations_filtered) == 0:
        raise ValueError(
            (
                "Could not find any chunk combinations satisfying "
                "the size constraint. Consider increasing tolerance"
            )
        )

    # Now that we have cominations in the memory size range we want, we can check which is
    # closest to our desired chunk ratio.
    # We can think of this as comparing the angle of two vectors.
    # To compare them we need to normalize (we dont care about the amplitude here)

    # For the size estimation we needed the chunk combinations to be complete
    # (cover all dimensions, even the unchunked ones). To find the closest fit
    # to the desired aspect ratio we need to remove the unchunked dimensions.

    combinations_filtered_chunked_only = [
        {dim: chunk for dim, chunk in c.items() if dim not in unchunked_dims}
        for c in combinations_filtered
    ]

    # convert each combination into an array of resulting chunks per dimension, then normalize
    dims_chunked_only = list(
        target_chunks_aspect_ratio_chunked_only.keys()
    )  # the order of these does matter

    shape_chunked_only = np.array([ds.dims[dim] for dim in dims_chunked_only])
    ratio = [
        shape_chunked_only / np.array([c[dim] for dim in dims_chunked_only])
        for c in combinations_filtered_chunked_only
    ]
    ratio_normalized = [normalize(r) for r in ratio]

    # Find the 'closest' fit between normalized ratios
    # cartesian difference between vectors ok?
    target_ratio_normalized = normalize(
        np.array([target_chunks_aspect_ratio_chunked_only[dim] for dim in dims_chunked_only])
    )
    ratio_similarity = [similarity(target_ratio_normalized, r) for r in ratio_normalized]

    # sort by similarity and return the corresponding full combination
    # (including the unchunked dimensions)
    combinations_sorted = [
        c for _, c in sorted(zip(ratio_similarity, combinations_filtered), key=lambda a: a[0])
    ]

    # Return the chunk combination with the closest fit
    return combinations_sorted[0]


def iterative_ratio_increase_algo(
    ds: xr.Dataset,
    target_chunk_size: int,
    target_chunks_aspect_ratio: Dict[str, int],
    size_tolerance: float,
) -> Dict[str, int]:
    logger.info("Running secondary dynamic chunking")
    # Alternative algorithm that starts with a normalized chunk aspect ratio and iteratively scales
    # it until the desired chunk size is reached.

    # Steps
    # Deduce the maximum chunksize that would adhere to the given aspect ratio by
    # dividing the dimension length by the aspect ratio

    # Then iteratively divide this chunksize by a scaling factor until the
    # resulting chunksize is below the largest size within tolerance

    # Test for the resulting chunk size. If the size is within the tolerance, return the chunk size.
    # If the size is below the tolerance, raise an error. In this case we need some more
    # sophisicated logic or increase the tolerance.

    def maybe_scale_chunk(ratio, scale_factor, dim_length):
        """Scale a single dimension of a unit chunk by a given scaling factor"""
        if ratio == -1:
            return dim_length
        else:
            max_chunk = (
                dim_length / ratio
            )  # determine the largest chunksize that would adhere to the given aspect ratio
            scaled_chunk = max(1, round(max_chunk / scale_factor))
            return scaled_chunk

    def scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, scale_factor):
        scaled_normalized_chunks = {
            dim: maybe_scale_chunk(ratio, scale_factor, ds.dims[dim])
            for dim, ratio in target_chunks_aspect_ratio.items()
        }
        return scaled_normalized_chunks

    max_chunks = scale_and_normalize_chunks(
        ds, target_chunks_aspect_ratio, 1
    )  # largest possible chunk size for each dimension
    logger.info(f"{max_chunks=}")
    max_scale_factor = max(max_chunks.values())
    logger.info(f"{max_scale_factor=}")
    # Compute the size for each scaling factor and choose the
    # closest fit to the desired chunk size
    scale_factors = np.arange(1, max_scale_factor + 1)
    # TODO: There is probably a smarter way (binary search?) to narrow this
    # TODO: range down and speed this up. For now this should work.
    sizes = np.array(
        [
            get_memory_size(
                ds, scale_and_normalize_chunks(ds, target_chunks_aspect_ratio, scale_factor)
            )
            for scale_factor in scale_factors
        ]
    )
    logger.info(f"{sizes=}")
    logger.info(f" Min size{sizes[-1]}")
    logger.info(f" Max size{sizes[0]}")

    size_mismatch = abs(sizes - target_chunk_size)

    # find the clostest match to the target chunk size
    optimal_scale_factor = [sf for _, sf in sorted(zip(size_mismatch, scale_factors))][0]

    optimal_target_chunks = scale_and_normalize_chunks(
        ds, target_chunks_aspect_ratio, optimal_scale_factor
    )
    optimal_size = get_memory_size(ds, optimal_target_chunks)

    # check if the resulting chunk size is within tolerance
    lower_bound = target_chunk_size * (1 - size_tolerance)
    upper_bound = target_chunk_size * (1 + size_tolerance)
    logger.info(f"{optimal_size=} {lower_bound=} {upper_bound=}")
    if not (optimal_size >= lower_bound and optimal_size <= upper_bound):
        raise ValueError(
            (
                "Could not find any chunk combinations satisfying "
                "the size constraint. Consider increasing tolerance"
            )
        )
    return optimal_target_chunks


def dynamic_target_chunks_from_schema(
    schema: XarraySchema,
    target_chunk_size: Union[int, str],
    target_chunks_aspect_ratio: Dict[str, int],
    size_tolerance: float,
    default_ratio: int = -1,
    allow_extra_dims: bool = False,
    allow_fallback_algo: bool = False,
) -> dict[str, int]:
    """Determine chunksizes based on desired chunk size and the ratio of total chunks
    along each dimension of the dataset. The algorithm finds even divisors, and chooses
    possible combinations that produce chunk sizes close to the target. From this set
    of combinations the chunks that most closely resemble the aspect ratio of total
    chunks along the given dimensions is returned.

    Parameters
    ----------
    schema : XarraySchema
        Schema of the input dataset
    target_chunk_size : Union[int, str]
        Desired single chunks size. Can be provided as
        integer (bytes) or as a str like '100MB' etc.
    target_chunks_aspect_ratio: Dict[str, int]
        Dictionary mapping dimension names to desired
        aspect ratio of total number of chunks along each dimension. Dimensions present
        in the dataset but not in target_chunks_aspect_ratio will be filled with
        default_ratio. If allow_extra_dims is true, target_chunks_aspect_ratio can contain
        dimensions not present in the dataset, which will be removed in the ouput.
        A value of -1 can be passed to entirely prevent chunking along that dimension.
    size_tolerance : float
        Chunksize tolerance. Resulting chunk size will be within
        [target_chunk_size*(1-size_tolerance),
        target_chunk_size*(1+size_tolerance)]
    default_ratio : int, optional
        Default value to use for dimensions on the dataset not specified in
        target_chunks_aspect_ratio, by default -1, meaning that the ommited dimension will
        not be chunked.
    allow_extra_dims : bool, optional
        Allow to pass dimensions not present in the dataset to be passed in
        target_chunks_aspect_ratio, by default False
    allow_fallback_algo : bool, optional
        If True, allows the use of secondary algorithm if finding chunking scheme with
        even divisors fails.

    Returns
    -------
    dict[str, int]
        Target chunk dictionary. Can be passed directly to `ds.chunk()`

    """
    target_chunk_size = _maybe_parse_bytes(target_chunk_size)

    ds = schema_to_template_ds(schema)

    missing_dims = set(ds.dims) - set(target_chunks_aspect_ratio.keys())
    extra_dims = set(target_chunks_aspect_ratio.keys()) - set(ds.dims)

    if not allow_extra_dims and len(extra_dims) > 0:
        raise ValueError(
            f"target_chunks_aspect_ratio contains dimensions not present in dataset. "
            f"Got {list(extra_dims)} but expected {list(ds.dims.keys())}. You can pass "
            "additional dimensions by setting allow_extra_dims=True"
        )
    elif allow_extra_dims and len(extra_dims) > 0:
        # trim extra dimension out of target_chunks_aspect_ratio
        warnings.warn(f"Trimming dimensions {list(extra_dims)} from target_chunks_aspect_ratio")
        target_chunks_aspect_ratio = {
            dim: v for dim, v in target_chunks_aspect_ratio.items() if dim in ds.dims
        }

    if len(missing_dims) > 0:
        # set default ratio for missing dimensions
        warnings.warn(
            f"dimensions {list(missing_dims)} are not specified in target_chunks_aspect_ratio."
            f"Setting default value of {default_ratio} for these dimensions."
        )
        target_chunks_aspect_ratio.update({dim: default_ratio for dim in missing_dims})

    # check values in target_chunks_aspect_ratio
    for dim, ratio in target_chunks_aspect_ratio.items():
        if not (ratio >= 1 or ratio == -1):
            raise ValueError(
                f"Ratio value can only be larger than 0 or -1. Got {ratio} for dimension {dim}"
            )
        if not isinstance(ratio, int):
            raise ValueError(f"Ratio value must be an integer. Got {ratio} for dimension {dim}")
    try:
        target_chunks = even_divisor_algo(
            ds,
            target_chunk_size,
            target_chunks_aspect_ratio,
            size_tolerance,
        )
    except ValueError as e:
        if allow_fallback_algo:
            warnings.warn(
                "Primary algorithm using even divisors along each dimension failed "
                f"with {e}. Trying secondary algorithm."
            )
            target_chunks = iterative_ratio_increase_algo(
                ds,
                target_chunk_size,
                target_chunks_aspect_ratio,
                size_tolerance,
            )
        else:
            raise ValueError(
                (
                    "Could not find any chunk combinations satisfying "
                    "the size constraint. Consider increasing size_tolerance"
                    " or enabling allow_fallback_algo."
                )
            )
    return target_chunks
