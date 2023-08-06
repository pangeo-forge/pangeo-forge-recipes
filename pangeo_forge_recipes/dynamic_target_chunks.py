import itertools
import warnings
from typing import Dict, List, Union

import numpy as np
import xarray as xr
from dask.utils import parse_bytes

from pangeo_forge_recipes.aggregation import XarraySchema, schema_to_template_ds


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


def dynamic_target_chunks_from_schema(
    schema: XarraySchema,
    target_chunk_size: Union[int, str],
    target_chunks_aspect_ratio: Dict[str, int],
    size_tolerance: float,
    default_ratio: int = -1,
    allow_extra_dims: bool = False,
) -> dict[str, int]:
    """Determine chunksizes based on desired chunksize (max size of any variable in the
    dataset) and the ratio of total chunks along each dimension of the dataset. The
    algorithm finds even divisors, and chooses possible combination that produce chunk
    sizes close to the target. From this set of combination the chunks that most closely
    produce the aspect ratio of total chunks along the given dimensions is returned.

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

    dims, shape = zip(*ds.dims.items())
    ratio = [target_chunks_aspect_ratio[dim] for dim in dims]

    possible_chunks = []
    for s, r, dim in zip(shape, ratio, dims):
        if r > 0:
            # Get a list of all the even divisors
            possible_chunks.append(even_divisor_chunks(s))
        elif r == -1:
            # Always keep this dimension unchunked
            possible_chunks.append([s])
        else:
            raise ValueError(
                f"Ratio value can only be larger than 0 or -1. Got {r} for dimension {dim}"
            )

    combinations = [p for p in itertools.product(*possible_chunks)]
    # Check the size of each combination on the dataset
    combination_sizes = [
        get_memory_size(ds, {dim: chunk for dim, chunk in zip(dims, c)}) for c in combinations
    ]

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

    # convert each combination into an array of resulting chunks per dimension, then normalize
    ratio_combinations = [normalize(np.array(shape) / np.array(c)) for c in combinations_filtered]

    # Find the 'closest' fit between normalized ratios
    # cartesian difference between vectors ok?
    ratio_normalized = normalize(np.array(ratio))
    ratio_similarity = [similarity(ratio_normalized, r) for r in ratio_combinations]

    # sort by the mostl similar (smallest value of ratio_similarity)
    combinations_sorted = [c for _, c in sorted(zip(ratio_similarity, combinations_filtered))]

    # Return the chunk combination with the closest fit
    optimal_combination = combinations_sorted[0]

    return {dim: chunk for dim, chunk in zip(dims, optimal_combination)}
