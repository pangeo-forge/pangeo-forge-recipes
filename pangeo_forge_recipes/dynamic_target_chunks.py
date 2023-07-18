import itertools
from typing import Dict, List

import numpy as np
import xarray as xr

from pangeo_forge_recipes.aggregation import XarraySchema, schema_to_template_ds


def get_memory_size(ds: xr.Dataset, chunks: Dict[str, int]) -> int:
    """Returns an estimate of memory size based on input chunks.
    Currently this applies the chunks input to the dataset, then
    iterates through the variables and returns the maximum.
    """
    ds_single_chunk = ds.isel({dim: slice(0, chunk) for dim, chunk in chunks.items()})
    mem_size = max([ds_single_chunk[var].nbytes for var in ds_single_chunk.data_vars])
    return mem_size


def difference(a: np.ndarray, b: np.ndarray) -> np.ndarray:
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


def dynamic_target_chunks_from_schema(
    schema: XarraySchema,
    target_chunk_nbytes: int,  # TODO: Accept a str like `100MB`
    target_chunk_ratio: Dict[str, int],
    nbytes_tolerance: float = 0.2,
) -> dict[str, int]:

    ds = schema_to_template_ds(schema)

    if set(target_chunk_ratio.keys()) != set(ds.dims):
        raise ValueError(
            f"target_chunk_ratio must contain all dimensions in dataset. "
            f"Got {target_chunk_ratio.keys()} but expected {list(ds.dims.keys())}"
        )

    dims, shape = zip(*ds.dims.items())
    ratio = [target_chunk_ratio[dim] for dim in dims]
    # The target ratio is defined for total chunks along a certain axis
    # This means we need to scale the ratio by the shape
    ratio_scaled = np.array(ratio)/np.array(shape)
    # the input ratio targets the total number of 
    ratio_normalized = normalize(ratio_scaled)

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
    tolerance = nbytes_tolerance * target_chunk_nbytes
    combinations_filtered = [
        c
        for c, s in zip(combinations, combination_sizes)
        if abs(s - target_chunk_nbytes) < tolerance
    ]

    # If there are no matches in the range, the user has to increase the tolerance for this to work.
    if len(combinations_filtered) == 0:
        raise ValueError(
            "Could not find any chunk combinations satisfying the size constraint. Consider increasing tolerance"
        )

    # Now that we have cominations in the memory size range we want, we can check which is closest to our
    # desired chunk ratio. We can think of this as comparing the angle of two vectors.
    # To compare them we need to normalize (we dont care about the amplitude here)

    # convert the combinations into the normalized inverse
    ratio_combinations = [normalize(1 / np.array(c)) for c in combinations_filtered]

    # Find the 'closest' fit of chunk ratio to the target ratio
    # cartesian difference between vectors ok?
    ratio_difference = [difference(ratio_normalized, r) for r in ratio_combinations]

    combinations_sorted = [c for _, c in sorted(zip(ratio_difference, combinations_filtered))]

    # Return the chunk combination with the closest fit
    optimal_combination = combinations_sorted[0]

    return {dim: chunk for dim, chunk in zip(dims, optimal_combination)}
