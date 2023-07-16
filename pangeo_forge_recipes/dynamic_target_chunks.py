import xarray as xr
import numpy as np
from typing import Dict, List
from pangeo_forge_recipes.aggregation import schema_to_template_ds
from pangeo_forge_recipes.aggregation import XarraySchema

import itertools
import numpy as np


def get_memory_size(ds:xr.Dataset, chunks: Dict[str, int]) -> int:
    """Returns an estimate of memory size based on input chunks.
    Currently this applies the chunks input to the dataset, then 
    iterates through the variables and returns the maximum.
    """
    ds_single_chunk = ds.isel({dim:slice(0,chunk) for dim, chunk in chunks.items()})
    mem_size = max([ds_single_chunk[var].nbytes for var in ds_single_chunk.data_vars])
    return mem_size

def difference(a:np.ndarray, b:np.ndarray) -> np.ndarray:
    return np.sqrt(np.sum((a-b)**2))

def normalize(a: np.ndarray) -> np.ndarray:
    """Convert to a unit vector"""
    return a/np.sqrt(np.sum(a**2))

def even_divisor_chunks(n:int) -> List[int]:
    """Returns values that evenly divide n"""
    divisors = []
    for i in range(1,n+1):
        if n % i == 0:
            divisors.append(n//i)
    return divisors

def dynamic_target_chunks_from_schema(
    schema: XarraySchema,
    target_chunk_nbytes: int, #TODO: Accept a str like `100MB`
    target_chunk_ratio: Dict[str,int],
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
    ratio_normalized = normalize(np.array(ratio))

    possible_chunks = []
    for s, r, dim in zip(shape, ratio, dims):
        if r > 0:
            # Get a list of all the even divisors
            possible_chunks.append(even_divisor_chunks(s))
        elif r == -1:
            # Always keep this dimension unchunked
            possible_chunks.append([s])
        else:
            raise ValueError(f'Ratio value can only be larger than 0 or -1. Got {r} for dimension {dim}')    

    combinations = [p for p in itertools.product(*possible_chunks)]
    # Check the size of each combination on the dataset 
    combination_sizes = [get_memory_size(ds, {dim:chunk for dim, chunk in zip(dims, c)}) for c in combinations]

    # And select a subset with some form of tolerance based on the size requirement
    tolerance = nbytes_tolerance * target_chunk_nbytes
    combinations_filtered = [c for c, s in zip(combinations, combination_sizes) if abs(s - target_chunk_nbytes) < tolerance]

    # If there are no matches in the range, the user has to increase the tolerance for this to work.
    if len(combinations_filtered) == 0:
        raise ValueError(
            'Could not find any chunk combinations satisfying the size constraint. Consider increasing tolerance'
            )

    # Now that we have cominations in the memory size range we want, we can check which is closest to our 
    # desired chunk ratio. We can think of this as comparing the angle of two vectors. 
    # To compare them we need to normalize (we dont care about the amplitude here)

    # convert the combinations into the normalized inverse
    ratio_combinations = [normalize(1/np.array(c)) for c in combinations_filtered]
    # ratio_combinations = [normalize(np.array(c)) for c in combinations_filtered]

    # Find the 'closest' fit of chunk ratio to the target ratio
    # cartesian difference between vectors ok?
    ratio_difference = [difference(ratio_normalized, r) for r in ratio_combinations]

    combinations_sorted = [c for _,c in sorted(zip(ratio_difference, combinations_filtered))]
    
    # Return the chunk combination with the closest fit
    optimal_combination = combinations_sorted[0]
    
    return {dim:chunk for dim, chunk in zip(dims, optimal_combination)}



    # """Dynamically determine target_chunks based on relative number of chunks for 
    # each dimension
    # Example: 
    # assume a dataset with dimensions (x, y, time, depth)
    # dynamic_target_chunks_from_schema(
    #     schema,
    #     1e9,
    #     target_chunks={'time': 8, 'x':1, 'y':1, 'depth':-1}
    #     )
    # will return a dataset with 8 times more chunks along time than along x and y 
    # dimension () while the depth dimension remains in a single chunk.

    # """

    # ds = schema_to_template_ds(schema)

    # if set(target_chunk_ratio.keys()) != set(ds.dims):
    #     raise ValueError(
    #         f"target_chunk_length must contain all dimensions in dataset. "
    #         f"Got {target_chunk_ratio.keys()} but expected {ds.dims}"
    #     )
    
    # min_len = min([len(ds[dim]) for dim in ds.dims])

    # # Find the chunk sizes for a 'unit chunk', 
    # # the smallest chunk that represents the ratio of dimension lengths on the dataset
    # unit_chunks = {dim: int(len(ds[dim])/min_len) for dim in ds.dims}
    # # Now scale the unit chunk with the desired target_chunk_length for each dimension. 
    # optimal_chunks = {dim: unit_chunks[dim]/target_chunk_ratio[dim] for dim in target_chunk_ratio.keys()}
    # # Normalize the optimal chunks (which will not be guaranteed to be ints) so that the minimal value is 1
    # min_optimal = min(list(optimal_chunks.values()))
    # optimal_chunks_normalized = {dim: int(chunk/min_optimal) for dim, chunk in optimal_chunks.items()}

    # # Estimate the size of the `optimal_chunks_normalized` for each variable and use the largest as an estimate
    # # for the memory use of each chunk. 
    # # Then find a scaling factor that will bring the memory use of each chunk close to the target_chunk_nbytes
    # ds = ds.isel({dim:slice(0, chunks) for dim, chunks in optimal_chunks_normalized.items()})
    # mem_size = max([ds[var].nbytes for var in ds.data_vars])
    # scale_factor = (target_chunk_nbytes / mem_size) ** 1/len(ds.dims)

    # # Apply the scaling factor
    # best_chunks = {
    #     dim:int(scale_factor*chunks) for dim, chunks in optimal_chunks_normalized.items()
    # }
    # # Finally make sure the chunks are not larger than the dataset
    # best_chunks = {
    #     dim: best_chunk if best_chunk <= len(ds[dim]) else len(ds[dim]) for dim, best_chunk in best_chunks.items()
    #     }
    # return best_chunks