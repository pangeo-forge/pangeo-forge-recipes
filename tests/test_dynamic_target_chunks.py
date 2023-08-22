from typing import Dict

import dask.array as dsa
import pytest
import xarray as xr

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.dynamic_target_chunks import (
    dynamic_target_chunks_from_schema,
    even_divisor_algo,
    iterative_ratio_increase_algo,
)


def _create_ds(dims_shape: Dict[str, int]) -> xr.Dataset:
    return xr.DataArray(
        dsa.random.random(list(dims_shape.values())),
        dims=list(dims_shape.keys()),
    ).to_dataset(name="data")


@pytest.mark.parametrize(
    ("dims_shape", "target_chunks_aspect_ratio", "expected_target_chunks"),
    [
        # make sure that for the same dataset we get smaller chunksize along
        # a dimension if the ratio is larger
        (
            {"x": 200, "y": 200, "z": 200},
            {"x": 1, "y": 1, "z": 10},
            {"x": 50, "y": 100, "z": 25},
        ),
        (
            {"x": 200, "y": 200, "z": 200},
            {"x": 10, "y": 1, "z": 1},
            {"x": 25, "y": 50, "z": 100},
        ),
        # test the special case where we want to just chunk along a single dimension
        (
            {"x": 100, "y": 300, "z": 400},
            {"x": -1, "y": -1, "z": 1},
            {"x": 100, "y": 300, "z": 4},
        ),
    ],
)
def test_dynamic_rechunking(dims_shape, target_chunks_aspect_ratio, expected_target_chunks):
    ds = _create_ds(dims_shape)
    schema = dataset_to_schema(ds)
    target_chunks = dynamic_target_chunks_from_schema(
        schema, 1e6, target_chunks_aspect_ratio=target_chunks_aspect_ratio, size_tolerance=0.2
    )
    print(target_chunks)
    for dim, chunks in expected_target_chunks.items():
        assert target_chunks[dim] == chunks


def test_nbytes_str_input():
    ds = _create_ds({"x": 100, "y": 100, "z": 100})
    schema = dataset_to_schema(ds)
    target_chunks_aspect_ratio = {"x": 1, "y": 1, "z": 1}
    target_chunks_int = dynamic_target_chunks_from_schema(
        schema, 1e6, target_chunks_aspect_ratio=target_chunks_aspect_ratio, size_tolerance=0.2
    )
    target_chunks_str = dynamic_target_chunks_from_schema(
        schema, "1MB", target_chunks_aspect_ratio=target_chunks_aspect_ratio, size_tolerance=0.2
    )
    for dim in target_chunks_aspect_ratio.keys():
        assert target_chunks_int[dim] == target_chunks_str[dim]


def test_maintain_ratio():
    """Confirm that for a given ratio with two differently sized datasets we
    maintain a constant ratio between total number of chunks"""
    ds_equal = _create_ds({"x": 64, "y": 64})
    ds_long = _create_ds({"x": 64, "y": 256})

    for ds in [ds_equal, ds_long]:
        print(ds)
        schema = dataset_to_schema(ds)
        target_chunks = dynamic_target_chunks_from_schema(
            schema, 1e4, target_chunks_aspect_ratio={"x": 1, "y": 4}, size_tolerance=0.2
        )
        ds_rechunked = ds.chunk(target_chunks)
        assert len(ds_rechunked.chunks["y"]) / len(ds_rechunked.chunks["x"]) == 4


@pytest.mark.parametrize(
    "target_chunks_aspect_ratio", [{"x": 1, "y": -1, "z": 10}, {"x": 6, "y": -1, "z": 2}]
)  # always keep y unchunked, and vary the others
@pytest.mark.parametrize("target_chunk_nbytes", [1e6, 1e7])
def test_skip_dimension(target_chunks_aspect_ratio, target_chunk_nbytes):
    ds = _create_ds({"x": 100, "y": 200, "z": 300})
    # Mark dimension as 'not-to-chunk' with -1
    schema = dataset_to_schema(ds)
    target_chunks = dynamic_target_chunks_from_schema(
        schema,
        target_chunk_nbytes,
        target_chunks_aspect_ratio=target_chunks_aspect_ratio,
        size_tolerance=0.2,
    )
    assert target_chunks["y"] == len(ds["y"])


@pytest.mark.parametrize("default_ratio", [-1, 1])
def test_missing_dimensions(default_ratio):
    ds = _create_ds({"x": 100, "y": 200, "z": 300})
    schema = dataset_to_schema(ds)
    # Test that a warning is raised
    msg = "are not specified in target_chunks_aspect_ratio.Setting default value of"
    with pytest.warns(UserWarning, match=msg):
        chunks_from_default = dynamic_target_chunks_from_schema(
            schema,
            1e6,
            target_chunks_aspect_ratio={"x": 1, "z": 10},
            size_tolerance=0.2,
            default_ratio=default_ratio,
        )
    chunks_explicit = dynamic_target_chunks_from_schema(
        schema,
        1e6,
        target_chunks_aspect_ratio={"x": 1, "y": default_ratio, "z": 10},
        size_tolerance=0.2,
    )
    assert chunks_from_default == chunks_explicit


def test_error_extra_dimensions_not_allowed():
    ds = _create_ds({"x": 100, "y": 200, "z": 300})
    schema = dataset_to_schema(ds)
    msg = "target_chunks_aspect_ratio contains dimensions not present in dataset."
    with pytest.raises(ValueError, match=msg):
        dynamic_target_chunks_from_schema(
            schema,
            1e6,
            target_chunks_aspect_ratio={"x": 1, "y_other_name": 1, "y": 1, "z": 10},
            size_tolerance=0.2,
        )


def test_extra_dimensions_allowed():
    ds = _create_ds({"x": 100, "y": 200, "z": 300})
    schema = dataset_to_schema(ds)
    with pytest.warns(UserWarning, match="Trimming dimensions"):
        chunks_with_extra = dynamic_target_chunks_from_schema(
            schema,
            1e6,
            target_chunks_aspect_ratio={"x": 1, "y_other_name": 1, "y": 1, "z": 10},
            size_tolerance=0.2,
            allow_extra_dims=True,
        )
    chunks_without_extra = dynamic_target_chunks_from_schema(
        schema,
        1e6,
        target_chunks_aspect_ratio={"x": 1, "y": 1, "z": 10},
        size_tolerance=0.2,
    )
    assert chunks_with_extra == chunks_without_extra


def test_non_int_ratio_input():
    ds = _create_ds({"x": 1, "y": 2, "z": 3})
    schema = dataset_to_schema(ds)
    with pytest.raises(ValueError, match="Ratio value must be an integer. Got 1.5 for dimension y"):
        dynamic_target_chunks_from_schema(
            schema,
            1e6,
            target_chunks_aspect_ratio={"x": 1, "y": 1.5, "z": 10},
            size_tolerance=0.2,
        )


def test_large_negative_ratio_input():
    ds = _create_ds({"x": 1, "y": 2, "z": 3})
    schema = dataset_to_schema(ds)
    with pytest.raises(
        ValueError, match="Ratio value can only be larger than 0 or -1. Got -100 for dimension y"
    ):
        dynamic_target_chunks_from_schema(
            schema,
            1e6,
            target_chunks_aspect_ratio={"x": 1, "y": -100, "z": 10},
            size_tolerance=0.2,
        )


def test_algo_comparison():
    """test that we get the same result from both algorithms for a known simple case"""
    ds = _create_ds({"x": 100, "y": 100, "z": 100})
    target_chunk_size = 4e5
    target_chunks_aspect_ratio = {"x": -1, "y": 2, "z": 10}
    size_tolerance = 0.01
    chunks_a = even_divisor_algo(
        ds,
        target_chunk_size,
        target_chunks_aspect_ratio=target_chunks_aspect_ratio,
        size_tolerance=size_tolerance,
    )
    chunks_b = iterative_ratio_increase_algo(
        ds,
        target_chunk_size,
        target_chunks_aspect_ratio=target_chunks_aspect_ratio,
        size_tolerance=size_tolerance,
    )
    assert chunks_a == chunks_b


def test_allow_fallback_algo():
    """test that we get a ValueError when we can't find a solution"""
    # create a dataset that has
    ds = _create_ds({"x": 100, "z": 1111})
    schema = dataset_to_schema(ds)
    target_chunk_size = 7e4
    target_chunks_aspect_ratio = {"x": -1, "z": 1}
    size_tolerance = 0.1

    msg = (
        "Could not find any chunk combinations satisfying the size constraint."
        " Consider increasing size_tolerance or enabling allow_fallback_algo."
    )
    with pytest.raises(ValueError, match=msg):
        dynamic_target_chunks_from_schema(
            schema,
            target_chunk_size,
            target_chunks_aspect_ratio=target_chunks_aspect_ratio,
            size_tolerance=size_tolerance,
            allow_fallback_algo=False,
        )
    with pytest.warns(
        UserWarning, match="Primary algorithm using even divisors along each dimension failed with"
    ):
        target_chunks = dynamic_target_chunks_from_schema(
            schema,
            target_chunk_size,
            target_chunks_aspect_ratio=target_chunks_aspect_ratio,
            size_tolerance=size_tolerance,
            allow_fallback_algo=True,
        )
    assert target_chunks == {"x": 100, "z": 85}
