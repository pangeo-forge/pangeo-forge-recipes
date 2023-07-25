from typing import Dict

import dask.array as dsa
import pytest
import xarray as xr

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.dynamic_target_chunks import dynamic_target_chunks_from_schema


def _create_ds(dims_shape: Dict[str, int]) -> xr.Dataset:
    return xr.DataArray(
        dsa.random.random(list(dims_shape.values())),
        dims=list(dims_shape.keys()),
    ).to_dataset(name="data")


class TestDynamicTargetChunks:
    @pytest.mark.parametrize(
        ("dims_shape", "target_chunks_aspect_ratio", "expected_target_chunks"),
        [
            # make sure that for the same dataset we get smaller chunksize along a dimension if the ratio is larger
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
    def test_dynamic_rechunking(self, dims_shape, target_chunks_aspect_ratio, expected_target_chunks):
        ds = _create_ds(dims_shape)
        schema = dataset_to_schema(ds)
        target_chunks = dynamic_target_chunks_from_schema(
            schema, 1e6, target_chunks_aspect_ratio=target_chunks_aspect_ratio
        )
        print(target_chunks)
        for dim, chunks in expected_target_chunks.items():
            assert target_chunks[dim] == chunks

    def test_nbytes_str_input(self):
        ds = _create_ds({"x": 100, "y": 100, "z": 100})
        schema = dataset_to_schema(ds)
        target_chunks_aspect_ratio = {'x':1, 'y':1, 'z':1}
        target_chunks_int = dynamic_target_chunks_from_schema(
             schema, 1e6, target_chunks_aspect_ratio=target_chunks_aspect_ratio
        )
        target_chunks_str = dynamic_target_chunks_from_schema(
             schema, '1MB', target_chunks_aspect_ratio=target_chunks_aspect_ratio
        )
        for dim in target_chunks_aspect_ratio.keys():
            assert target_chunks_int[dim] == target_chunks_str[dim]

    def test_dynamic_rechunking_maintain_ratio(self):
        """Confirm that for a given ratio with two differently sized datasets we maintain a constant ratio
        between total number of chunks"""
        ds_equal = _create_ds({"x": 64, "y": 64})
        ds_long = _create_ds({"x": 64, "y": 256})

        for ds in [ds_equal, ds_long]:
            print(ds)
            schema = dataset_to_schema(ds)
            target_chunks = dynamic_target_chunks_from_schema(
                schema, 1e4, target_chunks_aspect_ratio={"x": 1, "y": 4}
            )
            ds_rechunked = ds.chunk(target_chunks)
            assert len(ds_rechunked.chunks["y"]) / len(ds_rechunked.chunks["x"]) == 4

    @pytest.mark.parametrize(
        "target_chunks_aspect_ratio", [{"x": 1, "y": -1, "z": 10}, {"x": 6, "y": -1, "z": 2}]
    )  # always keep y unchunked, and vary the others
    @pytest.mark.parametrize("target_chunk_nbytes", [1e6, 1e7])
    def test_dynamic_skip_dimension(self, target_chunks_aspect_ratio, target_chunk_nbytes):
        ds = _create_ds({"x": 100, "y": 200, "z": 300})
        # Mark dimension as 'not-to-chunk' with -1
        schema = dataset_to_schema(ds)
        target_chunks = dynamic_target_chunks_from_schema(
            schema, target_chunk_nbytes, target_chunks_aspect_ratio=target_chunks_aspect_ratio
        )
        assert target_chunks["y"] == len(ds["y"])

    def test_dynamic_rechunking_error_dimension_missing(self):
        # make sure that an error is raised if some dimension is not specified
        ds = _create_ds({"x": 100, "y": 200, "z": 300})
        schema = dataset_to_schema(ds)

        with pytest.raises(
            ValueError, match="target_chunks_aspect_ratio must contain all dimensions in dataset."
        ):
            dynamic_target_chunks_from_schema(schema, 1e6, target_chunks_aspect_ratio={"x": 1, "z": 10})

    def test_dynamic_rechunking_error_dimension_wrong(self):
        ds = _create_ds({"x": 100, "y": 200, "z": 300})
        schema = dataset_to_schema(ds)
        with pytest.raises(
            ValueError, match="target_chunks_aspect_ratio must contain all dimensions in dataset."
        ):
            dynamic_target_chunks_from_schema(
                schema, 1e6, target_chunks_aspect_ratio={"x": 1, "y_wrong": 1, "z": 10}
            )
