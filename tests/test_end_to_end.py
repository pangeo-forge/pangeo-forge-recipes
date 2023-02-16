import os

import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr

# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


# TODO: this test currently passes for any chunk size without taking any care to
# align chunk writes with tasks. This means that the test is not using any
# concurrency. Instead of using the TestPipeline, we should use a runner with
# actual concurrency.
@pytest.mark.parametrize("target_chunks", [{"time": 1}, {"time": 2}, {"time": 3}])
def test_xarray_zarr(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
    target_chunks,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target=tmp_target_url,
                target_chunks=target_chunks,
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(tmp_target_url, engine="zarr")
    assert ds.time.encoding["chunks"] == (target_chunks["time"],)
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_xarray_zarr_subpath(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target=tmp_target_url,
                target_subpath="subpath",
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target_url, "subpath"), engine="zarr")
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)
