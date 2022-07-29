import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.patterns import CombineOp, DimKey
from pangeo_forge_recipes.transforms import OpenWithXarray, StoreToZarr

# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


def test_xarray_zarr(
    daily_xarray_dataset, netcdf_local_file_pattern_sequential, pipeline, tmp_target_url
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        inputs = p | beam.Create(pattern.items())
        datasets = inputs | OpenWithXarray(file_type=pattern.file_type)
        datasets | StoreToZarr(
            target_url=tmp_target_url,
            target_chunks={"time": 1},
            combine_dims=[DimKey("time", CombineOp.CONCAT)],
        )

    ds = xr.open_dataset(tmp_target_url, engine="zarr").load()
    xr.testing.assert_equal(ds, daily_xarray_dataset)
