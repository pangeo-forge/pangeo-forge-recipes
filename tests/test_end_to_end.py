import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty

from pangeo_forge_recipes.patterns import DimKey, CombineOp
from pangeo_forge_recipes.transforms import (
    OpenWithXarray, DatasetToSchema, PrepareZarrTarget,
    DetermineSchema, StoreToZarr, IndexItems
)

@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=True)
    with TestPipeline(options=options) as p:
        yield p

def test_xarray_zarr(daily_xarray_dataset, netcdf_local_file_pattern_sequential, pipeline, tmp_target_url):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        inputs = p | beam.Create(pattern.items())
        datasets = inputs | OpenWithXarray(file_type=pattern.file_type)
        # TODO determine this dynamically
        combine_dims = [DimKey("time", operation=CombineOp.CONCAT)]
        schemas = datasets | DatasetToSchema()
        schema = schemas | DetermineSchema(combine_dims=combine_dims)
        indexed_datasets = datasets | IndexItems(schema=schema)
        target = schema | PrepareZarrTarget(target_url=tmp_target_url)
        _ = indexed_datasets | StoreToZarr(target_store=target)

    ds = xr.open_dataset(tmp_target_url, engine="zarr").load()
    xr.testing.assert_equal(ds, daily_xarray_dataset)
