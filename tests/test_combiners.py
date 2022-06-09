import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.combiners import ConcatXarraySchemas

# to make these tests more isolated, we create a schema fixture
# from scratch


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=True)
    with TestPipeline(options=options) as p:
        yield p


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
    ],
    ids=["sequential"],
)
def schema_pcoll(request):
    pattern = request.param

    def _get_schema(path):
        ds = xr.open_dataset(path)
        return ds.to_dict(data=False)

    return pattern, beam.Create(
        ((key, _get_schema(url)) for key, url in pattern.items())
    )


def _get_concat_dim(pattern):
    cdims = pattern.concat_dims
    assert len(cdims) == 1, "Only one concat_dim allowed for now"
    return cdims[0]


def _strip_keys(item):
    return item[1]


def test_OpenWithXarray_via_fsspec(schema_pcoll, pipeline):
    pattern, pcoll = schema_pcoll
    concat_dim = _get_concat_dim(pattern)
    with pipeline as p:
        input = p | pcoll
        output = input | beam.CombineGlobally(ConcatXarraySchemas(concat_dim=concat_dim))
        assert_that(output, is_not_empty())
