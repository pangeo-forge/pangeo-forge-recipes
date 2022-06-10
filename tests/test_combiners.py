import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import assert_that
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.combiners import ConcatXarraySchemas

# to make these tests more isolated, we create a schema fixture
# from scratch


@pytest.fixture
def pipeline():
    # Runtime type checking doesn't play well with our Combiner
    # https://github.com/apache/beam/blob/3cddfaf58c69acc624dac16df10357a78ececf59/sdks/python/apache_beam/transforms/core.py#L2505-L2509
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


# TODO: make this fixture leaner; don't need netcdf3 and netcdf4
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

    return pattern, beam.Create(((key, _get_schema(url)) for key, url in pattern.items()))


def _get_concat_dim(pattern):
    cdims = pattern.concat_dims
    assert len(cdims) == 1, "Only one concat_dim allowed for now"
    return cdims[0]


def _strip_keys(item):
    return item[1]


def test_ConcatXarraySchemas(schema_pcoll, pipeline):
    pattern, pcoll = schema_pcoll
    concat_dim = _get_concat_dim(pattern)

    expected_ds = xr.open_mfdataset(item[1] for item in pattern.items())
    expected_schema = expected_ds.to_dict(data=False)

    def has_correct_schema():
        def _check_results(actual):
            assert len(actual) == 1
            schema, chunk_lens = actual[0]
            assert schema == expected_schema

        return _check_results

    with pipeline as p:
        input = p | pcoll
        output = input | beam.CombineGlobally(ConcatXarraySchemas(concat_dim=concat_dim))
        assert_that(output, has_correct_schema())
