import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from pangeo_forge_recipes.options import PangeoForgeOptions


def test_PangeoForgeOptions():
    def get_options(item):
        target_url = RuntimeValueProvider.get_value("target_url", str, "")
        cache_url = RuntimeValueProvider.get_value("cache_url", str, "")
        return {"target_url": target_url, "cache_url": cache_url}

    def has_correct_options():
        def _check_results(actual):
            for item in actual:
                assert item["target_url"] == "foo"
                assert item["cache_url"] == "bar"

        return _check_results

    pipeline_options = PipelineOptions(target_url="foo", cache_url="bar")
    custom_options = pipeline_options.view_as(PangeoForgeOptions)

    with TestPipeline(options=custom_options) as p:
        pcol = p | beam.Create([None]) | beam.Map(get_options)
        assert_that(pcol, has_correct_options())
