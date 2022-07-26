from dataclasses import dataclass

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from pangeo_forge_recipes.options import PangeoForgeOptions


def test_PangeoForgeOptions():
    # This test is basically just a demonstration of how we can set pipeline options.
    # It is based on https://beam.apache.org/documentation/patterns/pipeline-options/

    @dataclass
    class GetPangeoForgeOptions(beam.DoFn):
        target_url: str
        cache_url: str

        def process(self, item):
            yield {"target_url": self.target_url, "cache_url": self.cache_url}

    def has_correct_options():
        def _check_results(actual):
            for item in actual:
                assert item["target_url"] == "foo"
                assert item["cache_url"] == "bar"

        return _check_results

    pipeline_options = PipelineOptions(target_url="foo", cache_url="bar")
    pf_options = pipeline_options.view_as(PangeoForgeOptions)

    with TestPipeline(options=pipeline_options) as p:
        pcol = (
            p
            | beam.Create([None])
            | beam.ParDo(GetPangeoForgeOptions(pf_options.target_url, pf_options.cache_url))
        )
        assert_that(pcol, has_correct_options())
