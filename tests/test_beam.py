import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty

from pangeo_forge_recipes.transforms import OpenWithFSSpec


@pytest.fixture
def pcoll_opened_files(netcdf_local_file_pattern_sequential):
    pattern = netcdf_local_file_pattern_sequential
    return pattern, beam.Create(pattern.items()) | OpenWithFSSpec()


def test_OpenWithFSSpec(pcoll_opened_files):
    pattern, pcoll = pcoll_opened_files

    def expected_len(n):
        def _expected_len(actual):
            actual_len = len(actual)
            if actual_len != n:
                raise BeamAssertException(
                    f"Failed assert: actual len is {actual_len}, expected {n}"
                )

        return _expected_len

    def is_readable():
        def _is_readable(actual):
            for index, item in actual:
                with item as fp:
                    _ = fp.read()

        return _is_readable

    with TestPipeline() as p:
        output = p | pcoll

        assert_that(output, is_not_empty(), label="ouputs not empty")
        assert_that(output, expected_len(pattern.shape[0]), label="expected len")
        assert_that(output, is_readable(), label="output is readable")
