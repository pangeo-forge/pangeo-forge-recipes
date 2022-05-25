import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty

from pangeo_forge_recipes.recipes.xarray_zarr_beam import OpenWithFSSpec


# a beam testing assertion matcher
def expected_len(n):
    def _expected_len(actual):
        actual_len = len(actual)
        if actual_len != n:
            raise BeamAssertException(f"Failed assert: actual len is {actual_len}, expected {n}")

    return _expected_len


def test_OpenWithFSSpec(netcdf_local_file_pattern_sequential):
    pattern = netcdf_local_file_pattern_sequential

    with TestPipeline() as p:
        input = p | beam.Create(pattern.items())
        output = input | OpenWithFSSpec()

        assert_that(output, is_not_empty(), label="inputs not empty")
        assert_that(output, is_not_empty(), label="ouputs not empty")
        assert_that(output, expected_len(pattern.shape[0]), label="expected len")
