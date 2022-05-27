import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty

from pangeo_forge_recipes.transforms import OpenWithFSSpec, OpenWithXarray


# a beam testing assertion matcher
def expected_len(n):
    def _expected_len(actual):
        actual_len = len(actual)
        if actual_len != n:
            raise BeamAssertException(f"Failed assert: actual len is {actual_len}, expected {n}")

    return _expected_len


@pytest.fixture
def pcoll_opened_files(netcdf_local_file_pattern_sequential):
    pattern = netcdf_local_file_pattern_sequential
    return pattern, beam.Create(pattern.items()) | OpenWithFSSpec()


def test_OpenWithFSSpec(pcoll_opened_files):
    pattern, pcoll = pcoll_opened_files
    with TestPipeline() as p:
        output = p | pcoll

        assert_that(output, is_not_empty(), label="ouputs not empty")
        assert_that(output, expected_len(pattern.shape[0]), label="expected len")


def is_xr_dataset():
    def _is_xr_dataset(actual):
        for _, ds in actual:
            if not isinstance(ds, xr.Dataset):
                raise BeamAssertException(f"Object {ds} has type {type(ds)}, expected xr.Dataset.")

    return _is_xr_dataset


@pytest.fixture
def pcoll_xarray_datasets(pcoll_opened_files):
    _, open_files = pcoll_opened_files
    return open_files | OpenWithXarray()


def test_OpenWithXarray(pcoll_xarray_datasets):
    with TestPipeline() as p:
        output = p | pcoll_xarray_datasets

        assert_that(output, is_xr_dataset(), label="is xr.Dataset")
