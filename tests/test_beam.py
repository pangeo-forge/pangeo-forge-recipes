import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty
from pytest_lazyfixture import lazy_fixture

from cloudpickle import loads

from pangeo_forge_recipes.transforms import OpenWithFSSpec, OpenWithXarray, LazilyOpenDataset


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
        lazy_fixture("netcdf_http_file_pattern_sequential_1d"),
    ],
    ids=['local', 'http']
)
def pattern(request):
    return request.param


@pytest.fixture(params=[True, False], ids=['with_cache', 'no_cache'])
def cache(tmp_cache, request):
    if request.param:
        return tmp_cache
    else:
        return None


@pytest.fixture
def pcoll_opened_files(pattern, cache):
    input = beam.Create(pattern.items())
    output = input | OpenWithFSSpec(
        cache=cache, secrets=pattern.query_string_secrets, open_kwargs=pattern.fsspec_open_kwargs
    )
    return output, pattern, cache


def test_OpenWithFSSpec(pcoll_opened_files):
    pcoll, pattern, cache = pcoll_opened_files

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

    if cache:
        for key, fname in pattern.items():
            assert cache.exists(fname)


def is_xr_dataset():
    def _is_xr_dataset(actual):
        for ds in actual:
            if not isinstance(ds, xr.Dataset):
                raise BeamAssertException(f"Object {ds} has type {type(ds)}, expected xr.Dataset.")
            ds.load()

    return _is_xr_dataset


@pytest.fixture
def pcoll_xarray_datasets(pcoll_opened_files):
    open_files, _, _ = pcoll_opened_files
    return open_files | OpenWithXarray()


def test_OpenWithXarray(pcoll_xarray_datasets):
    with TestPipeline() as p:
        output = p | pcoll_xarray_datasets

        assert_that(output, is_xr_dataset(), label="is xr.Dataset")


def test_LazilyOpenDataset(netcdf_public_http_paths_sequential_1d):
    urls = netcdf_public_http_paths_sequential_1d[0][:1]

    def load_xarray_ds(ds):
        return ds.load()

    with TestPipeline() as p:
        inputs = p | beam.Create(urls)
        dsets = inputs | LazilyOpenDataset()
        output = dsets | beam.Map(load_xarray_ds)

        assert_that(output, is_xr_dataset(), label="is xr.Dataset")
