import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.transforms import OpenWithFSSpec, OpenWithXarray


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
        lazy_fixture("netcdf_http_file_pattern_sequential_1d"),
    ],
    ids=["local", "http"],
)
def pattern(request):
    return request.param


@pytest.fixture(params=[True, False], ids=["with_cache", "no_cache"])
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


def is_xr_dataset(in_memory=False):
    def _is_xr_dataset(actual):
        for _, ds in actual:
            if not isinstance(ds, xr.Dataset):
                raise BeamAssertException(f"Object {ds} has type {type(ds)}, expected xr.Dataset.")
            offending_vars = [
                vname for vname in ds.data_vars if ds[vname].variable._in_memory != in_memory
            ]
            if offending_vars:
                msg = "were NOT in memory" if in_memory else "were in memory"
                raise BeamAssertException(f"The following vars {msg}: {offending_vars}")

    return _is_xr_dataset


@pytest.fixture
def pcoll_xarray_datasets(pcoll_opened_files):
    open_files, _, _ = pcoll_opened_files
    return open_files | OpenWithXarray()


@pytest.mark.parametrize("load", [False, True])
def test_OpenWithXarray(pcoll_opened_files, load):
    input, pattern, cache = pcoll_opened_files
    with TestPipeline() as p:
        output = p | input | OpenWithXarray(file_type=pattern.file_type, load=load)
        assert_that(output, is_xr_dataset(in_memory=load))


def test_OpenWithXarray_downstream_load(pcoll_opened_files):
    input, pattern, cache = pcoll_opened_files

    def manually_load(item):
        key, ds = item
        return key, ds.load()

    with TestPipeline() as p:
        output = p | input | OpenWithXarray(file_type=pattern.file_type, load=False)
        loaded_dsets = output | beam.Map(manually_load)
        assert_that(loaded_dsets, is_xr_dataset(in_memory=True))
