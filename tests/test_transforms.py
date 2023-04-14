import apache_beam as beam
import pytest
import xarray as xr
import zarr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

# from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.storage import CacheFSSpecTarget
from pangeo_forge_recipes.transforms import (
    DetermineSchema,
    IndexItems,
    OpenURLWithFSSpec,
    OpenWithKerchunk,
    OpenWithXarray,
    PrepareZarrTarget,
    Rechunk,
)
from pangeo_forge_recipes.types import CombineOp

from .data_generation import make_ds


@pytest.fixture
def pipeline():
    # TODO: make this True and fix the weird ensuing type check errors
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


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


# the items from these patterns are suitable to be opened directly
# by open_with_xarray, bypassing fsspec
@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
        lazy_fixture("zarr_http_file_pattern_sequential_1d"),
    ],
    ids=["local_netcdf", "http_zarr"],
)
def pattern_direct(request):
    return request.param


@pytest.fixture(params=[True, False], ids=["with_cache", "no_cache"])
def cache_url(tmp_cache_url, request):
    if request.param:
        return tmp_cache_url
    else:
        return None


@pytest.fixture
def pcoll_opened_files(pattern, cache_url):
    input = beam.Create(pattern.items())
    output = input | OpenURLWithFSSpec(
        cache=cache_url,
        secrets=pattern.query_string_secrets,
        open_kwargs=pattern.fsspec_open_kwargs,
    )
    return output, pattern, cache_url


def test_OpenURLWithFSSpec(pcoll_opened_files):
    pcoll, pattern, cache_url = pcoll_opened_files

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

    if cache_url:
        cache = CacheFSSpecTarget.from_url(cache_url)
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


@pytest.mark.parametrize("load", [False, True], ids=["lazy", "eager"])
def test_OpenWithXarray_via_fsspec(pcoll_opened_files, load, pipeline):
    input, pattern, cache_url = pcoll_opened_files
    with pipeline as p:
        output = p | input | OpenWithXarray(file_type=pattern.file_type, load=load)
        assert_that(output, is_xr_dataset(in_memory=load))


@pytest.mark.parametrize("load", [False, True], ids=["lazy", "eager"])
def test_OpenWithXarray_direct(pattern_direct, load, pipeline):
    with pipeline as p:
        input = p | beam.Create(pattern_direct.items())
        output = input | OpenWithXarray(file_type=pattern_direct.file_type, load=load)
        assert_that(output, is_xr_dataset(in_memory=load))


def test_OpenWithXarray_via_fsspec_load(pcoll_opened_files, pipeline):
    input, pattern, cache_url = pcoll_opened_files

    def manually_load(item):
        key, ds = item
        return key, ds.load()

    with pipeline as p:
        output = p | input | OpenWithXarray(file_type=pattern.file_type, load=False)
        loaded_dsets = output | beam.Map(manually_load)
        assert_that(loaded_dsets, is_xr_dataset(in_memory=True))


def test_OpenWithKerchunk_via_fsspec(pcoll_opened_files, pipeline):
    input, pattern, cache_url = pcoll_opened_files
    with pipeline as p:
        output = p | input | OpenWithKerchunk(pattern.file_type)
        # FIXME: very simple test case only checks if reference is a dict
        assert isinstance(output, dict)
        assert output, "Reference dict is Empty"


@pytest.mark.xfail(reason="zarr parametrization of pattern fails filetype")
def test_OpenWithKerchunk_direct(pattern_direct, pipeline):
    with pipeline as p:
        output = (
            p
            | beam.Create(pattern_direct.items())
            | OpenWithKerchunk(file_type=pattern_direct.file_type)
            | beam.Map(print)
        )
        assert isinstance(output, dict)
        assert output, "Reference dict is Empty"


@pytest.mark.parametrize("target_chunks", [{}, {"time": 1}, {"time": 2}, {"time": 2, "lon": 9}])
def test_PrepareZarrTarget(pipeline, tmp_target_url, target_chunks):

    ds = make_ds()
    schema = dataset_to_schema(ds)

    def correct_target():
        def _check_target(actual):
            assert len(actual) == 1
            item = actual[0]
            ds_target = xr.open_zarr(item, consolidated=False, chunks={})
            zgroup = zarr.open_group(item)
            # the datasets contents shouldn't be set yet, just metadata
            assert ds_target.attrs == ds.attrs
            for vname, v in ds.items():
                v_actual = ds_target[vname]
                assert v.dims == v_actual.dims
                assert v.data.shape == v_actual.data.shape
                assert v.data.dtype == v_actual.data.dtype
                assert v.attrs == v_actual.attrs

                zarr_chunks = zgroup[vname].chunks
                expected_chunks = tuple(
                    target_chunks.get(dim) or dimsize for dim, dimsize in v.sizes.items()
                )
                assert zarr_chunks == expected_chunks

        return _check_target

    with pipeline as p:
        input = p | beam.Create([schema])
        target = input | PrepareZarrTarget(target=tmp_target_url, target_chunks=target_chunks)
        assert_that(target, correct_target())


@pytest.mark.parametrize(
    "target_chunks",
    [
        {"time": 1},
        {"time": 2},
        {"time": 3},
        {"time": 10},
        {"time": 10, "lat": 3},
        {"time": 7, "lat": 5},
    ],
)
def test_rechunk(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    target_chunks,
):
    def correct_chunks():
        def _check_chunks(actual):
            for index, ds in actual:
                actual_chunked_dims = {dim: ds.dims[dim] for dim in target_chunks}
                assert all(
                    position.indexed
                    for dimension, position in index.items()
                    if dimension.operation == CombineOp.CONCAT
                )
                max_possible_chunk_size = {
                    dimension.name: (position.dimsize - position.value)
                    for dimension, position in index.items()
                    if dimension.operation == CombineOp.CONCAT
                }
                expected_chunks = {
                    dim: min(target_chunks[dim], max_possible_chunk_size[dim])
                    for dim in target_chunks
                }
                assert actual_chunked_dims == expected_chunks

        return _check_chunks

    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        inputs = p | beam.Create(pattern.items())
        datasets = inputs | OpenWithXarray(file_type=pattern.file_type)
        schema = datasets | DetermineSchema(combine_dims=pattern.combine_dim_keys)
        indexed_datasets = datasets | IndexItems(schema=schema)
        rechunked = indexed_datasets | Rechunk(target_chunks=target_chunks, schema=schema)
        assert_that(rechunked, correct_chunks())
