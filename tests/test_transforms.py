import os
from dataclasses import dataclass

import apache_beam as beam
import pytest
import xarray as xr
import zarr
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.patterns import FilePattern, FileType
from pangeo_forge_recipes.storage import CacheFSSpecTarget
from pangeo_forge_recipes.transforms import (
    DetermineSchema,
    IndexItems,
    OpenWithKerchunk,
    OpenWithXarray,
    PrepareZarrTarget,
    Rechunk,
    StoreToZarr,
)
from pangeo_forge_recipes.types import CombineOp

from .data_generation import make_ds


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


def is_list_of_refs_dicts():
    def _is_list_of_refs_dicts(refs):
        for r in refs[0]:
            assert isinstance(r, dict)
            assert "refs" in r

    return _is_list_of_refs_dicts


def test_OpenWithKerchunk_via_fsspec(pcoll_opened_files, pipeline):
    input, pattern, cache_url = pcoll_opened_files
    with pipeline as p:
        output = p | input | OpenWithKerchunk(pattern.file_type)
        assert_that(output, is_list_of_refs_dicts())


def test_OpenWithKerchunk_direct(pattern_direct, pipeline):
    if pattern_direct.file_type == FileType.zarr:
        pytest.skip("Zarr filetype not supported for Reference recipe.")

    with pipeline as p:
        output = (
            p
            | beam.Create(pattern_direct.items())
            | OpenWithKerchunk(file_type=pattern_direct.file_type)
        )
        assert_that(output, is_list_of_refs_dicts())


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


@pytest.mark.parametrize("consolidate", [True, False])
def test_StoreToZarr_emits_openable_fsstore(
    pipeline,
    netcdf_local_file_pattern_sequential,
    tmp_target_url,
    consolidate,
):
    def is_consolidated():
        def _is_consolidated(actual):
            assert len(actual) == 1
            item: zarr.storage.FSSTore = actual[0]
            assert ".zmetadata" in os.listdir(item.path)

        return _is_consolidated

    def is_not_consolidated():
        def _is_not_consolidated(actual):
            assert len(actual) == 1
            item = actual[0]  # noqa: F841
            ...

        return _is_not_consolidated

    def _open_zarr(store, consolidated):
        return xr.open_dataset(store, engine="zarr", consolidated=consolidated)

    @dataclass
    class OpenZarrStore(beam.PTransform):
        consolidated: bool

        def expand(self, pcoll):
            return pcoll | beam.Map(_open_zarr, consolidated=self.consolidated)

    def is_xrdataset():
        def _is_xr_dataset(actual):
            assert len(actual) == 1
            item = actual[0]
            assert isinstance(item, xr.Dataset)

        return _is_xr_dataset

    pattern: FilePattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        datasets = p | beam.Create(pattern.items()) | OpenWithXarray()
        target_store = datasets | StoreToZarr(
            target_root=tmp_target_url,
            store_name="test.zarr",
            combine_dims=pattern.combine_dim_keys,
            consolidate_metadata=consolidate,
        )
        if consolidate:
            assert_that(target_store, is_consolidated(), label="is_consolidated")
        else:
            assert_that(target_store, is_not_consolidated(), label="is_not_consolidated")

        open_store = target_store | OpenZarrStore(consolidated=consolidate)
        assert_that(open_store, is_xrdataset(), label="is_xrdataset")
