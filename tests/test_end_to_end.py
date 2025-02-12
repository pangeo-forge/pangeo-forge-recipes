import os

import apache_beam as beam
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenWithXarray,
    StoreToZarr,
)


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


@pytest.mark.parametrize("target_chunks", [{"time": 1}, {"time": 2}, {"time": 3}])
def test_xarray_zarr(
    daily_xarray_dataset,
    netcdf_local_file_pattern,
    pipeline,
    tmp_target,
    target_chunks,
):
    pattern = netcdf_local_file_pattern
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target,
                store_name="store",
                target_chunks=target_chunks,
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target.root_path, "store"), engine="zarr")
    assert ds.time.encoding["chunks"] == (target_chunks["time"],)
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_xarray_zarr_subpath(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target,
                store_name="subpath",
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target.root_path, "subpath"), engine="zarr")
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_xarray_zarr_append(
    daily_xarray_datasets_to_append,
    netcdf_local_file_patterns_to_append,
    tmp_target,
):
    ds0_fixture, ds1_fixture = daily_xarray_datasets_to_append
    pattern0, pattern1 = netcdf_local_file_patterns_to_append
    assert pattern0.combine_dim_keys == pattern1.combine_dim_keys

    # these kws are reused across both initial and append pipelines
    common_kws = dict(
        target_root=tmp_target,
        store_name="store",
        combine_dims=pattern0.combine_dim_keys,
    )
    store_path = os.path.join(tmp_target.root_path, "store")
    # build an initial zarr store, to which we will append
    options = PipelineOptions(runtime_type_check=False)
    # we run two pipelines in this test, so instantiate them separately to
    # avoid any potential of strange co-mingling between the same pipeline
    with TestPipeline(options=options) as p0:
        (
            p0
            | "CreateInitial" >> beam.Create(pattern0.items())
            | "OpenInitial" >> OpenWithXarray()
            | "StoreInitial" >> StoreToZarr(**common_kws)
        )

    # make sure the initial zarr store looks good
    initial_actual = xr.open_dataset(store_path, engine="zarr")
    assert len(initial_actual.time) == 10
    xr.testing.assert_equal(initial_actual.load(), ds0_fixture)

    # now append to it. the two differences here are
    # passing `pattern1` in `Create` and `append_dim="time"` in `StoreToZarr`
    with TestPipeline(options=options) as p1:
        (
            p1
            | "CreateAppend" >> beam.Create(pattern1.items())
            | "OpenAppend" >> OpenWithXarray()
            | "StoreAppend" >> StoreToZarr(append_dim="time", **common_kws)
        )

    # now see if we have appended to time dimension as intended
    append_actual = xr.open_dataset(store_path, engine="zarr")
    assert len(append_actual.time) == 20
    append_expected = xr.concat([ds0_fixture, ds1_fixture], dim="time")
    xr.testing.assert_equal(append_actual.load(), append_expected)


def test_xarray_zarr_consolidate_dimension_coordinates(
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target,
                store_name="subpath",
                combine_dims=pattern.combine_dim_keys,
            )
            | ConsolidateDimensionCoordinates()
            | ConsolidateMetadata()
        )

    path = os.path.join(tmp_target.root_path, "subpath")
    ds = xr.open_dataset(path, engine="zarr", consolidated=True, chunks={})

    assert ds.time.encoding["chunks"][0] == ds.time.shape[0]
