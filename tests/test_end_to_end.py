import importlib.util
import os
from pathlib import Path

import apache_beam as beam
import fsspec
import fsspec.implementations.reference
import numpy as np
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from fsspec.implementations.reference import ReferenceFileSystem

from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenWithKerchunk,
    OpenWithXarray,
    StoreToZarr,
    WriteCombinedReference,
)

# from apache_beam.testing.util import assert_that, equal_to
# from apache_beam.testing.util import BeamAssertException, assert_that, is_not_empty


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
    ds = ds.load()
    for dim, chunk_size in target_chunks.items():
        assert ds[dim].encoding["chunks"] == (chunk_size,)
    for dim, length in ds.sizes.items():
        if dim not in target_chunks:
            assert len(ds[dim]) == length
    xr.testing.assert_equal(ds, daily_xarray_dataset)


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


@pytest.mark.parametrize("output_file_name", ["reference.json", "reference.parquet"])
def test_reference_netcdf(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target,
    output_file_name,
):
    pattern = netcdf_local_file_pattern_sequential
    store_name = "daily-xarray-dataset"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithKerchunk(file_type=pattern.file_type)
            | WriteCombinedReference(
                identical_dims=["lat", "lon"],
                target_root=tmp_target,
                store_name=store_name,
                concat_dims=["time"],
                output_file_name=output_file_name,
            )
        )
    full_path = os.path.join(tmp_target.root_path, store_name, output_file_name)
    file_ext = os.path.splitext(output_file_name)[-1]

    if file_ext == ".json":
        mapper = fsspec.get_mapper("reference://", fo=full_path)
        ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
        xr.testing.assert_equal(ds.load(), daily_xarray_dataset)

    elif file_ext == ".parquet":
        fs = ReferenceFileSystem(
            full_path, remote_protocol="file", target_protocol="file", lazy=True
        )
        ds = xr.open_dataset(fs.get_mapper(), engine="zarr", backend_kwargs={"consolidated": False})
        xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_reference_netcdf_parallel(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential_multivariable,
    pipeline_parallel,
    tmp_target,
    output_file_name="reference.json",
):
    pattern = netcdf_local_file_pattern_sequential_multivariable
    store_name = "daily-xarray-dataset"
    with pipeline_parallel as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithKerchunk(file_type=pattern.file_type)
            | WriteCombinedReference(
                identical_dims=["lat", "lon"],
                target_root=tmp_target,
                store_name=store_name,
                concat_dims=["time"],
                output_file_name=output_file_name,
            )
        )
    full_path = os.path.join(tmp_target.root_path, store_name, output_file_name)

    file_ext = os.path.splitext(output_file_name)[-1]

    if file_ext == ".json":
        mapper = fsspec.get_mapper("reference://", fo=full_path)
        ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
        xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


@pytest.mark.xfail(
    importlib.util.find_spec("cfgrib") is None,
    reason=(
        "Requires cfgrib, which should be installed via conda. "
        "FIXME: Setup separate testing environment for `requires-conda` tests. "
        "NOTE: The HRRR integration tests would also fall into this category."
    ),
    raises=ImportError,
)
def test_reference_grib(
    pipeline,
    tmp_target,
):
    # This test adapted from:
    # https://github.com/fsspec/kerchunk/blob/33b00d60d02b0da3f05ccee70d6ebc42d8e09932/kerchunk/tests/test_grib.py#L14-L31

    fn = Path(__file__).parent / "data" / "CMC_reg_DEPR_ISBL_10_ps10km_2022072000_P000.grib2"
    pattern: FilePattern = pattern_from_file_sequence(
        [str(fn)], concat_dim="time", file_type="grib"
    )
    store_name = "grib-test-store"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithKerchunk(file_type=pattern.file_type)
            | WriteCombinedReference(
                concat_dims=[pattern.concat_dims[0]],
                identical_dims=["latitude", "longitude"],
                target_root=tmp_target,
                store_name=store_name,
            )
        )
    full_path = os.path.join(tmp_target.root_path, store_name, "reference.json")
    mapper = fsspec.get_mapper("reference://", fo=full_path)
    ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
    assert ds.attrs["GRIB_centre"] == "cwao"

    # ds2 is the original dataset as stored on disk;
    # keeping `ds2` name for consistency with kerchunk test on which this is based
    ds2 = xr.open_dataset(fn, engine="cfgrib", backend_kwargs={"indexpath": ""})

    # these assertions copied directly from kerchunk test suite (link above)
    for var in ["latitude", "longitude", "unknown", "isobaricInhPa", "time"]:
        d1 = ds[var].values
        d2 = ds2[var].values
        assert (np.isnan(d1) == np.isnan(d2)).all()
        assert (d1[~np.isnan(d1)] == d2[~np.isnan(d2)]).all()

    # FIXME: The (apparently stricter) `xr.testing.assert_equal`` commented out below fails due to
    # various inconsistencies (of dtype casting int to float, etc.). With the right combination of
    # options passed to the pipeline, seems like these should pass?
    # xr.testing.assert_equal(ds.load(), ds2)


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
