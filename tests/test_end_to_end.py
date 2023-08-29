import os
from pathlib import Path

import apache_beam as beam
import fsspec
import fsspec.implementations.reference
import numpy as np
import pytest
import xarray as xr
import zarr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    CombineReferences,
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
    tmp_target_url,
    target_chunks,
):
    pattern = netcdf_local_file_pattern
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target_url,
                store_name="store",
                target_chunks=target_chunks,
                combine_dims=pattern.combine_dim_keys,
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target_url, "store"), engine="zarr")
    assert ds.time.encoding["chunks"] == (target_chunks["time"],)
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


@pytest.mark.parametrize("consolidate_coords", [True])
def test_xarray_zarr_consolidate_coords(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
    consolidate_coords,
):
    pattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target_url,
                store_name="subpath",
                combine_dims=pattern.combine_dim_keys,
                consolidate_coords=consolidate_coords,
            )
        )
    # TODO: This test needs to check if the consolidate_coords transform
    # within StoreToZarr is consolidating the chunks of the coordinates

    store = zarr.open(os.path.join(tmp_target_url, "subpath"))

    # fails
    assert netcdf_local_file_pattern_sequential.dims["time"] == store.time.chunks[0]


def test_reference_netcdf(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
):
    pattern = netcdf_local_file_pattern_sequential
    store_name = "daily-xarray-dataset"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithKerchunk(file_type=pattern.file_type)
            | CombineReferences(concat_dims=["time"], identical_dims=["lat", "lon"])
            | WriteCombinedReference(
                target_root=tmp_target_url,
                store_name=store_name,
            )
        )
    full_path = os.path.join(tmp_target_url, store_name, "reference.json")
    mapper = fsspec.get_mapper("reference://", fo=full_path)
    ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


def test_reference_grib(
    pipeline,
    tmp_target_url,
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
            | CombineReferences(
                concat_dims=[pattern.concat_dims[0]],
                identical_dims=["latitude", "longitude"],
            )
            | WriteCombinedReference(
                target_root=tmp_target_url,
                store_name=store_name,
            )
        )
    full_path = os.path.join(tmp_target_url, store_name, "reference.json")
    mapper = fsspec.get_mapper("reference://", fo=full_path)
    ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
    assert ds.attrs["centre"] == "cwao"

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
