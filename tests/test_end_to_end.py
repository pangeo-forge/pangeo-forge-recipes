import os

import apache_beam as beam
import fsspec
import fsspec.implementations.reference
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline

from pangeo_forge_recipes.transforms import (
    CombineReferences,
    DropKeys,
    OpenURLWithFSSpec,
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
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
    target_chunks,
):
    pattern = netcdf_local_file_pattern_sequential
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


def test_xarray_zarr_subpath(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
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
            )
        )

    ds = xr.open_dataset(os.path.join(tmp_target_url, "subpath"), engine="zarr")
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)


@pytest.mark.parametrize("reference_file_type", ["json"])
def test_reference(
    daily_xarray_dataset,
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target_url,
    reference_file_type,
):
    pattern = netcdf_local_file_pattern_sequential
    store_name = "daily-xarray-dataset"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenURLWithFSSpec()
            | OpenWithKerchunk(file_type=pattern.file_type)
            | DropKeys()
            | CombineReferences(concat_dims=["time"], identical_dims=["lat", "lon"])
            | WriteCombinedReference(
                target_root=tmp_target_url,
                store_name=store_name,
                reference_file_type=reference_file_type,
            )
        )
    full_path = os.path.join(tmp_target_url, store_name, f"target.{reference_file_type}")
    mapper = fsspec.get_mapper("reference://", fo=full_path)
    ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
    xr.testing.assert_equal(ds.load(), daily_xarray_dataset)
