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
    Indexed,
    OpenWithKerchunk,
    OpenWithXarray,
    StoreToPyramid,
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


@pytest.mark.parametrize("target_chunks", [{"time": 10}])
def test_pyramid(
    pyramid_datatree,
    netcdf_local_file_pattern,
    target_chunks,
    pipeline,
    tmp_target,
):
    import datatree

    class SetCRS(beam.PTransform):
        """Updates CRS and coord naming"""

        @staticmethod
        def _set_CRS(item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
            index, ds = item

            import rioxarray  # noqa

            ds = ds.rename({"lon": "longitude", "lat": "latitude"})
            ds.rio.write_crs("epsg:4326", inplace=True)
            return index, ds

        def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
            return pcoll | beam.Map(self._set_CRS)

    pattern = netcdf_local_file_pattern
    with pipeline as p:
        process = (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | SetCRS()
        )

        base_store = process | "Write Base Level" >> StoreToZarr(
            target_root=tmp_target,
            store_name="store",
            combine_dims=pattern.combine_dim_keys,
        )
        pyramid_store = process | "Write Pyramid Levels" >> StoreToPyramid(
            target_root=tmp_target,
            store_name="pyramid",
            n_levels=2,
            target_chunks=target_chunks,
            combine_dims=pattern.combine_dim_keys,
        )

    import datatree as dt
    from datatree.testing import assert_isomorphic

    assert xr.open_dataset(os.path.join(tmp_target.root_path, "store"), engine="zarr", chunks={})

    pgf_dt = dt.open_datatree(
        os.path.join(tmp_target.root_path, "pyramid"), engine="zarr", consolidated=False, chunks={}
    )
    import pdb

    pdb.set_trace()
    assert_isomorphic(pgf_dt, pyramid_datatree)  # every node has same # of children
    # pyramid_datatree 1/ has mismatch between dimensions and variable dims (256 vs 128)
    # dt.testing.assert_identical(pgf_dt, pyramid_datatree)
