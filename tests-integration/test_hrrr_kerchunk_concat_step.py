"""Integration test for Pangeo Forge pipeline which creates a combined Kerchunk dataset from
HRRR data. Based on prior discussion and examples provided in:
    - https://github.com/pangeo-forge/pangeo-forge-recipes/issues/387#issuecomment-1193514343
    - https://gist.github.com/darothen/5380e223ae5bc894006a5b6ed5a27cbb

This module can be run with pytest:
    ```
    pytest tests-integration/test_hrrr_kerchunk_concat_step.py
    ```
The use of pytest-specific fixturing here is deliberately minimal, so that this module can be
easily adapted for other real-world HRRR use cases.
"""

import json
import os
from tempfile import TemporaryDirectory

import apache_beam as beam
import fsspec
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from fsspec.implementations.reference import ReferenceFileSystem

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    OpenWithKerchunk,
    WriteCombinedReference,
)

remote_protocol = "s3"
storage_options = {"anon": True}


def format_function(step: int) -> str:
    url_template = "s3://noaa-hrrr-bdp-pds/hrrr.20220722/conus/hrrr.t22z.wrfsfcf{step:02d}.grib2"
    return url_template.format(step=step)


pattern = FilePattern(format_function, ConcatDim("step", [0, 1, 2, 3]), file_type="grib")

identical_dims = ["time", "surface", "latitude", "longitude", "y", "x"]
grib_filters = {"typeOfLevel": "surface", "shortName": "t"}

td = TemporaryDirectory()
store_name = "grib-test-store"
options = PipelineOptions(runtime_type_check=False)
with TestPipeline(options=options) as p:
    (
        p
        | beam.Create(pattern.items())
        | OpenWithKerchunk(
            file_type=pattern.file_type,
            remote_protocol=remote_protocol,
            storage_options=storage_options,
            kerchunk_open_kwargs={"filter": grib_filters},
        )
        | CombineReferences(
            concat_dims=pattern.concat_dims,
            identical_dims=identical_dims,
            precombine_inputs=True,
        )
        | WriteCombinedReference(
            target_root=td.name,
            store_name=store_name,
        )
    )

full_path = os.path.join(td.name, store_name, "reference.json")

with open(full_path) as f:
    fs: ReferenceFileSystem = fsspec.filesystem(
        "reference",
        fo=json.load(f),
        remote_protocol=remote_protocol,
        remote_options=storage_options,
    )

ds = xr.open_dataset(
    fs.get_mapper(""),
    engine="zarr",
    backend_kwargs=dict(consolidated=False),
    chunks={"step": 1},
)
ds = ds.set_coords(("latitude", "longitude"))


def test_ds():
    assert ds.attrs["centre"] == "kwbc"
    assert len(ds["step"]) == 4
    assert len(ds["time"]) == 1
    assert "t" in ds.data_vars
    for coord in ["time", "surface", "latitude", "longitude"]:
        assert coord in ds.coords
