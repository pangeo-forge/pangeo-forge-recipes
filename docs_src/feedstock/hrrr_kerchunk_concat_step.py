"""Integration test for Pangeo Forge pipeline which creates a combined Kerchunk dataset from
HRRR data. Based on prior discussion and examples provided in:
    - https://github.com/pangeo-forge/pangeo-forge-recipes/issues/387#issuecomment-1193514343
    - https://gist.github.com/darothen/5380e223ae5bc894006a5b6ed5a27cbb
"""

import apache_beam as beam
import zarr

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


def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", chunks={})
    ds = ds.set_coords(("latitude", "longitude"))
    assert ds.attrs["centre"] == "kwbc"
    assert len(ds["step"]) == 4
    assert len(ds["time"]) == 1
    assert "t" in ds.data_vars
    for coord in ["time", "surface", "latitude", "longitude"]:
        assert coord in ds.coords
    return store


class TestDataset(beam.PTransform):
    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(test_ds)


recipe = (
    beam.Create(pattern.items())
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
        store_name="hrrr-concat-step",
    )
    | TestDataset()
)
