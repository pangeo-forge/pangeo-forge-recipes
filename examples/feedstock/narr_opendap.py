"""NARR: Subsetting and OPeNDAP

This tutorial uses data from NOAA's North American Regional Reanalysis (NARR). From
https://www.ncei.noaa.gov/products/weather-climate-models/north-american-regional:

    The North American Regional Reanalysis (NARR) is a model produced by the National Centers
    for Environmental Prediction (NCEP) that generates reanalyzed data for temperature, wind,
    moisture, soil, and dozens of other parameters. The NARR model assimilates a large amount
    of observational data from a variety of sources to produce a long-term picture of weather
    over North America.

For this recipe, we will access the data via OPeNDAP
(https://earthdata.nasa.gov/collaborate/open-data-services-and-software/api/opendap), a widely-used
API for remote access of environmental data over HTTP. A key point is that, since we use using
OPeNDAP, _there are no input files to download / cache_. We open the data directly from the remote
server.

The data we will use are catalogged here (3D data on pressure levels):
https://psl.noaa.gov/thredds/catalog/Datasets/NARR/pressure/catalog.html
"""

import apache_beam as beam
import xarray as xr
import zarr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import Indexed, OpenWithXarray, StoreToZarr

time_dim = ConcatDim("time", ["197901"])


def format_function(time):
    return f"https://psl.noaa.gov/thredds/dodsC/Datasets/NARR/pressure/air.{time}.nc"


pattern = FilePattern(format_function, time_dim, file_type="opendap")


class SetProjectionAsCoord(beam.PTransform):
    """A preprocessing function which will assign the `Lambert_Conformal
    variable as a coordinate variable.
    """

    @staticmethod
    def _set_projection_as_coord(item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
        index, ds = item
        ds = ds.set_coords(["Lambert_Conformal"])
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._set_projection_as_coord)


def test_ds(store: zarr.storage.FsspecStore) -> zarr.storage.FsspecStore:
    # This fails integration test if not imported here
    # TODO: see if --setup-file option for runner fixes this
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", chunks={})
    assert "air" in ds.data_vars


recipe = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=pattern.file_type)
    | SetProjectionAsCoord()
    | StoreToZarr(
        store_name="narr.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 1},
    )
    | "Test dataset" >> beam.Map(test_ds)
)
