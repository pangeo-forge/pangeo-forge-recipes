import apache_beam as beam
import pandas as pd
import zarr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, Indexed,PyramidToZarr
import xarray as xr 

dates = pd.date_range("1981-09-01", "2022-02-01", freq="D")

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_url(time):
    return URL_FORMAT.format(time=time)


time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)

pattern = pattern.prune()


def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    # This fails integration test if not imported here
    # TODO: see if --setup-file option for runner fixes this
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", chunks={})
    for var in ["anom", "err", "ice", "sst"]:
        assert var in ds.data_vars
    return store



class CreatePyramid(beam.PTransform):

    @staticmethod
    def _create_pyramid(item: Indexed[xr.Dataset]) -> Indexed[xr.Dataset]:
        index, ds = item
        import rioxarray
        from ndpyramid import reproject_single_level, pyramid_reproject


        ds = ds.rename_dims({"lon": "longitude", "lat": "latitude"})
        ds = ds.rename({"lon": "longitude", "lat": "latitude"})
        ds.rio.write_crs("epsg:4326", inplace=True)
        # ds = ds.anom.rio.set_spatial_dims(x_dim='lat',y_dim='lon',inplace=True)

        level_ds = reproject_single_level(ds, level=1,extra_dim = 'zlev')
        return index, level_ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        pyr_ds = pcoll | beam.Map(self._create_pyramid)
        return pyr_ds | StoreToZarr(target_root='noaa-oisst.zarr', store_name="l1" ,combine_dims=pattern.combine_dim_keys)
    

recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | PyramidToZarr(
        target_root='.',store_name="noaa-oisst.zarr",n_levels=2,combine_dims=pattern.combine_dim_keys)
    | beam.Map(test_ds)
)

with beam.Pipeline() as p:
    p | recipe