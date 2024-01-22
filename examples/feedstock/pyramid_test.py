import apache_beam as beam
import pandas as pd
import zarr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, PyramidToZarr

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


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | PyramidToZarr(
        target_root=".",
        store_name="pyramid_test.zarr",
        n_levels=5,
        combine_dims=pattern.combine_dim_keys,
    )
)

with beam.Pipeline() as p:
    p | recipe
