from typing import Union

import apache_beam as beam
from dataclasses import dataclass, field
import pandas as pd
import zarr

from pangeo_forge_recipes.config import Config
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    RequiredAtRuntimeDefault,
)

dates = [
    d.to_pydatetime().strftime("%Y%m%d")
    for d in pd.date_range("1996-10-01", "1999-02-01", freq="D")
]


def make_url(time):
    url_base = "https://storage.googleapis.com/pforge-test-data"
    return f"{url_base}/gpcp/v01r03_daily_d{time}.nc"


concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, concat_dim)


def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    # This fails integration test if not imported here
    # TODO: see if --setup-file option for runner fixes this
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", chunks={})
    assert ds.title == (
        "Global Precipitation Climatatology Project (GPCP) " "Climate Data Record (CDR), Daily V1.3"
    )
    # Making sure that the native chunking is different from the dynamic chunking
    assert ds.chunks["time"][0] == 1

    return store


@dataclass
class MyCustomTransform(beam.PTransform):

    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )

    def expand(
        self,
        pcoll: beam.PCollection,
    ) -> beam.PCollection:
        return pcoll


config = Config()

recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type, xarray_open_kwargs={"decode_coords": "all"})
    | MyCustomTransform(target_root=config.target_storage)
    | StoreToZarr(
        store_name="gpcp.zarr",
        combine_dims=pattern.combine_dim_keys,
    )
    | "Test dataset" >> beam.Map(test_ds)
)
