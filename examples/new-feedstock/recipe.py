import apache_beam as beam
import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, FileType
from pangeo_forge_recipes.transforms import (
    ConsolidateDimensionCoordinates,
    ConsolidateMetadata,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

dates = pd.date_range("2022-02-01", freq="D", periods=28)

URL_FORMAT = "/".join(
    [
        "s3://mdl-native-06/native/SST_BAL_SST_L4_NRT_OBSERVATIONS_010_007_b",
        "DMI-BALTIC-SST-L4-NRT-OBS_FULL_TIME_SERIE/{time:%Y}/{time:%m}",
        "{time:%Y%m%d%H%M%S}-DMI-L4_GHRSST-SSTfnd-DMI_OI-NSEABALTIC-v02.0-fv01.0.nc",
    ]
)
storage_options = {"endpoint_url": "https://s3.waw3-1.cloudferro.com", "anon": True}


def make_url(time):
    return URL_FORMAT.format(time=time)


def recipe(pipeline, *, target_root, store_kwargs):
    time_concat_dim = ConcatDim("time", dates)
    pattern = FilePattern(make_url, time_concat_dim)

    return (
        pipeline
        | beam.Create(pattern.items())
        | OpenURLWithFSSpec(open_kwargs=storage_options)
        | OpenWithXarray(file_type=FileType.netcdf3)
        | StoreToZarr(
            combine_dims=pattern.combine_dim_keys,
            target_root=target_root,
            **store_kwargs,
        )
        | ConsolidateDimensionCoordinates()
        | ConsolidateMetadata()
    )
