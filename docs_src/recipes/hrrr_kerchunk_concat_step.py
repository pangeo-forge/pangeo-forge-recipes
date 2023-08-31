"""Integration test for Pangeo Forge pipeline which creates a combined Kerchunk dataset from
HRRR data. Based on prior discussion and examples provided in:
    - https://github.com/pangeo-forge/pangeo-forge-recipes/issues/387#issuecomment-1193514343
    - https://gist.github.com/darothen/5380e223ae5bc894006a5b6ed5a27cbb
"""

import apache_beam as beam

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
)
