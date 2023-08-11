"""Integration test that replicates the results of the following tutorial:

https://projectpythia.org/kerchunk-cookbook/notebooks/case_studies/HRRR.html

To verify that dataset built by the Pangeo Forge pipeline is correct, this test replicates
the dataset produced by the above-linked tutorial using the same methods demonstrated therein,
and then compares the output of the Pangeo Forge pipeline to this expected output.
"""
from typing import Any

import apache_beam as beam
import fsspec
import s3fs

from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    OpenWithKerchunk,
    WriteCombinedReference,
)

storage_options = {"anon": True}
remote_protocol = "s3"
concat_dims = ["valid_time"]
identical_dims = ["latitude", "longitude", "heightAboveGround", "step"]
grib_filter = {"typeOfLevel": "heightAboveGround", "level": [2, 10]}


def drop_unknown(refs: dict[str, Any]):
    for k in list(refs):
        if k.startswith("unknown"):
            refs.pop(k)
    return refs


fs_read: s3fs.S3FileSystem = fsspec.filesystem(
    remote_protocol,
    skip_instance_cache=True,
    **storage_options,
)
days_available = fs_read.glob("s3://noaa-hrrr-bdp-pds/hrrr.*")
files = fs_read.glob(f"s3://{days_available[-1]}/conus/*wrfsfcf01.grib2")
files = sorted(["s3://" + f for f in files])
files = files[0:2]

pattern: FilePattern = pattern_from_file_sequence(
    files,
    concat_dim=concat_dims[0],
    file_type="grib",
)

recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        file_type=pattern.file_type,
        inline_threshold=100,
        remote_protocol=remote_protocol,
        storage_options=storage_options,
        kerchunk_open_kwargs={"filter": grib_filter},
    )
    | CombineReferences(
        concat_dims=concat_dims,
        identical_dims=identical_dims,
        mzz_kwargs=dict(preprocess=drop_unknown),
        precombine_inputs=True,
    )
    | WriteCombinedReference(
        store_name="hrrr-kerchunk-concat-valid-time",
    )
)
