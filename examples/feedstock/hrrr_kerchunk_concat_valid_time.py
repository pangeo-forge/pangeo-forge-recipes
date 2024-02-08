"""Integration test that replicates the results of the following tutorial:

https://projectpythia.org/kerchunk-cookbook/notebooks/case_studies/HRRR.html
"""
from typing import Any

import apache_beam as beam
import fsspec
import s3fs
import zarr

from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import OpenWithKerchunk, WriteCombinedReference

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


def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", chunks={})
    # TODO: more detailed asserts
    assert "t2m" in ds.data_vars


recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        file_type=pattern.file_type,
        inline_threshold=100,
        remote_protocol=remote_protocol,
        storage_options=storage_options,
        kerchunk_open_kwargs={"filter": grib_filter},
    )
    | WriteCombinedReference(
        store_name="hrrr-concat-valid-time",
        concat_dims=concat_dims,
        identical_dims=identical_dims,
        # fails due to: _pickle.PicklingError: Can't pickle <function drop_unknown
        #  at 0x290e46a70>: attribute lookup drop_unknown on __main__ failed
        mzz_kwargs=dict(preprocess=drop_unknown),
        # precombine_inputs=True,
    )
    | "Test dataset" >> beam.Map(test_ds)
)
