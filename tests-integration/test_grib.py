import glob
import os

import apache_beam as beam
import fsspec
import pytest
import s3fs
import ujson  # type: ignore
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from fsspec.implementations.reference import ReferenceFileSystem
from kerchunk.combine import MultiZarrToZarr
from kerchunk.grib2 import scan_grib

from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    DropKeys,
    OpenURLWithFSSpec,
    OpenWithKerchunk,
    WriteCombinedReference,
)


def open_reference_ds(multi_kerchunk: dict, remote_protocol: str, remote_options: dict):
    # open dataset as zarr object using fsspec reference file system and Xarray
    fs: ReferenceFileSystem = fsspec.filesystem(
        "reference",
        fo=multi_kerchunk,
        remote_protocol=remote_protocol,
        remote_options=remote_options,
    )
    m = fs.get_mapper("")
    ds = xr.open_dataset(
        m, engine="zarr", backend_kwargs=dict(consolidated=False), chunks={"valid_time": 1}
    )
    return ds


@pytest.fixture()
def tmp_target_url(tmpdir_factory):
    return str(tmpdir_factory.mktemp("target"))


@pytest.fixture
def remote_protocol() -> str:
    return "s3"


@pytest.fixture
def storage_options():
    return {"anon": True}


@pytest.fixture
def grib_filters() -> dict:
    return {"typeOfLevel": "heightAboveGround", "level": [2, 10]}


@pytest.fixture
def concat_dims() -> list[str]:
    return ["valid_time"]


@pytest.fixture
def identical_dims() -> list[str]:
    return ["latitude", "longitude", "heightAboveGround", "step"]


@pytest.fixture
def src_files(remote_protocol, storage_options) -> list[str]:
    fs_read: s3fs.S3FileSystem = fsspec.filesystem(
        remote_protocol,
        skip_instance_cache=True,
        **storage_options,
    )
    days_available = fs_read.glob("s3://noaa-hrrr-bdp-pds/hrrr.*")
    files = fs_read.glob(f"s3://{days_available[-1]}/conus/*wrfsfcf01.grib2")
    files = sorted(["s3://" + f for f in files])
    return files[0:2]


@pytest.fixture
def vanilla_kerchunk_ds(
    tmpdir_factory,
    src_files: list[str],
    remote_protocol: str,
    storage_options: dict,
    grib_filters: dict,
    concat_dims: list[str],
    identical_dims: list[str],
):
    """Based on https://projectpythia.org/kerchunk-cookbook/notebooks/case_studies/HRRR.html"""

    temp_dir = str(tmpdir_factory.mktemp("target"))
    for url in src_files:
        out = scan_grib(
            url,
            storage_options=storage_options,
            inline_threshold=100,
            filter=grib_filters,
        )
        for msg_number, msg in enumerate(out):
            date = url.split("/")[3].split(".")[1]
            name = url.split("/")[5].split(".")[1:3]
            out_file_name = f"{temp_dir}/{date}_{name[0]}_{name[1]}_message{msg_number}.json"
            with open(out_file_name, "w") as f:
                f.write(ujson.dumps(msg))

    output_files = glob.glob(f"{temp_dir}/*.json")

    # Combine individual references into single consolidated reference
    mzz = MultiZarrToZarr(
        output_files,
        concat_dims=concat_dims,
        identical_dims=identical_dims,
    )
    multi_kerchunk = mzz.translate()
    ds = open_reference_ds(multi_kerchunk, remote_protocol, storage_options)
    return ds


@pytest.fixture
def pipeline():
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


@pytest.fixture
def pangeo_forge_ds(
    src_files,
    pipeline,
    concat_dims: list[str],
    identical_dims: list[str],
    tmp_target_url: str,
    remote_protocol: str,
    storage_options: dict,
):
    """Aims to create the same dataset as `vanilla_kerchunk_ds` fixture, but with Pangeo Forge."""

    pattern: FilePattern = pattern_from_file_sequence(
        src_files,
        concat_dim=concat_dims[0],
        file_type="grib",
    )
    store_name = "grib-test-store"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenURLWithFSSpec()
            | OpenWithKerchunk(
                file_type=pattern.file_type,
                remote_protocol=remote_protocol,
                storage_options=storage_options,
            )
            | DropKeys()
            | CombineReferences(
                concat_dims=concat_dims,
                identical_dims=identical_dims,
            )
            | WriteCombinedReference(
                target_root=tmp_target_url,
                store_name=store_name,
            )
        )
    full_path = os.path.join(tmp_target_url, store_name, "reference.json")
    with open(full_path) as f:
        multi_kerchunk = ujson.load(f)
    ds = open_reference_ds(multi_kerchunk, remote_protocol, storage_options)
    return ds


def test_consolidated_refs(vanilla_kerchunk_ds, pangeo_forge_ds):
    xr.testing.assert_equal(vanilla_kerchunk_ds, pangeo_forge_ds)
