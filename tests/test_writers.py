import os

import apache_beam as beam
import fsspec
import pytest
import xarray as xr
import zarr

from pangeo_forge_recipes.aggregation import schema_to_zarr
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata,
    OpenWithKerchunk,
    OpenWithXarray,
    StoreToZarr,
    WriteCombinedReference,
)
from pangeo_forge_recipes.types import CombineOp, Dimension, Index, IndexedPosition, Position
from pangeo_forge_recipes.writers import store_dataset_fragment

from .data_generation import make_ds


@pytest.fixture
def temp_store(tmp_path):
    from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

    fs = AsyncFileSystemWrapper(fsspec.filesystem("file", auto_mkdir=True))
    return zarr.storage.FsspecStore(path=str(tmp_path), fs=fs)


def test_store_dataset_fragment(temp_store):
    ds = make_ds(non_dim_coords=True)
    mapper = temp_store.fs.sync_fs.get_mapper(temp_store.path)

    schema = ds.to_dict(data=False, encoding=True)
    schema["chunks"] = {}

    ds.to_zarr(temp_store)
    ds_target = xr.open_dataset(temp_store, engine="zarr").load()

    # at this point, all dimension coordinates are written
    schema_to_zarr(schema, temp_store, {"time": 2})

    # at this point the dimension coordinates are just dummy values
    expected_chunks = ["lon/c/0", "lat/c/0"] + [f"time/c/{n}" for n in range(5)]

    actual_chunks = [item for item in mapper if ".z" not in item and not item.endswith("zarr.json")]
    assert set(expected_chunks) == set(actual_chunks)

    # variables are not written
    assert "foo/c/0.0.0" not in temp_store
    assert "bar/c/0.0.0" not in temp_store
    # non-dim coords are not written
    assert "baz/c/0" not in temp_store
    assert "timestep/c/0" not in temp_store

    # for testing purposed, we now delete all dimension coordinates.
    # this helps us verify that chunks are re-written at the correct time
    temp_store.delitems(actual_chunks)

    # Now we spoof that we are writing data.
    # The Index tells where to put it.
    # this is deliberatly not the first element;
    fragment_1_1 = ds[["bar"]].isel(time=slice(2, 4))
    index_1_1 = Index(
        {
            Dimension("time", CombineOp.CONCAT): IndexedPosition(2),
            Dimension("variable", CombineOp.MERGE): Position(1),
        }
    )

    store_dataset_fragment((index_1_1, fragment_1_1), temp_store)

    # check that only the expected data has been stored
    assert "bar/1.0.0" in temp_store
    assert "bar/0.0.0" not in temp_store
    assert "foo/1.0.0" not in temp_store
    assert "foo/0.0.0" not in temp_store

    # because this was not the first element in a merge dim, no coords were written
    assert "time/1" not in temp_store
    assert "timestep/1" not in temp_store
    assert "baz/0.0" not in temp_store

    # this is the first element of merge dim but NOT concat dim
    fragment_0_1 = ds[["foo"]].isel(time=slice(2, 4))
    index_0_1 = Index(
        {
            Dimension("time", CombineOp.CONCAT): IndexedPosition(2),
            Dimension("variable", CombineOp.MERGE): Position(0),
        }
    )

    store_dataset_fragment((index_0_1, fragment_0_1), temp_store)

    assert "foo/1.0.0" in temp_store
    assert "foo/0.0.0" not in temp_store

    # the coords with time in them should be stored
    assert "time/1" in temp_store
    assert "timestep/1" in temp_store

    # but other coords are not
    assert "lon/0" not in temp_store
    assert "lat/0" not in temp_store
    assert "baz/0.0" not in temp_store

    # let's finally store the first piece
    fragment_0_0 = ds[["foo"]].isel(time=slice(0, 2))
    index_0_0 = Index(
        {
            Dimension("time", CombineOp.CONCAT): IndexedPosition(0),
            Dimension("variable", CombineOp.MERGE): Position(0),
        }
    )

    store_dataset_fragment((index_0_0, fragment_0_0), temp_store)

    # now vars and coords should be there
    assert "foo/0.0.0" in temp_store
    assert "time/0" in temp_store
    assert "timestep/0" in temp_store
    assert "lon/0" in temp_store
    assert "lat/0" in temp_store
    assert "baz/0.0" in temp_store

    # but we haven't stored this var yet
    assert "bar/0.0.0" not in temp_store

    fragment_1_0 = ds[["bar"]].isel(time=slice(0, 2))
    index_1_0 = Index(
        {
            Dimension("time", CombineOp.CONCAT): IndexedPosition(0),
            Dimension("variable", CombineOp.MERGE): Position(1),
        }
    )

    store_dataset_fragment((index_1_0, fragment_1_0), temp_store)

    assert "bar/0.0.0" in temp_store

    # now store everything else
    for nvar, vname in enumerate(["foo", "bar"]):
        for t_start in range(4, 10, 2):
            index = Index(
                {
                    Dimension("time", CombineOp.CONCAT): IndexedPosition(t_start),
                    Dimension("variable", CombineOp.MERGE): Position(nvar),
                }
            )
            fragment = ds[[vname]].isel(time=slice(t_start, t_start + 2))
            store_dataset_fragment((index, fragment), temp_store)

    ds_target = xr.open_dataset(temp_store, engine="zarr").load()

    xr.testing.assert_identical(ds, ds_target)
    # assert_identical() doesn't check encoding
    # Checking the original time encoding units should be sufficient
    assert ds.time.encoding.get("units") == ds_target.time.encoding.get("units")


@pytest.mark.skip(reason="consolidated metadata is not supported in Zarr V3. ")
def test_zarr_consolidate_metadata(
    netcdf_local_file_pattern,
    pipeline,
    tmp_target,
):
    pattern = netcdf_local_file_pattern
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target,
                store_name="store",
                combine_dims=pattern.combine_dim_keys,
            )
            | ConsolidateMetadata()
        )

    path = os.path.join(tmp_target.root_path, "store")
    fs = fsspec.filesystem("file")
    zc = zarr.storage.FsspecStore(path=path, fs=fs)
    assert zc[".zmetadata"] is not None

    assert xr.open_zarr(path, consolidated=True)


def test_zarr_encoding(
    netcdf_local_file_pattern,
    pipeline,
    tmp_target,
):
    pattern = netcdf_local_file_pattern
    compressors = (zarr.codecs.BloscCodec(typesize=8, cname="zstd", clevel=3, shuffle="shuffle"),)
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray(file_type=pattern.file_type)
            | StoreToZarr(
                target_root=tmp_target,
                store_name="store",
                combine_dims=pattern.combine_dim_keys,
                encoding={"foo": {"compressors": compressors}},
            )
            # | ConsolidateMetadata()
        )
    from fsspec.implementations.asyn_wrapper import AsyncFileSystemWrapper

    fs = AsyncFileSystemWrapper(fsspec.filesystem("file"))
    zc = zarr.storage.FsspecStore(path=os.path.join(tmp_target.root_path, "store"), fs=fs)
    z = zarr.open(zc)
    assert z["foo"].compressors == compressors


@pytest.mark.skip(reason="kerchunk related issue with Zarr V3")
@pytest.mark.parametrize("output_file_name", ["reference.json", "reference.parquet"])
def test_reference_netcdf(
    netcdf_local_file_pattern_sequential,
    pipeline,
    tmp_target,
    output_file_name,
):
    pattern = netcdf_local_file_pattern_sequential
    store_name = "daily-xarray-dataset"
    with pipeline as p:
        (
            p
            | beam.Create(pattern.items())
            | OpenWithKerchunk(file_type=pattern.file_type)
            | WriteCombinedReference(
                identical_dims=["lat", "lon"],
                target_root=tmp_target,
                store_name=store_name,
                concat_dims=["time"],
                output_file_name=output_file_name,
            )
        )

    full_path = os.path.join(tmp_target.root_path, store_name, output_file_name)
    mapper = fsspec.get_mapper(
        "reference://",
        target_protocol=tmp_target.get_fsspec_remote_protocol(),
        remote_protocol=tmp_target.get_fsspec_remote_protocol(),
        fo=full_path,
    )

    assert xr.open_zarr(mapper, consolidated=False)
