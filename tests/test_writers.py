import fsspec
import pytest
import xarray as xr
import zarr

from pangeo_forge_recipes.aggregation import schema_to_zarr
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.types import CombineOp, Dimension, Index, IndexedPosition, Position
from pangeo_forge_recipes.writers import _select_single_protocol, store_dataset_fragment

from .data_generation import make_ds


@pytest.fixture
def temp_store(tmp_path):
    return zarr.storage.FSStore(str(tmp_path))


def test_store_dataset_fragment(temp_store):

    ds = make_ds(non_dim_coords=True)
    schema = ds.to_dict(data=False, encoding=True)
    schema["chunks"] = {}

    ds.to_zarr(temp_store)
    ds_target = xr.open_dataset(temp_store, engine="zarr").load()

    # at this point, all dimension coordinates are written
    schema_to_zarr(schema, temp_store, {"time": 2})

    # at this point the dimension coordinates are just dummy values
    expected_chunks = ["lon/0", "lat/0"] + [f"time/{n}" for n in range(5)]
    actual_chunks = [item for item in temp_store if ".z" not in item]
    assert set(expected_chunks) == set(actual_chunks)

    # variables are not written
    assert "foo/0.0.0" not in temp_store
    assert "bar/0.0.0" not in temp_store
    # non-dim coords are not written
    assert "baz/0" not in temp_store
    assert "timestep/0" not in temp_store

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


@pytest.mark.parametrize("protocol", ["s3", "https"])
def test_select_single_protocol(protocol):
    fsspec_target = FSSpecTarget(fsspec.filesystem(protocol))
    assert isinstance(_select_single_protocol(fsspec_target), str)
