import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import fsspec
import pytest
import xarray as xr
from fsspec.implementations.reference import ReferenceFileSystem

recipes = Path(__file__).parent / "recipes"


def open_reference_ds(
    path: Path,
    remote_protocol: str,
    remote_options: dict,
    chunks: dict,
):
    """Open dataset as zarr object using fsspec reference file system and xarray."""
    with open(path) as f:
        fs: ReferenceFileSystem = fsspec.filesystem(
            "reference",
            fo=json.load(f),
            remote_protocol=remote_protocol,
            remote_options=remote_options,
        )
    m = fs.get_mapper("")
    ds = xr.open_dataset(
        m,
        engine="zarr",
        backend_kwargs=dict(consolidated=False),
        chunks=chunks,
    )
    return ds


@dataclass
class RecipeIntegrationTests(ABC):

    target_root: str

    @property
    def ds(self) -> xr.Dataset:
        # TODO: make it clearer what's going on here; fix mypy ignores
        dir = Path(self.target_root).joinpath(self.store_name)  # type: ignore
        if self.store_name.endswith(".zarr"):  # type: ignore
            return xr.open_dataset(dir.as_posix(), engine="zarr")
        else:
            return open_reference_ds(
                dir / "reference.json",
                self.remote_protocol,  # type: ignore
                self.storage_options,  # type: ignore
                self.chunks,  # type: ignore
            )

    @abstractmethod
    def test_ds(self):
        pass


class gpcp_from_gcs(RecipeIntegrationTests):

    store_name = "gpcp.zarr"

    def test_ds(self):
        assert self.ds.title == (
            "Global Precipitation Climatatology Project (GPCP) "
            "Climate Data Record (CDR), Daily V1.3"
        )


class hrrr_kerchunk_concat_step(RecipeIntegrationTests):

    store_name = "hrrr-concat-step"

    def test_ds(self):
        ds = self.ds.set_coords(("latitude", "longitude"))
        assert ds.attrs["centre"] == "kwbc"
        assert len(ds["step"]) == 4
        assert len(ds["time"]) == 1
        assert "t" in ds.data_vars
        for coord in ["time", "surface", "latitude", "longitude"]:
            assert coord in ds.coords


class hrrr_kerchunk_concat_valid_time(RecipeIntegrationTests):
    ...


@pytest.fixture(
    params=[
        (recipes / "gpcp_from_gcs.py", gpcp_from_gcs),
        (recipes / "hrrr_kerchunk_concat_step.py", hrrr_kerchunk_concat_step),
        (recipes / "hrrr_kerchunk_concat_valid_time.py", hrrr_kerchunk_concat_valid_time),
    ],
    ids=[
        "gpcp_from_gcs",
        "hrrr_kerchunk_concat_step",
        "hrrr_kerchunk_concat_valid_time",
    ],
)
def recipe_modules_with_test_cls(request):
    return request.param
