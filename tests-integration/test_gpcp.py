from pathlib import Path

import xarray as xr
from conftest import bake_recipe


def test(recipes_dir: Path, confpath: str, tmpdir: Path):

    bake_recipe(recipes_dir / "gpcp_from_gcs.py", confpath, tmpdir)
    ds = xr.open_dataset(tmpdir / "target" / "gpcp.zarr", engine="zarr")
    assert ds.title == (
        "Global Precipitation Climatatology Project (GPCP) " "Climate Data Record (CDR), Daily V1.3"
    )
