from pathlib import Path

import xarray as xr
from conftest import bake_recipe


def test(recipes_dir: Path, confpath_json: str, tmpdir: Path):

    bake_recipe(recipes_dir / "narr_opendap.py", confpath_json, tmpdir)
    ds = xr.open_dataset(tmpdir / "target" / "narr.zarr", engine="zarr")
    assert "air" in ds.data_vars
