from pathlib import Path

import xarray as xr
from conftest import bake_recipe


def test(recipes_dir: Path, confpath_json: str, tmpdir: Path):

    bake_recipe(recipes_dir / "terraclimate.py", confpath_json, tmpdir)
    ds = xr.open_dataset(tmpdir / "target" / "terraclimate.zarr", engine="zarr")
    assert "soil" in ds.data_vars
    assert "srad" in ds.data_vars
    assert len(ds.time) == 24
