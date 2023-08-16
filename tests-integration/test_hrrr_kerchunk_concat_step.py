from pathlib import Path

from conftest import bake_recipe, open_reference_ds


def test(recipes_dir: Path, confpath_json: str, tmpdir: Path):

    bake_recipe(recipes_dir / "hrrr_kerchunk_concat_step.py", confpath_json, tmpdir)
    ds = open_reference_ds(
        tmpdir / "target" / "hrrr-concat-step" / "reference.json",
        remote_protocol="s3",
        remote_options={"anon": True},
        chunks={},
    )
    ds = ds.set_coords(("latitude", "longitude"))
    assert ds.attrs["centre"] == "kwbc"
    assert len(ds["step"]) == 4
    assert len(ds["time"]) == 1
    assert "t" in ds.data_vars
    for coord in ["time", "surface", "latitude", "longitude"]:
        assert coord in ds.coords
