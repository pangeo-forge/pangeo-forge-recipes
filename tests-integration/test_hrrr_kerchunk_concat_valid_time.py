from pathlib import Path

import pytest
from conftest import bake_recipe, open_reference_ds


@pytest.xfail(reason="pickle.PicklingError: Can't pickle <function drop_unknown at 0x...>")
def test(recipes_dir: Path, confpath_json: str, tmpdir: Path):

    bake_recipe(recipes_dir / "hrrr_kerchunk_concat_valid_time.py", confpath_json, tmpdir)
    ds = open_reference_ds(
        tmpdir / "target" / "hrrr-concat-valid-time" / "reference.json",
        remote_protocol="s3",
        remote_options={"anon": True},
        chunks={},
    )
    # TODO: more detailed asserts
    assert "t2m" in ds.data_vars
