import json
import subprocess
import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Type

import pytest
import xarray as xr
from conftest import RecipeIntegrationTests
from pytest import TempPathFactory


@pytest.fixture
def runner_config(tmp_path_factory: TempPathFactory):
    tmp = tmp_path_factory.mktemp("tmp")
    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.local.LocalDirectBakery",
        },
        "TargetStorage": {
            "fsspec_class": "fsspec.implementations.local.LocalFileSystem",
            "root_path": (tmp / "target").absolute().as_posix(),
        },
        "InputCacheStorage": {
            "fsspec_class": "fsspec.implementations.local.LocalFileSystem",
            "root_path": (tmp / "cache").absolute().as_posix(),
        },
    }
    confpath = tmp / "config.json"
    with confpath.open(mode="w") as f:
        json.dump(config, f)
    return config, confpath.absolute().as_posix()


def test_integration(
    recipe_modules_with_test_cls: tuple[Path, Type[RecipeIntegrationTests]],
    runner_config: tuple[dict, str],
):
    recipe_module, test_cls = recipe_modules_with_test_cls
    config_dict, config_abspath = runner_config

    with tempfile.TemporaryDirectory() as tmpdir:
        as_path = Path(tmpdir)
        (as_path / "feedstock").mkdir(parents=True)

        dstpath = as_path / "feedstock" / recipe_module.name
        with recipe_module.open() as src:
            with dstpath.open(mode="w") as dst:
                dst.write(src.read())
                dst.flush()

        metapath = as_path / "feedstock" / "meta.yaml"
        with metapath.open(mode="w") as meta:
            meta.write(
                dedent(
                    f"""\
                    recipes:
                    - id: my-recipe
                      object: {recipe_module.stem}:recipe
                    """
                )
            )

        cmd = [
            "pangeo-forge-runner",
            "bake",
            "--repo=.",
            "--json",
            f"-f={config_abspath}",
            f"--Bake.job_name={'abc'}",  # TODO: make this a unique identifier
        ]
        proc = subprocess.run(cmd, capture_output=True, cwd=tmpdir)
        assert proc.returncode == 0
        target_root = Path(config_dict["TargetStorage"]["root_path"])
        # TODO: explain what we're doing here with full_path
        full_path = next(target_root.iterdir()).absolute().as_posix()
        ds = xr.open_dataset(full_path, engine="zarr")  # TODO: support kerchunk
        t = test_cls(ds=ds)
        t.test_ds()
