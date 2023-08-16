import subprocess
import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Type

from conftest import RecipeIntegrationTests


def test_integration(
    recipe_modules_with_test_cls: tuple[Path, Type[RecipeIntegrationTests]],
    config_abspath: str,
    target_and_cache_tmppaths: tuple[str, str],
):
    recipe_module, test_cls = recipe_modules_with_test_cls

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
        # t = test_cls(target_root=target_tmppath)
        # t.test_ds()
