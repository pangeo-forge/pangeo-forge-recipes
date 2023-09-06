import json
import subprocess
from pathlib import Path

import pytest
import yaml

# Run only when the `--run-integration` option is passed.
# See also `pytest_addoption` in conftest. Reference:
# https://jwodder.github.io/kbits/posts/pytest-mark-off/
pytestmark = pytest.mark.skipif(
    "not config.getoption('--run-integration')",
    reason="Only run when --run-integration is given",
)

DOCS_SRC = Path(__file__).parent.parent / "docs_src"


def bake_recipe(recipe_module: Path, confpath: str, tmpdir: Path):
    (tmpdir / "feedstock").mkdir(parents=True)

    with recipe_module.open() as src:
        with (tmpdir / "feedstock" / recipe_module.name).open(mode="w") as dst:
            dst.write(src.read())

    meta_yaml = {"recipes": [{"id": "my-recipe", "object": f"{recipe_module.stem}:recipe"}]}
    with (tmpdir / "feedstock" / "meta.yaml").open(mode="w") as dst:
        yaml.safe_dump(meta_yaml, dst)

    cmd = [
        "pangeo-forge-runner",
        "bake",
        "--repo=.",
        f"-f={confpath}",
        f"--Bake.job_name={'abc'}",  # TODO: make this a unique identifier
    ]
    proc = subprocess.run(cmd, capture_output=True, cwd=tmpdir.absolute().as_posix())
    assert proc.returncode == 0


@pytest.fixture
def tmpdir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("tmp")


# TODO: test that json and python configs in docs_src are identical
# this way we can confidently use just one of the two
@pytest.fixture
def confpath(tmpdir: Path):
    fname = "local.json"
    tmp_target = (tmpdir / "target").absolute().as_posix()
    tmp_cache = (tmpdir / "cache").absolute().as_posix()
    dstpath = tmpdir / fname
    with open(DOCS_SRC / "runner-config" / fname) as src:
        with dstpath.open(mode="w") as dst:
            c = json.load(src)
            c["TargetStorage"]["root_path"] = tmp_target
            c["InputCacheStorage"]["root_path"] = tmp_cache
            json.dump(c, dst)

    return dstpath.absolute().as_posix()


def test_integration(confpath: str, tmpdir: Path):
    bake_recipe(DOCS_SRC / "feedstock" / "gpcp_from_gcs.py", confpath, tmpdir)
