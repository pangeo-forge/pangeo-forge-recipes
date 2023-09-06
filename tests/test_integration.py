import json
import subprocess
from pathlib import Path

import pytest

# Run only when the `--run-integration` option is passed.
# See also `pytest_addoption` in conftest. Reference:
# https://jwodder.github.io/kbits/posts/pytest-mark-off/
pytestmark = pytest.mark.skipif(
    "not config.getoption('--run-integration')",
    reason="Only run when --run-integration is given",
)

DOCS_SRC = Path(__file__).parent.parent / "docs_src"


@pytest.fixture
def tmpdir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("tmp")


# TODO: test that json and python configs in docs_src are identical
# this way we can confidently use just one of the two
@pytest.fixture
def confpath(tmpdir: Path):
    fname = "local.json"
    dstpath = tmpdir / fname
    with open(DOCS_SRC / "runner-config" / fname) as src:
        with dstpath.open(mode="w") as dst:
            c = json.load(src)
            c["TargetStorage"]["root_path"] = (tmpdir / "target").absolute().as_posix()
            c["InputCacheStorage"]["root_path"] = (tmpdir / "cache").absolute().as_posix()
            json.dump(c, dst)

    return dstpath.absolute().as_posix()


@pytest.mark.parametrize("recipe_id", ["gpcp-from-gcs"])
def test_integration(recipe_id: str, confpath: str, tmpdir: Path):
    cmd = [
        "pangeo-forge-runner",
        "bake",
        f"--repo={DOCS_SRC.absolute().as_posix()}",
        f"-f={confpath}",
        f"--Bake.recipe_id={recipe_id}",
        f"--Bake.job_name={'abc'}",  # TODO: make this a unique identifier
    ]
    proc = subprocess.run(cmd, capture_output=True, cwd=tmpdir.absolute().as_posix())
    assert proc.returncode == 0
