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

EXAMPLES = Path(__file__).parent.parent / "examples"


def test_python_json_configs_identical():
    """We provide examples of both Python and JSON config. By ensuring they are
    identical, we can confidently use just one of them for the integration tests.
    """
    from pangeo_forge_runner.commands.base import BaseCommand  # type: ignore

    python_, json_ = BaseCommand(), BaseCommand()
    python_.load_config_file((EXAMPLES / "runner-config" / "local.py").absolute().as_posix())
    json_.load_config_file((EXAMPLES / "runner-config" / "local.json").absolute().as_posix())

    assert python_.config and json_.config  # make sure we actually loaded something
    assert python_.config == json_.config


@pytest.fixture
def confpath(tmp_path_factory: pytest.TempPathFactory):
    """The JSON config is easier to modify with tempdirs, so we use that here for
    convenience. But we know it's the same as the Python config, because we test that.
    """
    tmp = tmp_path_factory.mktemp("tmp")
    fname = "local.json"
    dstpath = tmp / fname
    with open(EXAMPLES / "runner-config" / fname) as src:
        with dstpath.open(mode="w") as dst:
            c = json.load(src)
            c["TargetStorage"]["root_path"] = (tmp / "target").absolute().as_posix()
            c["InputCacheStorage"]["root_path"] = (tmp / "cache").absolute().as_posix()
            json.dump(c, dst)

    return dstpath.absolute().as_posix()


@pytest.mark.parametrize(
    "recipe_id",
    [
        p.stem.replace("_", "-")
        for p in (EXAMPLES / "feedstock").iterdir()
        if p.suffix == ".py" and not p.stem.startswith("_")
    ],
)
def test_integration(recipe_id: str, confpath: str):
    """Run the example recipes in the ``examples/feedstock`` directory."""

    xfails = {
        "hrrr-kerchunk-concat-step": "WriteCombineReference doesn't return zarr.storage.FSStore",
        "hrrr-kerchunk-concat-valid-time": "Can't serialize drop_unknown callback function.",
        "narr-opendap": "Hangs for unkown reason. Requires further debugging.",
        "terraclimate": "Hangs for unkown reason. Requires further debugging.",
    }
    if recipe_id in xfails:
        pytest.xfail(xfails[recipe_id])

    cmd = [
        "pangeo-forge-runner",
        "bake",
        f"--repo={EXAMPLES.absolute().as_posix()}",
        f"-f={confpath}",
        f"--Bake.recipe_id={recipe_id}",
        f"--Bake.job_name={'abc'}",  # TODO: make this a unique identifier
        "--prune",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    assert proc.returncode == 0
