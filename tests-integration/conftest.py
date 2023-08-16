import json
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import fsspec
import pytest
import xarray as xr
import yaml
from fsspec.implementations.reference import ReferenceFileSystem
from pytest_lazyfixture import lazy_fixture

docs_src = Path(__file__).parent.parent / "docs_src"


@pytest.fixture
def recipes_dir():
    return Path(__file__).parent.parent / "docs_src" / "recipes"


def open_reference_ds(
    path: Path,
    remote_protocol: str,
    remote_options: dict,
    chunks: dict,
):
    """Open dataset as zarr object using fsspec reference file system and xarray."""
    with open(path) as f:
        fs: ReferenceFileSystem = fsspec.filesystem(
            "reference",
            fo=json.load(f),
            remote_protocol=remote_protocol,
            remote_options=remote_options,
        )
    m = fs.get_mapper("")
    ds = xr.open_dataset(
        m,
        engine="zarr",
        backend_kwargs=dict(consolidated=False),
        chunks=chunks,
    )
    return ds


@dataclass
class RecipeIntegrationTests(ABC):

    target_root: str

    @property
    def ds(self) -> xr.Dataset:
        # TODO: make it clearer what's going on here; fix mypy ignores
        dir = Path(self.target_root).joinpath(self.store_name)  # type: ignore
        if self.store_name.endswith(".zarr"):  # type: ignore
            return xr.open_dataset(dir.as_posix(), engine="zarr")
        else:
            return open_reference_ds(
                dir / "reference.json",
                self.remote_protocol,  # type: ignore
                self.storage_options,  # type: ignore
                self.chunks,  # type: ignore
            )

    @abstractmethod
    def test_ds(self):
        pass


class hrrr_kerchunk_concat_step(RecipeIntegrationTests):

    store_name = "hrrr-concat-step"

    def test_ds(self):
        ds = self.ds.set_coords(("latitude", "longitude"))
        assert ds.attrs["centre"] == "kwbc"
        assert len(ds["step"]) == 4
        assert len(ds["time"]) == 1
        assert "t" in ds.data_vars
        for coord in ["time", "surface", "latitude", "longitude"]:
            assert coord in ds.coords


class hrrr_kerchunk_concat_valid_time(RecipeIntegrationTests):
    ...


@pytest.fixture(
    params=[
        (docs_src / "recipes" / "hrrr_kerchunk_concat_step.py", hrrr_kerchunk_concat_step),
        (
            docs_src / "recipes" / "hrrr_kerchunk_concat_valid_time.py",
            hrrr_kerchunk_concat_valid_time,
        ),
    ],
    ids=[
        "gpcp_from_gcs",
        "hrrr_kerchunk_concat_step",
        "hrrr_kerchunk_concat_valid_time",
    ],
)
def recipe_modules_with_test_cls(request):
    return request.param


@pytest.fixture
def tmpdir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("tmp")


@pytest.fixture
def target_and_cache_tmppaths(tmpdir: Path):
    return ((tmpdir / "target").absolute().as_posix(), (tmpdir / "cache").absolute().as_posix())


def rewrite_config_with_tmppaths(
    fname: str,
    tmpdir: Path,
    target_and_cache_tmppaths: tuple[str, str],
) -> str:
    dstpath = tmpdir / fname
    tmp_target, tmp_cache = target_and_cache_tmppaths

    with open(docs_src / "runner-config" / fname) as src:
        with dstpath.open(mode="w") as dst:
            if Path(fname).suffix == ".json":
                c = json.load(src)
                c["TargetStorage"]["root_path"] = tmp_target
                c["InputCacheStorage"]["root_path"] = tmp_cache
                json.dump(c, dst)
            else:
                c = src.read().replace("./target", tmp_target).replace("./cache", tmp_cache)
                dst.write(c)

    return dstpath.absolute().as_posix()


@pytest.fixture
def confpath_json(tmpdir, target_and_cache_tmppaths):
    return rewrite_config_with_tmppaths("local.json", tmpdir, target_and_cache_tmppaths)


@pytest.fixture
def confpath_python(tmpdir, target_and_cache_tmppaths):
    return rewrite_config_with_tmppaths("local.py", tmpdir, target_and_cache_tmppaths)


@pytest.fixture(
    scope="session",
    params=[
        lazy_fixture("confpath_json"),
        lazy_fixture("confpath_python"),
    ],
)
def confpath(request):
    return request.param


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
