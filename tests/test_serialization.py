from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pytest
from fsspec.implementations.local import LocalFileSystem

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, FileType
from pangeo_forge_recipes.recipes import HDFReferenceRecipe, XarrayZarrRecipe
from pangeo_forge_recipes.serialization import match_pattern_blockchain
from pangeo_forge_recipes.storage import FSSpecTarget, StorageConfig

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_file_pattern(dates, nitems_per_file):
    def make_url(time):
        return URL_FORMAT.format(time=time)

    time_concat_dim = ConcatDim("time", dates, nitems_per_file=nitems_per_file)
    pattern = FilePattern(make_url, time_concat_dim)

    return pattern


@pytest.fixture
def end_date():
    return "1981-10-01"


@pytest.fixture
def base_pattern(end_date):
    dates = pd.date_range("1981-09-01", end_date, freq="D")
    return make_file_pattern(dates, nitems_per_file=1)


def get_new_pattern_with_next_url(end_date, nitems_per_file):

    fmt = "%Y-%m-%d"

    def increment_end_date(ndays):
        return datetime.strptime(end_date, fmt) + timedelta(days=ndays)

    next_day = increment_end_date(ndays=1)
    new_end_date = increment_end_date(ndays=10).strftime(fmt)
    new_dates = pd.date_range("1981-09-01", new_end_date, freq="D")
    new_pattern = make_file_pattern(new_dates, nitems_per_file=nitems_per_file)
    return new_pattern, URL_FORMAT.format(time=next_day)


@pytest.fixture(params=["matching", "not_matching"])
def pattern_pair(base_pattern, end_date, request):
    if request.param == "matching":
        return (base_pattern, base_pattern)
    elif request.param == "not_matching":
        new_pattern, _ = get_new_pattern_with_next_url(end_date, nitems_per_file=1)
        return (base_pattern, new_pattern)


@pytest.mark.parametrize("new_pattern_nitems_per_file", [1, 2])
@pytest.mark.parametrize(
    "kwargs",
    [
        ({}, {}),
        ({}, dict(fsspec_open_kwargs={"block_size": 0})),
        (dict(fsspec_open_kwargs={"block_size": 0}), dict(fsspec_open_kwargs={"block_size": 0})),
        (dict(file_type=FileType.opendap), dict(fsspec_open_kwargs={"block_size": 0})),
        (dict(file_type=FileType.opendap), dict(file_type=FileType.opendap)),
    ],
)
def test_match_pattern_blockchain(base_pattern, end_date, new_pattern_nitems_per_file, kwargs):
    new_pattern, next_url = get_new_pattern_with_next_url(end_date, new_pattern_nitems_per_file)

    for i, pattern in enumerate((base_pattern, new_pattern)):
        for k, v in kwargs[i].items():
            setattr(pattern, k, v)

    matching_key = match_pattern_blockchain(base_pattern.sha256(), new_pattern)

    if kwargs[0] == kwargs[1] and new_pattern_nitems_per_file == 1:
        assert new_pattern[matching_key] == next_url
    elif kwargs[0] != kwargs[1] or new_pattern_nitems_per_file == 2:
        assert matching_key is None


@pytest.mark.parametrize("recipe_cls", [XarrayZarrRecipe, HDFReferenceRecipe])
def test_recipe_sha256_hash_exclude(base_pattern, recipe_cls, tmpdir_factory):
    recipe_0 = recipe_cls(base_pattern)
    recipe_1 = recipe_cls(base_pattern)

    assert recipe_0.sha256() == recipe_1.sha256()

    local_fs = LocalFileSystem()
    custom_target_path = tmpdir_factory.mktemp("custom_target")
    custom_storage_config = StorageConfig(target=FSSpecTarget(local_fs, custom_target_path))
    recipe_1.storage_config = custom_storage_config

    assert recipe_0.sha256() == recipe_1.sha256()


@pytest.mark.parametrize(
    "kwargs",
    [
        ({}, {}),
        ({}, dict(target_chunks={"time": 1})),
        (dict(target_chunks={"time": 1}), dict(target_chunks={"time": 1})),
        (dict(target_chunks={"time": 1}), dict(target_chunks={"time": 2})),
        (dict(subset_inputs={"time": 2}), dict(target_chunks={"time": 2})),
    ],
)
def test_xarray_zarr_sha265(pattern_pair, kwargs):
    recipe_0 = XarrayZarrRecipe(pattern_pair[0], **kwargs[0])
    recipe_1 = XarrayZarrRecipe(pattern_pair[1], **kwargs[1])

    if pattern_pair[0] == pattern_pair[1] and kwargs[0] == kwargs[1]:
        assert recipe_0.sha256() == recipe_1.sha256()
    else:
        assert recipe_0.sha256() != recipe_1.sha256()


@pytest.mark.parametrize(
    "kwargs",
    [
        ({}, {}),
        ({}, dict(output_json_fname="custom_name.json")),
        (dict(output_json_fname="custom_name.json"), dict(output_json_fname="custom_name.json")),
        (dict(netcdf_storage_options={"anon": True}), dict(output_json_fname="custom_name.json")),
    ],
)
def test_kerchunk_sha265(pattern_pair, kwargs):
    recipe_0 = HDFReferenceRecipe(pattern_pair[0], **kwargs[0])
    recipe_1 = HDFReferenceRecipe(pattern_pair[1], **kwargs[1])

    if pattern_pair[0] == pattern_pair[1] and kwargs[0] == kwargs[1]:
        assert recipe_0.sha256() == recipe_1.sha256()
    else:
        assert recipe_0.sha256() != recipe_1.sha256()


@pytest.mark.parametrize("recipe_cls", [XarrayZarrRecipe, HDFReferenceRecipe])
@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"new_optional_str": "hello"},
        {"new_dict": dict(a=1)},
        {"new_list": [1, 2, 3]},
    ],
)
def test_additional_fields(base_pattern, recipe_cls, kwargs):
    # simulates a new release in which recipe class fields are added

    @dataclass
    class UpdatedRecipe(recipe_cls):
        new_optional_str: Optional[str] = None
        new_dict: dict = field(default_factory=dict)
        new_list: list = field(default_factory=list)

    recipe_0 = recipe_cls(base_pattern)
    recipe_1 = UpdatedRecipe(base_pattern, **kwargs)

    if not kwargs:
        assert recipe_0.sha256() == recipe_1.sha256()
    else:
        assert recipe_0.sha256() != recipe_1.sha256()
