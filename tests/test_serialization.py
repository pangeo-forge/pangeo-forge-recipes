from datetime import datetime, timedelta

import pandas as pd
import pytest
from fsspec.implementations.local import LocalFileSystem

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
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


@pytest.mark.parametrize("new_pattern_nitems_per_file", [1, 2])
def test_match_pattern_blockchain(
    base_pattern,
    end_date,
    new_pattern_nitems_per_file,
):
    new_pattern, next_url = get_new_pattern_with_next_url(end_date, new_pattern_nitems_per_file)
    matching_key = match_pattern_blockchain(base_pattern.sha256(), new_pattern)

    if new_pattern_nitems_per_file == 1:
        assert new_pattern[matching_key] == next_url
    elif new_pattern_nitems_per_file == 2:
        assert matching_key is None


def test_xarray_zarr_sha256_hash_exclude(base_pattern, tmpdir_factory):
    recipe_0 = XarrayZarrRecipe(base_pattern)
    recipe_1 = XarrayZarrRecipe(base_pattern)

    assert recipe_0.sha256() == recipe_1.sha256()

    local_fs = LocalFileSystem()
    custom_target_path = tmpdir_factory.mktemp("custom_target")
    custom_storage_config = StorageConfig(target=FSSpecTarget(local_fs, custom_target_path))
    recipe_1.storage_config = custom_storage_config

    assert recipe_0.sha256() == recipe_1.sha256()
