from datetime import datetime, timedelta

import pandas as pd
import pytest

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.serialization import match_pattern_blockchain, pattern_blockchain

URL_FORMAT = (
    "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/"
    "v2.1/access/avhrr/{time:%Y%m}/oisst-avhrr-v02r01.{time:%Y%m%d}.nc"
)


def make_file_pattern(dates):
    def make_url(time):
        return URL_FORMAT.format(time=time)

    time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
    pattern = FilePattern(make_url, time_concat_dim)

    return pattern


@pytest.fixture
def end_date():
    return "2022-02-01"


@pytest.fixture
def old_pattern_with_last_hash(end_date):
    dates = pd.date_range("1981-09-01", end_date, freq="D")
    pattern = make_file_pattern(dates)
    chain = pattern_blockchain(pattern)
    last_hash = chain[-1]
    return pattern, last_hash


@pytest.fixture
def new_pattern_with_next_url(end_date):

    fmt = "%Y-%m-%d"

    def increment_end_date(ndays):
        return datetime.strptime(end_date, fmt) + timedelta(days=ndays)

    next_day = increment_end_date(ndays=1)
    new_end_date = increment_end_date(ndays=90).strftime(fmt)
    new_dates = pd.date_range("1981-09-01", new_end_date, freq="D")
    new_pattern = make_file_pattern(new_dates)
    return new_pattern, URL_FORMAT.format(time=next_day)


def test_match_pattern_blockchain(old_pattern_with_last_hash, new_pattern_with_next_url):
    _, last_hash = old_pattern_with_last_hash
    new_pattern, next_url = new_pattern_with_next_url
    matching_key = match_pattern_blockchain(last_hash, new_pattern)
    assert new_pattern[matching_key] == next_url
