from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Callable

import pandas as pd
import pytest

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, FileType
from pangeo_forge_recipes.serialization import dict_to_sha256, either_encode_or_hash

# TODO: revise this test once refactor is farther along
pytest.skip(allow_module_level=True)

# TODO: revise this test once refactor is farther along
pytest.skip(allow_module_level=True)

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
def test_start_processing_from(base_pattern, end_date, new_pattern_nitems_per_file, kwargs):
    new_pattern, next_url = get_new_pattern_with_next_url(end_date, new_pattern_nitems_per_file)

    for i, pattern in enumerate((base_pattern, new_pattern)):
        for k, v in kwargs[i].items():
            setattr(pattern, k, v)

    matching_key = new_pattern.start_processing_from(base_pattern.sha256())

    if kwargs[0] == kwargs[1] and new_pattern_nitems_per_file == 1:
        assert new_pattern[matching_key] == next_url
    elif kwargs[0] != kwargs[1] or new_pattern_nitems_per_file == 2:
        assert matching_key is None


def test_either_encode_or_hash_raises():
    def f():
        pass

    @dataclass
    class HasUnserializableField:
        unserializable_field: Callable = f

    expected_msg = f"object of type {type(f).__name__} not serializable"

    with pytest.raises(TypeError, match=expected_msg):
        either_encode_or_hash(f)

    with pytest.raises(TypeError, match=expected_msg):
        # in practice, we never actually call ``either_encode_or_hash`` directly.
        # it's actually called from within ``dict_to_sha256``.
        dict_to_sha256(asdict(HasUnserializableField()))
