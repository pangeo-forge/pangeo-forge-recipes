import copy
import logging

import apache_beam as beam
import numpy as np
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.combiners import CombineXarraySchemas
from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    DatasetToSchema,
    DetermineSchema,
    _nest_dim,
)
from pangeo_forge_recipes.types import CombineOp, Dimension, Index, Position

from .data_generation import make_ds


@pytest.fixture
def pipeline():
    # Runtime type checking doesn't play well with our Combiner
    # https://github.com/apache/beam/blob/3cddfaf58c69acc624dac16df10357a78ececf59/sdks/python/apache_beam/transforms/core.py#L2505-L2509
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


def _get_schema(path):
    ds = xr.open_dataset(path)
    return dataset_to_schema(ds)


def _expected_schema(pattern):
    expected_ds = xr.open_mfdataset(item[1] for item in pattern.items())
    expected_schema = dataset_to_schema(expected_ds)
    expected_schema["chunks"] = {
        "time": {pos: v for pos, v in enumerate(expected_ds.chunks["time"])}
    }
    return expected_schema


# TODO: make this fixture leaner; don't need netcdf3 and netcdf4
@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential"),
    ],
    ids=["sequential"],
)
def dsets_pcoll_concat(request):
    pattern = request.param
    expected_schema = _expected_schema(pattern)
    return (
        pattern,
        expected_schema,
        beam.Create((key, xr.open_dataset(url)) for key, url in pattern.items()),
    )


@pytest.fixture
def schema_pcoll_concat(dsets_pcoll_concat):
    pattern, expected_schema, pcoll = dsets_pcoll_concat
    pcoll_schema = pcoll | DatasetToSchema()
    return pattern, expected_schema, pcoll_schema


@pytest.fixture(
    scope="module",
    params=[
        lazy_fixture("netcdf_local_file_pattern_sequential_multivariable"),
    ],
    ids=["sequential_multivariable"],
)
def dsets_pcoll_concat_merge(request):
    pattern = request.param
    expected_schema = _expected_schema(pattern)
    return (
        pattern,
        expected_schema,
        beam.Create((key, xr.open_dataset(url)) for key, url in pattern.items()),
    )


@pytest.fixture
def schema_pcoll_concat_merge(dsets_pcoll_concat_merge):
    pattern, expected_schema, pcoll = dsets_pcoll_concat_merge
    pcoll_schema = pcoll | DatasetToSchema()
    return pattern, expected_schema, pcoll_schema


def _get_concat_dim(pattern):
    cdims = pattern.concat_dims
    assert len(cdims) == 1, "Only one concat_dim allowed for now"
    return cdims[0]


def _strip_keys(item):
    return item[1]


def _assert_schema_equal(a, b):
    # This is used instead of ``assert dict1 == dict2`` so that NaNs are treated as equal.
    assert set(a.keys()) == set(b.keys())

    for key, value1 in a.items():
        value2 = b[key]
        if (
            isinstance(value1, np.floating)
            and isinstance(value2, np.floating)
            and np.isnan(value1)
            and np.isnan(value2)
        ):
            continue

        if isinstance(value1, dict) and isinstance(value2, dict):
            _assert_schema_equal(value1, value2)
        else:
            assert value1 == value2


def has_correct_schema(expected_schema):
    def _check_results(actual):
        assert len(actual) == 1
        schema = actual[0]
        _assert_schema_equal(schema, expected_schema)

    return _check_results


def test_CombineXarraySchemas_aggregation(schema_pcoll_concat):
    concat_dim = "time"
    cxs = CombineXarraySchemas(Dimension(name=concat_dim, operation=CombineOp.CONCAT))

    def idx_at_position(position: int) -> Index:
        return Index({Dimension(concat_dim, CombineOp.CONCAT): Position(value=position)})

    # Test create initial accumulator
    assert cxs.create_accumulator() == (None, concat_dim)

    # Test per-element combine
    ds = make_ds(nt=3)
    s = dataset_to_schema(ds)

    combined_s_0 = cxs.add_input(cxs.create_accumulator(), (idx_at_position(0), s))
    s1 = s.copy()
    s1["chunks"] = {"time": {0: 3}}
    assert combined_s_0[0] == s1

    combined_s_1 = cxs.add_input(combined_s_0, (idx_at_position(1), s))
    s2 = dataset_to_schema(make_ds(nt=6))
    s2["chunks"] = {"time": {0: 3, 1: 3}}
    assert combined_s_1[0] == s2

    # test merger
    to_merge = cxs.add_input(cxs.create_accumulator(), (idx_at_position(2), s))
    merged = cxs.merge_accumulators([combined_s_1, to_merge])
    s3 = dataset_to_schema(make_ds(nt=9))
    s3["chunks"] = {"time": {0: 3, 1: 3, 2: 3}}
    assert merged[0] == s3

    # modify attrs to see that fields get dropped correctly
    ds2 = make_ds(nt=4)
    ds2.attrs["conventions"] = "wack conventions"
    ds2.bar.attrs["long_name"] = "nonsense name"
    dropped_fields = cxs.add_input(merged, (idx_at_position(3), dataset_to_schema(ds2)))
    ds_expected = make_ds(nt=13)
    del ds_expected.attrs["conventions"]
    del ds_expected.bar.attrs["long_name"]
    s4 = dataset_to_schema(ds_expected)
    s4["chunks"] = {"time": {0: 3, 1: 3, 2: 3, 3: 4}}
    assert dropped_fields[0] == s4

    # make sure we can add in different order
    first_add = cxs.add_input(
        dropped_fields, (idx_at_position(5), dataset_to_schema(make_ds(nt=1)))
    )
    second_add = cxs.add_input(first_add, (idx_at_position(4), dataset_to_schema(make_ds(nt=2))))
    time_chunks = {0: 3, 1: 3, 2: 3, 3: 4, 4: 2, 5: 1}
    assert second_add[0]["chunks"]["time"] == time_chunks

    # now start checking errors
    ds3 = make_ds(nt=1).isel(lon=slice(1, None))
    with pytest.raises(ValueError, match="different sizes"):
        cxs.add_input(second_add, (idx_at_position(6), dataset_to_schema(ds3)))

    with pytest.raises(ValueError, match="overlapping keys"):
        cxs.add_input(second_add, (idx_at_position(5), s))

    # now pretend we are concatenating along a new dimension
    concat_dim = "lon"
    cxs = CombineXarraySchemas(Dimension(name=concat_dim, operation=CombineOp.CONCAT))
    s_time_concat = second_add[0]
    first_lon = cxs.add_input(cxs.create_accumulator(), (idx_at_position(0), s_time_concat))
    second_lon = cxs.add_input(first_lon, (idx_at_position(1), s_time_concat))
    assert second_lon[0]["chunks"]["time"] == time_chunks
    assert second_lon[0]["chunks"]["lon"] == {0: 36, 1: 36}
    assert second_lon[0]["dims"] == {"time": 16, "lat": 18, "lon": 72}

    # check error if we try to concat something with incompatible chunks
    bad_schema = copy.deepcopy(s_time_concat)
    bad_schema["chunks"]["time"] = {0: 3, 1: 3, 2: 3, 3: 4, 4: 1, 5: 2}
    with pytest.raises(ValueError, match="Non concat_dim chunks must be the same"):
        cxs.add_input(second_lon, (idx_at_position(2), bad_schema))

    # test merge combination behavior
    modified_schema = copy.deepcopy(s_time_concat)
    modified_schema["data_vars"] = {k.upper(): v for k, v in s_time_concat["data_vars"].items()}

    cxs = CombineXarraySchemas(Dimension(name=concat_dim, operation=CombineOp.MERGE))
    first_result = cxs.add_input(cxs.create_accumulator(), (idx_at_position(0), s_time_concat))
    second_result = cxs.add_input(first_result, (idx_at_position(0), modified_schema))
    assert second_result[0]["data_vars"]["foo"] == second_result[0]["data_vars"]["FOO"]
    assert second_result[0]["data_vars"]["bar"] == second_result[0]["data_vars"]["BAR"]


def test_CombineXarraySchemas_concat_1D(schema_pcoll_concat, pipeline):
    pattern, expected_schema, pcoll = schema_pcoll_concat
    concat_dim = _get_concat_dim(pattern)

    with pipeline as p:
        input = p | pcoll
        output = input | beam.CombineGlobally(
            CombineXarraySchemas(Dimension(name=concat_dim, operation=CombineOp.CONCAT))
        )
        assert_that(output, has_correct_schema(expected_schema))


# TODO: maybe remove this test. It testing an implementation detail
def test_NestDim(schema_pcoll_concat_merge, pipeline):
    pattern, _, pcoll = schema_pcoll_concat_merge
    pattern_merge_only = FilePattern(
        pattern.format_function,
        *[cdim for cdim in pattern.combine_dims if cdim.operation == CombineOp.MERGE],
    )
    merge_only_indexes = list(pattern_merge_only)
    pattern_concat_only = FilePattern(
        pattern.format_function,
        *[cdim for cdim in pattern.combine_dims if cdim.operation == CombineOp.CONCAT],
    )
    concat_only_indexes = list(pattern_concat_only)

    def check_key(expected_outer_indexes, expected_inner_indexes):
        def _check(actual):
            outer_indexes = [item[0] for item in actual]
            assert outer_indexes == expected_outer_indexes
            for outer_index, group in actual:
                assert isinstance(outer_index, Index)
                inner_indexes = [item[0] for item in group]
                assert inner_indexes == expected_inner_indexes
                for idx in inner_indexes:
                    assert isinstance(idx, Index)

        return _check

    with pipeline as p:
        input = p | pcoll
        group1 = (
            input
            | "Nest CONCAT" >> beam.Map(_nest_dim, dimension=Dimension("time", CombineOp.CONCAT))
            | "Groupby CONCAT" >> beam.GroupByKey()
        )
        group2 = (
            input
            | "Nest MERGE" >> beam.Map(_nest_dim, dimension=Dimension("variable", CombineOp.MERGE))
            | "Groupy MERGE" >> beam.GroupByKey()
        )
        assert_that(group1, check_key(merge_only_indexes, concat_only_indexes), label="merge")
        assert_that(group2, check_key(concat_only_indexes, merge_only_indexes), label="concat")


def test_DetermineSchema_concat_1D(dsets_pcoll_concat, pipeline):
    pattern, expected_schema, pcoll = dsets_pcoll_concat
    concat_dim = _get_concat_dim(pattern)

    with pipeline as p:
        input = p | pcoll
        output = input | DetermineSchema([Dimension(name=concat_dim, operation=CombineOp.CONCAT)])
        assert_that(output, has_correct_schema(expected_schema), label="correct schema")


_dimensions = [
    Dimension("time", operation=CombineOp.CONCAT),
    Dimension("variable", operation=CombineOp.MERGE),
]


@pytest.mark.parametrize(
    "dimensions", [_dimensions, _dimensions[::-1]], ids=["concat_first", "merge_first"]
)
def test_DetermineSchema_concat_merge(dimensions, dsets_pcoll_concat_merge, pipeline):
    _, expected_schema, pcoll = dsets_pcoll_concat_merge

    with pipeline as p:
        input = p | pcoll
        output = input | DetermineSchema(dimensions)
        assert_that(output, has_correct_schema(expected_schema))


def _is_expected_dataset(expected_ds):
    def _impl(actual):
        actual_ds = xr.open_dataset(actual[0], engine="zarr")

        assert expected_ds == actual_ds

    return _impl


@pytest.fixture
def combine_references_fixture():
    return CombineReferences(
        concat_dims=["time"],
        identical_dims=["x", "y"],
    )


@pytest.mark.parametrize(
    "indexed_reference, global_position_min_max_count, expected",
    [
        # assume contiguous data but show examples offsets
        # across the array and assume default max_refs_per_merge==5
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(0)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (0, {"url": "s3://blah.hdf5"}),
        ),
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(4)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (0, {"url": "s3://blah.hdf5"}),
        ),
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(5)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (1, {"url": "s3://blah.hdf5"}),
        ),
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(10)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (2, {"url": "s3://blah.hdf5"}),
        ),
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(25)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (5, {"url": "s3://blah.hdf5"}),
        ),
        (
            (Index({Dimension("time", CombineOp.CONCAT): Position(50)}), {"url": "s3://blah.hdf5"}),
            (0, 100, 101),
            (10, {"url": "s3://blah.hdf5"}),
        ),
        (
            (
                Index({Dimension("time", CombineOp.CONCAT): Position(100)}),
                {"url": "s3://blah.hdf5"},
            ),
            (0, 100, 101),
            (21, {"url": "s3://blah.hdf5"}),
        ),
        (
            (
                Index({Dimension("time", CombineOp.CONCAT): Position(80)}),
                {"url": "s3://blah.hdf5"},
            ),
            (0, 80, 101),
            False,
        ),
    ],
)
def test_bucket_by_position_contiguous_offsets(
    combine_references_fixture, indexed_reference, global_position_min_max_count, expected, caplog
):
    with caplog.at_level(logging.WARNING):
        result = combine_references_fixture.bucket_by_position(
            indexed_reference, global_position_min_max_count
        )

    if not expected:
        assert "The distribution of indexes is not contiguous/uniform" in caplog.text
    else:
        assert result == expected
