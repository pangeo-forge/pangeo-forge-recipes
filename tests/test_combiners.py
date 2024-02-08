import apache_beam as beam
import fsspec
import pytest
import xarray as xr
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.aggregation import dataset_to_schema
from pangeo_forge_recipes.combiners import CombineXarraySchemas, CombineZarrRefs
from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.transforms import DatasetToSchema, DetermineSchema, _NestDim
from pangeo_forge_recipes.types import CombineOp, Dimension, Index


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


def has_correct_schema(expected_schema):
    def _check_results(actual):
        assert len(actual) == 1
        schema = actual[0]
        assert schema == expected_schema

    return _check_results


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
        *[cdim for cdim in pattern.combine_dims if cdim.operation == CombineOp.MERGE]
    )
    merge_only_indexes = list(pattern_merge_only)
    pattern_concat_only = FilePattern(
        pattern.format_function,
        *[cdim for cdim in pattern.combine_dims if cdim.operation == CombineOp.CONCAT]
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
            | "Nest CONCAT" >> _NestDim(Dimension("time", CombineOp.CONCAT))
            | "Groupby CONCAT" >> beam.GroupByKey()
        )
        group2 = (
            input
            | "Nest MERGE" >> _NestDim(Dimension("variable", CombineOp.MERGE))
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
    pattern, expected_schema, pcoll = dsets_pcoll_concat_merge

    with pipeline as p:
        input = p | pcoll
        output = input | DetermineSchema(dimensions)
        assert_that(output, has_correct_schema(expected_schema))


def _is_expected_dataset(expected_ds):
    def _impl(actual):
        actual_ds = xr.open_dataset(actual[0], engine="zarr")

        assert expected_ds == actual_ds

    return _impl


def test_CombineReferences(netcdf_local_paths_sequential_1d, pipeline):
    urls = netcdf_local_paths_sequential_1d[0]

    def generate_refs(urls) -> list[dict]:
        for url in urls:
            with fsspec.open(url) as inf:
                h5chunks = SingleHdf5ToZarr(inf, url, inline_threshold=100)
                yield h5chunks.translate()

    refs = generate_refs(urls)
    concat_dims = ["time"]
    identical_dims = ["lat", "lon"]
    mzz = MultiZarrToZarr(
        refs, concat_dims=concat_dims, identical_dims=identical_dims, remote_protocol="file"
    ).translate()

    mapper = fsspec.filesystem("reference", fo=mzz).get_mapper()

    expected_dataset = xr.open_dataset(mapper, engine="kerchunk")
    with pipeline as p:
        input = p | beam.Create(generate_refs(urls))
        output = input | beam.CombineGlobally(
            CombineZarrRefs(concat_dims=concat_dims, identical_dims=identical_dims)
        )

        assert_that(output, _is_expected_dataset(expected_dataset))
