import apache_beam as beam
import fsspec
import pytest
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from zarr.storage import FSStore

from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    ConsolidateMetadata,
    OpenWithXarray,
    StoreToZarr,
    WriteReference,
)


@pytest.fixture
def pipeline():
    # Runtime type checking doesn't play well with our Combiner
    # https://github.com/apache/beam/blob/3cddfaf58c69acc624dac16df10357a78ececf59/sdks/python/apache_beam/transforms/core.py#L2505-L2509
    options = PipelineOptions(runtime_type_check=False)
    with TestPipeline(options=options) as p:
        yield p


def test_StoreToZarr_emits_openable_fsstore(
    pipeline,
    netcdf_local_file_pattern_sequential,
    tmp_target_url,
):
    def is_dataset():
        def _impl(actual):
            assert len(actual) == 1
            item = actual[0]
            assert isinstance(item, FSStore)

        return _impl

    pattern: FilePattern = netcdf_local_file_pattern_sequential
    with pipeline as p:
        store = (
            p
            | beam.Create(pattern.items())
            | OpenWithXarray()
            | StoreToZarr(
                target_root=tmp_target_url,
                store_name="test.zarr",
                combine_dims=pattern.combine_dim_keys,
            )
            | ConsolidateMetadata()
        )
        assert_that(store, is_dataset())


def test_CombineReferences(
    pipeline,
    netcdf_public_http_paths_sequential_1d,
    tmp_target_url,
):
    urls = netcdf_public_http_paths_sequential_1d[0]

    def is_expected_mzz(expected_mzz):
        def _impl(actual):
            result = actual[0].fs.references
            expected = expected_mzz["refs"]
            assert result.get(".zmetadata") is not None
            expected[".zmetadata"] = result[".zmetadata"]
            assert expected == result

        return _impl

    def generate_refs(urls):
        for url in urls:
            with fsspec.open(url) as inf:
                h5chunks = SingleHdf5ToZarr(inf, url, inline_threshold=100)
                yield [h5chunks.translate()]

    refs = [ref[0] for ref in generate_refs(urls)]

    concat_dims = ["time"]
    identical_dims = ["lat", "lon"]
    expected_mzz = MultiZarrToZarr(refs, concat_dims=concat_dims, identical_dims=identical_dims)

    with pipeline as p:
        output = (
            p
            | beam.Create(generate_refs(urls))
            | CombineReferences(concat_dims=concat_dims, identical_dims=identical_dims)
            | ConsolidateMetadata()
            | WriteReference(
                target_root=tmp_target_url,
                store_name="test",
                concat_dims=concat_dims,
            )
        )

        assert_that(output, is_expected_mzz(expected_mzz.translate()))
