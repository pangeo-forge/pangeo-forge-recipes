import dask.array as dsa
import xarray as xr
import pytest
from pangeo_forge_recipes.dynamic_target_chunks import dynamic_target_chunks_from_schema
from pangeo_forge_recipes.aggregation import dataset_to_schema


class TestDynamicTargetChunks:
    @pytest.fixture
    def ds(self) -> int:
        return xr.DataArray(dsa.random.random([100, 300, 400]), dims=['x', 'y','z']).to_dataset(name='data')

    
    @pytest.mark.parametrize(
            ('target_chunk_ratio', 'expected_target_chunks'), 
            [
                # make sure that for the same dataset we get smaller chunksize along a dimension if the ratio is larger
                ({'x':1, 'y':1, 'z':10}, {'x':100, 'y':150, 'z':8}),
                ({'x':10, 'y':1, 'z':1}, {'x':10, 'y':150, 'z':80}),
                # test the special case where we want to just chunk along a single dimension
                ({'x':-1, 'y':-1, 'z':1}, {'x':100, 'y':300, 'z':4})
            ]
            )
    def test_dynamic_rechunking1(self, ds, target_chunk_ratio, expected_target_chunks):
        schema = dataset_to_schema(ds)
        target_chunks = dynamic_target_chunks_from_schema(schema, 1e6, target_chunk_ratio=target_chunk_ratio)
        print(target_chunks)
        for dim, chunks in expected_target_chunks.items():
            assert target_chunks[dim] == chunks

    @pytest.mark.parametrize('target_chunk_ratio', [{'x':1, 'y':-1, 'z':10}, {'x':6, 'y':-1, 'z':2}]) # always keep y unchunked, and vary the others
    @pytest.mark.parametrize('target_chunk_nbytes', [1e6, 1e7])
    def test_dynamic_skip_dimension(self, ds, target_chunk_ratio, target_chunk_nbytes):
        # Mark dimension as 'not-to-chunk' with -1
        schema = dataset_to_schema(ds)
        target_chunks = dynamic_target_chunks_from_schema(schema, target_chunk_nbytes, target_chunk_ratio=target_chunk_ratio)
        assert target_chunks['y'] == len(ds['y'])

    def test_dynamic_rechunking_error_dimension_missing(self, ds):
    # make sure that an error is raised if some dimension is not specified
        schema = dataset_to_schema(ds)

        with pytest.raises(ValueError, match='target_chunk_ratio must contain all dimensions in dataset.'):
            dynamic_target_chunks_from_schema(schema, 1e6, target_chunk_ratio={'x':1, 'z':10})

    def test_dynamic_rechunking_error_dimension_wrong(self, ds):
        schema = dataset_to_schema(ds)
        with pytest.raises(ValueError, match='target_chunk_ratio must contain all dimensions in dataset.'):
            dynamic_target_chunks_from_schema(schema, 1e6, target_chunk_ratio={'x':1, 'y_wrong':1, 'z':10})
