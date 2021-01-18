import pytest
import xarray as xr
from rechunker.executors import (
    DaskPipelineExecutor,
    PrefectPipelineExecutor,
    PythonPipelineExecutor,
)


@pytest.mark.parametrize(
    "Executor", [PythonPipelineExecutor, DaskPipelineExecutor, PrefectPipelineExecutor]
)
def test_recipe_w_executor(Executor, netCDFtoZarr_sequential_recipe):
    rec, ds_expected, target = netCDFtoZarr_sequential_recipe
    pipeline = rec.to_pipelines()
    ex = Executor()
    plan = ex.pipelines_to_plan(pipeline)
    ex.execute_plan(plan)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    assert ds_actual.identical(ds_expected)
