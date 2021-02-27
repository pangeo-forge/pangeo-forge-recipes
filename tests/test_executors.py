import pytest
import xarray as xr

from pangeo_forge.executors import (
    DaskPipelineExecutor,
    PrefectPipelineExecutor,
    PythonPipelineExecutor,
)


@pytest.mark.parametrize(
    "Executor", [PythonPipelineExecutor, DaskPipelineExecutor, PrefectPipelineExecutor]
)
def test_recipe_w_executor(Executor, netCDFtoZarr_sequential_recipe):
    Recipe, kwargs, ds_expected, target = netCDFtoZarr_sequential_recipe
    rec = Recipe(**kwargs)
    pipeline = rec.to_pipelines()
    ex = Executor()
    plan = ex.pipelines_to_plan(pipeline)
    ex.execute_plan(plan)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    assert ds_actual.identical(ds_expected)
