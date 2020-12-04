import pytest
import xarray as xr

from pangeo_forge.executors import PrefectExecutor, PythonExecutor


@pytest.mark.parametrize("Executor", [PythonExecutor, PrefectExecutor])
def test_recipe_w_executor(Executor, sequential_recipe):
    rec, ds_expected, target = sequential_recipe
    ex = Executor()
    plan = ex.prepare_plan(rec)
    ex.execute_plan(plan)
    ds_actual = xr.open_zarr(target.get_mapper()).load()
    assert ds_actual.identical(ds_expected)
