"""
Test Pipline Executors
"""

import pytest

# from pangeo_forge_recipes.executors.dask import DaskPipelineExecutor
from pangeo_forge_recipes.executors.prefect import PrefectPipelineExecutor
from pangeo_forge_recipes.executors.function import FunctionPipelineExecutor
from pangeo_forge_recipes.executors.base import Stage, Pipeline


@pytest.fixture
def example_pipeline_no_config(tmpdir_factory):

    tmp = tmpdir_factory.mktemp("pipeline_data")

    def func0(config=None):
        tmp.join("func0.log").ensure(file=True)
        assert not tmp.join("func1_a.log").check(file=True)

    stage0 = Stage(function=func0, name="create_first_file")

    def func1(arg, config=None):
        tmp.join(f"func1_{arg}.log").ensure(file=True)

    stage1 = Stage(function=func1, name="create_many_files", mappable=["a", "b", 3])

    # MultiStagePipeline
    pipeline = Pipeline(stages=[stage0, stage1])
    return pipeline, tmp


@pytest.mark.parametrize(
    "Executor", [FunctionPipelineExecutor, PrefectPipelineExecutor]
)
def test_pipeline(example_pipeline_no_config, Executor):
    pipeline, tmpdir = example_pipeline_no_config
    plan = Executor.compile(pipeline)
    Executor.execute(plan)
    for fname in ["func0.log", "func1_a.log", "func1_b.log", "func1_3.log"]:
        assert tmpdir.join(fname).check(file=True)
