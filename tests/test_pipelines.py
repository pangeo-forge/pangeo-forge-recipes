"""
Test Pipline Executors
"""
import pytest
from pytest_lazyfixture import lazy_fixture

from pangeo_forge_recipes.executors.base import Pipeline, Stage


@pytest.fixture
def pipeline_no_config(tmpdir_factory):
    tmp = tmpdir_factory.mktemp("pipeline_data")

    def func0(config=None):
        tmp.join("func0.log").ensure(file=True)
        assert not tmp.join("func1_a.log").check(file=True)

    stage0 = Stage(function=func0, name="create_first_file")

    def func1(arg, config=None):
        tmp.join(f"func1_{arg}.log").ensure(file=True)

    stage1 = Stage(function=func1, name="create_many_files", mappable=["a", "b", 3])
    pipeline = Pipeline(stages=[stage0, stage1])
    return pipeline, {}, tmp


@pytest.fixture
def pipeline_with_config(tmpdir_factory):
    tmp = tmpdir_factory.mktemp("pipeline_data")

    def func0(config=None):
        prefix = config["prefix"]
        tmp.join(f"{prefix}func0.log").ensure(file=True)
        assert not tmp.join(f"{prefix}-func1_a.log").check(file=True)

    stage0 = Stage(function=func0, name="create_first_file")

    def func1(arg, config=None):
        prefix = config["prefix"]
        tmp.join(f"{prefix}func1_{arg}.log").ensure(file=True)

    stage1 = Stage(function=func1, name="create_many_files", mappable=["a", "b", 3])
    config = {"prefix": "special-"}
    pipeline = Pipeline(stages=[stage0, stage1], config=config)
    return pipeline, config, tmp


@pytest.fixture(
    scope="session",
    params=[
        "DaskPipelineExecutor",
        "FunctionPipelineExecutor",
        "GeneratorPipelineExecutor",
        "PrefectPipelineExecutor",
        "BeamPipelineExecutor",
    ],
)
def Executor(request):
    try:
        import pangeo_forge_recipes.executors as exec_module

        return getattr(exec_module, request.param)
    except AttributeError:
        pytest.skip(f"Couldn't import {request.param}")


@pytest.mark.parametrize(
    "pipeline_config_tmpdir",
    [lazy_fixture("pipeline_no_config"), lazy_fixture("pipeline_with_config")],
)
def test_pipeline(pipeline_config_tmpdir, Executor):
    pipeline, config, tmpdir = pipeline_config_tmpdir
    plan = Executor.compile(pipeline)
    Executor.execute(plan)

    prefix = config.get("prefix", "")
    for fname in [
        f"{prefix}func0.log",
        f"{prefix}func1_a.log",
        f"{prefix}func1_b.log",
        f"{prefix}func1_3.log",
    ]:
        assert tmpdir.join(fname).check(file=True), f"File not found in temp directory: {fname}."
