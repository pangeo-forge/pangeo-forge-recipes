import runpy
import sys

import click
from prefect.environments.execution.base import Environment
from prefect.environments.storage.base import Storage


@click.group()
@click.version_option()
def main():
    pass


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
def check(pipeline):
    """
    Check that the pipeline definition is valid. This does not run the
    pipeline.

    pipeline : path to the pipeline module (e.g. recipe/pipeline.py)
    """
    # result returns the namespace of the module as a dict of {name: value}.
    return_code = 0
    result = runpy.run_path(pipeline)
    # The toplevel of the recipe must have two instances
    # 1. pipeline: required by pangeo-forge for metadata.
    # 2. flow: required by Prefect for flow execution.
    missing = [key for key in ["pipeline", "flow"] if key not in result]

    if missing:
        click.echo(f"missing {missing}", err=True)
        return_code = 1
    pipe = result["pipeline"]

    if not isinstance(pipe.flow.environment, Environment):
        click.echo(f"Incorrect flow.environment {type(pipe.flow.environment)}", err=True)
        return_code = 1
    if not isinstance(pipe.flow.storage, Storage):
        click.echo(f"Incorrect flow.storage {type(pipe.flow.storage)}", err=True)
        return_code = 1
    pipe.flow.validate()
    sys.exit(return_code)


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
@click.argument("run-file", type=click.Path(), default="run.py")
def generate(pipeline, run_file):
    """Generate a run file."""
    result = runpy.run_path(pipeline)
    template = result["Pipeline"]()._generate_run()
    with open(run_file, "w", encoding="utf-8") as f:
        f.write(template)


@click.command()
@click.argument("run-file", type=click.Path(exists=True), default="run.py")
def register(run_file):
    """
    Register a pipeline with prefect.

    pipeline : path to the run-file module (e.g. "run.py")
    """
    env = runpy.run_path(run_file)
    flow = env["flow"]
    flow.register(project_name="pangeo-forge", labels=["gcp"])


main.add_command(check)
main.add_command(generate)
main.add_command(register)


if __name__ == "__main__":
    main()
