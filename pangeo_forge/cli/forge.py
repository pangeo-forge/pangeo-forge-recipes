import pathlib
import runpy
import subprocess

import prefect
import typer
from rich.table import Table

from .console import console

app = typer.Typer(help="Running and validating recipes locally.")


@app.command(help="Check that the pipeline definition is valid.")
def check(
    recipe: pathlib.Path = typer.Argument(
        "recipe",
        show_default=True,
        exists=True,
        dir_okay=True,
        help="path to the recipe directory (e.g. 'recipe')",
    )
):
    """
    Check that the pipeline definition is valid. This does not run the pipeline.
    """

    errors = []
    pipeline = recipe / "pipeline.py"
    if not pipeline.exists():
        errors.append(f"File '{recipe}/pipeline.py' does not exist")

    else:
        # result returns the namespace of the module as a dict of {name: value}.
        pipeline = pipeline.as_posix()
        result = runpy.run_path(pipeline)

        if "Pipeline" not in result:
            errors.append(f"File '{recipe}/pipeline.py' must have a class named 'Pipeline'")
        if "flow" not in result:
            errors.append(f"File '{recipe}/pipeline.py' must have a prefect Flow named 'flow'")
        elif not isinstance(result["flow"], prefect.Flow):
            errors.append(f"File '{recipe}/pipeline.py' must have a prefect Flow named 'flow'")
        else:
            flow = result["flow"]
            flow.validate()

    if errors:
        table = Table()
        table.add_column("Errors", style="magenta")
        for error in errors:
            table.add_row(error)

        console.print(table)

    else:
        console.print(f"[green]:heavy_check_mark: [bold cyan] The recipe '{recipe}' looks great!")


@app.command(help="Register a pipeline with prefect.")
def register(
    pipeline: pathlib.Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        help="path to the pipeline module (e.g. recipe/pipeline.py)",
    )
):
    """
    Register a pipeline with prefect
    """

    # TODO: Get from meta.yaml rather than executing code.
    env = runpy.run_path(pipeline)
    flow = env["flow"]
    flow.register(project_name="pangeo-forge", labels=["gcp"])


@app.command(help="Run a pipeline with prefect.")
def run(
    pipeline: pathlib.Path = typer.Argument(
        "recipe/pipeline.py",
        show_default=True,
        file_okay=True,
        exists=True,
        help="path to the pipeline module (e.g. recipe/pipeline.py)",
    )
):
    env = runpy.run_path(pipeline.as_posix())
    name = env["Pipeline"].name
    subprocess.check_output(["prefect", "run", "flow", "--project", "pangeo-forge", "--name", name])


if __name__ == "__main__":
    app()
