import pathlib
import runpy
import sys

import click


@click.group()
@click.version_option()
def main():
    """
    CLI for working with pangeo-forge.

    Once a pipeline recipe is written, the typical workflow is

    * pangeo-forge check  # validate the pipeline
    * pangeo-forge generate  # generate the run.py file for Prefect
    * pangeo-forge register  # register with Prefect

    At that point, the prefect flow run can be manually triggered.
    """
    pass


@click.command()
@click.argument("recipe", type=click.Path(exists=True), default="recipe")
@click.option("--verbose/--no-verbose", help="Whether to print verbose output")
def check(recipe, verbose):
    """
    Check that the pipeline definition is valid. This does not run the
    pipeline.

    recipe : path to the recipe directory (e.g. 'recipe')
    """
    # Validate the package structure
    p = pathlib.Path(recipe)
    errors = []

    if not p.exists():
        errors.append("Cannot find a directory named recipe")

    if not (p / "__init__.py").exists():
        errors.append("File 'recipe/__init__.py' does not exist")

    if not (p / "pipeline.py").exists():
        errors.append("File 'recipe/pipeline.py' does not exist")

    # result returns the namespace of the module as a dict of {name: value}.
    pipeline = str(p / "pipeline.py")
    result = runpy.run_path(pipeline)

    if "Pipeline" not in result:
        errors.append("File 'recipe/pipeline.py' must have a class named 'Pipeline'")

    pipe = result["Pipeline"]()  # TODO: parameters
    pipe.flow.validate()

    if verbose:
        if not errors:
            print(f"The recipe '{recipe}' looks great!")

    for error in errors:
        click.echo(error, err=True)

    sys.exit(int(bool(errors)))


@click.command()
@click.argument("pipeline", type=click.Path(exists=True), default="recipe/pipeline.py")
@click.argument("run-file", type=click.Path(), default="recipe/run.py")
def generate(pipeline, run_file):
    """Generate a run file for Prefect.

    pipeline : Path to the pipeline definition (e.g. recipe/pipeline.py)
    run-file : Path to the file for Prefect to run (e.g. recipe/run.py)
    """
    result = runpy.run_path(pipeline)
    template = result["Pipeline"]()._generate_run(pipeline)
    with open(run_file, "w", encoding="utf-8") as f:
        f.write(template)


@click.command()
@click.argument("run-file", type=click.Path(exists=True), default="recipe/run.py")
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
