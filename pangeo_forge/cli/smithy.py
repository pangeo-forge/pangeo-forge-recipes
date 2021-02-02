import pathlib

import typer

app = typer.Typer(help="Generation of recipes and validation of YAML configs.")


@app.command(help="Lint a single pangeo-forge recipe.")
def lint():
    ...


@app.command(help="Create a new pangeo-forge recipe.")
def init(
    recipe_directory: pathlib.Path = typer.Argument(
        "./recipe",
        dir_okay=True,
        show_default=True,
        help="The path to the source recipe directory.",
    )
):
    ...


if __name__ == "__main__":
    app()
