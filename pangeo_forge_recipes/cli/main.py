import pathlib

import rich_click as click
import yaml

from .init import initialize_recipe
from .run import run_recipe

click.rich_click.SHOW_ARGUMENTS = True


@click.group()
def main():
    pass


@main.command(help="create a new recipe in PATH")
@click.argument(
    "path", type=click.Path(path_type=pathlib.Path, exists=False, writable=True, file_okay=False)
)
def init(path):
    initialize_recipe(path)


@main.command(help="run the recipe in PATH")
@click.option(
    "-f",
    "--function-name",
    "func_name",
    default="recipe",
    type=str,
    help="The name of the recipe function.",
)
@click.option(
    "--runtime-config",
    type=click.File("r"),
    required=True,
    help="runtime configuration. Must be in yaml format.",
)
@click.argument(
    "path",
    type=click.Path(
        path_type=pathlib.Path,
        exists=True,
        readable=True,
        writable=False,
        file_okay=False,
        dir_okay=True,
    ),
)
def run(path, func_name, runtime_config):
    config = yaml.safe_load(runtime_config)

    run_recipe(path, func_name, config)
