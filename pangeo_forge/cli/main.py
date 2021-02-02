import typer

from .bakery import app as bakery_app
from .forge import app as forge_app
from .smithy import app as smithy_app

app = typer.Typer(help="CLI for working with pangeo-forge.")
app.add_typer(bakery_app, name="bakery")
app.add_typer(forge_app, name="forge")
app.add_typer(smithy_app, name="smithy")


def version_callback(value: bool):
    from pkg_resources import get_distribution

    __version__ = get_distribution("pangeo_forge").version
    if value:
        typer.echo(f"Pangeo-forge CLI Version: {__version__}")
        raise typer.Exit()


@app.callback()
def cli(
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Display pangeo-forge version.",
    ),
):
    # Do other global stuff, handle other global options here
    return


def main():
    typer.run(app())
