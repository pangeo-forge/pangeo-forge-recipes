import typer

app = typer.Typer(help="Generation of recipes and validation of YAML configs.")


@app.command()
def validate():
    ...


@app.command()
def create():
    ...


if __name__ == "__main__":
    app()
