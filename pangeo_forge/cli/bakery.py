import typer

app = typer.Typer(help="Managing submissions of recipes to bakeries.")


@app.command()
def check():
    ...


@app.command()
def submit():
    ...


@app.command()
def list():
    ...


if __name__ == "__main__":
    app()
