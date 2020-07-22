import click


@click.command()
@click.version_option()
def main():
    """Pangeo-Forge: A tool for building and publishing analysis ready datasets.
    """
    click.secho("Hello World!", fg="green")


if __name__ == "__main__":
    main()
