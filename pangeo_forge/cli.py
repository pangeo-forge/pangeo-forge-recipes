import runpy

import click


@click.group()
@click.version_option()
def main():
    pass


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
def lint(pipeline):
    """
    Check that the pipeline definition is valid. This does not run the pipeline.
    """
    result = runpy.run_path(pipeline)
    missing = [key for key in ["pipeline", "flow"] if key not in result]
    if missing:
        click.echo(f"missing {missing}")
    pipe = result["pipeline"]

    pipe.flow.sorted_tasks()
    pipe.flow.environment
    pipe.flow.storage
    pipe.flow.validate()
    print("ok!")


@click.command()
@click.argument("pipeline", type=click.Path(exists=True))
def register(pipeline):
    env = runpy.run_path(pipeline)
    pipe = env["pipeline"]
    flow = env["flow"]

    flow.environment = pipe.environment
    flow.storage = pipe.storage
    flow.register(project_name="pangeo-forge", labels=[])


main.add_command(lint)
main.add_command(register)


if __name__ == "__main__":
    main()
