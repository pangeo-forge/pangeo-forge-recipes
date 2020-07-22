from click.testing import CliRunner

from pangeo_forge import __version__
from pangeo_forge.cli import main as cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert cli.__doc__.strip() in result.output
