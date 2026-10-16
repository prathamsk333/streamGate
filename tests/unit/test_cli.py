from click.testing import CliRunner

from streamgate import __version__
from streamgate.cli import main


def test_version_command() -> None:
    result = CliRunner().invoke(main, ["version"])
    assert result.exit_code == 0
    assert result.output.startswith(f"streamgate {__version__} (confluent-kafka ")


def test_version_option() -> None:
    result = CliRunner().invoke(main, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.output
