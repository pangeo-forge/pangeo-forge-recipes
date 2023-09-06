import pytest

# Run only when the `--run-integration` option is passed.
# See also `pytest_addoption` in conftest. Reference:
# https://jwodder.github.io/kbits/posts/pytest-mark-off/
pytestmark = pytest.mark.skipif(
    "not config.getoption('--run-integration')",
    reason="Only run when --run-integration is given",
)


def test_integration():
    pass
