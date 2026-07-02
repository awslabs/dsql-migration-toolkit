"""dsql_migrator: RDS/Aurora MySQL to Amazon Aurora DSQL migration toolkit.

The package is organized into three clearly separated layers so the core
engine can be used independently of the web UI:

- ``dsql_migrator.core``: the importable migration engine (no UI dependencies).
- ``dsql_migrator.ui``: the NiceGUI web application.
- ``dsql_migrator.cli``: the command-line entrypoint.

Configuration is loaded from environment variables / session state via
``dsql_migrator.config`` and never persists credential values.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    # Single source of truth: the installed distribution's version (driven by
    # pyproject.toml). The container image installs the package fresh on every
    # build, so the UI's displayed version always matches the released image.
    __version__ = _pkg_version("mysql-dsql-migrator")
except PackageNotFoundError:  # pragma: no cover - source tree without an install
    __version__ = "0.0.0+dev"

__all__ = ["__version__"]
