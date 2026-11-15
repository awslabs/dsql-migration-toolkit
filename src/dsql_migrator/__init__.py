# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""dsql_migrator: the Aurora DSQL Migration Toolkit (RDS/Aurora MySQL and PostgreSQL to
Amazon Aurora DSQL).

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

# The installed distribution's names, newest first: it was mysql-dsql-migrator before
# v0.1.590, and a virtualenv installed before the rename and not synced since has only
# that name.
_DISTRIBUTION_NAMES = ("dsql-migration-toolkit", "mysql-dsql-migrator")


def _installed_version() -> str:
    """The installed distribution's version, or ``0.0.0+dev`` for a source tree that
    has no install."""
    for name in _DISTRIBUTION_NAMES:
        try:
            return _pkg_version(name)
        except PackageNotFoundError:
            continue
    return "0.0.0+dev"


# Single source of truth: the installed distribution's version (driven by
# pyproject.toml). The container image installs the package fresh on every
# build, so the UI's displayed version always matches the released image.
__version__ = _installed_version()

__all__ = ["__version__"]
