# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Command-line entrypoint for the DSQL migration tool.

Provides a minimal, extensible CLI. At this scaffolding stage it supports
inspecting the loaded (non-secret) configuration and launching the NiceGUI UI.
Engine subcommands (introspect, assess, convert, migrate, validate) are added in
later tasks.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional, Sequence

from dsql_migrator import __version__
from dsql_migrator.config import load_config


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="mysql-dsql-migrator",
        description="RDS/Aurora MySQL or PostgreSQL to Amazon Aurora DSQL migration toolkit.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")

    config_parser = subparsers.add_parser(
        "config",
        help="Show the loaded (non-secret) configuration.",
    )
    config_parser.set_defaults(func=_cmd_config)

    ui_parser = subparsers.add_parser(
        "ui",
        help="Launch the NiceGUI web UI.",
    )
    ui_parser.set_defaults(func=_cmd_ui)

    return parser


def _cmd_config(_args: argparse.Namespace) -> int:
    """Print non-secret configuration values."""
    config = load_config()
    # model_dump() contains only non-secret settings by construction.
    for key, value in config.model_dump().items():
        print(f"{key}={value}")
    return 0


def _cmd_ui(_args: argparse.Namespace) -> int:
    """Launch the NiceGUI web UI."""
    from dsql_migrator.ui.app import main as run_ui

    run_ui()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entrypoint. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not getattr(args, "command", None):
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
