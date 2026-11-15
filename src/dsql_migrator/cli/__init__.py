# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Command-line interface layer.

Exposes a single console entrypoint (``dsql-migration-toolkit``; ``mysql-dsql-migrator``,
its name before v0.1.590, still runs it) that wraps the core engine and can launch the
NiceGUI UI. Commands are extended in later tasks.
"""

__all__: list[str] = []
