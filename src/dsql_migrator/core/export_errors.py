# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The Full Load export's error types that the code below the exporter raises.

:class:`ExportError` and :class:`ValueConversionError` are raised by the MySQL value
conversion (:mod:`dsql_migrator.core.exporter_mysql`) and by the MySQL source dialect's
keyset walk, as well as by the exporter itself. The exporter imports the source dialects,
so these live in a leaf module that each of them imports at its top level;
:mod:`dsql_migrator.core.exporter` re-exports both.
"""

from __future__ import annotations


class ExportError(RuntimeError):
    """Base error for failures while exporting a source table."""


class ValueConversionError(ExportError):
    """Raised when a source cell cannot be written without silently changing it.

    Surfaces a value that would otherwise be silently corrupted -- e.g. a
    ``TINYINT(1)`` column (mapped to DSQL ``boolean``) holding a value outside
    ``{0, 1}`` (MySQL's ``(1)`` is display width, not a value constraint, so a
    ``TINYINT(1)`` legally stores -128..127 / 0..255), or a decimal with more places
    than its ``numeric(p,s)`` column keeps (see
    :class:`~dsql_migrator.core.exporter.ValueFitGuard`).
    ``bool(int(value))`` would flatten any non-zero magnitude to ``True`` and lose the
    original number, so we fail loudly instead of corrupting the row -- naming the
    column and the rule, never the value (the message reaches logs and the UI).
    """


__all__ = ["ExportError", "ValueConversionError"]
