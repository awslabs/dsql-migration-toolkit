# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Aurora DSQL type limits that several modules enforce, each defined once.

Constants only, with no imports, so any module can read them without an import cycle.
Every figure is from the service documentation ("Supported data types in Aurora DSQL") and
was re-verified live on a real cluster. A hardcoded quota goes stale without anything
failing: the numeric limit was once 38 in three modules, and when the service raised it to
1000 none of them followed. Re-check a figure against the documentation before relying on
it. The modules that enforce these keep their own private names as aliases.

The 1 MiB per-value ceiling is not here: it has one definition,
``prerequisites.DSQL_MAX_VALUE_BYTES``, beside the per-type table it anchors.
"""

# Numeric: "The maximum precision is 1000 and scale can be between -1000 and 1000"
# (default numeric(18,6), max storage 510 bytes). Re-confirmed live 2026-09-25:
# numeric(1000,1000) is accepted, numeric(1001,0) is rejected with "NUMERIC precision 1001
# must be between 1 and 1000", and a 500-integer-digit + 500-decimal-digit value
# round-tripped EXACT. These were 38/37, the limit DSQL enforced when the code was written
# (its "precision 39 must be between 1 and 38" error); the service raised it and the
# constants did not, so every numeric(p>38) column was silently NARROWED, losing digits
# DSQL would have stored, while the warning asserted a maximum the service no longer had.
DSQL_NUMERIC_MAX_PRECISION = 1000
DSQL_NUMERIC_MAX_SCALE = 1000

# The DECLARED-LENGTH ceilings of the character types, live-verified:
#   CREATE ... VARCHAR(65536) -> ProgramLimitExceeded
#       "Datatype limit greater than 65535 bytes not supported for varchar"
#   CREATE ... CHAR(4097)     -> "Datatype limit greater than 4096 bytes not supported for char"
# VARCHAR(65535) / CHAR(4096) are accepted. The same figures cap a single VALUE of each
# type (see prerequisites._DSQL_VALUE_LIMIT_BY_BASE_TYPE).
DSQL_MAX_VARCHAR_LENGTH = 65535
DSQL_MAX_CHAR_LENGTH = 4096

__all__ = [
    "DSQL_MAX_CHAR_LENGTH",
    "DSQL_MAX_VARCHAR_LENGTH",
    "DSQL_NUMERIC_MAX_PRECISION",
    "DSQL_NUMERIC_MAX_SCALE",
]
