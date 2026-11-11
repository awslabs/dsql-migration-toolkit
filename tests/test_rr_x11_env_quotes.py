# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""X11: ``config.read_env_file`` strips one matching pair of quotes around a value.

The local-dev ``.env`` in the repo root prefills the Connect screen
(``load_connect_defaults``) and can set ``DSQL_MIGRATOR_STORAGE_SECRET``; the app reads
it with ``read_env_file``. That reader kept quotes, so ``DB_HOST="db.example.com"``
prefilled the host WITH its quotes and ``DB_PORT="3306"`` left the port blank (a quoted
port is not all digits). A shell that sources the file (``set -a; source .env``, as
scripts/README.md says) and every script's own ``.env`` parser remove the quotes.

Now, after the whitespace strip, one matching pair of single or double quotes around
the value is removed. Mismatched and lone quotes, and quotes inside the value, are kept
as written; ``KEY=""`` gives an empty value, which callers treat as unset, as a blank
value. There is no escape processing and no inline-comment stripping, and a line is
read on its own: a quote never continues a value onto the next line.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.config import load_connect_defaults, read_env_file


def _read(tmp_path: Path, text: str) -> dict[str, str]:
    env_file = tmp_path / ".env"
    env_file.write_text(text, encoding="utf-8")
    return read_env_file(str(env_file))


# (line as written in .env, the value read for KEY)
_CASES = {
    # one matching pair, either style, goes
    "double-quoted": ('KEY="value"', "value"),
    "single-quoted": ("KEY='value'", "value"),
    "double-quoted-with-spaces": ('KEY="a b c"', "a b c"),
    "single-quoted-with-spaces": ("KEY='a b c'", "a b c"),
    # mismatched quotes stay
    "mismatched-double-then-single": ("KEY=\"abc'", "\"abc'"),
    "mismatched-single-then-double": ("KEY='abc\"", "'abc\""),
    # a lone quote at either end stays
    "lone-leading-double": ('KEY="abc', '"abc'),
    "lone-leading-single": ("KEY='abc", "'abc"),
    "lone-trailing-double": ('KEY=abc"', 'abc"'),
    "lone-trailing-single": ("KEY=abc'", "abc'"),
    # quotes inside the value stay, also inside a removed pair
    "inner-double": ('KEY=a"b', 'a"b'),
    "inner-single": ("KEY=it's", "it's"),
    "inner-quotes-inside-a-pair": ('KEY="say "hi" twice"', 'say "hi" twice'),
    "other-style-inside-a-pair": ("KEY=\"it's\"", "it's"),
    # only ONE pair goes
    "two-double-pairs": ('KEY=""value""', '"value"'),
    "mixed-pairs": ("KEY=\"'value'\"", "'value'"),
    "four-single-quotes": ("KEY=''''", "''"),
    # an empty quoted value is an empty value, as a blank one
    "empty-double": ('KEY=""', ""),
    "empty-single": ("KEY=''", ""),
    "blank": ("KEY=", ""),
    # whitespace around the quotes (and the key) goes; inside them it stays
    "spaces-around-quotes": ('KEY=   "value"   ', "value"),
    "tabs-around-quotes": ("KEY=\t'value'\t", "value"),
    "spaces-around-key-and-quotes": ('   KEY   =   "value"', "value"),
    "spaces-inside-quotes": ('KEY="  value  "', "  value  "),
    # a value that is just a quote character stays
    "just-a-double-quote": ('KEY="', '"'),
    "just-a-single-quote": ("KEY='", "'"),
    "just-a-quote-with-spaces": ('KEY=   "   ', '"'),
    # the line splits at its first "=": the rest is the value
    "equals-in-value": (
        "KEY=postgres://h:5432/db?sslmode=require",
        "postgres://h:5432/db?sslmode=require",
    ),
    "equals-in-quoted-value": ('KEY="a=b=c"', "a=b=c"),
    "base64-padding-in-quotes": ('KEY="dGVzdA=="', "dGVzdA=="),
    "value-starting-with-equals": ("KEY==x", "=x"),
    # no escape processing
    "backslashes-kept": ('KEY="a\\"b\\n"', 'a\\"b\\n'),
    "single-quoted-backslash-kept": ("KEY='a\\tb'", "a\\tb"),
    # no inline-comment stripping
    "hash-in-value": ("KEY=value # note", "value # note"),
    "hash-after-quotes": ('KEY="value" # note', '"value" # note'),
    "hash-inside-quotes": ('KEY="a # b"', "a # b"),
}


@pytest.mark.parametrize(("line", "expected"), list(_CASES.values()), ids=list(_CASES))
def test_read_env_file_value(tmp_path: Path, line: str, expected: str) -> None:
    assert _read(tmp_path, line + "\n") == {"KEY": expected}


def test_each_line_is_read_on_its_own(tmp_path: Path) -> None:
    """An unbalanced quote does not continue the value onto the next line, and the
    other lines (comments, blanks, lines without ``=``) read as they always did."""
    values = _read(
        tmp_path,
        'OPEN="abc\n'
        "NEXT=def\n"
        "# COMMENTED='x'\n"
        "\n"
        "NO_EQUALS_SIGN\n"
        "CLOSE=ghi'\n"
        "LAST='jkl'\r\n",
    )
    assert values == {"OPEN": '"abc', "NEXT": "def", "CLOSE": "ghi'", "LAST": "jkl"}


def test_connect_prefill_has_no_quotes(tmp_path: Path) -> None:
    """The Connect screen's prefill, as the app builds it from the .env."""
    defaults = load_connect_defaults(
        _read(
            tmp_path,
            'DB_HOST="db.example.com"\n'
            "DB_PORT='3306'\n"
            'DB_USER="app_user"\n'
            "DB_PASSWORD='not-a-real-password'\n"
            'TARGET_ENDPOINT="c.dsql.us-east-1.on.aws"\n'
            "TARGET_REGION='us-east-1'\n"
            'TARGET_DATABASE="postgres"\n'
            "TARGET_USERNAME='admin'\n"
            'BEDROCK_MODEL_ID="example.model-v1:0"\n'
            "BEDROCK_REGION='us-west-2'\n",
        )
    )
    assert defaults.source_host == "db.example.com"
    assert defaults.source_port == 3306  # with its quotes kept, it was not a number
    assert defaults.source_username == "app_user"
    assert defaults.source_password is not None
    assert defaults.source_password.reveal() == "not-a-real-password"
    assert defaults.target_endpoint == "c.dsql.us-east-1.on.aws"
    assert defaults.target_region == "us-east-1"
    assert defaults.target_database == "postgres"
    assert defaults.target_username == "admin"
    assert defaults.bedrock_model_id == "example.model-v1:0"
    assert defaults.bedrock_region == "us-west-2"


def test_empty_quoted_values_prefill_nothing(tmp_path: Path) -> None:
    """``KEY=""`` leaves its Connect field unset, as ``KEY=`` does."""
    quoted = load_connect_defaults(
        _read(tmp_path, "DB_HOST=\"\"\nDB_PORT=''\nDB_PASSWORD=\"\"\nTARGET_ENDPOINT=''\n")
    )
    blank = load_connect_defaults(
        _read(tmp_path, "DB_HOST=\nDB_PORT=\nDB_PASSWORD=\nTARGET_ENDPOINT=\n")
    )
    assert quoted == blank
    assert (
        quoted.source_host,
        quoted.source_port,
        quoted.source_password,
        quoted.target_endpoint,
    ) == (None, None, None, None)
