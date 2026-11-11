# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""X11: ``config.read_env_file`` strips one matching pair of quotes around a value.

The local-dev ``.env`` in the repo root prefills the Connect screen
(``load_connect_defaults``) and can set ``DSQL_MIGRATOR_STORAGE_SECRET``; the app reads
it with ``read_env_file``. That reader kept quotes, so ``DB_HOST="db.example.com"``
prefilled the host WITH its quotes, and a quoted ``DB_PORT`` (not all digits) was
ignored: the Port field showed the engine's default port (3306 for MySQL, 5432 for
PostgreSQL) instead, so with a MySQL source ``DB_PORT="3307"`` showed 3306. A shell that
sources the file (``set -a; source .env``, as scripts/README.md says) and every script's
own ``.env`` parser remove the quotes.

Now, after the whitespace strip, one matching pair of single or double quotes around
the value is removed. Mismatched, lone and backtick quotes, and quotes inside the value,
are kept as written; ``KEY=""`` gives an empty value, which callers treat as unset, as
they treat ``KEY=``. Whitespace inside the quotes is kept, but the Connect prefill
strips every value again. There is no escape processing and no inline-comment
stripping, and a line is read on its own: a quote never continues a value onto the
next line.
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
    # backticks are not quotes here: a pair of them stays
    "backtick-pair-kept": ("KEY=`value`", "`value`"),
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
    "only-spaces-inside-quotes": ('KEY="   "', "   "),
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
    # ... so a backslash does not escape the closing quote: the pair still goes
    "backslash-before-closing-quote": ('KEY="abc\\"', "abc\\"),
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
            "DB_PORT='3307'\n"
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
    # With its quotes kept it was not a number, so it was dropped (None) and the form
    # fell back to the engine's default port.
    assert defaults.source_port == 3307
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
    """``KEY=""`` leaves its Connect prefill unset (None), as ``KEY=`` does; what the
    form then shows is checked on the rendered page below."""
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


def test_the_connect_prefill_strips_the_spaces_kept_inside_quotes(tmp_path: Path) -> None:
    """``read_env_file`` keeps whitespace inside the quotes, but every Connect prefill
    value is stripped again, so quoting does not keep a field's edge spaces."""
    values = _read(tmp_path, 'DB_HOST="  db.example.com  "\nDB_PASSWORD="  pw  "\n')
    assert values == {"DB_HOST": "  db.example.com  ", "DB_PASSWORD": "  pw  "}
    defaults = load_connect_defaults(values)
    assert defaults.source_host == "db.example.com"
    assert defaults.source_password is not None
    assert defaults.source_password.reveal() == "pw"


def _form(tmp_path: Path, text: str) -> list[tuple[str, object]]:
    """The Host, Port and Username fields (the source's, then the target's) of the REAL
    Connect form the app builds from this .env, rendered through the suite's NiceGUI
    double. A fresh session, so the source engine is MySQL."""
    from tests.test_ui_connect_ai_status import _render

    defaults = load_connect_defaults(_read(tmp_path, text))
    ui, _state = _render(lambda _state: None, defaults=defaults)
    return [
        (el.text, el.value)
        for el in ui.els
        if el.kind in ("input", "number") and el.text in ("Host", "Port", "Username")
    ]


def test_the_connect_form_shows_quoted_values_without_their_quotes(tmp_path: Path) -> None:
    """What the operator sees. Before X11 this .env showed the host and both usernames
    with their quotes, and Port 3306: the quoted port was not a number, so the field
    fell back to the engine's default port."""
    shown = _form(
        tmp_path,
        'DB_HOST="db.example.com"\nDB_PORT="3307"\n'
        "DB_USER='app_user'\nTARGET_USERNAME=\"migrator\"\n",
    )
    assert shown == [
        ("Host", "db.example.com"),
        ("Port", 3307),
        ("Username", "app_user"),
        ("Username", "migrator"),
    ]


def test_an_empty_quoted_value_renders_as_a_blank_one(tmp_path: Path) -> None:
    """``KEY=""`` is unset, as ``KEY=`` is: a field with a default shows it (the
    engine's port, ``admin``) and the others stay empty. Before X11 the host and the
    usernames showed the two quote characters."""
    quoted = _form(tmp_path, "DB_HOST=\"\"\nDB_PORT=\"\"\nDB_USER=''\nTARGET_USERNAME=''\n")
    blank = _form(tmp_path, "DB_HOST=\nDB_PORT=\nDB_USER=\nTARGET_USERNAME=\n")
    assert quoted == blank == [
        ("Host", ""),
        ("Port", 3306),
        ("Username", ""),
        ("Username", "admin"),
    ]


def test_a_non_ascii_digit_port_is_ignored_not_a_startup_crash() -> None:
    """``str.isdigit`` accepts characters ``int`` rejects (a superscript two); since X11 a
    quoted ``DB_PORT="²"`` reaches the port parse unquoted, so the parse checks
    ``isdecimal`` and ASCII and treats anything else as no port, as an unparsable port
    always was."""
    from dsql_migrator.config import load_connect_defaults

    for raw in ("²", "٣٣٠٦", "3306"):
        defaults = load_connect_defaults({"DB_HOST": "h", "DB_PORT": raw})
        assert defaults.source_port == (3306 if raw == "3306" else None), raw
