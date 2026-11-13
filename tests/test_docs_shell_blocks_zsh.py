# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Every shell block of the READMEs and the manual runs alike pasted into zsh and bash.

macOS's default shell is an interactive zsh, where ``#`` starts no comment (its
INTERACTIVE_COMMENTS option is off): a trailing ``# comment`` reaches the command as
arguments, and an apostrophe or a backquote in a comment line opens a quote or a command
substitution. ``tests/test_s3_bucket_owner.py`` checks the deployment guides and the cdc-stack
README this way; this checks the other user-facing docs: the READMEs, the manual, the scripts
README and the sink connector's README.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_s3_bucket_owner import _SHELLS, _logging_stub

_REPO = Path(__file__).resolve().parents[1]
_DOCS = sorted(
    {
        *_REPO.glob("README*.md"),
        *_REPO.glob("docs/manual/*/*.md"),
        *_REPO.glob("scripts/README*.md"),
        *_REPO.glob("connectors/**/README*.md"),
    }
)
_FENCED_SHELL = re.compile(r"^```(?:bash|sh|shell|zsh)\n(.*?)^```$", re.M | re.S)
# Commands a block runs from PATH, and the repository files it runs by a relative path: each
# is a logging stub in the directory a paste runs in, so no pasted block runs the real one.
_PATH_STUBS = ("aws", "docker", "finch", "git", "mvn", "uv", "envprobe")
_REPO_STUBS = (
    ".venv/bin/python",
    "deploy/build_and_push.sh",
    "deploy/build_in_codebuild.sh",
    "deploy/run_measure_on_fargate.sh",
)
_CLONE = "dsql-migration-toolkit"  # the README's `git clone` then `cd` into it


def _blocks() -> list[tuple[str, str]]:
    return [
        (str(doc.relative_to(_REPO)), block)
        for doc in _DOCS
        for block in _FENCED_SHELL.findall(doc.read_text(encoding="utf-8"))
    ]


def _paste(tmp_path: Path, block: str, shell: str) -> list[list[str]]:
    """The stub commands ``block`` called, pasted into ``shell`` in a scratch directory, then
    the values of the variables it exports (as a last ``envprobe`` call)."""
    executable = shutil.which(shell)
    assert executable is not None
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name in _PATH_STUBS:
        (bin_dir / name).write_text(_logging_stub(name))
        (bin_dir / name).chmod(0o755)
    for root in (tmp_path, tmp_path / _CLONE):
        for rel in _REPO_STUBS:
            stub = root / rel
            stub.parent.mkdir(parents=True, exist_ok=True)
            stub.write_text(_logging_stub(rel))
            stub.chmod(0o755)
        (root / ".env").write_text("DB_HOST=source.example\n")
    log = tmp_path / "calls.log"
    log.write_text("")
    exported = tuple(dict.fromkeys(re.findall(r"^export ([A-Z_][A-Z0-9_]*)=", block, re.M)))
    command = block + "\ncd " + str(tmp_path) + "\nenvprobe \"$DB_HOST\" " + " ".join(
        f'"${name}"' for name in exported
    ) + "\n"
    subprocess.run(
        [executable, *_SHELLS[shell]], input=command.encode("utf-8"),
        env={"PATH": str(bin_dir), "HOME": str(tmp_path), "STUB_LOG": str(log)},
        cwd=tmp_path, capture_output=True, timeout=60, check=False,
    )
    return [line.split("\t") for line in log.read_text().splitlines()]


def test_the_docs_have_shell_blocks_to_check() -> None:
    # The glob still finds the docs: READMEs (3), the manual's setup and performance pages
    # (2 + 1 per language), the scripts README (5 per language) and the sink README (1).
    assert len(_blocks()) == 23, [doc for doc, _ in _blocks()]


@pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("zsh") is None, reason="needs bash and zsh"
)
@pytest.mark.parametrize(
    ("doc", "block"), _blocks(), ids=[f"{doc}#{i}" for i, (doc, _) in enumerate(_blocks())]
)
def test_every_docs_shell_block_runs_alike_when_pasted_into_zsh(
    doc: str, block: str, tmp_path: Path
) -> None:
    # FAILS WITHOUT THE FIX: pasted into zsh, the trailing comments after `uv sync` and
    # `uv run ... ui` (README, manual 1) reached uv as arguments; the one after
    # build_in_codebuild.sh made the image tag "#" (the script reads $1); the scripts
    # README's "# plan" / "# run" made run_full_load.py refuse its arguments; the manual's
    # backquoted `aws cloudformation deploy` in a comment ran it; and the sink README's
    # trailing comments reached mvn. Each comment is now on its own line, with no apostrophe
    # or backquote, so zsh runs the same commands, with the same arguments, as bash.
    in_bash = _paste(tmp_path, block, "bash")
    # The stubs cover the block: it runs something or sets a variable (a block with a
    # <placeholder> to edit first runs nothing as written).
    assert in_bash != _paste(tmp_path, "", "bash") or "<" in block, block
    assert _paste(tmp_path, block, "zsh") == in_bash, block
