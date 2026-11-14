# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The offset-seeder zip build is reproducible: exact pins, no drift, no local paths.

``scripts/build_lambda_seeder.sh`` used to vendor kafka-python and the MSK IAM signer
with an unpinned ``pip install``. A rebuild then bumped kafka-python from the
live-verified 3.0.10 to 3.0.13, and it wrote the build machine's ``/Users/...`` .pyc
paths into 10 ``.dist-info/RECORD`` files (58ca1657 removed exactly those lines by
hand). Now the script pins every distribution the committed zip vendors, installs them
with ``--no-deps``, stops on a version drift, and drops each RECORD line that names a
file the zip does not ship.

Everything here is offline. The committed zip is read as is. The script runs for real
(bash, zip, the version guard and the RECORD cleanup), with only ``python3 -m pip``
swapped for a fake that lays out a ``pip install --target`` tree the way pip did on the
maintainer's machine, including the leaking RECORD lines.

``scripts/build_lambda_seeder.sh`` is a maintainer script that ``.gitignore`` keeps
out of the published repo (``scripts/*``), so the script tests skip on a clone without
it. The committed-zip checks always run.
"""

from __future__ import annotations

import csv
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import zipfile
from email.parser import HeaderParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "build_lambda_seeder.sh"
COMMITTED_ZIP = ROOT / "connectors" / "plugins" / "offset-seeder-lambda.zip"

needs_script = pytest.mark.skipif(
    not SCRIPT.exists(),
    reason="scripts/build_lambda_seeder.sh is a git-ignored maintainer script",
)


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _record_rows(data: bytes) -> list[tuple[str, str]]:
    """(path, raw line) for every line of a RECORD."""
    lines = data.decode("utf-8").splitlines(keepends=True)
    return [(next(csv.reader([line]))[0], line) for line in lines if line.strip()]


def _zip_dists(zf: zipfile.ZipFile) -> dict[str, tuple[str, str]]:
    """normalised name -> (version from METADATA, dist-info dir) for every dist-info."""
    dists: dict[str, tuple[str, str]] = {}
    for name in zf.namelist():
        if re.fullmatch(r"[^/]+\.dist-info/METADATA", name):
            meta = HeaderParser().parsestr(zf.read(name).decode("utf-8"))
            dists[_norm(meta["Name"])] = (meta["Version"], name.split("/")[0])
    return dists


def _script_pins(text: str, array: str) -> dict[str, str]:
    block = re.search(rf"^{array}=\(\n(.*?)^\)", text, re.S | re.M)
    assert block, f"{array}=( ... ) not found in {SCRIPT.name}"
    pins: dict[str, str] = {}
    for line in block.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9.!+_-]+)", line)
        assert match, f"{array}: {line!r} is not an exact name==version pin"
        pins[match.group(1)] = match.group(2)
    return pins


# --------------------------------------------------------------------------- #
# The committed zip
# --------------------------------------------------------------------------- #
def test_no_record_line_in_the_committed_zip_names_a_local_path() -> None:
    """58ca1657 removed 457 RECORD lines that named the maintainer's /Users/... .pyc
    cache by hand. None may come back: no absolute path, no /Users/ path, no .pyc (the
    zip ships none), and no /Users/ string in any entry at all."""
    with zipfile.ZipFile(COMMITTED_ZIP) as zf:
        records = [n for n in zf.namelist() if n.endswith(".dist-info/RECORD")]
        assert len(records) == 10
        for record in records:
            for path, line in _record_rows(zf.read(record)):
                assert not path.startswith("/"), f"{record}: absolute path {line!r}"
                assert "/Users/" not in line, f"{record}: local path {line!r}"
                assert ".pyc" not in path, f"{record}: .pyc path {line!r}"
        for name in zf.namelist():
            assert b"/Users/" not in zf.read(name), f"{name} carries a /Users/ path"


@needs_script
def test_every_vendored_distribution_has_an_exact_pin_and_vice_versa() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    requested = _script_pins(text, "REQUESTED_PINS")
    transitive = _script_pins(text, "TRANSITIVE_PINS")
    assert not set(requested) & set(transitive)
    pins = {_norm(n): v for n, v in {**requested, **transitive}.items()}
    with zipfile.ZipFile(COMMITTED_ZIP) as zf:
        dists = _zip_dists(zf)
        names = set(zf.namelist())
    marked = {name for name, (_, d) in dists.items() if f"{d}/REQUESTED" in names}
    assert {n: v for n, (v, _) in dists.items()} == pins
    # The dist-info dir names carry the same version as METADATA.
    for name, (version, dist_info) in dists.items():
        assert dist_info.endswith(f"-{version}.dist-info"), (name, dist_info)
    # kafka-python 3.0.10 is the live-verified one; 3.0.13 is what an unpinned
    # install pulled.
    assert requested["kafka-python"] == "3.0.10"
    # Only the two runtime dependencies carry pip's REQUESTED marker.
    assert marked == {_norm(n) for n in requested} == {
        "kafka-python", "aws-msk-iam-sasl-signer-python"}


# --------------------------------------------------------------------------- #
# The script, run for real against a fake pip
# --------------------------------------------------------------------------- #
# What each distribution lays out, the way a real `pip install --target` did.
_LAYOUT = {
    "kafka-python": ["kafka/__init__.py", "kafka/admin/__init__.py",
                     "kafka/tests/test_x.py", "bin/kafka-python"],
    "aws-msk-iam-sasl-signer-python": ["aws_msk_iam_sasl_signer/__init__.py",
                                       "aws_msk_iam_sasl_signer/MSKAuthTokenProvider.py",
                                       "bin/aws_msk_get_auth_token"],
    "boto3": ["boto3/__init__.py"],
    "botocore": ["botocore/__init__.py", "botocore/data/endpoints.json"],
    "s3transfer": ["s3transfer/__init__.py"],
    "jmespath": ["jmespath/__init__.py", "bin/jp.py"],
    "python-dateutil": ["dateutil/__init__.py"],
    "six": ["six.py"],
    "urllib3": ["urllib3/__init__.py"],
    "click": ["click/__init__.py"],
}
# What an unpinned install resolved on the maintainer's machine.
_UNPINNED_LATEST = {"kafka-python": "3.0.13", "aws-msk-iam-sasl-signer-python": "1.0.2"}

_FAKE_PIP = textwrap.dedent(
    r'''
    import json, os, sys

    args = sys.argv[1:]
    assert args[:2] == ["-m", "pip"], args
    with open(os.environ["FAKE_PIP_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps(args[2:]) + "\n")
    layout = json.loads(os.environ["FAKE_PIP_LAYOUT"])
    latest = json.loads(os.environ["FAKE_PIP_LATEST"])
    override = json.loads(os.environ.get("FAKE_PIP_OVERRIDE", "{}"))
    target = args[args.index("--target") + 1]
    specs = [a for a in args[3:] if not a.startswith("-") and a != target]
    named = {spec.partition("==")[0] for spec in specs}
    wanted = {}
    for spec in specs:
        name, _, version = spec.partition("==")
        wanted[name] = version or latest[name]
    if "--no-deps" not in args:  # pip resolves the signer's whole tree as well
        for name in layout:
            wanted.setdefault(name, "9.9.9")
    for name, version in wanted.items():
        version = override.get(name, version)
        dist_info = f"{name.replace('-', '_')}-{version}.dist-info"
        files = []
        for rel in layout[name]:
            path = os.path.join(target, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(f"# {name} {version} {rel}\n")
            files.append("../../" + rel if rel.startswith("bin/") else rel)
            if rel.endswith(".py") and not rel.startswith("bin/"):
                pyc = os.path.join(os.path.dirname(rel), "__pycache__",
                                   os.path.basename(rel)[:-3] + ".cpython-39.pyc")
                os.makedirs(os.path.join(target, os.path.dirname(pyc)), exist_ok=True)
                open(os.path.join(target, pyc), "wb").close()
                files.append(pyc)
                # Apple's python3 caches .pyc under ~/Library/Caches; pip recorded
                # them relative to its temp dir, through the maintainer's home.
                files.append("../../../../../../../../../Users/maintainer/Library/Caches/"
                             "com.apple.python/private/var/folders/T/pip-target-x/lib/"
                             "python/" + rel[:-3] + ".cpython-39.pyc")
        os.makedirs(os.path.join(target, dist_info))
        meta = {"METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n",
                "INSTALLER": "pip\n", "WHEEL": "Wheel-Version: 1.0\n", "REQUESTED": ""}
        if name not in named:
            del meta["REQUESTED"]  # pip marks only the distributions it was given
        for fname, body in meta.items():
            with open(os.path.join(target, dist_info, fname), "w") as f:
                f.write(body)
            files.append(f"{dist_info}/{fname}")
        with open(os.path.join(target, dist_info, "RECORD"), "w", newline="") as f:
            for rel in files:
                f.write(f"{rel},,\n" if rel.endswith(".pyc") else f"{rel},sha256=x,1\n")
            f.write(f"{dist_info}/RECORD,,\n")
    '''
)


@pytest.fixture
def run_build(tmp_path: Path):
    """Run a copy of the real script in a scratch repo tree, with a fake `python3 -m pip`."""
    if not (shutil.which("zip") and shutil.which("unzip") and shutil.which("bash")):
        pytest.skip("bash, zip and unzip are needed to run the build script")
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(SCRIPT, repo / "scripts" / SCRIPT.name)
    lambda_src = repo / "deploy" / "cdc-stack" / "lambda"
    lambda_src.mkdir(parents=True)
    (lambda_src / "seeder.py").write_text("def handler(event, context):\n    pass\n")
    (lambda_src / "cfnresponse.py").write_text("SUCCESS = 'SUCCESS'\n")
    (repo / "connectors" / "plugins").mkdir(parents=True)

    shim = tmp_path / "bin"
    shim.mkdir()
    (tmp_path / "fake_pip.py").write_text(_FAKE_PIP)
    python3 = shim / "python3"
    py, fake_pip = shlex.quote(sys.executable), shlex.quote(str(tmp_path / "fake_pip.py"))
    python3.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        if [ "$1" = "-m" ] && [ "$2" = "pip" ]; then
          exec {py} -I {fake_pip} "$@"
        fi
        exec {py} "$@"
    """))
    python3.chmod(0o755)
    log = tmp_path / "pip.log"

    def run(override: dict[str, str] | None = None):
        env = dict(os.environ)
        env.update(
            PATH=f"{shim}{os.pathsep}{env.get('PATH', '')}",
            FAKE_PIP_LOG=str(log),
            FAKE_PIP_LAYOUT=json.dumps(_LAYOUT),
            FAKE_PIP_LATEST=json.dumps(_UNPINNED_LATEST),
            FAKE_PIP_OVERRIDE=json.dumps(override or {}),
        )
        proc = subprocess.run(
            ["bash", str(repo / "scripts" / SCRIPT.name)],
            env=env, capture_output=True, text=True, timeout=120,
        )
        calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
        return proc, calls, repo / "connectors" / "plugins" / "offset-seeder-lambda.zip"

    return run


@needs_script
def test_the_script_installs_exactly_the_pins_with_no_deps(run_build) -> None:
    """FAILS WITHOUT THE FIX: pip got `kafka-python aws-msk-iam-sasl-signer-python`
    unpinned and resolved their dependencies itself."""
    proc, calls, _ = run_build()
    assert proc.returncode == 0, proc.stderr
    assert len(calls) == 1, calls
    args = calls[0]
    assert args[0] == "install"
    assert "--no-deps" in args
    specs = [a for a in args[1:] if not a.startswith("-")]
    specs.remove(args[args.index("--target") + 1])
    text = SCRIPT.read_text(encoding="utf-8")
    pins = {**_script_pins(text, "REQUESTED_PINS"), **_script_pins(text, "TRANSITIVE_PINS")}
    assert specs == [f"{n}=={v}" for n, v in pins.items()]
    assert len(specs) == len(_LAYOUT) == 10


@needs_script
def test_the_built_zip_ships_no_local_path_and_no_record_line_for_a_pruned_file(
    run_build,
) -> None:
    """FAILS WITHOUT THE FIX: every vendored RECORD kept its lines for the .pyc cache
    (through /Users/<maintainer>/...), bin/ and the pruned AWS SDK."""
    proc, _, out_zip = run_build()
    assert proc.returncode == 0, proc.stderr
    with zipfile.ZipFile(out_zip) as zf:
        names = set(zf.namelist())
        files = {n for n in names if not n.endswith("/")}
        dists = _zip_dists(zf)
        rows = {
            n: _record_rows(zf.read(n)) for n in names if n.endswith(".dist-info/RECORD")
        }
    # The handler at the root; the pruned code, bin/, caches and tests are gone.
    assert {"seeder.py", "cfnresponse.py"} <= files
    tops = {n.split("/")[0] for n in names}
    assert tops.isdisjoint({"boto3", "botocore", "s3transfer", "jmespath", "dateutil",
                            "urllib3", "six.py", "click", "bin"})
    assert not any("__pycache__" in n or "/tests/" in n for n in names)
    # Each RECORD names only files the zip ships, so no local path survives, and the
    # kept lines are pip's own, byte for byte.
    assert len(rows) == 10
    for record, lines in rows.items():
        for path, line in lines:
            assert path in files, f"{record}: {line!r} names a file the zip lacks"
            assert "/Users/" not in line and not path.startswith(("/", "../"))
        assert f"{record},,\n" in [line for _, line in lines]
    # Every pinned dist-info stays, at its pin; only the two runtime deps are REQUESTED.
    text = SCRIPT.read_text(encoding="utf-8")
    requested = _script_pins(text, "REQUESTED_PINS")
    pins = {_norm(n): v for n, v in {**requested, **_script_pins(text, "TRANSITIVE_PINS")}.items()}
    assert {n: v for n, (v, _) in dists.items()} == pins
    assert {n for n, (_, d) in dists.items() if f"{d}/REQUESTED" in files} == {
        _norm(n) for n in requested}
    kafka = next(r for r in rows if r.startswith("kafka_python-"))
    assert [line for _, line in rows[kafka]] == [
        "kafka/__init__.py,sha256=x,1\n",
        "kafka/admin/__init__.py,sha256=x,1\n",
        "kafka_python-3.0.10.dist-info/METADATA,sha256=x,1\n",
        "kafka_python-3.0.10.dist-info/INSTALLER,sha256=x,1\n",
        "kafka_python-3.0.10.dist-info/WHEEL,sha256=x,1\n",
        "kafka_python-3.0.10.dist-info/REQUESTED,sha256=x,1\n",
        "kafka_python-3.0.10.dist-info/RECORD,,\n",
    ]


@needs_script
def test_the_script_refuses_a_version_drift(run_build) -> None:
    """FAILS WITHOUT THE FIX: whatever pip installed was zipped. Now an installed
    version other than its pin (here a pip index that serves kafka-python 3.0.13 for
    the 3.0.10 pin) stops the build before the zip is touched."""
    proc, _, out_zip = run_build(override={"kafka-python": "3.0.13"})
    assert proc.returncode != 0
    assert "kafka-python: pinned 3.0.10, installed 3.0.13" in proc.stderr
    assert "ERROR: vendored version drift" in proc.stderr
    assert not out_zip.exists()


@needs_script
def test_the_version_guard_refuses_a_missing_or_an_unpinned_distribution(tmp_path) -> None:
    """The guard compares both ways: a pin pip did not install and a distribution
    nobody pinned each stop the build. Runs the script's own guard block."""
    text = SCRIPT.read_text(encoding="utf-8")
    guard = re.search(
        r"^if ! python3 - \"\$\{BUILD_DIR\}\" \"\$\{REQUESTED_PINS\[@\]\}\" "
        r"\"\$\{TRANSITIVE_PINS\[@\]\}\" <<'PIN_GUARD'\n.*?^PIN_GUARD\nthen\n.*?exit 1\nfi\n",
        text, re.S | re.M,
    )
    assert guard, "the version guard block is missing from the script"
    arrays = "".join(
        re.search(rf"^{a}=\(\n.*?^\)\n", text, re.S | re.M).group(0)
        for a in ("REQUESTED_PINS", "TRANSITIVE_PINS")
    )
    pins = {**_script_pins(text, "REQUESTED_PINS"), **_script_pins(text, "TRANSITIVE_PINS")}

    def guard_exit(installed: dict[str, str]) -> tuple[int, str]:
        build = tmp_path / f"build{len(list(tmp_path.iterdir()))}"
        build.mkdir()
        for name, version in installed.items():
            d = build / f"{name.replace('-', '_')}-{version}.dist-info"
            d.mkdir()
            (d / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n")
        program = (f"set -euo pipefail\npython3() {{ {shlex.quote(sys.executable)} \"$@\"; }}\n"
                   f"BUILD_DIR={shlex.quote(str(build))}\n{arrays}{guard.group(0)}"
                   "echo GUARD-PASSED\n")
        proc = subprocess.run(["bash", "-c", program], capture_output=True, text=True,
                              timeout=60)
        return proc.returncode, proc.stdout + proc.stderr

    code, out = guard_exit(pins)
    assert code == 0 and "GUARD-PASSED" in out, out
    code, out = guard_exit({n: v for n, v in pins.items() if n != "click"})
    assert code == 1 and "click: pinned 8.1.8, installed (not installed)" in out, out
    code, out = guard_exit({**pins, "requests": "2.32.3"})
    assert code == 1 and "requests: pinned (not pinned), installed 2.32.3" in out, out
