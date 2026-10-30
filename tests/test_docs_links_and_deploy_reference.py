# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The docs a clone ships link only to what it ships, and say what the template and the
Settings dialog do (follow-ups of the v0.1.577 review).

* docs/README.md listed ``docs/dev/`` and linked into it, but .gitignore keeps that folder
  out of the repository, so both links were dead in a clone. Every relative link in the
  tracked markdown (the vendored Debezium connector's own pages aside) must reach a file or
  folder git tracks, and its anchor a heading of that page.
* The deployment guide's parameter reference, which promises every parameter, had no row
  for ``EnableActivityLogCloudWatch``. Each row's default is the template's.
* Its ``ContainerMemory`` row asked for >= 4096 MiB for large ``TEXT``/``BLOB`` values where
  the template's description and the guide's own sizing bullet say 8192+; the template's
  description gave the memory of CPU 512 to 4096 only, though the guide goes up to 16384.
* README's environment-variable reference said "the four" Full Load / Validation knobs can be
  retuned in Settings; five of its rows can, and it now names them.

Fact pins: the Fargate pairs are AWS's table (below), the knobs come from the Settings
registry (``config.TUNABLE_KNOBS``), and the defaults and descriptions from the template.
"""

from __future__ import annotations

import posixpath
import re
import subprocess
import urllib.parse

import pytest
import yaml

from tests.test_g25_docs import DEPLOY, LANGS, ROOT, _flat

_DEPLOYMENT = {"en": "DEPLOYMENT.md", "ko": "DEPLOYMENT.ko.md", "ja": "DEPLOYMENT.ja.md"}
_README = {"en": "README.md", "ko": "README.ko.md", "ja": "README.ja.md"}


@pytest.fixture(scope="module")
def template() -> dict:
    return yaml.safe_load((DEPLOY / "cloudformation.yaml").read_text(encoding="utf-8"))


# -- every relative link reaches what a clone has --------------------------------------------

# The Debezium MySQL connector's own pages, committed as they ship (the plugin's input).
_VENDORED = "connectors/plugins/"
_FENCE = re.compile(r"^\s*(`{3,}|~{3,})")
_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(.*?)(?:\s+#+)?\s*$")
_LINK = re.compile(
    r"!?\[(?:[^\[\]]|\[[^\[\]]*\])*\]"  # [text], one level of nested brackets
    r"\(\s*<?([^\s<>()]+)>?(?:\s+\"[^\"]*\")?\s*\)"  # (target "optional title")
)
_HTML_LINK = re.compile(r"\b(?:href|src)=\"([^\"]+)\"")
_CODE_SPAN = re.compile(r"(`+)(?:(?!\1).)+?\1")


def _tracked() -> list[str]:
    try:
        listing = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "-z"],
            capture_output=True,
            check=True,
            timeout=60,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        pytest.skip("git cannot list this checkout's tracked files")
    return [name for name in listing.decode("utf-8").split("\0") if name]


def _outside_fences(text: str) -> str:
    """``text`` with its fenced code blanked: a link or a heading there is not one."""
    lines, fence = [], ""
    for line in text.split("\n"):
        if fence:
            stripped = line.strip()
            if stripped.startswith(fence) and not stripped.strip(fence[0]):
                fence = ""
            lines.append("")
            continue
        opening = _FENCE.match(line)
        fence = opening.group(1) if opening else ""
        lines.append("" if fence else line)
    return "\n".join(lines)


def _anchors(text: str) -> set[str]:
    """The anchors GitHub gives a page's headings: the text lower-cased, its punctuation
    dropped and its spaces made hyphens, with ``-1``, ``-2``... after a repeated one."""
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    for line in _outside_fences(text).split("\n"):
        heading = _HEADING.match(line)
        if not heading:
            continue
        words = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading.group(1))  # a link's text
        words = re.sub(r"<[^>]+>", "", words)  # an HTML tag
        slug = re.sub(r"[^\w\- ]", "", words.strip().lower()).replace(" ", "-")
        repeat = seen.get(slug, 0)
        seen[slug] = repeat + 1
        anchors.add(f"{slug}-{repeat}" if repeat else slug)
    return anchors | set(re.findall(r"<a\s+(?:id|name)=\"([^\"]+)\"", text))


def test_the_anchor_rule_matches_github_for_known_headings() -> None:
    text = (
        "# 3. Full Load\n### Task sizing — `ContainerCpu` / `ContainerMemory`\n"
        "### 1. Prerequisites\n```bash\n# not a heading\n```\n### 1. Prerequisites\n"
        "## 단일 EC2 호스트에서 실행 (소스에서, Lambda-free)\n### 3.8 並列ロード\n"
    )
    assert _anchors(text) == {
        "3-full-load",
        "task-sizing--containercpu--containermemory",
        "1-prerequisites",
        "1-prerequisites-1",
        "단일-ec2-호스트에서-실행-소스에서-lambda-free",
        "38-並列ロード",
    }


def test_every_relative_link_in_the_tracked_markdown_reaches_a_tracked_file() -> None:
    # A page and folder that exist here can still be missing from a clone: docs/dev/ is
    # git-ignored, so docs/README.md's two links into it were dead there. Hence git's list,
    # not the disk, decides (and the disk too, for a file deleted but not yet staged).
    tracked = _tracked()
    files = set(tracked)
    folders = set()
    for name in tracked:
        folder = posixpath.dirname(name)
        while folder and folder not in folders:
            folders.add(folder)
            folder = posixpath.dirname(folder)
    pages = [name for name in tracked if name.endswith(".md") and not name.startswith(_VENDORED)]
    anchors: dict[str, set[str]] = {}
    broken: list[str] = []
    checked = 0
    for page in pages:
        text = (ROOT / page).read_text(encoding="utf-8")
        prose = _CODE_SPAN.sub(lambda span: " " * len(span.group()), _outside_fences(text))
        for target in _LINK.findall(prose) + _HTML_LINK.findall(prose):
            if re.match(r"[A-Za-z][A-Za-z0-9+.-]*:|//", target):
                continue  # a URL: https:, mailto:, ...
            checked += 1
            path, _, anchor = target.partition("#")
            path = urllib.parse.unquote(path)
            name = page
            if path:
                name = posixpath.normpath(posixpath.join(posixpath.dirname(page), path))
            if path and not ((name in files or name in folders) and (ROOT / name).exists()):
                broken.append(f"{page}: {target} (no file or folder of that name is tracked)")
            elif anchor and name.endswith(".md"):
                if name not in anchors:
                    anchors[name] = _anchors((ROOT / name).read_text(encoding="utf-8"))
                if urllib.parse.unquote(anchor) not in anchors[name]:
                    broken.append(f"{page}: {target} (no such heading)")
    # Guard the guard: a scan that found nothing would pass vacuously.
    assert len(pages) > 40 and checked > 500, (len(pages), checked)
    assert not broken, "dead links in the tracked markdown:\n  " + "\n  ".join(broken)


# -- the deployment guide's parameter reference ----------------------------------------------

_PARAMETER_REFERENCE = {
    "en": "### Parameter reference",
    "ko": "### 파라미터 레퍼런스",
    "ja": "### パラメータリファレンス",
}
# The one default the guide describes rather than quotes: the image tag moves every release.
_DESCRIBED_DEFAULTS = {"ContainerImageUri"}


def _reference_rows(lang: str) -> dict[str, list[str]]:
    """The cells of each row of the guide's (Fargate) parameter reference, by parameter."""
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n{_PARAMETER_REFERENCE[lang]}\n")
    section = text[start : text.index("\n### ", start + 1)]
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in section.splitlines()
        if line.startswith("| `")
    ]
    names = [re.fullmatch(r"`(\w+)`", row[0]).group(1) for row in rows]
    assert len(set(names)) == len(names), (lang, names)  # one row each
    return dict(zip(names, rows))


@pytest.mark.parametrize("lang", LANGS)
def test_the_parameter_reference_has_a_row_per_parameter_with_its_default(
    lang: str, template: dict
) -> None:
    parameters = template["Parameters"]
    rows = _reference_rows(lang)
    assert sorted(rows) == sorted(parameters), lang  # EnableActivityLogCloudWatch was missing
    for name, row in rows.items():
        default = row[2]
        if "Default" not in parameters[name]:
            assert default == "—", (lang, name, default)
        elif name not in _DESCRIBED_DEFAULTS:
            quoted = re.fullmatch(r"`([^`]*)`", default)
            assert quoted, (lang, name, default)
            value = "" if quoted.group(1) == '""' else quoted.group(1)
            assert value == str(parameters[name]["Default"]), (lang, name, default)


@pytest.mark.parametrize("lang", LANGS)
def test_the_activity_log_row_says_what_the_parameter_does(lang: str, template: dict) -> None:
    # The row carries the template's own description -- the container's awslogs driver
    # forwards the stdout copy to the stack's log group, the rotating file lives on the
    # task's /tmp -- and where the app switches the mirror at runtime.
    description = " ".join(
        template["Parameters"]["EnableActivityLogCloudWatch"]["Description"].split()
    )
    assert "awslogs driver" in description and "ephemeral /tmp" in description
    app = (ROOT / "src" / "dsql_migrator" / "ui" / "app.py").read_text(encoding="utf-8")
    assert 'ui.tab("Activity log"' in app and 'label="Mirror to CloudWatch Logs"' in app

    row = _reference_rows(lang)["EnableActivityLogCloudWatch"][3]
    for fact in (
        "JSON",
        "`awslogs`",
        "CloudWatch",
        "`/tmp`",
        "IAM",
        "**Settings → Activity log**",
        "*Mirror to CloudWatch Logs*",
    ):
        assert fact in row, (lang, fact)


# -- task memory: the template, the guide's row, its sizing bullet and AWS's table agree -----

# AWS Fargate's task CPU / memory pairs (Amazon ECS Developer Guide, "Task CPU and memory":
# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html):
# CPU units -> (lowest GB, highest GB, step GB). CPU 256 takes 512 MiB, 1 GB or 2 GB only.
_FARGATE_GB = {
    512: (1, 4, 1),
    1024: (2, 8, 1),
    2048: (4, 16, 1),
    4096: (8, 30, 1),
    8192: (16, 60, 4),
    16384: (32, 120, 8),
}


def _cpus_taking(mib: int) -> list[int]:
    """The CPU values (256 aside) Fargate pairs with exactly ``mib`` MiB of memory."""
    gb, rest = divmod(mib, 1024)
    return [
        cpu
        for cpu, (low, high, step) in _FARGATE_GB.items()
        if not rest and low <= gb <= high and (gb - low) % step == 0
    ]


def _memory_description(template: dict) -> str:
    return " ".join(template["Parameters"]["ContainerMemory"]["Description"].split())


def test_the_template_gives_the_memory_each_cpu_takes(template: dict) -> None:
    described = {
        int(cpu): (int(low), int(high), int(step or 1))
        for cpu, low, high, step in re.findall(
            r"(\d+)->(\d+)-(\d+)GB(?: in (\d+)GB steps)?", _memory_description(template)
        )
    }
    assert described == _FARGATE_GB  # 8192 and 16384 were missing
    # Every CPU the parameter allows but 256 (0.25 vCPU), which the guide never recommends;
    # its three sizes are in the guide's sizing table.
    allowed = set(template["Parameters"]["ContainerCpu"]["AllowedValues"])
    assert allowed - set(described) == {256}


@pytest.mark.parametrize("lang", LANGS)
def test_the_sizing_table_is_fargates(lang: str) -> None:
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    rows = re.findall(r"(?m)^\| `(\d+)` \([\d.]+\) \| (\d+)–(\d+) GB \| (\d+) GB \|$", text)
    assert {int(cpu): (int(lo), int(hi), int(step)) for cpu, lo, hi, step in rows} == _FARGATE_GB
    assert re.search(r"(?m)^\| `256` \(0\.25\) \| 512, 1024, 2048 MiB \|", text), lang


_ITSELF = {"en": "itself is valid with", "ko": "자체는", "ja": "自体は"}


@pytest.mark.parametrize("lang", LANGS)
def test_the_memory_row_recommends_what_the_template_and_the_sizing_bullet_do(
    lang: str, template: dict
) -> None:
    described = _memory_description(template)
    full_load = int(re.search(r"use (\d+)\+ for a Full Load", described).group(1))
    lobs = int(re.search(r"(\d+)\+ for large LOBs", described).group(1))
    assert (full_load, lobs) == (2048, 8192)

    row = _reference_rows(lang)["ContainerMemory"][3]
    # "≥ 2048" for a real Full Load, then "≥ 8192 (`8192` itself is valid with `ContainerCpu`
    # `1024`, `2048` or `4096`)" for large TEXT/BLOB values (it said ≥ 4096 with 512-2048).
    assert [int(n) for n in re.findall(r"\*\*≥ (\d+)\*\*", row)] == [full_load, lobs], lang
    clause = re.search(rf"\*\*≥ {lobs}\*\*\s*[(（]`{lobs}` ([^)）]*)[)）]", row).group(1)
    assert _ITSELF[lang] in clause, lang
    assert [int(n) for n in re.findall(r"`(\d+)`", clause)] == _cpus_taking(lobs), lang
    assert _cpus_taking(lobs) == [1024, 2048, 4096]
    if lang == "en":  # the English row also spells the pairs out, as the template does
        inline = {
            int(cpu): (int(low), int(high), int(step or 1))
            for cpu, low, high, step in re.findall(
                r"`(\d+)`(?: CPU)? with (\d+)–(\d+) GB(?: in (\d+) GB steps)?", row
            )
        }
        assert inline == _FARGATE_GB

    # The sizing bullet for the same tables asks for the same memory, at a CPU that takes it.
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    ((cpu, memory),) = re.findall(r"\*\*`(\d+)` CPU /\s+`(\d+)`\+ MiB\*\*", text)
    assert int(memory) == lobs and int(cpu) in _cpus_taking(lobs), lang


# -- README: the knobs Settings retunes ------------------------------------------------------

_KNOB_COUNT = {
    "en": "{word} of the Full Load / Validation knobs below",
    "ko": "아래 Full Load / Validation 노브 중 {n}개",
    "ja": "下記の Full Load / Validation のパラメータのうち {n} つ",
}
_EN_NUMBER = {2: "Two", 3: "Three", 4: "Four", 5: "Five", 6: "Six", 7: "Seven", 8: "Eight"}


@pytest.mark.parametrize("lang", LANGS)
def test_the_readme_counts_and_names_the_knobs_settings_retunes(lang: str) -> None:
    # The sentence before the environment-variable table said "the four" knobs from when
    # Settings had four; it also has Reader shards now (and the source-load throttle, whose
    # variable the table does not list). It names the table's rows Settings retunes.
    from dsql_migrator.config import ENV_PREFIX, TUNABLE_KNOBS

    blocks = (ROOT / _README[lang]).read_text(encoding="utf-8").split("\n\n")
    (at,) = [i for i, block in enumerate(blocks) if f"| `{ENV_PREFIX}APP_HOST` |" in block]
    listed = re.findall(rf"(?m)^\| `{ENV_PREFIX}(\w+)` \|", blocks[at])
    retunable = [
        knob.env_suffix
        for knob in TUNABLE_KNOBS
        if knob.group in ("Full Load", "Validation") and knob.env_suffix in listed
    ]
    assert retunable, lang  # the scan found the table's rows

    intro = _flat(lang, blocks[at - 1])
    assert re.findall(r"`(\w+)`", intro) == retunable, lang
    count = len(retunable)
    assert _flat(lang, _KNOB_COUNT[lang].format(word=_EN_NUMBER[count], n=count)) in intro, lang
    assert "Settings" in intro and "Validation" in intro, lang
