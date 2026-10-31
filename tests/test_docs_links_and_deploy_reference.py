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
  Neither it nor the row gave CPU 256's, which the parameter allows.
* The single-EC2-host parameter table had no row for ``EnableActivityLogCloudWatch``, which
  that template has too; on the host the stdout copy goes to journald, not CloudWatch. In ko
  and ja its stack-name warning and AI prompt named only the legacy ``mysql-dsql-cdc-``
  prefix, not ``dsql-cdc-`` too, as en does.
* README's environment-variable reference said "the four" Full Load / Validation knobs can be
  retuned in Settings; five of its rows can, and it now names them.
* The EC2 host takes its settings and its code at first boot only (its user data writes
  ``/etc/dsql-migrator.env`` and fetches the source once), so the guide says which parameters
  a later stack update leaves unapplied and how to apply them; its row for the mirror names
  the switch as the host shows it (*Mirror to the system journal*), and README's variable row
  and chapter 7 name it for each host.
* Fargate also takes CPU 32768 (32 vCPU), with 60, 120 or 244 GB only (on Linux platform
  1.4.0, which the service runs), so the template allows it and gives its memory, as the
  guide's rows and sizing table do. *Tables in parallel*
  stops at 16, so the guide says a 32 vCPU task loads no more tables or parts at once, and
  the manual no longer calls the 16 vCPU task the largest.
* The EC2 host's startup script traced (``set -x``) the command that read the cookie-signing
  secret into its log, with the secret's value, and any local user could read the log. The
  fixed script runs on a new host only, so the guide says how to find and delete that line
  on an earlier host, make the log root-only, and rotate the secret when no migration is in
  progress.

Fact pins: the Fargate pairs are AWS's table (below), the knobs come from the Settings
registry (``config.TUNABLE_KNOBS``) and the bound of *Tables in parallel* from
``AppConfig``, and the defaults and descriptions from the template, as do the EC2 host's
startup log, its secret's resource and generator settings, and the env-file line that
holds the secret.
"""

from __future__ import annotations

import posixpath
import re
import subprocess
import urllib.parse

import pytest
import yaml

from tests.test_g25_docs import DEPLOY, LANGS, ROOT, _all_pages, _flat, _has

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
    source = (ROOT / "src" / "dsql_migrator" / "ui" / "app.py").read_text(encoding="utf-8")
    assert 'ui.tab("Activity log"' in source
    from dsql_migrator.ui import app

    # The switch is named for where the app's stdout goes: CloudWatch Logs on ECS, the
    # system journal on the EC2 host (that template's row, below).
    assert app._MIRROR_TEXTS["cloudwatch"].label == "Mirror to CloudWatch Logs"
    assert app._MIRROR_TEXTS["journald"].label == "Mirror to the system journal"

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


_EC2_PARAMETERS = {
    "en": "### 2. Required / key parameters",
    "ko": "### 2. 필수 / 핵심 파라미터",
    "ja": "### 2. 必須 / 主要パラメータ",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_ec2_parameter_table_says_where_the_activity_log_mirror_goes(lang: str) -> None:
    # The EC2 template has the parameter too, but there the systemd service's stdout goes to
    # journald -- no awslogs driver, no CloudWatch log group -- as its description says.
    ec2 = yaml.safe_load((DEPLOY / "cloudformation-ec2.yaml").read_text(encoding="utf-8"))
    parameter = ec2["Parameters"]["EnableActivityLogCloudWatch"]
    assert "`journalctl -u dsql-migrator`" in " ".join(parameter["Description"].split())
    user_data = ec2["Resources"]["AppHost"]["Properties"]["UserData"]["Fn::Base64"]["Fn::Sub"]
    assert "DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT=${EnableActivityLogCloudWatch}" in user_data
    assert "StandardOutput" not in user_data  # the unit's stdout: systemd's default, the journal

    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n{_EC2_PARAMETERS[lang]}\n")
    section = text[start : text.index("\n### ", start + 1)]
    (row,) = [
        line
        for line in section.splitlines()
        if line.startswith("| `EnableActivityLogCloudWatch` |")
    ]
    cells = [cell.strip() for cell in row.strip().strip("|").split("|")]
    assert cells[2] == f"`{parameter['Default']}`", lang
    # The host names the switch for journald, and a restart returns it to the host's env
    # file -- written at first boot, so not to the parameter a later stack update sets.
    for fact in (
        "JSON",
        "journald",
        "`journalctl -u dsql-migrator`",
        "**Settings → Activity log**",
        "*Mirror to the system journal*",
        "`/etc/dsql-migrator.env`",
    ):
        assert fact in cells[3], (lang, fact)
    assert "awslogs" not in cells[3], lang
    assert "Mirror to CloudWatch Logs" not in cells[3], lang


@pytest.mark.parametrize("lang", LANGS)
def test_the_readme_and_chapter_7_name_the_mirror_as_each_host_shows_it(lang: str) -> None:
    # README's variable row and chapter 7's Activity log bullet name the switch as the app
    # shows it on each host -- CloudWatch Logs on ECS, the system journal on the EC2 host --
    # and say that journald keeps the EC2 host's copy.
    from dsql_migrator.ui import app

    readme = (ROOT / _README[lang]).read_text(encoding="utf-8")
    (row,) = [
        line
        for line in readme.splitlines()
        if line.startswith("| `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` |")
    ]
    chapter = ROOT / "docs" / "manual" / lang / "07-performance-and-tuning.md"
    lines = chapter.read_text(encoding="utf-8").split("\n")
    (at,) = [
        i
        for i, line in enumerate(lines)
        if line.startswith("- ") and "**Activity log**" in line
    ]
    bullet = [lines[at]]
    for line in lines[at + 1 :]:  # its continuation lines
        if not line.startswith("  "):
            break
        bullet.append(line)
    for text in (_flat(lang, row), _flat(lang, "\n".join(bullet))):
        for destination in ("cloudwatch", "journald"):
            label = app._MIRROR_TEXTS[destination].label
            assert _flat(lang, label) in text, (lang, destination)
        assert "journald" in text, lang


@pytest.mark.parametrize("lang", LANGS)
def test_the_ec2_stack_name_warning_and_prompt_name_both_cdc_prefixes(lang: str) -> None:
    # The host's CDC deploy role reaches the stacks of both cdc-stack families, so a host stack
    # named into either falls in its scope. ko and ja named the legacy prefix only (in the
    # warning under the parameter table and in the AI prompt), as en did before v0.1.439.
    from dsql_migrator.core.cdc import CDC_STACK_NAME_LEGACY_PREFIX, CDC_STACK_NAME_PREFIX

    ec2 = (DEPLOY / "cloudformation-ec2.yaml").read_text(encoding="utf-8")
    assert set(re.findall(r":stack/([a-z-]+)\*", ec2)) == {
        CDC_STACK_NAME_PREFIX,
        CDC_STACK_NAME_LEGACY_PREFIX,
    }
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n{_EC2_PARAMETERS[lang]}\n")
    part = _flat(lang, text[start : text.index("\n### 4", start)])  # sections 2 and 3

    def bare(prefix: str) -> str:  # the prefix itself, not a stack name or a pattern
        return rf"(?<![A-Za-z0-9_-]){re.escape(prefix)}(?![A-Za-z0-9_*-])"

    legacy = [m.start() for m in re.finditer(bare(CDC_STACK_NAME_LEGACY_PREFIX), part)]
    assert len(legacy) == 2, (lang, legacy)  # the warning and the AI prompt
    for at in legacy:  # each names the canonical prefix just before
        assert re.search(bare(CDC_STACK_NAME_PREFIX), part[max(0, at - 24) : at]), lang


def _first_boot_callout(lang: str) -> str:
    """The EC2 section's ``[!IMPORTANT]`` callout between the first-boot paragraph (the one
    naming the user-data log) and ``### 4``, its ``>`` markers stripped."""
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n{_EC2_PARAMETERS[lang]}\n")
    after_log = text.index("`/var/log/dsql-migrator-userdata.log`", start)
    lines = text[after_log : text.index("\n### 4", after_log)].split("\n")
    (at,) = [i for i, line in enumerate(lines) if line.strip() == "> [!IMPORTANT]"]
    block = []
    for line in lines[at + 1 :]:
        if not line.startswith(">"):
            break
        block.append(re.sub(r"^> ?", "", line))
    return "\n".join(block)


@pytest.mark.parametrize("lang", LANGS)
def test_the_ec2_guide_says_the_host_takes_its_settings_and_code_at_first_boot_only(
    lang: str,
) -> None:
    # The host's user data runs at first boot only, so a stack update that changes a
    # parameter it reads restarts the host (a user-data change stops and starts it) without
    # applying the change: the six lines of /etc/dsql-migrator.env it writes and the four
    # source parameters keep their first-boot values, while IAM and the outputs move on.
    # A restart then returns the activity-log mirror to the file's value, not the stack's.
    ec2 = yaml.safe_load((DEPLOY / "cloudformation-ec2.yaml").read_text(encoding="utf-8"))
    parameters = set(ec2["Parameters"])
    user_data = ec2["Resources"]["AppHost"]["Properties"]["UserData"]["Fn::Base64"]["Fn::Sub"]
    env_file = re.search(
        r"cat > /etc/dsql-migrator\.env <<EOF\n(.*?)\nEOF\n", user_data, re.S
    ).group(1)
    env_pairs = {
        parameter: variable
        for variable, parameter in re.findall(r"(?m)^(\w+)=\$\{(\w+)\}$", env_file)
        if parameter in parameters
    }
    assert env_pairs == {
        "AppPort": "DSQL_MIGRATOR_APP_PORT",
        "EnableActivityLogCloudWatch": "DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT",
        "BedrockModelId": "BEDROCK_MODEL_ID",
        "BedrockRegion": "BEDROCK_REGION",
        "SourceSecretArn": "DSQL_MIGRATOR_SOURCE_SECRET_ARN",
        "DsqlClusterArn": "DSQL_MIGRATOR_DSQL_CLUSTER_ARN",
    }
    # ${!VAR} is a shell variable and ${AWS::...} a pseudo parameter: neither matches.
    substituted = set(re.findall(r"\$\{(\w+)\}", user_data)) & parameters
    # HostSubnetId too, but a new subnet replaces the instance (a fresh first boot).
    source = substituted - set(env_pairs) - {"HostSubnetId"}
    assert source == {"SourceMode", "SourceS3Uri", "SourceRepoUrl", "SourceRepoRef"}

    callout = _first_boot_callout(lang)
    for name in [*env_pairs, *env_pairs.values(), *source]:
        assert f"`{name}`" in callout, (lang, name)
    # What the callout says DOES take effect on the update: none of it is user data.
    on_update = re.findall(r"`(\w+)`", re.split(r"\n\s*\n", callout.strip())[-1])
    assert set(on_update) == {
        "EnableAiAssist",
        "BedrockModelArns",
        "SourceDbSecurityGroupId",
        "SourceDbCidr",
        "SourceDbPort",
        "HttpsEgressCidr",
        "MskEgressCidr",
        "InstanceType",
        "StateVolumeSizeGiB",
    }, lang
    for name in on_update:
        assert name in parameters and name not in substituted, (lang, name)
    flat = _flat(lang, callout)
    for command in (
        "`sudoedit /etc/dsql-migrator.env`",
        "`sudo systemctl restart dsql-migrator`",
        "`sudo xfs_growfs /var/lib/dsql-migrator`",
    ):
        assert _flat(lang, command) in flat, (lang, command)


# Where the EC2 guide says the startup log is root-only, and when to rotate the secret.
_ROOT_ONLY = {
    "en": "(readable by root only, so use `sudo`)",
    "ko": "(root만 읽을 수 있으니 `sudo`로 보세요)",
    "ja": "（root だけが読めるので `sudo` で見てください）",
}
_NO_MIGRATION_IN_PROGRESS = {
    "en": "when no migration is in progress",
    "ko": "진행 중인 마이그레이션이 없을 때",
    "ja": "進行中の移行がないとき",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_ec2_guide_cleans_and_rotates_the_secret_an_earlier_startup_log_holds(
    lang: str,
) -> None:
    # The startup script traced (set -x) the command that read the cookie-signing secret into
    # its log, with the secret's value, and any local user could read the log. The fixed
    # script runs on a new host only, so the guide says how to find that line on an earlier
    # host, delete it and make the log root-only, then give the secret a new value and write
    # it into the host's env file, printing neither. The names and the generator settings are
    # the template's.
    ec2 = yaml.safe_load((DEPLOY / "cloudformation-ec2.yaml").read_text(encoding="utf-8"))
    user_data = ec2["Resources"]["AppHost"]["Properties"]["UserData"]["Fn::Base64"]["Fn::Sub"]
    (log,) = re.findall(r"(?m)^exec > (\S+) 2>&1$", user_data)
    ((variable, resource),) = re.findall(
        r"(?m)^(\w+)=\$\(aws secretsmanager get-secret-value --secret-id \$\{(\w+)\} ",
        user_data,
    )
    (env_variable,) = re.findall(rf"(?m)^(\w+)=\$\{{!{variable}\}}$", user_data)
    generated = ec2["Resources"][resource]["Properties"]["GenerateSecretString"]

    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n{_EC2_PARAMETERS[lang]}\n")
    part = _flat(lang, text[start : text.index("\n### 4", start)])  # sections 2 and 3
    for phrase in (
        _ROOT_ONLY[lang],
        f"sudo grep -c {variable}= {log}",  # finds the line on an earlier host
        f"sudo sed -i '/{variable}=/d' {log}",
        f"sudo chmod 600 {log}",
        _NO_MIGRATION_IN_PROGRESS[lang],
        f"--logical-resource-id {resource}",
        "put-secret-value",
        f"--password-length {generated['PasswordLength']}",
        *(["--exclude-punctuation"] if generated.get("ExcludePunctuation") else []),
        f"sudo sed -i '/^{env_variable}=/d' /etc/dsql-migrator.env",
        f"printf '{env_variable}=%s\\n'",
        "| sudo tee -a /etc/dsql-migrator.env >/dev/null",  # tee alone echoes the secret
    ):
        assert _has(lang, part, phrase), (lang, phrase)
    # The old line goes before the new one is written (the same sed would delete both), and
    # the restart that applies the new value comes after the line that writes it.
    deleted = part.index(_flat(lang, f"sudo sed -i '/^{env_variable}=/d' /etc/dsql-migrator.env"))
    written = part.index(_flat(lang, f"printf '{env_variable}=%s\\n'"))
    assert deleted < written, lang
    assert _flat(lang, "sudo systemctl restart dsql-migrator") in part[written:], lang


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
# CPU units -> the only sizes it takes, in GB (the same table).
_FARGATE_FIXED_GB = {32768: (60, 120, 244)}


def _cpus_taking(mib: int) -> list[int]:
    """The CPU values (256 aside) Fargate pairs with exactly ``mib`` MiB of memory."""
    gb, rest = divmod(mib, 1024)
    ranged = [
        cpu
        for cpu, (low, high, step) in _FARGATE_GB.items()
        if not rest and low <= gb <= high and (gb - low) % step == 0
    ]
    fixed = [cpu for cpu, sizes in _FARGATE_FIXED_GB.items() if not rest and gb in sizes]
    return ranged + fixed


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
    # CPU 256 (0.25 vCPU) takes three sizes, not a range (it was missing too), and so does
    # 32768 (32 vCPU).
    assert "256->512MiB/1GB/2GB" in _memory_description(template)
    assert "32768->60GB/120GB/244GB" in _memory_description(template)


@pytest.mark.parametrize("lang", LANGS)
def test_the_sizing_table_is_fargates(lang: str) -> None:
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    rows = re.findall(r"(?m)^\| `(\d+)` \([\d.]+\) \| (\d+)–(\d+) GB \| (\d+) GB \|$", text)
    assert {int(cpu): (int(lo), int(hi), int(step)) for cpu, lo, hi, step in rows} == _FARGATE_GB
    assert re.search(r"(?m)^\| `256` \(0\.25\) \| 512, 1024, 2048 MiB \|", text), lang
    assert re.search(r"(?m)^\| `32768` \(32\) \| 60, 120, 244 GB \|", text), lang


@pytest.mark.parametrize("lang", LANGS)
def test_every_cpu_the_parameter_allows_has_its_memory_in_the_template_and_the_guide(
    lang: str, template: dict
) -> None:
    # The template's description and the guide's sizing table give each allowed CPU's memory
    # (the description lacked 256), and so does the English row, which spells the pairs out.
    allowed = sorted(template["Parameters"]["ContainerCpu"]["AllowedValues"])
    assert allowed == sorted({256, 32768, *_FARGATE_GB})  # each one a Fargate task CPU
    described = re.findall(r"\b(\d+)->", _memory_description(template))
    assert [int(cpu) for cpu in described] == allowed
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    table = re.findall(r"(?m)^\| `(\d+)` \([\d.]+\) \| ", text)
    assert [int(cpu) for cpu in table] == allowed, lang
    if lang == "en":
        row = _reference_rows(lang)["ContainerMemory"][3]
        assert [int(cpu) for cpu in re.findall(r"`(\d+)`(?: CPU)? with ", row)] == allowed
        assert "`256` CPU with 512 MiB, 1 GB or 2 GB" in row
        assert "`32768` with 60 GB, 120 GB or 244 GB" in row


def test_the_service_runs_the_platform_the_larger_cpus_need(template: dict) -> None:
    # Fargate takes CPU 8192, 16384 and 32768 only on Linux platform 1.4.0 or later (the same
    # table). The service names no PlatformVersion, so it runs LATEST, which is 1.4.0, and the
    # task names no operating system, so it runs Linux.
    service = template["Resources"]["Service"]["Properties"]
    assert service.get("PlatformVersion", "LATEST") in ("LATEST", "1.4.0")
    task = template["Resources"]["TaskDefinition"]["Properties"]
    assert task.get("RuntimePlatform", {}).get("OperatingSystemFamily", "LINUX") == "LINUX"


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


# -- a 32 vCPU task: Fargate takes it, Tables in parallel stops at 16 ------------------------

_REAL_FULL_LOAD = {
    "en": "**Real Full Load:**",
    "ko": "**실제 Full Load:**",
    "ja": "**実運用の Full Load:**",
}
# Where the ContainerCpu row and that bullet give the bound of *Tables in parallel*.
_TABLES_IN_PARALLEL_BOUND = {
    "en": ("the vCPU count, up to its maximum of {n} —", "*Tables in parallel* stops at {n},"),
    "ko": ("vCPU 수(최대 {n})로 설정", "*Tables in parallel*은 최대 {n}이므로"),
    "ja": ("vCPU 数（最大 {n}）に設定", "*Tables in parallel* の上限は {n} なので"),
}


def _real_full_load_bullet(lang: str) -> str:
    """The sizing section's bullet for a real Full Load, up to the next bullet."""
    text = (DEPLOY / _DEPLOYMENT[lang]).read_text(encoding="utf-8")
    start = text.index(f"\n- {_REAL_FULL_LOAD[lang]}")
    return _flat(lang, text[start : text.index("\n- ", start + 1)])


@pytest.mark.parametrize("lang", LANGS)
def test_the_cpu_row_and_the_sizing_bullet_say_32768_loads_no_more_at_once(lang: str) -> None:
    # The guide sets *Tables in parallel* to the vCPU count, but it counts the tables and
    # parts loading at once, one worker process each, and stops at its bound: a 32 vCPU task
    # is accepted (above) and loads no more of them at once than a 16 vCPU one.
    from dsql_migrator.config import AppConfig

    field = AppConfig.model_fields["full_load_table_parallelism"]
    (limit,) = [meta.le for meta in field.metadata if getattr(meta, "le", None) is not None]
    assert limit <= 16  # what makes 32768 load no more at once than 16384
    row = _flat(lang, _reference_rows(lang)["ContainerCpu"][3])
    bullet = _real_full_load_bullet(lang)
    in_row, in_bullet = (phrase.format(n=limit) for phrase in _TABLES_IN_PARALLEL_BOUND[lang])
    assert "`32768`" in row and _has(lang, row, in_row), lang
    assert "`32768`" in bullet and _has(lang, bullet, in_bullet), lang
    if lang == "en":
        no_more = "loads no more tables or parts at once"
        assert f"`32768` (32 vCPU) is accepted too, but {no_more} (not measured)" in row
        assert f"stops at {limit}, so `32768` {no_more}." in bullet


# What the manual called the 16 vCPU runs while 16384 was the largest CPU the template took.
_LARGEST = {
    "en": ("largest task", "maximum-size runs"),
    "ko": ("가장 큰 태스크", "최대 규모 실행"),
    "ja": ("最大のタスク", "最大サイズでの実行", "最大サイズの実行"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_no_longer_calls_16_vcpu_the_largest_task(lang: str) -> None:
    for page, text in _all_pages(lang).items():
        for stale in _LARGEST[lang]:
            assert not _has(lang, text, stale), (lang, page, stale)


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
