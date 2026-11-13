# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assessment report export: the text, JSON and standalone HTML forms of an
:class:`~dsql_migrator.core.models.AssessmentReport`.

Split out of :mod:`dsql_migrator.core.assessor` (A8), which judges the inventory: this
module only RENDERS a finished report -- the importance order the screen and every export
share, the per-kind classification chart (its stats, labels, colours and order are read by
the Evaluation screen's chart too, so the two agree), the target-analysis section, and the
format dispatch. It reads no database and evaluates no rule. :mod:`~dsql_migrator.core.assessor`
re-exports every name below, so ``from dsql_migrator.core.assessor import ...`` keeps
working; this module imports nothing of it at import time (the HTML chart's kind label is
the assessor's ``kind_label``, imported where it is used), so either can be imported first.
"""

from __future__ import annotations

import html
import re
from typing import Optional, Sequence

from dsql_migrator.core.models import (
    AssessmentItem,
    AssessmentReport,
    Classification,
    EffortLevel,
    SourceType,
    TargetInventory,
)


def importance_order(items: "Sequence[AssessmentItem]") -> "list[AssessmentItem]":
    """``items`` most critical first: UNSUPPORTED > MANUAL > AUTO, then effort SIGNIFICANT >
    MEDIUM > SIMPLE > none, then the name -- the screen's order, so an export reads in the
    same order as the report it was taken from. Pure."""
    severity = {Classification.UNSUPPORTED: 2, Classification.MANUAL: 1, Classification.AUTO: 0}
    effort_rank = {EffortLevel.SIGNIFICANT: 3, EffortLevel.MEDIUM: 2, EffortLevel.SIMPLE: 1}
    return sorted(
        items,
        key=lambda item: (
            -severity.get(item.classification, 0),
            -effort_rank.get(item.effort, 0),
            item.object_name,
        ),
    )


def _attention_share(attention: int, total: int) -> str:
    """``attention / total`` as a whole percentage that never reads 0% or 100% when it is
    neither (1 of 1,000 is "<1%", not "0%")."""
    share = attention / total * 100
    if 0 < share < 1:
        return "<1%"
    if 99 < share < 100:
        return ">99%"
    return f"{round(share)}%"


def render_text_report(
    report: AssessmentReport,
    *,
    metadata: "Sequence[tuple[str, str]]" = (),
    conflicts: "Sequence[str]" = (),
    blocked: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Render a human-readable text version of an assessment report (English).

    ``metadata`` is the ``(label, value)`` header saying what was assessed (see the
    Evaluation export); ``conflicts`` / ``blocked`` are the target's existing and held
    names, listed after the items."""
    lines = ["Compatibility Assessment Report", "=" * 31, ""]
    if metadata:
        width = max(len(label) for label, _value in metadata) + 2
        lines.extend(f"{label + ':':<{width}}{value}" for label, value in metadata)
        lines.append("")
    lines.append("Difficulty summary (objects by classification):")
    for classification in Classification:
        lines.append(f"  {classification.value}: {report.summary.get(classification, 0)}")
    lines.append("")
    lines.append("Estimated manual effort (non-automatic objects):")
    for level in EffortLevel:
        lines.append(f"  {level.value}: {report.effort_summary.get(level, 0)}")
    lines.append("")
    lines.append(f"Items ({len(report.assessed_items)}):")
    for item in importance_order(report.assessed_items):
        effort = item.effort.value if item.effort is not None else "NONE"
        lines.append(
            f"- {item.object_name} [{item.classification.value}] "
            f"(effort: {effort}) ({item.rule_id})"
        )
        # One numbered block per matched rule, each risk beside its own fix. The joined
        # item.risk/item.recommendation are a single run-on sentence once an object
        # matches more than one rule, which is exactly when the report matters most.
        concerns = list(item.concerns or [])
        if concerns:
            for number, concern in enumerate(concerns, start=1):
                # An advisory finding is labeled RECOMMENDED rather than by severity: the
                # conversion is complete and correct, so its classification describes how
                # big the optional change would be, not that anything is wrong. Its
                # effort is likewise excluded from the object's estimate above, so the
                # text spells out that it is the cost of opting in.
                advisory = concern.is_advisory
                tag = "RECOMMENDED" if advisory else concern.classification.value
                head = f"    {number}. [{tag}] {concern.rule_id}"
                if concern.effort is not None:
                    head += (
                        f" (effort if you take it: {concern.effort.value})"
                        if advisory
                        else f" (effort: {concern.effort.value})"
                    )
                lines.append(head)
                if concern.risk:
                    label = "Note" if advisory else "Risk"
                    lines.append(f"       {label}: {concern.risk}")
                if concern.recommendation:
                    lines.append(f"       Fix:  {concern.recommendation}")
        else:
            if item.risk:
                lines.append(f"    Risk: {item.risk}")
            if item.recommendation:
                lines.append(f"    Recommendation: {item.recommendation}")
    if conflicts:
        lines.append("")
        lines.append(f"Already on the target ({len(conflicts)}):")
        lines.extend(f"- {name}" for name in conflicts)
    if blocked:
        lines.append("")
        lines.append(f"Names held on the target by another object ({len(blocked)}):")
        lines.extend(f"- {name}: {holder}" for name, holder in blocked)
    return "\n".join(lines)


def export_report(
    report: AssessmentReport,
    fmt: str = "json",
    *,
    source_type: SourceType = SourceType.MYSQL,
    metadata: "Sequence[tuple[str, str]]" = (),
    target_endpoint: Optional[str] = None,
    conflicts: "Sequence[str]" = (),
    blocked: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Export an assessment report as ``"json"``, ``"text"``, or ``"html"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable), with the
    ``metadata`` header under ``"metadata"`` and the target's endpoint and names under
    ``"target"`` beside the model's own keys; text is a readable summary; HTML is a
    styled, standalone document suitable for sharing (its title reflects the
    ``source_type``). Each lists the assessed objects only (never a report marker).
    Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        import json

        data: dict[str, object] = {}
        if metadata:
            data["metadata"] = {
                re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_"): value
                for label, value in metadata
            }
        # The report's own order (the human-read text and HTML are sorted by importance):
        # the JSON reloads as the same report.
        data.update(
            report.model_copy(update={"items": report.assessed_items}).model_dump(mode="json")
        )
        if target_endpoint or conflicts or blocked:
            data["target"] = {
                "endpoint": target_endpoint,
                "conflicts": list(conflicts),
                "blocked": [{"name": name, "holder": holder} for name, holder in blocked],
            }
        return json.dumps(data, indent=2)
    if normalized == "text":
        return render_text_report(
            report, metadata=metadata, conflicts=conflicts, blocked=blocked
        )
    if normalized == "html":
        return render_html_report(report, source_type=source_type, metadata=metadata)
    raise ValueError(
        f"unsupported report format: {fmt!r} (use 'json', 'text', or 'html')"
    )


# Background colors for the classification cell in the HTML report.
_HTML_CLASS_COLOR: dict[Classification, str] = {
    Classification.AUTO: "#e8f5e9",
    Classification.MANUAL: "#fff8e1",
    Classification.UNSUPPORTED: "#ffebee",
}

# Background for an ADVISORY finding's cell -- a calm info-blue, deliberately outside the
# green/amber/red severity ramp above, because such a finding reports no defect at all.
# Matches the sky tone the Evaluation screen uses for the same finding.
_HTML_ADVISORY_COLOR = "#e1f5fe"

# Solid bar colors per CLASSIFICATION for the HTML chart. The _HTML_CLASS_COLOR values
# above are pale table-cell backgrounds; a bar needs full-strength fills to be legible at
# 18px. Same green/amber/red severity ramp as the classification badges in the UI.
# Fixed order for the chart: best outcome first, so a bar reads left-to-right from
# "converts by itself" to "cannot be converted".
_CHART_ORDER: tuple[Classification, ...] = (
    Classification.AUTO,
    Classification.MANUAL,
    Classification.UNSUPPORTED,
)

# Chart labels. "Review needed" rather than the raw MANUAL, matching the wording the UI
# badges use, so the exported report and the screen agree.
_CLASS_CHART_LABELS: dict[Classification, str] = {
    Classification.AUTO: "Automatic",
    Classification.MANUAL: "Review needed",
    Classification.UNSUPPORTED: "Unsupported",
}

# The design system's one palette (Tailwind green / amber / red 600): amber, never orange,
# for a warning tone.
_CHART_CLASS_COLORS: dict[Classification, str] = {
    Classification.AUTO: "#16a34a",
    Classification.MANUAL: "#d97706",
    Classification.UNSUPPORTED: "#dc2626",
}


def classification_stats_by_kind(
    report: AssessmentReport,
) -> list[tuple[str, dict[Classification, int], int]]:
    """Per-kind counts split by CLASSIFICATION, largest kind first.

    This is what both the UI chart and the HTML export are built from, so the two always
    agree. Classification rather than effort, because the chart sits beside the
    classification summary and a table whose Classification column uses the same three
    words -- splitting the bars by effort instead made the reader translate between two
    vocabularies to reconcile them. Effort is still reported, in its own summary and per
    object; it is just not what this chart answers.

    Ordered by TOTAL OBJECT COUNT descending (ties broken by kind name), so the bars step
    down in length and the chart reads as a size ranking -- TABLE, then PROCEDURE, and so
    on. Ordering by trouble-share instead put a single unsupported TRIGGER above 200
    tables: it made short bars float above long ones, which reads as a broken chart, and
    the "most blocked" reading was already carried by each bar's own red segment and its
    "% need attention" caption.
    """
    counts: dict[str, dict[Classification, int]] = {}
    for item in report.assessed_items:
        bucket = counts.setdefault(item.kind, {c: 0 for c in Classification})
        bucket[item.classification] += 1

    def sort_key(entry: tuple[str, dict[Classification, int]]):
        kind, by_class = entry
        return (-sum(by_class.values()), kind)

    return [
        (kind, by_class, sum(by_class.values()))
        for kind, by_class in sorted(counts.items(), key=sort_key)
    ]


def _render_html_chart(report: AssessmentReport) -> str:
    """Render the per-kind chart as a self-contained HTML/CSS block.

    Split by CLASSIFICATION (auto / review needed / unsupported), not by effort. The chart
    sits directly above the classification summary and a table whose third column is
    Classification, so an effort-based split made the reader translate between two
    vocabularies to reconcile them. Effort is still reported -- in its own summary list
    and per row -- it is just not what this bar answers.

    Bars are scaled to the largest kind so lengths reflect counts, and no external script
    is used, so the exported report stays portable.
    """
    # The assessor's label, imported here: it re-exports this module, so a top-level
    # import would be a cycle.
    from dsql_migrator.core.assessor import kind_label

    stats = classification_stats_by_kind(report)
    max_total = max((total for _kind, _by_class, total in stats), default=0)
    if not stats or max_total == 0:
        return ""

    legend = "".join(
        f'<span class="chip"><i style="background:{_CHART_CLASS_COLORS[cls]}"></i>'
        f"{html.escape(_CLASS_CHART_LABELS[cls])}</span>"
        for cls in _CHART_ORDER
    )

    rows: list[str] = []
    for kind, by_class, total in stats:
        bars = "".join(
            f'<span style="width:{by_class[cls] / max_total * 100:.2f}%;'
            f'background:{_CHART_CLASS_COLORS[cls]}"'
            f' title="{_CLASS_CHART_LABELS[cls]}: {by_class[cls]}"></span>'
            for cls in _CHART_ORDER
            if by_class[cls] > 0
        )
        # "needs attention" = anything not AUTO, which is the number an operator plans
        # around; the per-class counts are on the segment tooltips.
        attention = total - by_class[Classification.AUTO]
        meta = f"{total} object{'s' if total != 1 else ''}"
        if attention:
            meta += f" &middot; {_attention_share(attention, total)} need attention"
        rows.append(
            '<div class="bar-row">'
            f'<div class="bar-label">{html.escape(kind_label(kind))}</div>'
            f'<div class="bar">{bars}</div>'
            f'<div class="bar-meta">{meta}</div>'
            "</div>"
        )

    return (
        "<h2>Compatibility by object kind</h2>\n"
        f'<div class="legend">{legend}</div>\n'
        f'<div class="chart">{"".join(rows)}</div>\n'
    )


def _render_target_html_section(
    target: TargetInventory,
    conflicts: "list[str]",
    blocked: "Sequence[tuple[str, str]]" = (),
    endpoint: "Optional[str]" = None,
) -> str:
    """Render the Target analysis (Aurora DSQL) section for the HTML export.

    Mirrors the on-screen Target analysis: the cluster it was read from (the limit items
    count what that cluster holds), the target catalog summary (schemas / tables / views),
    the source objects that already exist on the target, and those whose name another kind
    of object holds there (``blocked``: neither apply mode can create them). The all-clear
    is printed only when both lists are empty.
    """
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    table_count = sum(len(schema.tables) for schema in target.schemas)
    view_count = sum(len(schema.views) for schema in target.schemas)
    parts = [
        '<section class="target-section">',
        "<h2>Target analysis (Aurora DSQL)</h2>",
    ]
    if endpoint:
        parts.append(f"<p>Assessed against <strong>{esc(endpoint)}</strong>.</p>")
    parts.append(
        f"<p>Target catalog: <strong>{len(target.schemas)}</strong> schemas, "
        f"<strong>{table_count}</strong> tables, <strong>{view_count}</strong> "
        "views.</p>"
    )
    if conflicts:
        parts.append(
            f'<p class="warn">{len(conflicts)} source object(s) already exist on '
            "the target and may conflict when applying converted DDL:</p>"
        )
        chips = " ".join(
            f'<span class="conflict">{esc(name)}</span>' for name in conflicts
        )
        parts.append(f"<p>{chips}</p>")
        parts.append(
            '<p class="muted">Resolve these in Schema Conversion: choose SKIP '
            "(keep the existing object) or REPLACE (recreate it) when you "
            "apply.</p>"
        )
    if blocked:
        from dsql_migrator.core.target_introspector import BLOCKED_NAMES_ADVICE

        parts.append(
            f'<p class="warn">{len(blocked)} source object(s) cannot be created in either '
            "apply mode while another object holds the name on the target:</p>"
        )
        chips = " ".join(
            f'<span class="conflict">{esc(name)}: {esc(holder)}</span>'
            for name, holder in blocked
        )
        parts.append(f"<p>{chips}</p>")
        parts.append(f'<p class="muted">{esc(BLOCKED_NAMES_ADVICE)}</p>')
    if not conflicts and not blocked:
        parts.append(
            '<p class="muted">No source objects conflict with existing target '
            "objects.</p>"
        )
    parts.append("</section>")
    return "\n".join(parts)


def render_html_report(
    report: AssessmentReport,
    *,
    target: Optional[TargetInventory] = None,
    conflicts: Optional[list[str]] = None,
    source_type: SourceType = SourceType.MYSQL,
    blocked: "Optional[Sequence[tuple[str, str]]]" = None,
    target_endpoint: Optional[str] = None,
    metadata: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Render a styled, standalone HTML assessment report (English).

    The document is self-contained (inline CSS) so it can be saved and opened
    directly. It shows the classification and effort summaries and a per-object
    table with color-coded classifications.
    """
    def esc(value: object) -> str:
        return html.escape(str(value)) if value is not None else ""

    source_engine = "PostgreSQL" if source_type is SourceType.POSTGRES else "MySQL"
    report_title = f"{source_engine} to Aurora DSQL Compatibility Assessment"

    classification_summary = "".join(
        f"<li>{c.value}: <strong>{report.summary.get(c, 0)}</strong></li>"
        for c in Classification
    )
    effort_summary = "".join(
        f"<li>{level.value}: <strong>{report.effort_summary.get(level, 0)}</strong></li>"
        for level in EffortLevel
    )

    rows = []
    for item in importance_order(report.assessed_items):
        color = _HTML_CLASS_COLOR.get(item.classification, "#ffffff")
        effort = item.effort.value if item.effort is not None else "-"
        # ONE ROW PER CONCERN, with the object/kind cells spanning them. A single row
        # holding a <ul> of risks beside a <ul> of fixes still asked the reader to count
        # list positions across two cells to pair them; a row per finding puts each risk
        # physically beside its own fix, its own rule id, class and effort. The object
        # name is not repeated -- rowspan keeps the grouping visible.
        concerns = list(item.concerns or [])
        # Filter attributes go on EVERY row of the group so hiding an object hides all of
        # its findings; ``data-concern`` marks the continuation rows so the "n of m shown"
        # counter still counts OBJECTS, not findings.
        attrs = (
            f'data-kind="{esc(item.kind)}" '
            f'data-classification="{esc(item.classification.value)}" '
            f'data-effort="{esc(effort)}" '
            f'data-name="{esc(item.object_name.lower())}"'
        )
        if not concerns:
            rows.append(
                f"<tr {attrs}>"
                f"<td>{esc(item.object_name)}</td>"
                f"<td>{esc(item.kind)}</td>"
                f'<td style="background:{color}">{esc(item.classification.value)}</td>'
                f"<td>{esc(effort)}</td>"
                f"<td>{esc(item.rule_id)}</td>"
                f"<td>{esc(item.risk)}</td>"
                f"<td>{esc(item.recommendation)}</td>"
                "</tr>"
            )
            continue
        span = f' rowspan="{len(concerns)}"' if len(concerns) > 1 else ""
        for index, concern in enumerate(concerns):
            concern_color = _HTML_CLASS_COLOR.get(concern.classification, "#ffffff")
            concern_effort = (
                concern.effort.value if concern.effort is not None else "-"
            )
            cells = ""
            if index == 0:
                cells += f"<td{span}>{esc(item.object_name)}</td>"
                cells += f"<td{span}>{esc(item.kind)}</td>"
            # An advisory finding reads RECOMMENDED on the calm info-blue background
            # instead of a severity on amber/red: the conversion is complete and correct,
            # so nothing about it is a defect. Its effort is marked as the cost of opting
            # in, matching its exclusion from the object's own estimate.
            if concern.is_advisory:
                concern_color = _HTML_ADVISORY_COLOR
                class_cell = "RECOMMENDED"
                if concern_effort != "-":
                    concern_effort = f"{concern_effort} (if taken)"
            else:
                class_cell = concern.classification.value
            cells += (
                f'<td style="background:{concern_color}">'
                f"{esc(class_cell)}</td>"
                f"<td>{esc(concern_effort)}</td>"
                f"<td>{esc(concern.rule_id)}</td>"
                f"<td>{esc(concern.risk)}</td>"
                f"<td>{esc(concern.recommendation)}</td>"
            )
            marker = "" if index == 0 else ' data-concern="1"'
            rows.append(f"<tr {attrs}{marker}>{cells}</tr>")
    table_rows = "\n".join(rows) if rows else (
        '<tr><td colspan="7">No objects were assessed.</td></tr>'
    )

    # Distinct kinds present, for the Kind filter dropdown.
    kinds_present = sorted({item.kind for item in report.assessed_items})

    def _options(values: "list[str]") -> str:
        return "".join(f'<option value="{esc(v)}">{esc(v)}</option>' for v in values)

    filter_bar = (
        '<div class="filters" id="assessed-filters">\n'
        '<label>Type <select id="f-kind"><option value="">All</option>'
        f"{_options(kinds_present)}</select></label>\n"
        '<label>Classification <select id="f-class"><option value="">All</option>'
        f"{_options([c.value for c in Classification])}</select></label>\n"
        '<label>Effort <select id="f-effort"><option value="">All</option>'
        f'{_options([level.value for level in EffortLevel] + ["-"])}</select></label>\n'
        '<label>Search <input id="f-search" type="search" '
        'placeholder="object name"></label>\n'
        '<span id="f-count" class="f-count"></span>\n'
        "</div>"
    )
    filter_script = (
        "<script>\n"
        "(function(){\n"
        "function apply(){\n"
        "var k=document.getElementById('f-kind').value;\n"
        "var c=document.getElementById('f-class').value;\n"
        "var e=document.getElementById('f-effort').value;\n"
        "var q=(document.getElementById('f-search').value||'').toLowerCase();\n"
        "var rows=document.querySelectorAll('#assessed-objects tbody tr[data-kind]');\n"
        "var shown=0;\n"
        "rows.forEach(function(r){\n"
        "var ok=(!k||r.dataset.kind===k)&&(!c||r.dataset.classification===c)"
        "&&(!e||r.dataset.effort===e)"
        "&&(!q||(r.dataset.name||'').indexOf(q)>=0);\n"
        # An object with several findings now spans several rows: hide/show them all
        # together, but count only the FIRST row of each group so the counter keeps
        # reading "n of m objects" rather than jumping to the number of findings.
        "r.style.display=ok?'':'none';\n"
        "if(ok&&!r.dataset.concern)shown++;});\n"
        "var total=document.querySelectorAll("
        "'#assessed-objects tbody tr[data-kind]:not([data-concern])').length;\n"
        "var cnt=document.getElementById('f-count');\n"
        "if(cnt)cnt.textContent=shown+' of '+total+' shown';\n"
        "}\n"
        "['f-kind','f-class','f-effort'].forEach(function(id){\n"
        "var el=document.getElementById(id);if(el)el.addEventListener('change',apply);});\n"
        "var s=document.getElementById('f-search');if(s)s.addEventListener('input',apply);\n"
        "apply();\n"
        "})();\n"
        "</script>"
    )

    target_section = (
        _render_target_html_section(
            target, conflicts or [], blocked or (), target_endpoint
        )
        if target is not None
        else ""
    )
    chart_section = _render_html_chart(report)
    # What was assessed, against what, when and by which version: a shared copy has to
    # stand on its own.
    meta_section = (
        '<table class="meta">'
        + "".join(
            f"<tr><th>{esc(label)}</th><td>{esc(value)}</td></tr>" for label, value in metadata
        )
        + "</table>\n"
        if metadata
        else ""
    )

    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        f"<title>{report_title}</title>\n"
        "<style>\n"
        "body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
        "margin:24px;color:#222;}\n"
        "h1{font-size:22px;} h2{font-size:16px;margin-top:24px;} "
        "h3{font-size:14px;margin-top:16px;}\n"
        "ul{margin:4px 0;} li{margin:2px 0;}\n"
        "table{border-collapse:collapse;width:100%;margin-top:8px;font-size:13px;}\n"
        "th,td{border:1px solid #ddd;padding:6px 8px;text-align:left;"
        "vertical-align:top;}\n"
        "th{background:#f5f5f5;}\n"
        ".legend{display:flex;flex-wrap:wrap;gap:12px;margin:8px 0;font-size:12px;}\n"
        ".chip{display:inline-flex;align-items:center;gap:4px;}\n"
        ".chip i{width:12px;height:12px;border-radius:2px;display:inline-block;}\n"
        ".chart{display:flex;flex-direction:column;gap:6px;margin-top:8px;}\n"
        ".bar-row{display:flex;align-items:center;gap:8px;font-size:12px;}\n"
        ".bar-label{width:90px;flex:none;font-weight:600;}\n"
        ".bar{flex:1;display:flex;height:18px;border-radius:3px;overflow:hidden;"
        "background:#f0f0f0;}\n"
        ".bar span{display:inline-block;height:100%;}\n"
        ".bar-meta{width:150px;flex:none;color:#666;}\n"
        ".filters{display:flex;flex-wrap:wrap;gap:12px;align-items:center;"
        "margin:8px 0;font-size:13px;}\n"
        ".filters label{display:inline-flex;align-items:center;gap:4px;}\n"
        ".filters select,.filters input{font-size:13px;padding:2px 4px;}\n"
        ".f-count{color:#666;}\n"
        ".target-section{border:1px solid #e0e0e0;border-radius:6px;"
        "padding:12px 16px;margin-top:24px;background:#fafafa;}\n"
        ".target-section h3{margin-top:14px;}\n"
        ".target-section .warn{color:#b35900;} .target-section .muted{color:#666;"
        "font-size:12px;}\n"
        ".conflict{display:inline-block;border:1px solid #e0a800;color:#8a6d00;"
        "border-radius:10px;padding:1px 8px;margin:2px;font-size:12px;}\n"
        "table.meta{width:auto;} table.meta th{width:9em;}\n"
        "</style>\n</head>\n<body>\n"
        f"<h1>{report_title}</h1>\n"
        f"{meta_section}"
        "<h2>Classification summary</h2>\n"
        f"<ul>{classification_summary}</ul>\n"
        "<h2>Estimated manual effort (non-automatic objects)</h2>\n"
        f"<ul>{effort_summary}</ul>\n"
        f"{chart_section}"
        "<h2>Assessed objects</h2>\n"
        f"{filter_bar}\n"
        '<table id="assessed-objects">\n<thead><tr>'
        "<th>Object</th><th>Kind</th><th>Classification</th><th>Effort</th>"
        "<th>Rule</th><th>Risk</th><th>Recommendation</th>"
        "</tr></thead>\n<tbody>\n"
        f"{table_rows}\n"
        "</tbody>\n</table>\n"
        f"{filter_script}\n"
        f"{target_section}\n"
        "</body>\n</html>\n"
    )

