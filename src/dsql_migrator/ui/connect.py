# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connect screen: bring-your-own (BYO) source/target connection setup.

This is the preliminary step of the four-step migration workflow. The user
enters their own source (RDS / Aurora MySQL) and target (Aurora DSQL) connection
details, and can validate each connection before proceeding.

Credential handling (Property 7 / Requirement 9.2):

- The source password is held only in process memory for the current session
  (see :mod:`dsql_migrator.ui.session`) and is never persisted or logged.
- The source connection is exercised through a read-only-guarded engine, so a
  connection test can never write to the source (Property 1).
- The target (DSQL) authenticates with short-lived IAM tokens, so no password is
  collected or stored.

The pure helpers below (config building and connection testing) are independent
of NiceGUI so they can be unit tested directly; the page builder wires them to
NiceGUI widgets.
"""

from __future__ import annotations

import inspect
import re
from typing import Callable, Optional, Sequence

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from dsql_migrator.config import ConnectDefaults, SecretValue
from dsql_migrator.core.introspector import (
    SourceIntrospector,
    install_read_only_guard,
    source_engine_kwargs,
)
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.models import (
    AiAccessCheckResult,
    AiAssistConfig,
    ConnectionResult,
    SourceConnectionConfig,
    TargetConnectionConfig,
)
from dsql_migrator.core.secrets import (
    SecretResolutionError,
    SourceSecretResolver,
    resolve_source_secret,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.ui.design import (
    INLINE_HINT_TEXT,
    inline_hint,
    render_notice,
    section_header,
)
from dsql_migrator.ui.ai_assist import (
    DEFAULT_BEDROCK_MODEL_ID,
    build_ai_assist_config,
    map_access_check_display,
    run_verify_ai_access,
)
from dsql_migrator.ui.session import SessionStore

MYSQL_DRIVER = "mysql+pymysql"

# An AWS region token, e.g. "us-east-1", "ap-southeast-2", "eu-central-1".
_AWS_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d+$")

# Label for the default profile option: the standard AWS credential chain
# (default chain + AWS_PROFILE), used when no named profile is selected.
ENV_CREDENTIAL_CHAIN_LABEL = "Environment credential chain (default)"

# Source authentication methods offered on the Connect screen. The default is a
# typed username/password; the alternative resolves both from an AWS Secrets
# Manager secret (e.g. an RDS/Aurora managed secret).
AUTH_METHOD_PASSWORD = "Username & password"
AUTH_METHOD_SECRET = "AWS Secrets Manager"

# Injection seams so the page can be unit tested with fakes (no real network).
SourceTester = Callable[[SourceConnectionConfig, Optional[SecretValue]], ConnectionResult]
TargetTester = Callable[[TargetConnectionConfig, Optional[str]], ConnectionResult]
# Lists the local named AWS profiles (botocore ``available_profiles``).
ProfileLister = Callable[[], Sequence[str]]
# Runs the "Verify AI access" preflight for a config + optional global profile.
VerifyAccessRunner = Callable[[AiAssistConfig, Optional[str]], AiAccessCheckResult]
# Best-effort fetch of the source RDS instance metadata for the overview
# diagram (injectable so the page is unit tested without AWS).
InstanceInfoFetcher = Callable[
    [SourceConnectionConfig, Optional[str]], Optional["SourceInstanceInfo"]
]


def _default_instance_info_fetcher(
    config: SourceConnectionConfig, aws_profile: Optional[str]
) -> "Optional[SourceInstanceInfo]":
    """Look up the source instance class via RDS (best effort; None on any miss).

    Only attempts the RDS API for an ``*.rds.amazonaws.com`` host; a non-RDS host
    or any error (e.g. missing ``rds:DescribeDBInstances``) yields ``None`` so the
    diagram simply omits the instance size.
    """
    from dsql_migrator.core.rds_metadata import (
        build_rds_client,
        describe_source_instance,
        parse_rds_region,
    )

    region = parse_rds_region(config.host)
    if region is None:
        return None
    try:
        client = build_rds_client(aws_profile, region)
        return describe_source_instance(client, config.host)
    except Exception:  # noqa: BLE001 - metadata is optional, never fatal
        return None


# Best-effort fetch of the target DSQL cluster "Name" tag for the overview
# diagram (injectable so the page is unit tested without AWS).
ClusterNameFetcher = Callable[
    [TargetConnectionConfig, Optional[str]], Optional[str]
]


def _default_cluster_name_fetcher(
    config: TargetConnectionConfig, aws_profile: Optional[str]
) -> Optional[str]:
    """Look up the target DSQL cluster's ``Name`` tag (best effort; None on miss).

    Uses the DSQL control plane (``GetCluster`` + ``ListTagsForResource``) with
    the shared session/global profile. Any error (missing permission, untagged,
    not found) yields ``None`` so the diagram falls back to the cluster id.
    """
    from dsql_migrator.core.dsql_metadata import (
        build_dsql_client,
        fetch_dsql_cluster_name,
    )

    try:
        client = build_dsql_client(aws_profile, config.region)
        return fetch_dsql_cluster_name(client, config.cluster_endpoint)
    except Exception:  # noqa: BLE001 - metadata is optional, never fatal
        return None



def build_source_config(
    *,
    host: str,
    port: int,
    database: Optional[str] = None,
    username: Optional[str] = None,
) -> SourceConnectionConfig:
    """Build and validate a :class:`SourceConnectionConfig` from form input.

    Validation (e.g. non-empty host, port range) is enforced by the Pydantic
    model and surfaces as a ``ValidationError`` for the caller to show. The
    database is optional: a connection test only needs to reach the server, so a
    blank database is normalized to ``None`` (a specific schema is selected later
    before introspection). The password is intentionally not part of this model.
    """
    return SourceConnectionConfig(
        host=host,
        port=port,
        database=(database or "").strip() or None,
        username=username or None,
    )


def parse_region_from_endpoint(endpoint: str) -> Optional[str]:
    """Extract the AWS region from a DSQL cluster endpoint, or ``None``.

    Aurora DSQL endpoints have the form ``<cluster-id>.dsql.<region>.on.aws``
    (e.g. ``abc123.dsql.us-east-1.on.aws``), so the region is the DNS label
    immediately following the ``dsql`` segment. Returns ``None`` when the
    endpoint is blank or has no recognizable region, so the caller can leave the
    region field for manual entry instead of overwriting it with a guess.
    """
    host = (endpoint or "").strip().rstrip(".").lower()
    if not host:
        return None
    labels = host.split(".")
    for index, label in enumerate(labels[:-1]):
        if label == "dsql" or label.startswith("dsql-"):
            candidate = labels[index + 1]
            if _AWS_REGION_RE.match(candidate):
                return candidate
    return None


def build_target_config(
    *,
    cluster_endpoint: str,
    region: str,
    database: str = "postgres",
    username: str = "admin",
) -> TargetConnectionConfig:
    """Build and validate a :class:`TargetConnectionConfig` from form input."""
    return TargetConnectionConfig(
        cluster_endpoint=cluster_endpoint,
        region=region,
        database=database or "postgres",
        username=username or "admin",
    )


# ---------------------------------------------------------------------------
# Global AWS profile discovery / selection (pure helpers)
# ---------------------------------------------------------------------------


def _default_profile_lister() -> ProfileLister:
    """Return a lister backed by botocore's ``available_profiles``.

    ``botocore`` is imported lazily (mirroring the AWS-session convention) so
    importing this module needs no AWS configuration.
    """

    def lister() -> Sequence[str]:
        import botocore.session  # local import: no AWS config needed at import time

        return botocore.session.Session().available_profiles

    return lister


def discover_aws_profiles(
    *, profile_lister: Optional[ProfileLister] = None
) -> list[str]:
    """Return the local named AWS profiles, or ``[]`` if none/undiscoverable.

    The global "AWS profile" selector is shown only when this list is non-empty
    (Connect/Settings UI behavior); an empty list is the selector-not-shown
    condition. Only non-secret profile *names* are returned (Property 7), and
    any discovery failure degrades to an empty list so the Connect screen still
    renders (Requirement 9.5).
    """
    lister = (
        profile_lister if profile_lister is not None else _default_profile_lister()
    )
    try:
        profiles = list(lister())
    except Exception:  # noqa: BLE001  # pylint: disable=broad-except
        # Discovery is best-effort; degrade to no selector.
        return []
    return [name for name in profiles if isinstance(name, str) and name.strip()]


def profile_selector_options(profiles: Sequence[str]) -> list[str]:
    """Return selector options with the env-credential-chain default first.

    The default option maps back to ``None`` (the standard credential chain) via
    :func:`normalize_profile_selection`.
    """
    return [ENV_CREDENTIAL_CHAIN_LABEL, *profiles]


def normalize_profile_selection(value: Optional[str]) -> Optional[str]:
    """Map a selector value to a non-secret profile name, or ``None``.

    The default option (or any blank value) maps to ``None``, meaning the
    standard AWS credential chain is used (Requirement 9.6). Otherwise the
    selected profile name is returned unchanged (it is non-secret -- Property 7).
    """
    if not value or value == ENV_CREDENTIAL_CHAIN_LABEL:
        return None
    return value


def connection_status_badge(verified: bool) -> tuple[str, str]:
    """Return the ``(label, Quasar color)`` for a connection's verified state.

    Drives the per-connection status badge on the Connect screen so the verdict
    is scannable at the card level and -- unlike the transient status label and
    notification -- survives a page rebuild (it is derived from the persisted
    session ``*_verified`` flag, not from a one-off text label).
    """
    if verified:
        return "Verified", "positive"
    return "Not verified", "grey"


def make_source_engine_factory(
    password: Optional[SecretValue],
    *,
    read_timeout_seconds: Optional[int] = None,
) -> Callable[[SourceConnectionConfig], Engine]:
    """Build an engine factory that injects the in-memory ``password``.

    The returned factory builds a MySQL engine and installs the read-only guard,
    so any introspection or connection test performed through it cannot write to
    the source (Property 1). The plaintext password is read from the
    :class:`SecretValue` only here, at connect time, and is never stored on the
    connection config (Property 7).

    ``read_timeout_seconds`` (opt-in) bounds each socket read/write so a
    connected-but-stalled stream raises instead of blocking forever. Pass it for
    the Full Load source stream (bounded keyset pages); leave it ``None`` for
    introspection/validation, whose single long queries must not be killed by a
    per-read deadline.
    """

    def factory(conn: SourceConnectionConfig) -> Engine:
        url = URL.create(
            MYSQL_DRIVER,
            username=conn.username,
            password=password.reveal() if password is not None else None,
            host=conn.host,
            port=conn.port,
            database=conn.database,
        )
        # Shared engine settings (pool_pre_ping + a bounded connect_timeout) so an
        # unreachable source fails fast instead of hanging the connection test /
        # introspection / validation (which would otherwise spin with no
        # interruptible point). The Full Load path also opts into a per-socket
        # read/write timeout so a mid-stream stall fails the table (retryable)
        # rather than hanging the job in RUNNING forever.
        engine = create_engine(
            url, **source_engine_kwargs(read_timeout_seconds=read_timeout_seconds)
        )
        install_read_only_guard(engine)
        return engine

    return factory


def check_source_connection(
    config: SourceConnectionConfig,
    password: Optional[SecretValue],
    *,
    introspector: Optional[SourceIntrospector] = None,
) -> ConnectionResult:
    """Validate the source connection using a read-only-guarded introspector.

    The password is supplied in memory via a per-session engine factory; on
    failure the introspector returns a reason with the credential redacted
    (Requirement 1.4 / 9.2).
    """
    introspector = introspector or SourceIntrospector(
        engine_factory=make_source_engine_factory(password)
    )
    return introspector.test_connection(config)


def check_target_connection(
    config: TargetConnectionConfig,
    aws_profile: Optional[str] = None,
    *,
    connector_factory: Optional[Callable[[TargetConnectionConfig], DsqlConnector]] = None,
) -> ConnectionResult:
    """Validate the target DSQL connection using an IAM-token connector.

    No password is collected; the connector generates a short-lived IAM token at
    connect time and redacts it from any failure message (Property 7).

    ``aws_profile`` MUST be threaded through so the IAM token is generated under
    the SAME identity the rest of the workflow (Evaluation / Schema Conversion /
    Data Migration) uses; otherwise the test can pass/fail under the env-default
    identity while later steps connect under the selected profile -- a misleading
    "Verified" status (Requirement 9.5/9.7, one credential context).
    """
    connector = (
        connector_factory(config)
        if connector_factory is not None
        else DsqlConnector(config, aws_profile=aws_profile)
    )
    return connector.test_connection()


def build_connect_page(
    store: SessionStore,
    session_id: str,
    *,
    source_tester: SourceTester = check_source_connection,
    target_tester: TargetTester = check_target_connection,
    profile_lister: Optional[ProfileLister] = None,
    verify_runner: VerifyAccessRunner = run_verify_ai_access,
    secret_resolver: SourceSecretResolver = resolve_source_secret,
    instance_info_fetcher: InstanceInfoFetcher = _default_instance_info_fetcher,
    cluster_name_fetcher: ClusterNameFetcher = _default_cluster_name_fetcher,
    on_next: Optional[Callable[[], None]] = None,
    on_connection_change: Optional[Callable[[], None]] = None,
    defaults: Optional[ConnectDefaults] = None,
) -> None:
    """Render the Connect screen for one session.

    Source/target details are entered separately and validated on demand. The
    source password is kept only in the session's in-memory state; the target
    uses IAM tokens and collects no password. A single optional global AWS
    profile selector is shown only when local named profiles exist, and the
    AI-assist section offers a non-blocking "Verify AI access" preflight.

    A "Next" button advances to the migration workflow, but it is locked until
    BOTH connection tests have succeeded; a failed test (or editing a verified
    connection) re-locks it. ``on_next`` is invoked when the user clicks Next;
    when ``None`` the button still gates on connection readiness but performs no
    navigation (e.g. in tests). ``on_connection_change`` is invoked whenever the
    verified state flips, so the caller (the sidebar) can refresh the
    workflow-step lock state live. ``defaults`` optionally prefills the form
    fields (dev convenience, e.g. from a local ``.env``); prefilled values are
    not auto-verified, so the user must still run the connection tests.
    """
    from nicegui import run, ui

    def _note(icon: str, text: str, icon_class: str = "text-gray-500") -> None:
        """Render a compact icon + short-text note row (shared Connect tone).

        Used for section-level guidance across Connect so every explanatory blurb
        scans the same way (small icon + one concise sentence) instead of long
        stacked gray paragraphs. Field-level hints stay as plain ``text-xs``.
        """
        with ui.row().classes("items-start gap-2 no-wrap w-full"):
            ui.icon(icon).classes(f"{icon_class} text-base mt-0.5")
            ui.label(text).classes("text-sm text-gray-600")

    def _section_header(icon: str, title: str, badge):
        """Section header via the shared design system (see ui.design).

        Thin adapter so this page's call sites keep their (icon, title, badge)
        shape; the AWS-console header band itself lives in one place.
        """
        return section_header(ui, icon=icon, title=title, badge=badge)

    def _info_callout(header: str, body: str) -> None:
        """Info callout via the shared design system (Cloudscape "Alert", info tone)."""
        render_notice(ui, tone="info", header=header, body=body)

    state = store.get_or_create(session_id)
    # Dev-only prefill values (blank/normal defaults when not provided).
    d = defaults or ConnectDefaults()

    # The form is a view of the session's connection state: when a connection was
    # already entered this session, prefill the (non-secret) fields from it so
    # returning to Connect (e.g. just to toggle AI assist) shows the real, still
    # verified connection instead of blank defaults -- the user does not have to
    # re-enter or re-test. The in-memory password/secret is intentionally not
    # re-shown (Property 7); a prior successful test stays valid regardless.
    _sc = state.source_config
    _tc = state.target_config

    def _eff(value: object, fallback: object) -> object:
        return value if value not in (None, "") else fallback

    src_host = _eff(getattr(_sc, "host", None), d.source_host or "")
    src_port = _eff(getattr(_sc, "port", None), d.source_port or 3306)
    src_database = _eff(getattr(_sc, "database", None), d.source_database)
    src_username = _eff(getattr(_sc, "username", None), d.source_username)
    tgt_endpoint = _eff(getattr(_tc, "cluster_endpoint", None), d.target_endpoint or "")
    tgt_region = _eff(
        getattr(_tc, "region", None),
        d.target_region or parse_region_from_endpoint(d.target_endpoint or "") or "",
    )
    # No tgt_database default: Aurora DSQL's single database ("postgres") is fixed
    # by build_target_config, so the form has no database field.
    tgt_username = _eff(getattr(_tc, "username", None), d.target_username or "admin")

    # Seed the AI-assist model id / region from the environment default
    # (BEDROCK_MODEL_ID / BEDROCK_REGION), so the form shows what the deployment
    # configured without overriding any user change.
    #
    # The test is per-FIELD: the model id is seeded while it still holds the app's built-in
    # default, and the region while it is unset. Comparing the whole config to
    # AiAssistConfig() instead -- as this did -- meant that merely flipping the Enable
    # switch made the config unequal and silently blocked the seed on every later render,
    # so a deployment that set BEDROCK_MODEL_ID could still end up invoking the app's
    # built-in default. That is invisible until Bedrock rejects the call, because the IAM
    # scope is derived from the deployment's value, not the app's.
    if d.bedrock_model_id and state.ai_assist.model_id == DEFAULT_BEDROCK_MODEL_ID:
        state.set_ai_assist(
            state.ai_assist.model_copy(update={"model_id": d.bedrock_model_id})
        )
    if d.bedrock_region and state.ai_assist.region is None:
        state.set_ai_assist(
            state.ai_assist.model_copy(update={"region": d.bedrock_region})
        )

    # The Next button/hint are created at the bottom; these closures update the
    # gate from the test and input handlers without a forward reference.
    next_button = None
    next_hint = None
    # Per-connection status badges (created in each card); updated from state so
    # the verified verdict survives a page rebuild (unlike the status label).
    source_badge = None
    target_badge = None
    # Track readiness so on_connection_change fires only on an actual change.
    last_ready = {"value": state.connection_ready()}

    async def run_busy(button: object, action: Callable[[], object]) -> None:
        """Run an async ``action`` while ``button`` shows a busy/disabled state.

        Prevents a double-submit of a connection/AI test and gives a visible
        in-progress cue. The button is restored in ``finally`` unless the page
        was rebuilt while awaiting (the element is then deleted -- NiceGUI #3028).

        Busy cue is the disabled state, NOT Quasar's button ``loading`` prop: the
        loading spinner overlays the button's border and reads as a "spinning
        border" artifact (especially on outline/flat buttons). Disabling the
        button is the artifact-free in-progress indicator we use app-wide.
        """
        button.disable()  # type: ignore[attr-defined]
        try:
            result = action()
            if inspect.isawaitable(result):
                await result
        finally:
            if not getattr(button, "is_deleted", False):
                button.enable()  # type: ignore[attr-defined]

    def update_next_state() -> None:
        """Enable Next only when both connections are verified; else lock it."""
        ready = state.connection_ready()
        # Refresh the per-connection status badges from the persisted flags.
        if source_badge is not None:
            label, color = connection_status_badge(state.source_verified)
            source_badge.set_text(label)
            source_badge.props(f"color={color}")
        if target_badge is not None:
            label, color = connection_status_badge(state.target_verified)
            target_badge.set_text(label)
            target_badge.props(f"color={color}")
        if next_button is not None and next_hint is not None:
            next_button.set_enabled(ready)
            next_hint.set_text(
                "Source and target connections verified. You can continue."
                if ready
                else "Test the source and target connections successfully to "
                "unlock the next step."
            )
        # Notify the sidebar to refresh its step lock state, but only when the
        # readiness actually changed (avoids churn on every keystroke).
        if on_connection_change is not None and ready != last_ready["value"]:
            last_ready["value"] = ready
            on_connection_change()

    def invalidate_source(_event: object = None) -> None:
        """Re-lock Next when the source details change (must be re-tested)."""
        state.set_source_verified(False)
        update_next_state()

    def invalidate_target(_event: object = None) -> None:
        """Re-lock Next when the target details change (must be re-tested)."""
        state.set_target_verified(False)
        update_next_state()

    with ui.column().classes("w-full max-w-3xl gap-4"):
        ui.label("Connect").classes("text-2xl font-bold")

        # First-run orientation: a brand-new user lands here with no context.
        # Collapsed by default so it never slows a returning user, but given a
        # tinted, bordered, primary-icon header so a new user actually NOTICES the
        # "New here?" affordance instead of skipping past a plain gray row.
        with ui.expansion(
            "New here? What this tool does & the steps ahead", icon="help_outline"
        ).classes(
            "w-full rounded-md border border-blue-200 bg-blue-50 "
            "text-blue-800 font-medium"
        ).props("header-class=text-blue-800"):
            ui.label(
                "This tool migrates a MySQL database (RDS / Aurora MySQL) to "
                "Amazon Aurora DSQL. You connect both ends here, then move through "
                "five steps:"
            ).classes("text-sm text-gray-600")
            ui.label(
                "1. Evaluation — read-only compatibility assessment of your schema.\n"
                "2. Schema Conversion — review/apply the converted DDL on DSQL.\n"
                "3. Data Migration — choose Full Load (one-shot copy) or add CDC "
                "(continuous streaming for a near-zero-downtime cutover), then run it.\n"
                "4. Validation — compare the migrated target against the source.\n"
                "5. Cut over — the runbook for switching your application to DSQL."
            ).classes("text-sm text-gray-600 whitespace-pre-line")
            # AWS-console-style info callout (tinted, bordered, info icon + bold
            # header) so the glossary reads as a deliberate "good to know" panel,
            # not a faint afterthought.
            _info_callout(
                "Terms to know",
                'A "watermark" is the exact source position the snapshot captured, '
                "so CDC can resume from it with no gap or duplicate. "
                '"CDC" = Change Data Capture (streaming ongoing changes).',
            )

        # One-line, low-weight reassurance (Property 7). Kept compact (inline hint,
        # not a full note row) so the primary Source/Target inputs stay near the
        # top instead of being pushed down by stacked guidance.
        inline_hint(
            ui,
            "Credentials are kept only in memory for this session — never written "
            "to logs, reports, or job state.",
            tone="neutral",
            classes="text-xs flex items-center gap-1",
        )

        # The optional AWS-profile selector is rendered just above the Target card
        # (see below), where it actually applies (DSQL token, Secrets Manager,
        # Bedrock); MySQL source auth does not use it. Discover profiles once here.
        profiles = discover_aws_profiles(profile_lister=profile_lister)

        def _render_aws_profile_card() -> None:
            """Render the optional global AWS-profile selector (when profiles exist).

            Placed next to the Target/AWS-auth context rather than at the very top:
            it is the credential identity for ALL AWS calls (DSQL token, Secrets
            Manager, Bedrock) (Requirements 9.5, 9.6, 9.8), but MySQL source auth
            does not use it, so leading with it confused the Source→Target flow.
            Collapsed by default; only the non-secret profile name is stored
            (Property 7).
            """
            if not profiles:
                return
            with ui.card().classes("w-full"):
                _section_header("badge", "AWS profile (optional)", None)
                with ui.expansion(
                    "Choose the AWS identity for DSQL, Secrets Manager & Bedrock"
                ).classes("w-full").props("dense"):
                    _note(
                        "key",
                        "Applies to all AWS calls (DSQL token, Secrets Manager, "
                        "Bedrock). Default is your environment credential chain; "
                        "cross-account is handled by the profile's ~/.aws/config.",
                    )
                    current_profile = state.aws_profile or ENV_CREDENTIAL_CHAIN_LABEL

                    def on_profile_change(event: object) -> None:
                        selected = normalize_profile_selection(
                            getattr(event, "value", None)
                        )
                        state.set_aws_profile(selected)
                        chosen = selected or "environment credential chain"
                        ui.notify(f"Using AWS profile: {chosen}.", type="positive")

                    ui.select(
                        profile_selector_options(profiles),
                        value=current_profile,
                        label="AWS profile",
                        on_change=on_profile_change,
                    ).classes("w-full")

        # --- Source: RDS / Aurora MySQL -----------------------------------
        with ui.card().classes("w-full"):
            source_badge = _section_header(
                "storage",
                "Source (RDS / Aurora MySQL)",
                connection_status_badge(state.source_verified),
            )
            source_host = ui.input("Host", value=src_host or "").classes(
                "w-full"
            )
            source_port = ui.number(
                "Port", value=src_port or 3306, format="%d"
            ).classes("w-full")
            # The "optional / scope" guidance lives on the field itself (Quasar
            # ``hint``) so it reads as part of the Database input, not as a separate
            # gray line below it.
            source_database = ui.input(
                "Database (optional)", value=src_database or ""
            ).classes("w-full").props(
                'hint="Empty assesses the whole cluster; set it to scope to a '
                'single database."'
            )

            # Authentication method: type a username/password, or resolve both
            # from an AWS Secrets Manager secret (e.g. an RDS/Aurora managed
            # secret). The secret is read with the AWS profile selected above, so
            # no password is typed or held beyond this session (Property 7).
            ui.label("Authentication").classes("text-sm font-medium")
            source_auth_method = ui.radio(
                [AUTH_METHOD_PASSWORD, AUTH_METHOD_SECRET],
                value=AUTH_METHOD_PASSWORD,
            ).props("inline")

            manual_auth = ui.column().classes("w-full gap-2")
            with manual_auth:
                source_username = ui.input(
                    "Username", value=src_username or ""
                ).classes("w-full")
                source_password = ui.input(
                    "Password",
                    value=d.source_password.reveal() if d.source_password else "",
                    password=True,
                    password_toggle_button=True,
                ).classes("w-full")

            secret_auth = ui.column().classes("w-full gap-2")
            with secret_auth:
                # The ARN example lives on the field as a Quasar ``hint`` (gray,
                # below the field) so it stays visible WHILE typing -- a reference
                # for the expected format, unlike a placeholder that vanishes on
                # the first keystroke.
                source_secret_id = ui.input(
                    "Secrets Manager secret ARN or name",
                    value="",
                    placeholder="arn:aws:secretsmanager:us-east-1:...:secret:my-db",
                ).props(
                    "hint=\"Example: arn:aws:secretsmanager:us-east-1:123456789012"
                    ":secret:my-db-AbCdEf  (a bare name also works)\""
                ).classes("w-full")
                ui.label(
                    "Secret must hold 'username' and 'password' (RDS/Aurora "
                    "style). Resolved at test time with the AWS profile above and "
                    "kept only in memory; region comes from the ARN when given."
                ).classes("text-xs text-gray-500")

            source_status = ui.label().classes("text-sm")

            def on_auth_method_change(_event: object = None) -> None:
                # Show only the inputs for the chosen method, and re-lock Next so
                # the user re-tests after switching credentials.
                use_secret = source_auth_method.value == AUTH_METHOD_SECRET
                manual_auth.set_visibility(not use_secret)
                secret_auth.set_visibility(use_secret)
                # Only invalidate on an actual user change (``_event`` present),
                # not the initial visibility setup on (re)build -- otherwise just
                # reopening Connect (e.g. to toggle AI assist) would wipe a prior
                # successful source test and force a needless re-check.
                if _event is not None:
                    invalidate_source()

            source_auth_method.on_value_change(on_auth_method_change)
            # Set the initial visibility for the default method.
            on_auth_method_change()

            # Editing any source field invalidates a prior successful test, so
            # the Next gate cannot pass with untested credentials.
            for _source_input in (
                source_host,
                source_port,
                source_database,
                source_username,
                source_password,
                source_secret_id,
            ):
                _source_input.on_value_change(invalidate_source)

            def fail_source(message: str) -> None:
                """Show a failure message and re-lock the Next gate."""
                if not source_status.is_deleted:
                    source_status.set_text(message)
                    source_status.classes(
                        replace=f"text-sm {INLINE_HINT_TEXT['error']}"
                    )
                    ui.notify(message, type="negative")
                state.set_source_verified(False)
                update_next_state()

            async def on_test_source() -> None:
                use_secret = source_auth_method.value == AUTH_METHOD_SECRET
                secret_id = ""
                if use_secret:
                    secret_id = (source_secret_id.value or "").strip()
                    source_status.set_text("Resolving secret...")
                    # For a bare secret NAME (no region in the id), resolve it in the
                    # source DB's region (parsed from the source host) rather than the
                    # session's default region -- a profile whose default region differs
                    # would otherwise return ResourceNotFound for a secret that exists.
                    # A full ARN carries its own region and ignores this.
                    from dsql_migrator.core.rds_metadata import parse_rds_region

                    secret_region = parse_rds_region((source_host.value or "").strip())
                    try:
                        username_value, password = await run.io_bound(
                            lambda: secret_resolver(
                                secret_id, state.aws_profile, region=secret_region
                            )
                        )
                    except SecretResolutionError as exc:
                        fail_source(str(exc))
                        return
                    except Exception:  # noqa: BLE001  # pylint: disable=broad-except
                        # Unexpected failure: degrade gracefully in the UI.
                        fail_source(
                            "Could not resolve the source secret. Check the "
                            "secret ARN/name, region, and AWS permissions."
                        )
                        return
                else:
                    username_value = (source_username.value or "").strip() or None
                    password = (
                        SecretValue(source_password.value)
                        if source_password.value
                        else None
                    )

                try:
                    config = build_source_config(
                        host=(source_host.value or "").strip(),
                        port=int(source_port.value or 0),
                        database=(source_database.value or "").strip(),
                        username=username_value,
                    )
                except (ValueError, TypeError):
                    fail_source("Please check the source connection fields.")
                    return

                # Keep credentials in memory for this session only.
                state.set_source(config, password)

                source_status.set_text("Testing source connection...")
                result = await run.io_bound(source_tester, config, password)
                # Record the verdict before any UI update (state is always safe).
                state.set_source_verified(result.success)
                # Remember the source secret reference (ARN/name) ONLY when the
                # connection used Secrets Manager auth and the test passed, so the
                # CDC deploy can auto-fill SourceSecretArn/Name. Clear it otherwise
                # so switching to username/password auth drops a stale reference.
                state.set_source_secret_id(
                    secret_id if (use_secret and result.success) else None
                )
                log_activity(
                    ActivityCategory.CONNECTION,
                    "test source connection",
                    status=(
                        ActivityStatus.SUCCESS
                        if result.success
                        else ActivityStatus.FAILURE
                    ),
                    target=f"{config.host}/{config.database}",
                    detail=result.detail,
                )
                # Capture the source server version (e.g. Aurora MySQL version)
                # for the overview diagram; None on failure.
                state.set_source_version(
                    result.server_version,
                    result.mysql_version,
                    result.aurora_version,
                )
                # Best-effort RDS instance class (e.g. db.r6g.large) for the
                # diagram; only attempted on success and ignored on any error.
                if result.success:
                    info = await run.io_bound(
                        instance_info_fetcher, config, state.aws_profile
                    )
                    state.set_source_instance_class(
                        info.instance_class if info is not None else None
                    )
                # The page may have been rebuilt while awaiting; skip UI updates
                # on a deleted element to avoid touching a stale slot (NiceGUI
                # #3028). The rebuilt page reflects the persisted state.
                if source_status.is_deleted:
                    return
                source_status.set_text(result.detail)
                source_status.classes(
                    replace="text-sm "
                    + INLINE_HINT_TEXT["success" if result.success else "error"]
                )
                ui.notify(
                    result.detail,
                    type="positive" if result.success else "negative",
                )
                update_next_state()

            source_test_button = ui.button("Test source connection")
            source_test_button.on_click(
                lambda: run_busy(source_test_button, on_test_source)
            )

        # The optional global AWS-profile selector sits here -- directly above the
        # Target, where the AWS identity is actually used (DSQL token, and below it
        # Secrets Manager / Bedrock) -- not at the very top above the Source.
        _render_aws_profile_card()

        # --- Target: Aurora DSQL ------------------------------------------
        with ui.card().classes("w-full"):
            target_badge = _section_header(
                "cloud",
                "Target (Aurora DSQL)",
                connection_status_badge(state.target_verified),
            )
            # Before-you-connect prerequisite (action-required) -> a real alert box,
            # not faint gray text, so it is not missed: the cluster is NOT created
            # here and the identity must already be authorized.
            render_notice(
                ui,
                tone="warning",
                header="The DSQL cluster must already exist",
                body=(
                    "Create the cluster in the Aurora DSQL console, copy its "
                    "endpoint below, and make sure your identity holds "
                    "dsql:DbConnectAdmin on it before you connect."
                ),
            )
            # IAM access model in one compact line (no password != open access):
            # auth uses short-lived IAM tokens via the AWS profile / credential
            # chain, gated by that identity's permissions (Req 9.5/9.7/9.9).
            _note(
                "vpn_key",
                "Short-lived IAM token auth (no password): the AWS profile / "
                "credential chain needs dsql:DbConnect[Admin] on this cluster; "
                "for a specific role use a named profile with role_arn.",
                "text-sky-600",
            )
            with ui.row().classes("items-center gap-3 no-wrap"):
                ui.link(
                    "Create an Aurora DSQL cluster",
                    "https://docs.aws.amazon.com/aurora-dsql/latest/userguide/getting-started.html",
                    new_tab=True,
                ).classes("text-xs")
                ui.link(
                    "Configure an AWS profile or IAM role",
                    "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html",
                    new_tab=True,
                ).classes("text-xs")
            target_endpoint = ui.input(
                "Cluster endpoint", value=tgt_endpoint or ""
            ).classes("w-full")
            target_region = ui.input(
                "AWS region",
                value=tgt_region or "",
            ).classes("w-full")
            # Aurora DSQL exposes a single database ("postgres"); show it read-only
            # so the destination is explicit, but it is not user-editable. The
            # value is fixed by build_target_config (no input is read from here).
            # The "why it's fixed" note lives on the field as a Quasar ``hint``
            # (a read-only field is always populated, so a placeholder would never
            # show -- a persistent hint is the right fit here).
            ui.input("Database", value="postgres").props(
                "readonly hint=\"Aurora DSQL has a single database -- migration "
                "always targets 'postgres'. This field is read-only.\""
            ).classes("w-full")
            target_username = ui.input(
                "Username", value=tgt_username or "admin"
            ).classes("w-full")
            target_status = ui.label().classes("text-sm")

            def on_endpoint_change(event: object) -> None:
                # Auto-fill the region by parsing the DSQL endpoint, so the user
                # does not have to repeat it. Leave the field untouched when the
                # endpoint has no recognizable region (Usability first).
                region = parse_region_from_endpoint(getattr(event, "value", "") or "")
                if region and region != (target_region.value or ""):
                    target_region.set_value(region)
                invalidate_target()

            target_endpoint.on_value_change(on_endpoint_change)
            # Editing any other target field also invalidates a prior test.
            for _target_input in (target_region, target_username):
                _target_input.on_value_change(invalidate_target)

            async def on_test_target() -> None:
                try:
                    config = build_target_config(
                        cluster_endpoint=(target_endpoint.value or "").strip(),
                        region=(target_region.value or "").strip(),
                        # Aurora DSQL has a single database; build_target_config
                        # defaults it to "postgres" -- no user input needed.
                        username=(target_username.value or "").strip() or "admin",
                    )
                except (ValueError, TypeError):
                    target_status.set_text("Please check the target connection fields.")
                    target_status.classes(
                        replace=f"text-sm {INLINE_HINT_TEXT['error']}"
                    )
                    ui.notify("Invalid target connection settings.", type="negative")
                    state.set_target_verified(False)
                    update_next_state()
                    return

                state.set_target(config)

                target_status.set_text("Testing target connection...")
                # Pass the selected AWS profile so the IAM token is generated under
                # the same identity the rest of the workflow uses (see
                # check_target_connection); a profile-less test misleads.
                result = await run.io_bound(target_tester, config, state.aws_profile)
                state.set_target_verified(result.success)
                log_activity(
                    ActivityCategory.CONNECTION,
                    "test target connection",
                    status=(
                        ActivityStatus.SUCCESS
                        if result.success
                        else ActivityStatus.FAILURE
                    ),
                    target=config.cluster_endpoint,
                    detail=result.detail,
                )
                # Best-effort DSQL cluster "Name" tag for the diagram; only on
                # success and ignored on any error (falls back to cluster id).
                if result.success:
                    cluster_name = await run.io_bound(
                        cluster_name_fetcher, config, state.aws_profile
                    )
                    state.set_target_cluster_name(cluster_name)
                if target_status.is_deleted:
                    return
                target_status.set_text(result.detail)
                target_status.classes(
                    replace="text-sm "
                    + INLINE_HINT_TEXT["success" if result.success else "error"]
                )
                ui.notify(
                    result.detail,
                    type="positive" if result.success else "negative",
                )
                update_next_state()

            target_test_button = ui.button("Test target connection")
            target_test_button.on_click(
                lambda: run_busy(target_test_button, on_test_target)
            )

        # --- AI-assisted conversion (optional, augmenting) -----------------
        with ui.card().classes("w-full"):
            # A configured named AWS profile is a strong signal the operator can
            # reach AWS (and likely Bedrock), so surface -- but never auto-enable
            # -- AI assist: expand the optional section by default. AI assist
            # stays opt-in / off by default (Requirement 11.1/11.2) because a
            # profile alone does not guarantee bedrock:InvokeModel access.
            ai_profile_configured = state.aws_profile is not None
            # Card header band (like the Source / Target / AWS-profile cards) so
            # this reads as one more step in the same console flow. The "Optional"
            # badge sets the expectation before the user reads a single word.
            _section_header(
                "auto_awesome", "AI Assist", ("Optional", "grey")
            )
            # The single sentence that says WHAT it is -- kept above the toggle so
            # the value proposition is read before the decision. AI Assist spans
            # the whole journey (per-object guidance in Evaluation, conversion
            # suggestions, and the AI chat), so the copy is no longer scoped to
            # "conversion" only.
            ui.label(
                "Augment the deterministic workflow with Amazon Bedrock help — "
                "per-object guidance, conversion suggestions for objects marked "
                "MANUAL or UNSUPPORTED, and the AI chat."
            ).classes("text-sm text-gray-700")

            # The decision itself, front and centre: a single switch, not buried
            # under a wall of caveats. The caveats live in the grouped panel below.
            ai_enabled = ui.switch(
                "Enable AI Assist", value=state.ai_assist.enabled
            )
            # When a profile is configured but AI is still off, one info notice
            # nudges -- replacing the old free-floating "lightbulb" prose row.
            if ai_profile_configured and not state.ai_assist.enabled:
                _info_callout(
                    "Ready to use with your AWS profile",
                    f"AWS profile '{state.aws_profile}' is configured, so AI "
                    "Assist is available -- switch it on above when you want it.",
                )

            def _ai_note(icon: str, text: str, icon_class: str) -> None:
                _note(icon, text, icon_class)

            # The three caveats, grouped into ONE compact bordered panel so they
            # read as a single "good to know" reference block instead of three
            # loose gray rows competing with the controls around them.
            with ui.column().classes(
                "w-full gap-2 rounded-md border border-gray-200 bg-gray-50 p-3"
            ):
                _ai_note(
                    "verified_user",
                    "Opt-in and off by default -- the deterministic path always "
                    "runs first; AI never replaces it.",
                    "text-sky-600",
                )
                _ai_note(
                    "rate_review",
                    "Calls Amazon Bedrock (may add cost and latency); every "
                    "suggestion is review-only and applied only after you approve it.",
                    "text-amber-700",
                )
                _ai_note(
                    "key",
                    "Uses the AWS profile above (or your environment credential "
                    "chain) and needs the bedrock:InvokeModel permission.",
                    "text-gray-500",
                )

            # Bedrock settings, collapsed by default so the optional model/region
            # detail never pushes the primary Enable toggle or the Next action
            # down the page; expanded when AI is already enabled or a named AWS
            # profile is configured. Each field's guidance lives ON the field as a
            # Quasar ``hint`` (the same pattern as the Source/Target inputs above),
            # so the long gray paragraphs become part of the inputs they explain.
            with ui.expansion(
                "Bedrock settings",
                icon="tune",
                value=state.ai_assist.enabled or ai_profile_configured,
            ).classes("w-full").props("dense"):
                ai_model = ui.input(
                    "Model ID (BEDROCK_MODEL_ID)",
                    value=state.ai_assist.model_id,
                    placeholder="global.anthropic.claude-sonnet-5",
                ).props(
                    "hint=\"Use the exact model / inference-profile ID, not the "
                    "display name (e.g. global.anthropic.claude-sonnet-5).\""
                ).classes("w-full")
                ai_region = ui.input(
                    "Region (BEDROCK_REGION, optional)",
                    value=state.ai_assist.region or "",
                    placeholder="us-east-1",
                ).props(
                    "hint=\"Blank uses the region from your AWS profile; set it "
                    "when the model lives in a different region.\""
                ).classes("w-full")
                ui.link(
                    "Bedrock model IDs (AWS docs)",
                    "https://docs.aws.amazon.com/bedrock/latest/userguide/model-cards.html",
                    new_tab=True,
                ).classes("text-xs")

            ai_status = ui.label().classes("text-sm")

            def persist_ai_settings(_event: object = None) -> None:
                """Persist AI settings on every change (no explicit Save).

                Reads the current form values straight into the session so a
                user who toggles "Enable" (or edits the model/region) and
                proceeds never loses that intent. The form values are not
                written back here, so typing in the model/region fields is
                never disrupted.
                """
                state.set_ai_assist(
                    build_ai_assist_config(
                        enabled=bool(ai_enabled.value),
                        model_id=ai_model.value,
                        region=ai_region.value,
                    )
                )

            ai_enabled.on_value_change(persist_ai_settings)
            ai_model.on_value_change(persist_ai_settings)
            ai_region.on_value_change(persist_ai_settings)

            async def on_verify_ai_access() -> None:
                # Verify against what is currently in the form (already
                # persisted on change). Uses the same global profile/session
                # as every other AWS client (Requirement 9.5).
                config = build_ai_assist_config(
                    enabled=bool(ai_enabled.value),
                    model_id=ai_model.value,
                    region=ai_region.value,
                )
                state.set_ai_assist(config)
                ai_model.set_value(config.model_id)

                ai_status.classes(replace=f"text-sm {INLINE_HINT_TEXT['neutral']}")
                ai_status.set_text("Verifying AI access...")
                # verify_access is non-blocking (never raises); run it off the
                # event loop like the connection tests.
                result = await run.io_bound(
                    verify_runner, config, state.aws_profile
                )
                # Pass the configured model so a pass that landed on a fallback is
                # shown as a warning, not as a clean green success.
                display = map_access_check_display(
                    result, configured_model_id=config.model_id
                )
                # Skip UI updates if the page was rebuilt while awaiting
                # (NiceGUI #3028): the element/slot may have been deleted.
                if ai_status.is_deleted:
                    return
                # Carry the verdict severity into the persistent status line (not
                # only the transient toast): route the notify type through the
                # design-system inline-hint palette instead of leaving it as bare
                # gray text with no severity cue.
                tone = {
                    "positive": "success",
                    "negative": "error",
                    "warning": "warning",
                }.get(display.notify_type, "neutral")
                ai_status.classes(replace=f"text-sm {INLINE_HINT_TEXT[tone]}")
                ai_status.set_text(display.message)
                ui.notify(display.message, type=display.notify_type)

            # Verify button + the "auto-saved" reassurance on one row, so the
            # action and its caption sit together at the foot of the card.
            with ui.row().classes("items-center gap-3 no-wrap"):
                ai_verify_button = ui.button("Verify AI access")
                inline_hint(
                    ui, "Settings are saved automatically.", tone="neutral"
                )
            ai_verify_button.on_click(
                lambda: run_busy(ai_verify_button, on_verify_ai_access)
            )

        # --- Continue to the migration workflow ---------------------------
        # Next is locked until BOTH connections have been verified by a
        # successful test; a failed test or an edit to a verified connection
        # re-locks it (Usability first: visible gate with clear guidance).
        ui.separator()
        next_hint = ui.label().classes("text-sm text-gray-500")

        def on_next_click() -> None:
            # Defensive: only navigate when truly ready, even if the button was
            # somehow clicked while disabled.
            if on_next is not None and state.connection_ready():
                on_next()

        next_button = ui.button("Next: Evaluation", on_click=on_next_click)
        update_next_state()


__all__ = [
    "build_source_config",
    "build_target_config",
    "parse_region_from_endpoint",
    "make_source_engine_factory",
    "check_source_connection",
    "check_target_connection",
    "discover_aws_profiles",
    "profile_selector_options",
    "normalize_profile_selection",
    "connection_status_badge",
    "ENV_CREDENTIAL_CHAIN_LABEL",
    "build_connect_page",
]
