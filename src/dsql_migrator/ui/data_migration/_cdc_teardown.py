# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Start over's CDC teardown and the cross-view teardown banner: the page's teardown hooks.

:func:`build_cdc_teardown_hooks` builds them for one browser session, and ``build_page``
(``ui/app.py``) hands them to ``build_workflow_sidebar``. Start over refreshes the session's
CDC state with a read-only probe, asks them whether any CDC infrastructure exists, which
cdc-stacks a teardown would act on and whether a teardown or a deploy is still running, and
submits the stop or delete the operator confirms (``on_reset_cdc``). The banner that follows a
teardown on every view, and the notice it leaves once the teardown finishes, read it from them;
their Retry cleanup and dismiss buttons call them.

They were closures inside ``build_page``, which a test could reach only through the whole page
or by parsing app.py. The factory takes what they read there -- the session id, the session
and Data Migration stores and the job manager (app.py's ``SESSION_STORE``,
``DATA_MIGRATION_STORE`` and ``JOB_MANAGER``) -- and binds them to those names, so each hook
body is the code that ran in ``build_page``. A hook still looks its session's state up in the
store when it is called, not when the page is built.

Every import a hook makes is inside it, as in ``build_page``, so a patch on the module that
defines the name reaches the hook. The one exception is ``cdc_teardown_estimate``, this
module's global.
"""

from __future__ import annotations

from typing import Callable, NamedTuple, Optional

from dsql_migrator.core.cdc import cdc_teardown_estimate


class CdcTeardownHooks(NamedTuple):
    """The hooks :func:`build_cdc_teardown_hooks` builds, in the order it defines them.

    Field ``x`` holds the hook ``_x``; ``build_page`` unpacks them under those names.
    """

    cdc_deployed: Callable[[], bool]
    cdc_teardown_stack_names: Callable[[], list]
    cdc_stack_name: Callable[[], Optional[str]]
    cdc_teardown_in_flight: Callable[[], bool]
    cdc_teardown_banner: Callable[[], Optional[dict]]
    cdc_teardown_done: Callable[[], Optional[dict]]
    cdc_teardown_done_dismiss: Callable[[], None]
    cdc_op_in_flight: Callable[[], Optional[str]]
    cdc_probe: Callable[[], None]
    cdc_teardown_on_reset: Callable[[str], None]
    cdc_teardown_retry: Callable[[], None]
    cdc_teardown_dismiss: Callable[[], None]


def build_cdc_teardown_hooks(
    *,
    session_id: str,
    session_store: object,
    data_migration_store: object,
    job_manager: object,
) -> CdcTeardownHooks:
    """Return the CDC teardown hooks for the page of ``session_id``.

    ``session_store``, ``data_migration_store`` and ``job_manager`` are the app's
    ``SESSION_STORE``, ``DATA_MIGRATION_STORE`` and ``JOB_MANAGER``.
    """
    # The names the hook bodies read: app.py's module globals while the hooks were
    # build_page's closures. Bound once, so each body below is a verbatim move.
    SESSION_STORE = session_store
    DATA_MIGRATION_STORE = data_migration_store
    JOB_MANAGER = job_manager

    def _cdc_deployed() -> bool:
        """True when ANY CDC AWS resource exists, so Start over can offer to tear it
        down. Existence -- not health -- is what matters: a connector or stack that
        is FAILED / mid-rollback / half-deployed still bills for MSK / NAT and must
        be offered for teardown just like a running one.

        So this is truthy when either (a) any of MY connectors exist in ANY state
        (``cdc_connector_names`` is populated existence-based by ``_filter_mine``,
        regardless of RUNNING), or (b) the cdc-stack phase is anything other than
        ``absent`` -- i.e. ``running`` / ``infra`` / ``unstable`` (a stuck/rolled-
        back stack is still deployed). Only ``absent`` (no stack at all) is safe.
        This matches the CDC step, which already offers Delete for the ``unstable``
        phase.

        (c) covers the case the first two MISS: a cdc-stack under a name this session
        does not target (``cdc_other_stacks``). Both signals above are scoped to
        ``cdc_stack_name``, so a stack deployed by an earlier session -- or with a
        custom suffix -- left Start over reporting no CDC at all while the Data
        Migration screen simultaneously offered to ATTACH to that very stack. The two
        prompts contradicted each other about the same resource, and the one that
        stayed silent is the one that would have offered to stop the MSK / NAT
        billing. Existence in the account is what matters for a teardown offer, not
        whether this session happens to own the name."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        if getattr(migration_state, "cdc_connector_names", None):
            return True
        phase = getattr(migration_state, "cdc_stack_phase", None)
        if phase is not None and phase != "absent":
            return True
        return bool(getattr(migration_state, "cdc_other_stacks", None))

    def _cdc_teardown_stack_names() -> list:
        """EVERY cdc-stack a Start-over teardown would act on, in teardown order.

        Delegates to the pure :func:`cdc_teardown_stack_names` so the offer
        (:func:`_cdc_deployed`), the dialog's tile listing, and the teardown itself all
        read one resolution -- they must never disagree about which stacks exist.

        This used to resolve a SINGLE name and adopt a discovered stack only when there
        was exactly one; with two or more it fell back to this session's own name, which
        in that branch does not exist. So the dialog offered "Delete all CDC
        infrastructure", the delete found nothing, and the operator kept paying for MSK /
        NAT behind a success toast. Every discovered stack is now returned and torn down.
        """
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_stack_names

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        return cdc_teardown_stack_names(
            own_stack_name=getattr(migration_state, "cdc_stack_name", None),
            stack_phase=getattr(migration_state, "cdc_stack_phase", None),
            connector_names=getattr(migration_state, "cdc_connector_names", None) or [],
            other_stacks=getattr(migration_state, "cdc_other_stacks", None) or [],
        )

    def _cdc_teardown_stack_name() -> Optional[str]:
        """The FIRST cdc-stack a teardown would act on (or the session's own name).

        Kept for the single-stack callers (the orphan-billing caution's custom-name
        wording). Prefer :func:`_cdc_teardown_stack_names` anywhere the full set matters.
        """
        names = _cdc_teardown_stack_names()
        if names:
            return names[0]
        return getattr(
            DATA_MIGRATION_STORE.get_or_create(session_id), "cdc_stack_name", None
        )

    def _cdc_stack_name() -> Optional[str]:
        """Back-compat alias for :func:`_cdc_teardown_stack_name` (same resolution)."""
        return _cdc_teardown_stack_name()

    def _cdc_teardown_in_flight() -> bool:
        """True when a CDC stop/delete is CURRENTLY running, so Start over must not
        race it (a second teardown + a session wipe that hides the running delete,
        unre-discoverable for a custom stack name). Thin session-bound wrapper over
        the pure :func:`cdc_teardown_in_flight` predicate: passes the durable teardown
        marker (which survives the reset and closes the post-Start-over race window),
        the local lifecycle job + kind, and the freshly-probed raw stack status. The
        job/stack signals are refreshed by ``_cdc_probe`` just before the dialog opens.
        """
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        return cdc_teardown_in_flight(
            JOB_MANAGER,
            teardown_job_id=getattr(migration_state, "cdc_teardown_job_id", None),
            deploy_job_id=getattr(migration_state, "cdc_deploy_job_id", None),
            action_kind=getattr(migration_state, "cdc_action_kind", None),
            stack_status=getattr(migration_state, "cdc_stack_phase_status", None),
        )

    def _cdc_teardown_banner() -> Optional[dict]:
        """Info for the persistent 'CDC teardown in progress' banner, or ``None``.

        Reads the durable teardown marker (which survives Start-over's session reset)
        and the JobManager status -- cheap, no AWS call. Returns ``{"kind", "stack"}``
        while the stop/delete job is PENDING/RUNNING so the banner shows on EVERY view
        (including the Connect screen a Start-over → delete lands on); once the job
        while running it returns ``{"state":"running",...}``; when the teardown
        FAILED it returns ``{"state":"failed",...}`` (an actionable banner, marker
        kept so the user can retry/dismiss); when it settled OK (or the job was lost)
        it clears the marker and returns ``None`` so the banner disappears and the
        Start-over guard releases.
        """
        from dsql_migrator.ui.data_migration._cdc_status import (
            cdc_teardown_banner_state,
            settled_teardown_banner_state as _settled_teardown_banner_state,
            stack_status_needs_cleanup as _stack_status_needs_cleanup,
        )

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        job_id = getattr(migration_state, "cdc_teardown_job_id", None)
        if not job_id:
            # No teardown -- but an in-flight INFRASTRUCTURE create also needs the
            # cross-view banner: it runs ~15-20 min and is meant to overlap the Full
            # Load, so once the user leaves the Data Migration screen there would
            # otherwise be no sign it is still going (and they might wait on it).
            # Reuses _cdc_op_in_flight, which already reads the deploy job + kind.
            if _cdc_op_in_flight() == "infra":
                return {
                    "state": "running",
                    "kind": "infra",
                    "stack": getattr(migration_state, "cdc_stack_name", None),
                }
            return None
        state = cdc_teardown_banner_state(JOB_MANAGER, job_id)
        # Self-heal a stale FAILED banner: a DELETE_FAILED freezes both the job record
        # (state=="failed") and the cached stack status, so an out-of-band cleanup
        # (operator finished the delete via the console/CLI) would leave the banner up
        # forever. When we WOULD show failed, do one best-effort read-only live check;
        # only if CloudFormation DEFINITIVELY reports the stack gone do we neutralize
        # BOTH failed sources -- the job-derived state here and the cached status below
        # -- and fall through to the existing clean-completion path (which records the
        # dismissable "deleted" notice and clears the marker). A still-present or
        # ambiguous/errored read leaves the failed banner untouched (never hides a
        # real, still-billing failure). This is the only self-poll-less banner, so the
        # extra read fires only while a failure would be shown.
        would_show_failed = state == "failed" or _stack_status_needs_cleanup(
            getattr(migration_state, "cdc_stack_phase_status", None)
        )
        if would_show_failed and _cdc_teardown_stack_confirmed_gone(migration_state):
            migration_state.set_cdc_stack_phase("absent", status=None)
            if state == "failed":
                state = None
        # Self-heal a stuck RUNNING banner: if the delete job hangs AFTER the stack is
        # already gone (its post-stack cleanup thread wedged, or a long-lived tab whose
        # self-poll chain broke), the "deleting…" banner would pin over a stack that no
        # longer exists. Keying purely on job status missed this (the failed-path
        # self-heal above did not cover a still-"running" job). Do a THROTTLED best-effort
        # live check (not on every 10s poll): on a definitive does-not-exist, treat the
        # teardown as complete and fall through to the clean-completion path below (record
        # the dismissable "deleted" notice + clear the marker).
        if state == "running":
            import time as _t

            _now = _t.monotonic()
            _last = getattr(migration_state, "_cdc_teardown_gone_check_monotonic", 0.0)
            if (_now - _last) >= 30.0:
                migration_state._cdc_teardown_gone_check_monotonic = _now
                if _cdc_teardown_stack_confirmed_gone(migration_state):
                    migration_state.set_cdc_stack_phase("absent", status=None)
                    state = None
        if state in ("running", "failed"):
            info = {
                "state": state,
                "kind": getattr(migration_state, "cdc_teardown_kind", None),
                "stack": getattr(migration_state, "cdc_teardown_stack", None),
            }
            # With several stacks, say WHICH one of how many is being torn down -- the
            # banner names a single stack, so without this it looked like the only one.
            from dsql_migrator.ui.data_migration._cdc_status import (
                teardown_queue_progress,
            )

            progress = teardown_queue_progress(
                getattr(migration_state, "cdc_teardown_queue", None) or [], job_id
            )
            if progress is not None:
                info["index"], info["total"] = progress
            return info

        # The tracked stack settled -- but with several stacks the others may still be
        # deleting. Re-point the marker at the next unfinished one instead of clearing,
        # which is what made the banner vanish while MSK / NAT kept billing.
        queue = list(getattr(migration_state, "cdc_teardown_queue", None) or [])
        if len(queue) > 1:
            from dsql_migrator.ui.data_migration._cdc_status import (
                next_unfinished_teardown,
            )

            following = next_unfinished_teardown(JOB_MANAGER, queue)
            if following is not None:
                next_job, next_stack, index, total = following
                migration_state.advance_cdc_teardown(next_job, next_stack)
                return {
                    "state": "running",
                    "kind": getattr(migration_state, "cdc_teardown_kind", None),
                    "stack": next_stack,
                    "index": index,
                    "total": total,
                }
        # The job settled OK (DONE) or is unknown (lost across a restart) -- but the
        # JOB finishing is not the same as the STACK being gone. A delete that ended in
        # DELETE_FAILED, or a job whose record vanished with the process, previously
        # cleared the marker here and the banner went silent: the leftover MSK / NAT
        # kept billing with nothing in the UI saying so. So before clearing, check the
        # last probed stack status (cached -- no AWS call on this render path) and keep
        # an actionable failed banner when the stack is still in a failed/rolled-back
        # state.
        _last_status = getattr(migration_state, "cdc_stack_phase_status", None)
        _settled_state = _settled_teardown_banner_state(_last_status)
        if _settled_state is not None:
            # needs_cleanup is True for the *_IN_PROGRESS statuses too, so a stack still
            # being deleted was reported as "CDC teardown failed — action needed". The
            # helper routes an in-flight status to the RUNNING banner instead; the observed
            # status rides along so the failed copy can name what CloudFormation actually
            # said rather than asserting a DELETE_FAILED it never read.
            return {
                "state": _settled_state,
                "kind": getattr(migration_state, "cdc_teardown_kind", None),
                "stack": getattr(migration_state, "cdc_teardown_stack", None),
                "status": _last_status,
            }
        # Everything finished cleanly. Record it as a DISMISSABLE completion notice before
        # clearing the marker: a 15-45 min teardown is meant to be left unattended, and
        # the only completion signal was a toast that a refresh throws away -- so the
        # operator came back to an empty screen with no way to tell whether it finished or
        # never ran. Names every stack, since a multi-stack teardown showed only one.
        from dsql_migrator.ui.data_migration._cdc_status import finished_teardown_stacks

        migration_state.set_cdc_teardown_done(
            kind=getattr(migration_state, "cdc_teardown_kind", None),
            stacks=finished_teardown_stacks(
                getattr(migration_state, "cdc_teardown_queue", None) or [],
                getattr(migration_state, "cdc_teardown_stack", None),
            ),
        )
        migration_state.clear_cdc_teardown()
        return None

    def _cdc_teardown_done() -> Optional[dict]:
        """A finished-but-undismissed teardown for the completion banner, else ``None``."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        done = dict(getattr(migration_state, "cdc_teardown_done", None) or {})
        return done or None

    def _cdc_teardown_done_dismiss() -> None:
        """Close the completion banner (the operator acknowledged the result)."""
        DATA_MIGRATION_STORE.get_or_create(session_id).dismiss_cdc_teardown_done()

    def _cdc_op_in_flight() -> Optional[str]:
        """Kind (``"infra"``/``"start"``) of a NON-teardown CDC lifecycle job still
        running, else ``None``.

        Lets Start over WARN (not hard-block) that a deploy/start will keep running in
        the background after the reset, so an orphaned job is never a silent surprise.
        Teardowns (stop/delete) are the destructive/billing-relevant case and stay
        hard-blocked by :func:`_cdc_teardown_in_flight`; a deploy/start is
        re-discoverable and must not trap a user trying to escape a stuck run.
        """
        from dsql_migrator.ui.data_migration._cdc_status import _current_job

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        job = _current_job(
            JOB_MANAGER, getattr(migration_state, "cdc_deploy_job_id", None)
        )
        kind = getattr(migration_state, "cdc_action_kind", None)
        if (
            job is not None
            and getattr(job, "status", None) in ("PENDING", "RUNNING")
            and kind in ("infra", "start")
        ):
            return kind
        return None

    def _cdc_probe() -> None:
        """Refresh the cached CDC deployment state from a live, read-only AWS probe.

        Called (off the event loop) when the user opens Start over, so the dialog
        reflects the ACTUAL deployed CDC -- and offers the stop/delete tiles --
        regardless of which step the user was on. ``_ensure_cdc_controller`` is
        throttled per session; clear the throttle timestamp first so this explicit
        user action always gets a fresh read. Best-effort and read-only."""
        from dsql_migrator.ui.data_migration._cdc_status import _ensure_cdc_controller

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        migration_state._cdc_discovery_monotonic = None  # bypass the render throttle
        try:
            _ensure_cdc_controller(migration_state, session)
        except Exception:  # noqa: BLE001 - leave cached state; dialog opens regardless
            pass

    def _launch_cdc_teardown(
        migration_state,
        *,
        mode,
        stack_name,
        region,
        role_arn,
        aws_profile,
        cleanup_secret,
        source_credentials,
        source_identity,
        track_in_queue: Optional[list] = None,
        own_stack: bool = False,
    ) -> Optional[str]:
        """Submit a CDC teardown (stop/delete) for Start over or the banner's one-click
        "Retry cleanup", through the CDC step's own ``submit_cdc_teardown`` -- the one
        submission Stop / Delete use too, so the lifecycle outcome line, the residue
        report, the source-write audit and the session's ``source_credentials`` (for
        the session's own stack only) reach these two paths as well. It records the
        durable marker + the retry context ({region, role_arn, profile, cleanup_secret,
        source}: ``source`` is the non-secret ``source_identity``; never the
        credentials). The deploy-log lines are dropped: that log belongs to the CDC
        card's own lifecycle job, which these paths do not set. Returns the submitted
        job id, or ``None`` when the ownership guard declined to overwrite a different,
        still-running teardown.
        """
        from dsql_migrator.ui.data_migration._cdc_ui import submit_cdc_teardown

        job_id, claimed = submit_cdc_teardown(
            migration_state,
            JOB_MANAGER,
            mode=mode,
            stack_name=stack_name,
            region=region,
            role_arn=role_arn,
            aws_profile=aws_profile,
            cleanup_secret=cleanup_secret,
            source_credentials=source_credentials,
            source_identity=source_identity,
            on_log=lambda _ts, _msg: None,
            track_in_queue=track_in_queue,
            own_stack=own_stack,
        )
        return job_id if claimed else None

    def _cdc_teardown_on_reset(mode: str) -> None:
        """Submit a CDC teardown as part of Start over (called BEFORE the reset).

        ``mode`` is ``"stop"`` (delete only the 2 MSK connectors, keep MSK/VPC/IAM
        for a fast restart) or ``"delete"`` (tear down the whole cdc-stack). All
        config is captured now (into the durable marker's retry ctx and the job
        closure; the session's source credentials into the job closure only), so the
        imminent session reset cannot race it; the teardown runs in the background and
        stays visible/retryable via the persistent banner.
        """
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        target = getattr(session, "target_config", None)
        region = getattr(target, "region", None) if target else None
        if not region and target is not None:
            endpoint = getattr(target, "cluster_endpoint", "") or ""
            if ".dsql." in endpoint and ".on.aws" in endpoint:
                region = endpoint.split(".dsql.")[1].split(".on.aws")[0]
        # Act on EVERY stack the offer was made about -- the dialog now lists them by
        # name, so tearing down only the first would contradict what the operator just
        # read and confirmed. Reading cdc_stack_name directly here (or resolving a single
        # name) targeted a non-existent stack whenever two or more were discovered:
        # delete nothing, leave MSK / NAT billing, report success.
        stack_names = _cdc_teardown_stack_names()
        if not region or not stack_names:
            return
        aws_profile = getattr(session, "aws_profile", None)
        role_arn = getattr(migration_state, "cdc_deploy_role_arn", None)
        cleanup_secret = mode == "delete" and not getattr(
            session, "source_secret_id", None
        )
        # The session's source username/password, read ONCE and NOW: the Start-over reset
        # runs right after this returns and wipes them, so a read when the job runs gets
        # None -- and on Fargate the app's identity cannot read the CDC secret instead, so
        # a PostgreSQL teardown could not drop the replication slot (it keeps pinning
        # source WAL). Held only by the job closures until they finish (Property 7), and
        # handed only to this session's own stack: submit_cdc_teardown gives a discovered
        # stack None, so it reads its own secret (it may capture a different source).
        # The source they were entered for is read with them (the reset wipes it too) and
        # recorded, non-secret, in the marker: a Retry cleanup after the operator has
        # connected another source must not hand that source's password to this stack.
        from dsql_migrator.ui.data_migration._cdc_ui import (
            _session_source_credentials,
            _session_source_identity,
        )

        source_credentials = _session_source_credentials(session)
        source_identity = _session_source_identity(session)
        # The durable marker tracks ONE teardown, so the banner follows the first stack
        # and the rest are launched behind it. Only the first claims the marker (the
        # ownership guard declines the others while it is still running); each still runs
        # to completion as its own background job. The per-stack plan (including where the
        # shared source-secret cleanup belongs) comes from the pure cdc_teardown_plan.
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_plan

        job_id = None
        launched_queue: list[tuple[str, str]] = []
        for stack_name, stack_cleanup_secret in cdc_teardown_plan(
            stack_names, cleanup_secret=cleanup_secret
        ):
            launched = _launch_cdc_teardown(
                migration_state,
                mode=mode,
                stack_name=stack_name,
                region=region,
                role_arn=role_arn,
                aws_profile=aws_profile,
                cleanup_secret=stack_cleanup_secret,
                source_credentials=source_credentials,
                source_identity=source_identity,
                # Only the first claims the single durable marker; the queue below is
                # what keeps the rest visible.
                track_in_queue=launched_queue,
            )
            if job_id is None:
                job_id = launched
        # Record EVERY launched teardown so the banner can advance to the next unfinished
        # stack when the tracked one settles. Without this the banner followed only the
        # first stack and went silent while the others were still deleting -- MSK / NAT
        # billing on, nothing on screen.
        if launched_queue:
            migration_state.set_cdc_teardown_queue(launched_queue)
        # The teardown runs in the background after the session resets; surface a
        # completion toast (the persistent banner also tracks it). If the marker was
        # not claimed (another teardown already running), poll whatever is tracked.
        from nicegui import ui

        if job_id is None:
            job_id = getattr(migration_state, "cdc_teardown_job_id", None)
        if mode == "delete":
            started = (
                "Deleting CDC infrastructure in the background "
                f"({cdc_teardown_estimate(has_seeder_lambda=None)})…"
            )
            done = "CDC infrastructure deleted — MSK/NAT billing stopped."
        else:
            started = "Removing the CDC connectors in the background…"
            done = "CDC connectors removed — you can start a new migration."
        ui.notify(started, type="info", position="top", timeout=6000)  # type: ignore[attr-defined]

        timer_ref: dict = {"t": None}

        def _poll() -> None:
            try:
                status = JOB_MANAGER.get_status(job_id).status
            except Exception:  # noqa: BLE001 - treat an unreadable job as failed
                status = "FAILED"
            if status not in ("DONE", "FAILED", "CANCELLED"):
                return
            if timer_ref["t"] is not None:
                timer_ref["t"].active = False
            if status == "DONE":
                ui.notify(done, type="positive", position="top", timeout=8000)  # type: ignore[attr-defined]
            else:
                ui.notify(  # type: ignore[attr-defined]
                    "CDC teardown did not complete cleanly — a banner with a "
                    "Retry cleanup button stays on screen until it succeeds.",
                    type="warning",
                    position="top",
                    timeout=8000,
                )

        timer_ref["t"] = ui.timer(10.0, _poll)  # type: ignore[attr-defined]

    def _cdc_teardown_retry() -> None:
        """Re-launch the tracked teardown from the durable marker's retry context.

        Backs the failed-teardown banner's one-click "Retry cleanup": rebuilds the
        deployer from the saved region/role/profile (the session may have been wiped
        by Start over) and re-submits the teardown, which retains any stuck resource
        and re-issues the delete (recover_delete_failed). Sets the marker back to
        running so the banner returns to the in-progress state.
        """
        from nicegui import ui

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        stack_name = getattr(migration_state, "cdc_teardown_stack", None)
        kind = getattr(migration_state, "cdc_teardown_kind", None) or "delete"
        ctx = dict(getattr(migration_state, "cdc_teardown_ctx", {}) or {})
        region = ctx.get("region")
        if not stack_name or not region:
            ui.notify(  # type: ignore[attr-defined]
                "Can't retry automatically here — open the Data Migration → CDC step "
                "and use Delete CDC infrastructure.",
                type="warning",
                position="top",
            )
            return
        # Clear first so the ownership guard sees a fresh claim (the old FAILED job is
        # a different, settled one, so it would allow the write anyway).
        migration_state.clear_cdc_teardown()
        from dsql_migrator.ui.data_migration._cdc_ui import (
            _session_source_credentials,
            _session_source_identity,
        )

        # The live session's credentials, but ONLY while it still targets the source the
        # teardown was submitted for (the marker's non-secret ctx["source"]): a failed
        # CDC-step Delete keeps its session, so the retry can drop the slot without the
        # secret, and so can a retry after Start over once the operator has reconnected
        # to that same source -- for a renamed stack too. Anything else gets None and the
        # delete reads the secrets, as before: a session wiped by Start over, one
        # connected to ANOTHER source since, or a marker with no recorded source (a
        # discovered stack's, or one the banner moved on to). The stack-name check in
        # submit_cdc_teardown does not cover the other-source case -- Start over resets
        # cdc_stack_name to the default name, and a reconnect without Start over keeps
        # the name -- and the slot drop tries the credentials it is handed first, against
        # the stack's own source host, with no fallback to the secret: the other
        # source's password would reach this one and the slot stay.
        session = SESSION_STORE.get_or_create(session_id)
        recorded_source = ctx.get("source")
        same_source = (
            recorded_source is not None
            and _session_source_identity(session) == recorded_source
        )
        _launch_cdc_teardown(
            migration_state,
            mode=kind,
            stack_name=stack_name,
            region=region,
            role_arn=ctx.get("role_arn"),
            aws_profile=ctx.get("profile"),
            cleanup_secret=bool(ctx.get("cleanup_secret")),
            source_credentials=(
                _session_source_credentials(session) if same_source else None
            ),
            # Carried forward, so a retry made while the session was empty does not stop
            # the next one (after a reconnect to the same source) from using its password.
            source_identity=recorded_source,
            # A recorded source is recorded only for the session's own stack (and dropped
            # when the marker moves to another), so it proves this is that stack: skip the
            # stack-name check, which Start over's reset to the default name would fail
            # for a stack the operator had renamed.
            own_stack=recorded_source is not None,
        )
        ui.notify("Retrying CDC cleanup…", type="info", position="top")  # type: ignore[attr-defined]

    def _cdc_teardown_dismiss() -> None:
        """Dismiss a FAILED teardown banner (clear the marker only). The AWS resources
        are NOT touched -- the user chooses to stop tracking it here (e.g. they will
        finish cleanup in the console)."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        migration_state.clear_cdc_teardown()

    def _cdc_teardown_stack_confirmed_gone(migration_state) -> bool:
        """Best-effort live check: is the tracked teardown stack DEFINITIVELY gone?

        Self-heals the stale "CDC teardown failed" banner. A DELETE_FAILED freezes the
        job record + cached status at failed, so if the operator finishes cleanup out
        of band (e.g. terminates the ENI-pinning bastion, re-runs delete-stack) the
        banner would otherwise linger forever. Rebuilds a deployer from the durable
        marker's retry ctx (region/role/profile -- the session may have been wiped by
        Start over, exactly as ``_cdc_teardown_retry`` does) and delegates to
        ``teardown_stack_confirmed_gone``, which returns True ONLY on a definitive
        does-not-exist. No ctx/region, or ANY error -> False (keep the banner).
        """
        stack_name = getattr(migration_state, "cdc_teardown_stack", None)
        ctx = dict(getattr(migration_state, "cdc_teardown_ctx", {}) or {})
        region = ctx.get("region")
        if not stack_name or not region:
            return False
        try:
            from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer
            from dsql_migrator.ui.data_migration._cdc_status import (
                teardown_stack_confirmed_gone,
            )

            deployer = build_cdc_stack_deployer(
                region,
                aws_profile=ctx.get("profile"),
                assume_role_arn=ctx.get("role_arn"),
            )
            return teardown_stack_confirmed_gone(deployer, stack_name)
        except Exception:  # noqa: BLE001 - never let the self-heal break the render
            return False

    return CdcTeardownHooks(
        cdc_deployed=_cdc_deployed,
        cdc_teardown_stack_names=_cdc_teardown_stack_names,
        cdc_stack_name=_cdc_stack_name,
        cdc_teardown_in_flight=_cdc_teardown_in_flight,
        cdc_teardown_banner=_cdc_teardown_banner,
        cdc_teardown_done=_cdc_teardown_done,
        cdc_teardown_done_dismiss=_cdc_teardown_done_dismiss,
        cdc_op_in_flight=_cdc_op_in_flight,
        cdc_probe=_cdc_probe,
        cdc_teardown_on_reset=_cdc_teardown_on_reset,
        cdc_teardown_retry=_cdc_teardown_retry,
        cdc_teardown_dismiss=_cdc_teardown_dismiss,
    )
