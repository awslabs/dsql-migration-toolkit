# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""cdcui-3: the Start CDC start-point refusal chain is resolved in ONE place, shared by the
Start button and the Start CDC worker's backstop -- the two sites that must agree on it."""

import inspect

from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration import _cdc_ui

from tests.test_g12_table_set_after_load import _Jobs
from tests.test_g137_mysql_reader_shards import _split_job, _start_state

MYSQL = SourceType.MYSQL


def _chain(state, jobs, job):
    return _cdc_ui._cdc_start_point_chain(  # noqa: SLF001
        state, jobs, job, _cdc_ui._cdc_watermark(job, state), MYSQL  # noqa: SLF001
    )


def test_the_chain_resolves_what_its_two_callers_resolved_by_hand() -> None:
    """Every field is what the four calls the two sites made give, with their arguments."""
    job = _split_job()
    jobs = _Jobs({"job-1": job})
    for state in (
        _start_state(),                                    # Automatic: nothing refused
        _start_state(manual=("mysql-bin.000012", 4)),      # G137: later than the Full Load's
        _start_state(manual=("mysql-bin.000009", 4)),      # at the Full Load's point
    ):
        watermark = _cdc_ui._cdc_watermark(job, state)  # noqa: SLF001
        seed, origin = _cdc_ui._cdc_start_seed(state, watermark, MYSQL)  # noqa: SLF001
        resumes = _cdc_ui.cdc_start_resumes(state, seed, MYSQL)
        chain = _chain(state, jobs, job)
        coords = (seed.binlog_file, seed.binlog_position, seed.gtid_executed)
        assert (chain.seed.binlog_file, chain.seed.binlog_position,
                chain.seed.gtid_executed) == coords
        assert (chain.origin, chain.resumes) == (origin, resumes)
        assert chain.newer == _cdc_ui.newer_start_point_refusal(
            state, seed, MYSQL, origin=origin, job_watermark=watermark
        )
        assert chain.split == _cdc_ui.split_start_point_refusal(
            state, job, seed, MYSQL, origin=origin, resumes=resumes, job_manager=jobs
        )
    # The case that makes the sharing worth it: the G137 refusal only the Manual point draws.
    assert _chain(_start_state(manual=("mysql-bin.000012", 4)), jobs, job).split
    assert _chain(_start_state(), jobs, job).split is None


def test_postgres_takes_the_watermark_and_draws_no_mysql_refusal() -> None:
    job = _split_job()
    jobs = _Jobs({"job-1": job})
    state = _start_state(manual=("mysql-bin.000012", 4))
    chain = _cdc_ui._cdc_start_point_chain(  # noqa: SLF001
        state, jobs, job, _cdc_ui._cdc_watermark(job, state), SourceType.POSTGRES  # noqa: SLF001
    )
    assert chain.newer is None and chain.split is None


def test_the_button_and_the_backstop_share_the_chain_and_no_one_else_does() -> None:
    """Pinned in the source: both resolve it through _cdc_start_point_chain, neither repeats
    the four calls at the click, and no third site reaches for the shared helper (the
    start-point card and the Start dialog judge a point they resolve with their own
    arguments). The Start CDC worker inside _start_cdc_deploy is NOT the click: it judges
    G25 again against its own fresh describe (stack_params/stack_status) and reads
    cdc_start_resumes for the activity line as state stands then."""
    button = inspect.getsource(_cdc_ui._render_cdc_start_button)  # noqa: SLF001
    backstop = inspect.getsource(_cdc_ui._start_cdc_deploy)  # noqa: SLF001
    click = backstop[: backstop.index("    def _check_mysql_start_on_worker(")]
    for src in (button, click):
        assert "_cdc_start_point_chain(" in src
        for by_hand in (
            "_cdc_start_seed(",
            "cdc_start_resumes(",
            "newer_start_point_refusal(",
            "split_start_point_refusal(",
        ):
            assert by_hand not in src
    module = inspect.getsource(_cdc_ui)
    # the definition, its docstring reference and the two callers
    assert module.count("_cdc_start_point_chain(") == 3


def test_the_backstop_still_checks_the_earlier_offer_before_the_chains_refusals() -> None:
    """The order of the backstop's refusals is unchanged: the earlier offered seed first
    (it names an offset no later start can rewind), then G25, then G137."""
    backstop = inspect.getsource(_cdc_ui._start_cdc_deploy)  # noqa: SLF001
    first = backstop.index("seed_refused_by_an_earlier_offer(")
    assert first < backstop.index("_newer = _chain.newer")
    assert backstop.index("_newer = _chain.newer") < backstop.index("_split_refusal = _chain.split")
