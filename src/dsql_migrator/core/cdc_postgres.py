# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL CDC control-plane: source config + cdc-stack parameter builders.

Sibling of :mod:`dsql_migrator.core.cdc` for the PostgreSQL source, kept in its own
module so MySQL and PostgreSQL CDC logic never tangle (same split as
``converter_postgres`` / ``assessor_postgres`` / ``exporter_postgres`` /
``validator_postgres``).

PostgreSQL CDC is Debezium's PostgreSQL connector (``pgoutput``): logical
replication via a **publication** + a **logical replication slot**, with NO
``server.id`` / GTID / schema-history. The gapless Full-Load -> CDC handoff resumes
from the WAL **LSN** captured in the Full Load watermark (the slot pins the WAL from
that LSN), so ``snapshot.mode=never``; a stand-alone CDC snapshots first
(``snapshot.mode=initial``).

The engine-neutral half of the pipeline -- the MSK infra, the DSQL sink, topic
creation, scaling, and every customer-environment parameter -- is identical to the
MySQL path. Rather than duplicate that ~35-parameter assembly (a drift hazard the
"declared-in-template" guard would catch), the builders here REUSE the MySQL
builders (:func:`~dsql_migrator.core.cdc.build_cdc_infra_params` /
:func:`~dsql_migrator.core.cdc.build_cdc_stack_params`) for the neutral base via a
source-config adapter, then swap the three MySQL-only source parameters
(``DebeziumPluginS3Key`` / ``SourceDbServerId`` / ``SnapshotMode``) for the
PostgreSQL ones (``EngineType`` / ``DebeziumPostgresPluginS3Key`` / the ``Pg*``
params). MySQL output is untouched (byte-identical). Pure: no AWS, no I/O.

CDC is deferred for a PostgreSQL source in the UI (the migration-type CDC tiles are
gated -- see :func:`dsql_migrator.ui.data_migration.source_supports_cdc`); this
module is the deploy-side foundation those tiles enable in a later phase. The live
slot creation / offset handoff wiring is a subsequent phase.
"""

from __future__ import annotations

from typing import Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
    CdcInfraParams,
    CdcResumePoint,
    CdcStackParams,
    DebeziumSourceConfig,
    SinkConnectorConfig,
    build_cdc_infra_params,
    build_cdc_stack_params,
)
from dsql_migrator.core.models import TableDef, Watermark

# The MySQL-only source parameters emitted by the shared MySQL builders that do NOT
# apply to a PostgreSQL source connector; dropped from the reused neutral base.
# (SourceDbServerId = MySQL binlog replication client id; SnapshotMode = the MySQL
# recovery/schema_only enum; DebeziumPluginS3Key = the MySQL plugin, whose
# CustomPlugin resource is gated off for a PostgreSQL stack.)
_MYSQL_ONLY_INFRA_KEYS: frozenset[str] = frozenset(
    {"DebeziumPluginS3Key", "SourceDbServerId", "SnapshotMode"}
)
_MYSQL_ONLY_STACK_KEYS: frozenset[str] = frozenset({"SnapshotMode"})

# The default PostgreSQL source port (Debezium database.port + connector egress).
PG_DEFAULT_SOURCE_PORT = 5432


class PostgresSourceConfig(BaseModel):
    """Tool-generated config for the managed Debezium PostgreSQL source connector.

    The PostgreSQL analog of :class:`~dsql_migrator.core.cdc.DebeziumSourceConfig`.
    Identifies the capture by ``database_name`` + a pre-created ``publication_name``
    + a logical replication ``slot_name`` (pgoutput), rather than MySQL's
    ``server.id`` / binlog coordinates. ``snapshot_mode`` is ``never`` on the gapless
    handoff (the slot holds the start LSN) and ``initial`` for a stand-alone CDC.
    The neutral ``table_include_list`` / ``column_exclude_list`` /
    ``message_key_columns`` mean the same as on the MySQL config.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    table_include_list: list[str] = Field(default_factory=list)
    database_name: str = Field(default="", description="Debezium database.dbname.")
    slot_name: str = Field(default="", description="Debezium slot.name (pre-created).")
    publication_name: str = Field(
        default="", description="Debezium publication.name (pre-created, scoped)."
    )
    publication_autocreate_mode: Literal["disabled", "filtered", "all_tables"] = (
        "disabled"
    )
    snapshot_mode: Literal["never", "initial", "no_data"] = "never"
    column_exclude_list: list[str] = Field(default_factory=list)
    message_key_columns: dict[str, list[str]] = Field(default_factory=dict)

    def as_neutral_source(self) -> DebeziumSourceConfig:
        """Adapt to a :class:`DebeziumSourceConfig` carrying only the neutral fields.

        Used to drive the shared MySQL builders for the engine-neutral parameter
        base (table/topic/column/message-key derivation). The MySQL-only fields it
        emits (snapshot_mode/server_id/plugin key) are dropped afterward, so the
        placeholder ``snapshot_mode`` here is irrelevant.
        """
        return DebeziumSourceConfig(
            name=self.name,
            table_include_list=list(self.table_include_list),
            snapshot_mode="schema_only",
            column_exclude_list=list(self.column_exclude_list),
            message_key_columns={
                table: list(cols) for table, cols in self.message_key_columns.items()
            },
        )


def build_pg_source_config(
    name: str,
    tables: Sequence[TableDef],
    watermark: Optional[Watermark],
    *,
    database_name: str,
    slot_name: str,
    publication_name: str,
    publication_autocreate_mode: str = "disabled",
    column_exclude_list: Optional[Sequence[str]] = None,
    message_key_columns: Optional[Mapping[str, Sequence[str]]] = None,
    resume_override: Optional[CdcResumePoint] = None,
) -> PostgresSourceConfig:
    """Build the Debezium PostgreSQL source config for the selected tables.

    ``snapshot.mode`` is chosen automatically, mirroring the MySQL builder's logic
    but on the PostgreSQL resume signal (the WAL LSN):

    - **Gapless (watermark) path:** ``never`` -- the tool pre-created a logical
      replication slot at the Full Load LSN, so Debezium resumes streaming from the
      slot without taking a snapshot (the row data is already loaded).
    - **Stand-alone / no-LSN path:** ``initial`` -- no slot LSN to resume from, so
      Debezium snapshots the tables first, then streams.

    ``slot_name`` / ``publication_name`` name the pre-created objects (the tool/DBA
    creates them out of band; the live creation wiring is a later phase, which is why
    they are passed in here rather than derived). ``column_exclude_list`` and
    ``message_key_columns`` mean the same as on the MySQL source config. Pure.
    """
    resume = (
        resume_override
        if resume_override is not None
        else (CdcResumePoint.from_watermark(watermark) if watermark is not None else None)
    )
    has_lsn = (
        resume_override is None
        and resume is not None
        and resume.can_resume_from_lsn()
    )
    mode: Literal["never", "initial", "no_data"] = "never" if has_lsn else "initial"
    return PostgresSourceConfig(
        name=name,
        table_include_list=[table.name for table in tables],
        database_name=database_name,
        slot_name=slot_name,
        publication_name=publication_name,
        publication_autocreate_mode=publication_autocreate_mode,  # type: ignore[arg-type]
        snapshot_mode=mode,
        column_exclude_list=list(column_exclude_list or []),
        message_key_columns={
            table: list(cols) for table, cols in (message_key_columns or {}).items()
        },
    )


def _pg_source_param_tuples(source_config: PostgresSourceConfig) -> list[tuple[str, str]]:
    """Return the PostgreSQL-specific cdc-stack source parameter tuples.

    These replace the dropped MySQL-only source params. ``EngineType=postgres``
    selects the PostgreSQL source connector in the (single) cdc-stack template; the
    rest map to the Debezium PostgreSQL connector's slot/publication/dbname/
    snapshot.mode. The ``DebeziumPostgresPluginS3Key`` value is filled by the deploy
    plugin-upload step (see ``cdc_deployer._patch_plugin_params``); here it is left
    empty so it is present (and thus fillable) in the parameter set.
    """
    return [
        ("EngineType", "postgres"),
        ("DebeziumPostgresPluginS3Key", ""),
        ("PgDatabaseName", source_config.database_name),
        ("PgSlotName", source_config.slot_name),
        ("PgPublicationName", source_config.publication_name),
        ("PgPublicationAutocreateMode", source_config.publication_autocreate_mode),
        ("PgSnapshotMode", source_config.snapshot_mode),
    ]


def build_pg_cdc_infra_params(
    source_config: PostgresSourceConfig,
    sink_config: SinkConnectorConfig,
    *,
    source_db_port: int = PG_DEFAULT_SOURCE_PORT,
    **infra_kwargs: object,
) -> CdcInfraParams:
    """Build the cdc-stack ``create_stack`` parameter set for a PostgreSQL source.

    Reuses :func:`~dsql_migrator.core.cdc.build_cdc_infra_params` for the entire
    engine-neutral base (VPC/subnets, plugin bucket + sink key, DSQL target,
    table/topic/scaling params, MSK/DeploySink pins, SeedMode, ...), then swaps the
    MySQL-only source params for the PostgreSQL ones. ``**infra_kwargs`` are the same
    neutral keyword arguments accepted by the MySQL builder (``vpc_id``,
    ``plugin_bucket_arn``, ``dsql_sink_plugin_s3_key``, ``source_db_hostname``,
    ``source_secret_arn`` / ``source_secret_name``, ``dsql_cluster_arn``,
    ``target_endpoint``, ``stack_name`` / ``topic_prefix`` / ``plugin_version``,
    scaling inputs, ``seed_mode``, ...). ``source_db_port`` defaults to 5432.

    ``debezium_plugin_s3_key`` (the MySQL plugin key) is forced empty and dropped:
    the MySQL source plugin resource is gated off for a PostgreSQL stack.

    ``seed_mode`` is forced to ``"external"``: PostgreSQL resumes streaming from a
    pre-created logical replication slot (not a seeded Kafka connect-offset), so the
    in-VPC binlog offset-seeder Lambda + its CdcStartPrepResource must NOT be created
    (they are ``IsMySqlSource``-gated in the template). Leaving the MySQL default
    ``"lambda"`` would emit ``SeedMode=Lambda`` and, at Start, create
    ``CdcStartPrepResource`` referencing the excluded seeder -> a CloudFormation
    rollback. (The PostgreSQL in-process topic pre-creation that External implies is
    wired in a later phase.) Pure.
    """
    infra_kwargs.pop("debezium_plugin_s3_key", None)  # MySQL plugin key: dropped
    infra_kwargs.pop("source_db_server_id", None)  # MySQL binlog server id: dropped
    infra_kwargs["seed_mode"] = "external"  # PG uses a slot, not the Lambda seeder
    base = build_cdc_infra_params(
        source_config.as_neutral_source(),
        sink_config,
        source_db_port=source_db_port,
        debezium_plugin_s3_key="",
        **infra_kwargs,  # type: ignore[arg-type]
    )
    filled = [(k, v) for (k, v) in base.filled if k not in _MYSQL_ONLY_INFRA_KEYS]
    filled.extend(_pg_source_param_tuples(source_config))
    return CdcInfraParams(
        filled=filled, stack_name=base.stack_name, topic_prefix=base.topic_prefix
    )


def build_pg_cdc_stack_params(
    source_config: PostgresSourceConfig,
    sink_config: SinkConnectorConfig,
    **stack_kwargs: object,
) -> CdcStackParams:
    """Build the Start-CDC connector parameter set for a PostgreSQL source.

    Reuses :func:`~dsql_migrator.core.cdc.build_cdc_stack_params` for the neutral
    connector params (table/topic/sink/DLQ/message-key/DSQL target/sink compute/
    DeploySink), then swaps the MySQL ``SnapshotMode`` for ``EngineType=postgres`` +
    the ``Pg*`` source params, and adds a ``DebeziumPostgresPluginS3Key`` placeholder
    for the manual CLI-deploy path. Pure.
    """
    base = build_cdc_stack_params(
        source_config.as_neutral_source(), sink_config, **stack_kwargs  # type: ignore[arg-type]
    )
    filled = [(k, v) for (k, v) in base.filled if k not in _MYSQL_ONLY_STACK_KEYS]
    filled.extend(_pg_source_param_tuples(source_config))
    # Placeholder set: swap the MySQL plugin-key placeholder for the PostgreSQL one
    # (the customer-environment values -- VPC, secrets, bucket -- are engine-neutral).
    placeholders: list[tuple[str, str]] = []
    for key, value in base.placeholders:
        if key == "DebeziumPluginS3Key":
            placeholders.append(
                (
                    "DebeziumPostgresPluginS3Key",
                    value.replace("Debezium MySQL plugin", "Debezium PostgreSQL plugin"),
                )
            )
        else:
            placeholders.append((key, value))
    return CdcStackParams(
        filled=filled,
        placeholders=placeholders,
        stack_name=base.stack_name,
        topic_prefix=base.topic_prefix,
    )


__all__ = [
    "PG_DEFAULT_SOURCE_PORT",
    "PostgresSourceConfig",
    "build_pg_source_config",
    "build_pg_cdc_infra_params",
    "build_pg_cdc_stack_params",
]
