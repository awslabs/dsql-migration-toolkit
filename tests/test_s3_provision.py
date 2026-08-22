# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for S3 plugin-bucket provisioning + upload (fake clients, no AWS)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from dsql_migrator.core.s3_provision import (
    S3ProvisionError,
    ensure_and_upload_plugins,
    ensure_plugin_bucket,
    extract_secret_name,
    get_account_id,
    plugin_bucket_name,
    upload_plugin,
)


class _FakeS3:
    def __init__(self, *, bucket_exists=False, head_object=None, fail_create=None):
        self._bucket_exists = bucket_exists
        self._head_object = head_object
        self._fail_create = fail_create
        self.calls: list[tuple[str, dict]] = []

    def head_bucket(self, **kw):
        self.calls.append(("head_bucket", kw))
        if not self._bucket_exists:
            raise RuntimeError("404 Not Found")
        return {}

    def create_bucket(self, **kw):
        self.calls.append(("create_bucket", kw))
        if self._fail_create:
            raise RuntimeError(self._fail_create)
        return {}

    def head_object(self, **kw):
        self.calls.append(("head_object", kw))
        if self._head_object is None:
            raise RuntimeError("404")
        return self._head_object

    def put_object(self, **kw):
        self.calls.append(("put_object", kw))
        return {}


class _FakeSts:
    def get_caller_identity(self):
        return {"Account": "123456789012"}


# ---------------------------------------------------------------------------
# plugin_bucket_name / get_account_id
# ---------------------------------------------------------------------------


def test_plugin_bucket_name_is_deterministic() -> None:
    assert (
        plugin_bucket_name("123", "us-east-1")
        == "mysql-dsql-migrator-plugins-123-us-east-1"
    )


def test_get_account_id() -> None:
    assert get_account_id(_FakeSts()) == "123456789012"


# ---------------------------------------------------------------------------
# ensure_plugin_bucket
# ---------------------------------------------------------------------------


def test_ensure_bucket_noop_when_exists() -> None:
    s3 = _FakeS3(bucket_exists=True)
    name = ensure_plugin_bucket(s3, "1", "us-east-1")
    assert name == "mysql-dsql-migrator-plugins-1-us-east-1"
    assert not any(c[0] == "create_bucket" for c in s3.calls)


def test_ensure_bucket_creates_when_absent_us_east_1() -> None:
    s3 = _FakeS3(bucket_exists=False)
    ensure_plugin_bucket(s3, "1", "us-east-1")
    create = next(c for c in s3.calls if c[0] == "create_bucket")
    assert "CreateBucketConfiguration" not in create[1]


def test_ensure_bucket_creates_with_constraint_other_region() -> None:
    s3 = _FakeS3(bucket_exists=False)
    ensure_plugin_bucket(s3, "1", "eu-west-1")
    create = next(c for c in s3.calls if c[0] == "create_bucket")
    assert create[1]["CreateBucketConfiguration"]["LocationConstraint"] == "eu-west-1"


def test_ensure_bucket_already_owned_is_ok() -> None:
    s3 = _FakeS3(bucket_exists=False, fail_create="BucketAlreadyOwnedByYou: x")
    assert ensure_plugin_bucket(s3, "1", "us-east-1").startswith("mysql-dsql-migrator-plugins")


def test_ensure_bucket_other_account_raises() -> None:
    s3 = _FakeS3(bucket_exists=False, fail_create="BucketAlreadyExists: taken")
    with pytest.raises(S3ProvisionError) as exc:
        ensure_plugin_bucket(s3, "1", "us-east-1")
    assert "another account" in str(exc.value)


# ---------------------------------------------------------------------------
# upload_plugin
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, data: bytes) -> Path:
    p = tmp_path / "artifact.bin"
    p.write_bytes(data)
    return p


def test_upload_skips_when_size_and_etag_match(tmp_path: Path) -> None:
    data = b"hello world"
    p = _write(tmp_path, data)
    # Mirrors the ETag S3 reports for a non-multipart object (see _local_md5): the
    # digest is the protocol, not a security primitive.
    etag = hashlib.md5(data, usedforsecurity=False).hexdigest()  # noqa: S324
    s3 = _FakeS3(head_object={"ContentLength": len(data), "ETag": f'"{etag}"'})
    upload_plugin(s3, "b", "k", p)
    assert not any(c[0] == "put_object" for c in s3.calls)


def test_upload_skips_when_multipart_etag_and_size_match(tmp_path: Path) -> None:
    data = b"x" * 100
    p = _write(tmp_path, data)
    s3 = _FakeS3(head_object={"ContentLength": 100, "ETag": '"abc-3"'})  # composite
    upload_plugin(s3, "b", "k", p)
    assert not any(c[0] == "put_object" for c in s3.calls)


def test_upload_when_size_differs(tmp_path: Path) -> None:
    p = _write(tmp_path, b"hello world")
    s3 = _FakeS3(head_object={"ContentLength": 5, "ETag": '"zzz"'})
    upload_plugin(s3, "b", "k", p)
    assert any(c[0] == "put_object" for c in s3.calls)


def test_upload_when_object_absent(tmp_path: Path) -> None:
    p = _write(tmp_path, b"data")
    s3 = _FakeS3(head_object=None)
    upload_plugin(s3, "b", "k", p)
    assert any(c[0] == "put_object" for c in s3.calls)


def test_upload_missing_local_file_raises(tmp_path: Path) -> None:
    s3 = _FakeS3(head_object=None)
    with pytest.raises(S3ProvisionError):
        upload_plugin(s3, "b", "k", tmp_path / "does-not-exist.bin")


# ---------------------------------------------------------------------------
# ensure_and_upload_plugins (orchestration) — patch artifact paths to temp files
# ---------------------------------------------------------------------------


def test_ensure_and_upload_returns_result(tmp_path: Path, monkeypatch) -> None:
    deb = _write(tmp_path, b"debezium-zip")
    deb = deb.rename(tmp_path / "deb.zip")
    deb_pg = tmp_path / "deb-pg.zip"
    deb_pg.write_bytes(b"debezium-pg-zip")
    sink = tmp_path / "sink.jar"
    sink.write_bytes(b"sink-jar")
    seeder = tmp_path / "seeder.zip"
    seeder.write_bytes(b"seeder-zip")
    monkeypatch.setattr(
        "dsql_migrator.core.s3_provision._artifact_paths",
        lambda: (deb, deb_pg, sink, seeder),
    )
    s3 = _FakeS3(head_object=None)  # always upload
    result = ensure_and_upload_plugins(s3, _FakeSts(), "us-east-1")
    assert result.bucket_name == "mysql-dsql-migrator-plugins-123456789012-us-east-1"
    assert result.bucket_arn.endswith(result.bucket_name)
    assert result.debezium_key.endswith("debezium-mysql-plugin.zip")
    # The PostgreSQL source plugin is uploaded alongside the MySQL one.
    assert result.debezium_pg_key.endswith("debezium-postgres-plugin.zip")
    # Sink ships as a ZIP bundle holding the single sink jar (the JSON converter is
    # provided by the MSK Connect runtime -- no Glue converter jar bundled).
    assert result.dsql_sink_key.endswith("dsql-sink-connector.zip")
    # The offset-seeder Lambda zip is uploaded under the same managed bucket.
    assert result.lambda_seeder_key.endswith("offset-seeder-lambda.zip")
    assert result.plugin_version
    # All four artifacts uploaded (MySQL + PostgreSQL source plugins, sink, seeder).
    assert sum(1 for c in s3.calls if c[0] == "put_object") == 4


def test_plugin_artifacts_are_zip_bundles_and_version_is_current() -> None:
    # Both CDC connector plugins ship as ZIP bundles. They no longer bundle the Glue
    # Avro converter (the pipeline uses the runtime JSON converter), so the artifacts
    # are far smaller. PLUGIN_VERSION must be past the v1/v2/v3 generations so MSK
    # Connect builds fresh CustomPlugin resources on the next deploy (v3 was the
    # defunct aws-msk-iam-auth generation that hit the AUTH_SCHEME_PROVIDER conflict).
    from dsql_migrator.core.s3_provision import (
        DEBEZIUM_PLUGIN_KEY,
        DEBEZIUM_PG_PLUGIN_KEY,
        DSQL_SINK_PLUGIN_KEY,
        PLUGIN_VERSION,
        _DEBEZIUM_PG_PLUGIN_RELPATH,
        _DSQL_SINK_PLUGIN_RELPATH,
    )

    assert DEBEZIUM_PLUGIN_KEY.endswith(".zip")
    assert DEBEZIUM_PG_PLUGIN_KEY.endswith("debezium-postgres-plugin.zip")
    assert _DEBEZIUM_PG_PLUGIN_RELPATH.endswith("debezium-postgres-plugin.zip")
    assert DSQL_SINK_PLUGIN_KEY.endswith(".zip")  # was a bare .jar before the fix
    assert _DSQL_SINK_PLUGIN_RELPATH.endswith("dsql-sink-plugin.zip")
    assert PLUGIN_VERSION not in ("v1", "v2", "v3")


def test_debezium_postgres_plugin_zip_is_committed_with_expected_layout() -> None:
    """The prebuilt Debezium PostgreSQL source plugin ships in the repo (deploy
    convenience: no Maven needed) with the connector jar + the msk-config-providers
    jar (Secrets Manager config provider) under a single top-level dir, mirroring the
    MySQL plugin's packaging.
    """
    import zipfile
    from dsql_migrator.core.s3_provision import _DEBEZIUM_PG_PLUGIN_RELPATH, _repo_root

    zip_path = _repo_root() / _DEBEZIUM_PG_PLUGIN_RELPATH
    assert zip_path.is_file(), f"missing committed PG plugin artifact: {zip_path}"
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    # Single top-level dir, matching the MySQL plugin's shape.
    assert any(n.startswith("debezium-connector-postgres/") for n in names)
    # The Debezium PostgreSQL connector jar (pgoutput) is present at 2.7.4.Final.
    assert any(
        "debezium-connector-postgres-2.7.4.Final.jar" in n for n in names
    ), names
    # The Secrets Manager config-provider jar is injected (not on the MSK runtime
    # classpath) -- same reason it is bundled in the MySQL plugin.
    assert any("msk-config-providers" in n for n in names), names
    # MySQL-only jars must NOT be present.
    assert not any("mysql-connector-j" in n for n in names)
    assert not any("debezium-connector-binlog" in n for n in names)


# ---------------------------------------------------------------------------
# extract_secret_name
# ---------------------------------------------------------------------------


def test_extract_secret_name_from_arn_strips_suffix() -> None:
    arn = "arn:aws:secretsmanager:us-east-1:123:secret:prod/mysql/cdc-user-AbCdEf"
    assert extract_secret_name(arn) == "prod/mysql/cdc-user"


def test_extract_secret_name_from_plain_name() -> None:
    assert extract_secret_name("prod/mysql/source-creds") == "prod/mysql/source-creds"


def test_extract_secret_name_arn_without_suffix() -> None:
    arn = "arn:aws:secretsmanager:us-east-1:123:secret:plainname"
    assert extract_secret_name(arn) == "plainname"
