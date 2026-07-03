"""Unit tests for the configuration module.

These tests verify environment-driven configuration loading and, critically,
that credential values are never exposed in plaintext (Requirement 9.2 /
Property 7: credential confidentiality).
"""

from __future__ import annotations

import os

import pytest
from pydantic import ValidationError

from dsql_migrator.config import (
    ENV_PREFIX,
    TUNABLE_KNOBS,
    ConnectDefaults,
    SecretRef,
    SecretSource,
    SecretValue,
    TuningValueError,
    current_tuning_values,
    load_config,
    load_connect_defaults,
    read_env_file,
    resolve_secret,
    set_tuning_value,
)


def test_load_config_uses_defaults_when_env_empty() -> None:
    config = load_config(env={})
    assert config.app_host == "127.0.0.1"
    assert config.app_port == 8080
    assert config.aws_region is None
    assert config.aws_profile is None
    assert config.log_level == "INFO"


def test_load_config_reads_prefixed_environment_variables() -> None:
    env = {
        f"{ENV_PREFIX}APP_HOST": "0.0.0.0",
        f"{ENV_PREFIX}APP_PORT": "9000",
        f"{ENV_PREFIX}AWS_REGION": "us-east-1",
        f"{ENV_PREFIX}LOG_LEVEL": "debug",
    }
    config = load_config(env=env)
    assert config.app_host == "0.0.0.0"
    assert config.app_port == 9000
    assert config.aws_region == "us-east-1"
    assert config.log_level == "DEBUG"


def test_load_config_aws_profile_defaults_to_none_when_unset() -> None:
    config = load_config(env={})
    assert config.aws_profile is None


def test_load_config_row_diff_sample_size_defaults_off() -> None:
    # The dev row-level diff is off by default (0).
    assert load_config(env={}).validate_row_diff_sample_size == 0


def test_load_config_reads_row_diff_sample_size() -> None:
    config = load_config(env={f"{ENV_PREFIX}VALIDATE_ROW_DIFF_SAMPLE_SIZE": "50"})
    assert config.validate_row_diff_sample_size == 50


def test_load_config_full_load_parallelism_defaults() -> None:
    config = load_config(env={})
    assert config.full_load_table_parallelism == 4
    assert config.full_load_batch_parallelism == 8
    assert config.full_load_batch_rows == 2000


def test_load_config_reads_full_load_parallelism() -> None:
    env = {
        f"{ENV_PREFIX}FULL_LOAD_TABLE_PARALLELISM": "8",
        f"{ENV_PREFIX}FULL_LOAD_BATCH_PARALLELISM": "16",
        f"{ENV_PREFIX}FULL_LOAD_BATCH_ROWS": "3000",
    }
    config = load_config(env=env)
    assert config.full_load_table_parallelism == 8
    assert config.full_load_batch_parallelism == 16
    assert config.full_load_batch_rows == 3000


def test_load_config_full_load_batch_rows_rejects_over_cap() -> None:
    # Hard-capped at DSQL's 3000-row per-transaction limit.
    with pytest.raises(ValidationError):
        load_config(env={f"{ENV_PREFIX}FULL_LOAD_BATCH_ROWS": "5000"})


def test_load_config_activity_log_to_stdout_defaults_false() -> None:
    config = load_config(env={})
    assert config.activity_log_to_stdout is False


def test_load_config_reads_activity_log_to_stdout_truthy() -> None:
    for value in ("true", "TRUE", "1", "yes", "on"):
        config = load_config(env={f"{ENV_PREFIX}ACTIVITY_LOG_STDOUT": value})
        assert config.activity_log_to_stdout is True, value
    for value in ("false", "0", "no", "off", "garbage", "   "):
        config = load_config(env={f"{ENV_PREFIX}ACTIVITY_LOG_STDOUT": value})
        assert config.activity_log_to_stdout is False, value


def test_load_config_reads_aws_profile_from_environment() -> None:
    env = {f"{ENV_PREFIX}AWS_PROFILE": "migration-admin"}
    config = load_config(env=env)
    assert config.aws_profile == "migration-admin"


def test_load_config_treats_blank_aws_profile_as_unset() -> None:
    env = {f"{ENV_PREFIX}AWS_PROFILE": "   "}
    config = load_config(env=env)
    assert config.aws_profile is None


def test_app_config_dump_includes_aws_profile_name_and_no_secret() -> None:
    env = {f"{ENV_PREFIX}AWS_PROFILE": "migration-admin"}
    config = load_config(env=env)
    dumped = config.model_dump()
    assert dumped["aws_profile"] == "migration-admin"
    assert "password" not in dumped
    assert "secret" not in dumped


def test_load_config_reads_cdc_deploy_role_arn() -> None:
    env = {
        f"{ENV_PREFIX}CDC_DEPLOY_ROLE_ARN": "arn:aws:iam::123456789012:role/CdcDeploy"
    }
    config = load_config(env=env)
    assert config.cdc_deploy_role_arn == "arn:aws:iam::123456789012:role/CdcDeploy"


def test_load_config_cdc_deploy_role_arn_defaults_to_none() -> None:
    assert load_config(env={}).cdc_deploy_role_arn is None


def test_load_config_treats_blank_cdc_deploy_role_arn_as_unset() -> None:
    env = {f"{ENV_PREFIX}CDC_DEPLOY_ROLE_ARN": "   "}
    assert load_config(env=env).cdc_deploy_role_arn is None


def test_load_config_reads_cdc_secret_kms_key_id() -> None:
    env = {f"{ENV_PREFIX}CDC_SECRET_KMS_KEY_ID": "alias/cdc-source-cmk"}
    assert load_config(env=env).cdc_secret_kms_key_id == "alias/cdc-source-cmk"


def test_load_config_cdc_secret_kms_key_id_defaults_to_none() -> None:
    assert load_config(env={}).cdc_secret_kms_key_id is None


def test_load_config_treats_blank_values_as_unset() -> None:
    env = {f"{ENV_PREFIX}APP_HOST": "   "}
    config = load_config(env=env)
    assert config.app_host == "127.0.0.1"


def test_load_config_rejects_out_of_range_port() -> None:
    env = {f"{ENV_PREFIX}APP_PORT": "70000"}
    with pytest.raises(Exception):
        load_config(env=env)


def test_app_config_dump_contains_no_credentials() -> None:
    config = load_config(env={})
    dumped = config.model_dump()
    assert "password" not in dumped
    assert "secret" not in dumped


def test_secret_value_is_masked_in_repr_and_str() -> None:
    secret = SecretValue("super-secret-password")
    assert "super-secret-password" not in repr(secret)
    assert "super-secret-password" not in str(secret)
    assert secret.reveal() == "super-secret-password"


def test_secret_ref_describe_is_log_safe() -> None:
    ref = SecretRef(source=SecretSource.SECRETS_MANAGER, locator="arn:aws:secret:db")
    description = ref.describe()
    assert description == "SECRETS_MANAGER:arn:aws:secret:db"


def test_resolve_secret_from_environment_returns_masked_value() -> None:
    ref = SecretRef(source=SecretSource.ENVIRONMENT, locator="DB_PASSWORD")
    resolved = resolve_secret(ref, env={"DB_PASSWORD": "p@ss"})
    assert isinstance(resolved, SecretValue)
    assert resolved.reveal() == "p@ss"
    assert "p@ss" not in str(resolved)


def test_resolve_secret_missing_environment_variable_raises() -> None:
    ref = SecretRef(source=SecretSource.ENVIRONMENT, locator="MISSING_VAR")
    with pytest.raises(KeyError):
        resolve_secret(ref, env={})


def test_resolve_secret_unimplemented_source_raises() -> None:
    ref = SecretRef(source=SecretSource.SESSION, locator="session-key")
    with pytest.raises(NotImplementedError):
        resolve_secret(ref, env={})


# ---------------------------------------------------------------------------
# Connect form prefill defaults (dev convenience)
# ---------------------------------------------------------------------------


def test_load_connect_defaults_empty_env_is_all_none() -> None:
    defaults = load_connect_defaults(env={})
    assert defaults == ConnectDefaults()
    assert defaults.source_host is None
    assert defaults.source_password is None
    assert defaults.target_endpoint is None


def test_load_connect_defaults_reads_source_from_db_vars() -> None:
    defaults = load_connect_defaults(
        env={
            "DB_HOST": "db.example.com",
            "DB_PORT": "3307",
            "DB_NAME": "app",
            "DB_USER": "reader",
            "DB_PASSWORD": "hunter2",
        }
    )
    assert defaults.source_host == "db.example.com"
    assert defaults.source_port == 3307
    # The source database name is intentionally NOT prefilled from DB_NAME.
    assert defaults.source_database is None
    assert defaults.source_username == "reader"
    # Password is wrapped so it never leaks in plaintext.
    assert isinstance(defaults.source_password, SecretValue)
    assert defaults.source_password.reveal() == "hunter2"
    assert "hunter2" not in repr(defaults.source_password)


def test_load_connect_defaults_reads_bedrock_model_id() -> None:
    defaults = load_connect_defaults(
        env={
            "BEDROCK_MODEL_ID": "us.anthropic.claude-opus-4-8",
            "BEDROCK_REGION": "us-east-1",
        }
    )
    assert defaults.bedrock_model_id == "us.anthropic.claude-opus-4-8"
    assert defaults.bedrock_region == "us-east-1"


def test_load_connect_defaults_reads_target_from_target_vars() -> None:
    defaults = load_connect_defaults(
        env={
            "TARGET_ENDPOINT": "c.dsql.us-east-1.on.aws",
            "TARGET_REGION": "us-east-1",
            "TARGET_DATABASE": "postgres",
            "TARGET_USERNAME": "admin",
        }
    )
    assert defaults.target_endpoint == "c.dsql.us-east-1.on.aws"
    assert defaults.target_region == "us-east-1"
    assert defaults.target_database == "postgres"
    assert defaults.target_username == "admin"


def test_load_connect_defaults_ignores_blank_and_non_numeric_port() -> None:
    defaults = load_connect_defaults(
        env={"DB_HOST": "   ", "DB_PORT": "not-a-number", "DB_PASSWORD": ""}
    )
    assert defaults.source_host is None
    assert defaults.source_port is None
    assert defaults.source_password is None


def test_read_env_file_parses_key_values(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# a comment\n"
        "DB_HOST=db.example.com\n"
        "\n"
        "DB_PASSWORD=Welcome1!\n"
        "INVALID_LINE_WITHOUT_EQUALS\n",
        encoding="utf-8",
    )
    values = read_env_file(str(env_file))
    assert values["DB_HOST"] == "db.example.com"
    assert values["DB_PASSWORD"] == "Welcome1!"
    assert "INVALID_LINE_WITHOUT_EQUALS" not in values


def test_read_env_file_missing_file_returns_empty() -> None:
    assert read_env_file("/nonexistent/path/.env") == {}


# --- Runtime performance-tuning knobs --------------------------------------


def test_tunable_knobs_bounds_match_appconfig_fields() -> None:
    """Each knob's advertised min/max is read from the AppConfig field metadata,
    so the UI and the config validate against the SAME bounds."""
    expected = {
        "full_load_table_parallelism": (1, 16),
        "full_load_batch_parallelism": (1, 32),
        "full_load_batch_rows": (1, 3000),
        "validate_max_workers": (1, 32),
    }
    got = {k.field: (k.minimum, k.maximum) for k in TUNABLE_KNOBS}
    assert got == expected
    # Every knob's env key is DSQL_MIGRATOR_-prefixed.
    assert all(k.env_key.startswith(ENV_PREFIX) for k in TUNABLE_KNOBS)


def test_tunable_knob_label_is_derived_from_group_and_short_label() -> None:
    """The fully-qualified ``label`` (used in notifications / errors) is derived
    from ``group`` + ``short_label`` so the UI form fields and the messages can
    never drift apart. Knobs are also ordered by group for section rendering."""
    for k in TUNABLE_KNOBS:
        assert k.group and k.short_label and k.description
        assert k.label == f"{k.group} — {k.short_label.lower()}"
    # A known knob renders the expected qualified label.
    by_field = {k.field: k for k in TUNABLE_KNOBS}
    assert by_field["full_load_table_parallelism"].label == (
        "Full Load — tables in parallel"
    )
    # Knobs are grouped contiguously (Full Load ..., then Validation ...) so the
    # UI can emit a section subheader on each group change without re-sorting.
    groups = [k.group for k in TUNABLE_KNOBS]
    assert groups == sorted(groups, key=groups.index)  # no group reappears later
    assert len(set(groups)) < len(groups)  # at least one group has >1 knob


def test_current_tuning_values_reflect_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in TUNABLE_KNOBS:
        monkeypatch.delenv(k.env_key, raising=False)
    assert current_tuning_values() == {
        "full_load_table_parallelism": 4,
        "full_load_batch_parallelism": 8,
        "full_load_batch_rows": 2000,
        "validate_max_workers": 4,
    }


def test_set_tuning_value_writes_env_and_is_picked_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """set_tuning_value writes os.environ so the NEXT load_config() (read per run)
    sees the new value -- no restart."""
    monkeypatch.delenv(f"{ENV_PREFIX}FULL_LOAD_TABLE_PARALLELISM", raising=False)
    returned = set_tuning_value("full_load_table_parallelism", 12)
    assert returned == 12
    assert os.environ[f"{ENV_PREFIX}FULL_LOAD_TABLE_PARALLELISM"] == "12"
    # A fresh load_config() (as each run does) picks it up.
    assert load_config().full_load_table_parallelism == 12
    assert current_tuning_values()["full_load_table_parallelism"] == 12


def test_set_tuning_value_rejects_out_of_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(f"{ENV_PREFIX}FULL_LOAD_TABLE_PARALLELISM", raising=False)
    with pytest.raises(TuningValueError):
        set_tuning_value("full_load_table_parallelism", 17)  # > 16
    with pytest.raises(TuningValueError):
        set_tuning_value("full_load_batch_rows", 0)  # < 1
    # A rejected value must NOT touch the environment.
    assert f"{ENV_PREFIX}FULL_LOAD_TABLE_PARALLELISM" not in os.environ


def test_set_tuning_value_rejects_non_integer_and_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(TuningValueError):
        set_tuning_value("full_load_batch_rows", "abc")  # type: ignore[arg-type]
    with pytest.raises(TuningValueError):
        set_tuning_value("no_such_knob", 5)
