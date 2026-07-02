"""Quick CDC replication check: read heartbeat rows + lag from Aurora DSQL.

Reads the target connection from the environment (TARGET_ENDPOINT / TARGET_REGION /
TARGET_DATABASE / TARGET_USERNAME), like the rest of the operational scripts -- no
hardcoded endpoint (Property 7 / don't commit real infra identifiers).

Run: set -a; source .env; set +a; .venv/bin/python scripts/check_cdc.py
"""
import os

from dsql_migrator.core.models import TargetConnectionConfig
from dsql_migrator.core.target_connection import DsqlConnector

_endpoint = os.environ.get("TARGET_ENDPOINT")
if not _endpoint:
    raise SystemExit("Set TARGET_ENDPOINT (e.g. `set -a; source .env; set +a`).")
cfg = TargetConnectionConfig(
    cluster_endpoint=_endpoint,
    region=os.environ.get("TARGET_REGION", "us-east-1"),
    database=os.environ.get("TARGET_DATABASE", "postgres"),
    username=os.environ.get("TARGET_USERNAME", "admin"),
)
conn = DsqlConnector(cfg).connect()
cur = conn.cursor()
cur.execute("SELECT count(*), max(ts), now() - max(ts) FROM cdc_monitor.heartbeat")
rows, max_ts, lag = cur.fetchone()
print(f"rows={rows}  max_ts={max_ts}  lag={lag}")
