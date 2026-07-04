# Container image for the MySQL -> Aurora DSQL migration tool.
#
# The same image runs the control-plane app (default: `mysql-dsql-migrator ui`). It is
# pushed to Amazon ECR and run as a single-task Amazon ECS Fargate service behind
# an ALB (see deploy/cloudformation.yaml). Dependencies are installed from the
# committed uv.lock with `uv sync --frozen` for reproducible builds.
#
# Base images come from ECR Public (public.ecr.aws), not Docker Hub, to avoid
# Docker Hub's anonymous pull rate limits in CI/CodeBuild.

FROM public.ecr.aws/docker/library/python:3.12-slim AS build

# Pinned uv (Astral) for reproducible dependency resolution.
COPY --from=ghcr.io/astral-sh/uv:0.9.2 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install dependencies first (better layer caching), then the project itself.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev

# Bundle the three CDC deploy artifacts so the tool can upload them to the
# customer's S3 bucket at deploy time (s3_provision._artifact_paths resolves these
# relative to the repo root = /app). These exact filenames are what _artifact_paths
# reads (debezium-mysql-plugin.zip / dsql-sink-plugin.zip / offset-seeder-lambda.zip);
# they MUST stay in sync with s3_provision._*_RELPATH or CDC deploy fails at the
# upload-plugins stage on a clean image. Only the three needed files are copied to
# keep the image lean; the rest of connectors/ (build scratch, extracted dirs) is not.
COPY connectors/plugins/debezium-mysql-plugin.zip \
     ./connectors/plugins/debezium-mysql-plugin.zip
COPY connectors/plugins/dsql-sink-plugin.zip \
     ./connectors/plugins/dsql-sink-plugin.zip
COPY connectors/plugins/offset-seeder-lambda.zip \
     ./connectors/plugins/offset-seeder-lambda.zip

# Bundle the cdc-stack CloudFormation template too: the "Deploy CDC infrastructure"
# action reads it at runtime (ui/data_migration/_status.py:_read_cdc_template_body
# resolves <repo root = /app>/deploy/cdc-stack/cdc-stack.yaml). Without it a clean
# image fails at the template-read step with "Could not read the cdc-stack template".
# Only the single template is copied (not the rest of deploy/cdc-stack/ scratch).
COPY deploy/cdc-stack/cdc-stack.yaml ./deploy/cdc-stack/cdc-stack.yaml

# Bundle the perf-measurement scripts. They add NO extra dependencies
# (pymysql/sqlalchemy/psycopg are already installed above) and are NOT used by the
# default `ui` entrypoint, so they don't affect the app -- they let the SAME deployed
# image run a one-shot measurement task in the same VPC as the source/target (e.g.
# `RunTask` overriding the command with `python scripts/measure_performance.py
# full-load ...`), which is the only way to measure real in-VPC throughput. Only the
# three measurement files are copied (they are also the only scripts/ entries
# un-ignored in .dockerignore) to keep the image lean -- not the seed/CDC scratch.
# measure_performance.py imports _e2e_tables at runtime, so both must be present.
COPY scripts/measure_performance.py scripts/perf_compare.py scripts/_e2e_tables.py \
     ./scripts/


FROM public.ecr.aws/docker/library/python:3.12-slim AS runtime

# Apply Debian security updates to the base image's transitive OS packages at
# build time. This pulls fixed versions from the Debian security suite as soon as
# they are published, so each rebuild picks them up automatically -- no new
# packages are installed, and the apt lists are removed to keep the image lean.
#
# NOTE on the perl CVEs ECR flags (e.g. CVE-2026-12087): perl is a transitive
# package of the python:3.12-slim (Debian trixie) base, NOT something this app
# uses -- the tool is pure Python and never invokes perl, so the vulnerable code
# path (Socket::pack_ip_mreq_source) is unreachable here. As of this build the
# fixes are still OPEN in Debian trixie/sid (no fixed perl package exists yet to
# upgrade to), so this layer cannot clear them today; it will the moment Debian
# ships the fix. See deploy/DEPLOYMENT.md "Security notes" for the full rationale.
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Prefer IPv4 over IPv6 for all outbound name resolution (glibc getaddrinfo, which
# psycopg/libpq uses to reach Aurora DSQL on 5432). DSQL's endpoint is DUAL-STACK
# (DNS returns both an A and an AAAA record), but a Fargate task on an IPv4-only
# subnet/ENI -- the common case here: no IPv6 CIDR on the subnet, no IPv6 ::/0 SG
# egress -- has no route to the IPv6 address. Without this, glibc can hand back the
# AAAA first and the driver blocks on the unreachable IPv6 until connect_timeout
# fires, which surfaces in the UI as "Connection failed: connection timeout
# expired" even though IPv4:5432 is wide open. Raising the precedence of
# IPv4-mapped addresses makes getaddrinfo return IPv4 first, so the connection
# succeeds immediately. Harmless on a genuine dual-stack task (IPv4 is simply tried
# first, IPv6 still works as a fallback). Baked at build time because the container
# runs as a non-root user (appuser) and cannot write /etc/gai.conf at runtime.
RUN printf 'precedence ::ffff:0:0/96  100\n' > /etc/gai.conf

# Non-root runtime user.
RUN useradd --system --uid 10001 --create-home appuser

WORKDIR /app
COPY --from=build /app /app

# Put the virtualenv on PATH so the `mysql-dsql-migrator` console script is available.
# The runtime state paths default to /tmp because the container runs as a non-root
# user (appuser) and WORKDIR /app is root-owned: the relative defaults
# (migration_activity.log / session_state.sqlite / job_state.sqlite) would resolve
# under /app and fail with PermissionError, crash-looping the task and failing the
# ECS deploy. The CloudFormation task definition also sets these, but baking the
# safe defaults into the image means the container is correct even when deployed
# with an older template that lacks the overrides.
ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    DSQL_MIGRATOR_APP_HOST=0.0.0.0 \
    DSQL_MIGRATOR_APP_PORT=8080 \
    DSQL_MIGRATOR_LOG_LEVEL=INFO \
    DSQL_MIGRATOR_JOB_STATE_PATH=/tmp/job_state.sqlite \
    DSQL_MIGRATOR_SESSION_STATE_PATH=/tmp/session_state.sqlite \
    DSQL_MIGRATOR_ACTIVITY_LOG_PATH=/tmp/migration_activity.log

USER appuser
EXPOSE 8080

# Default to the web UI; override the command for other entrypoints.
ENTRYPOINT ["mysql-dsql-migrator"]
CMD ["ui"]
