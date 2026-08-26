# Deployment

_The deployment guide is localized — [English](DEPLOYMENT.md) · [한국어](DEPLOYMENT.ko.md) · [日本語](DEPLOYMENT.ja.md)._

This folder holds everything needed to deploy the DSQL Migration Toolkit into an
AWS account, **co-located with the infrastructure it provisions** (the guide
references these templates and scripts by path).

## ▶ Start with the deployment guide

**[`DEPLOYMENT.md`](DEPLOYMENT.md)** ([한국어](DEPLOYMENT.ko.md) · [日本語](DEPLOYMENT.ja.md))
is the full step-by-step: run locally in one command, deploy on **ECS Fargate**
(AWS Console, AWS CLI, or an AI agent), or run **from source on a single EC2 host**
(no container / Lambda) — prerequisites, parameters, custom domain / Cognito / AI
assist, teardown, and troubleshooting.

## What's in this folder

| Path | What it is |
|---|---|
| [`DEPLOYMENT.md`](DEPLOYMENT.md) (+ `.ko.md` / `.ja.md`) | The deployment guide — **start here**. |
| `cloudformation.yaml` | The **ECS Fargate app-stack** template — the production default (no image build needed). |
| `cloudformation-ec2.yaml` | The **single-EC2-host** template — runs the tool from source, no container / Lambda, for accounts that can't use ECR/Lambda. |
| [`cdc-stack/`](cdc-stack/README.md) | The optional streaming **CDC** stack (Debezium → MSK → custom Aurora DSQL sink) and its own README. |
| `Dockerfile`, `buildspec.yml`, `codebuild.yaml`, `build_*.sh` | Container image build (CodeBuild-based; no local Docker required). |
| `teardown.sh`, `run_measure_on_fargate.sh`, `create_test_cert.sh` | Teardown and operational helper scripts. |

New to the project? See the [top-level README](../README.md) for the architecture,
and the [user manual](../docs/manual/README.md) for how to actually run a migration
once the tool is deployed.
