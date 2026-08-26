# DSQL Migration Toolkit — Documentation

Documentation for the DSQL Migration Toolkit — a web tool for migrating Amazon
RDS / Aurora **MySQL** *or* **PostgreSQL** to **Amazon Aurora DSQL**. New to the
project? Start with the [top-level README](../README.md) for the architecture and
AWS-services overview; this folder is the task-oriented documentation.

## What's here

| Path | What's inside |
|---|---|
| [**`manual/`**](manual/README.md) | The step-by-step **user manual** (EN / KO / JA) — the five migration steps (Evaluation → Schema Conversion → Data Migration → Validation → Cut over), plus performance tuning & measured results, testing / verification, and a customer FAQ. **Start at [`manual/README.md`](manual/README.md).** |
| [**`dev/`**](dev/) | Developer / maintainer procedures (e.g. the hands-on [Lambda-free "EC2 + MSK only" test](dev/TEST_EC2_MSK_ONLY.md)). Not needed to run a migration. |
| [**`images/`**](images/) | Architecture and UI diagrams referenced by the README, the deployment guide, and the manual. |

## Related docs (outside `docs/`)

| Doc | Where | What's inside |
|---|---|---|
| [**Deployment guide**](../deploy/DEPLOYMENT.md) | `deploy/` | How to run the tool locally, on ECS Fargate, or from source on a single EC2 host — co-located with the CloudFormation templates it documents. Localized: [한국어](../deploy/DEPLOYMENT.ko.md) · [日本語](../deploy/DEPLOYMENT.ja.md). |
| [**Changelog**](../CHANGELOG.md) | repo root | Per-release changes (semantic-versioned). Localized: [한국어](../CHANGELOG.ko.md) · [日本語](../CHANGELOG.ja.md). |

> **Localization.** The README, deployment guide, changelog, and user manual are
> translated to Korean and Japanese; English is the source of truth. The root
> README is available as [한국어](../README.ko.md) · [日本語](../README.ja.md).
