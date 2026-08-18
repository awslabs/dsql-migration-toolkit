# Deployment Guide — MySQL → Aurora DSQL Migration Tool (app-stack)

_Language: **English** | [한국어](DEPLOYMENT.ko.md) | [日本語](DEPLOYMENT.ja.md)_

Deploy the migration tool **inside your own AWS account** (single-tenant) — it's the
same tool and UI everywhere, only **where it runs** differs. The optional streaming
**CDC pipeline** (MSK + Debezium + sink) is a separate `cdc-stack`, not covered here.

---

## Choose where to run

Pick one — each mode has its own section below.

- **[Run locally](#run-locally)** — `uv run …`, **no infrastructure.** Your machine is
  the engine, so it must reach **both** the source MySQL and DSQL. Best for evaluation
  and small migrations. **👉 Try this first.**
- **[Deploy on ECS Fargate](#deploy-on-ecs-fargate)** — **recommended for real,
  large-scale migrations.** A single-task **ECS Fargate** service behind an **HTTPS ALB**
  in your VPC, image pulled from **ECR**, so the data path stays in AWS (not your laptop).
  The ALB is **`internal`** by default; **Cognito** (OIDC) login is an opt-in add-on for
  public exposure.
- **[Run on a single EC2 host](#run-on-a-single-ec2-host-from-source-lambda-free)** —
  runs the app **from source** (`git` + `uv` + a **systemd** service), reached over an
  **SSM port-forward** (no ALB, no public IP); state on a retained EBS volume, CDC seeded
  **in-process** (no offset-seeder Lambda). For accounts that can't use
  **containers/ECR** or **AWS Lambda**.

---

<br>

## Run locally

**Before you commit to an ECS Fargate deployment, try it locally first** — **one
command and the UI is up. That's it.**

```console
$ uv run mysql-dsql-migrator ui
NiceGUI ready to go on http://127.0.0.1:8080
```

Open that URL in your browser and you're in — **no infrastructure, no build, no AWS
resources to create.** Great for a first look, evaluation, and smaller migrations
before deciding on Fargate.

<details>
<summary><b>Screenshot</b> — the tool's UI (the guided five-step workflow)</summary>

<div align="center">
  <a href="../docs/images/demo-ui.png"><img src="../docs/images/demo-ui.png" alt="The tool's UI — the guided five-step migration workflow" width="900"></a>
</div>

</details>

The UI runs on your own machine (browser → `127.0.0.1:8080`), and **the migration
itself runs there too**: your workstation is the engine that reads the source and
writes to DSQL, so all data flows through your machine and its network. This means
your **desktop must be able to reach _both_** the source MySQL **and** the target
Aurora DSQL — a private source needs an SSM port-forward / VPN, and your machine
needs outbound HTTPS + AWS credentials to the DSQL region. Zero infra — best for
evaluation / smaller migrations / development. It is *not* the hosted architecture;
for a real migration use **[ECS Fargate](#deploy-on-ecs-fargate)**.

> [!TIP]
> **Keep your session (and edits) across restarts.** Launch with a fixed
> `DSQL_MIGRATOR_STORAGE_SECRET` so the browser-session id — the key your saved
> workbench is stored under — stays stable:
>
> ```bash
> DSQL_MIGRATOR_STORAGE_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))") \
>   uv run mysql-dsql-migrator ui
> ```
>
> - **Without it** — each restart gets a new session id, so workflow progress **and your
>   Schema Conversion edits** (a customized target DDL, e.g. `TINYINT(1)`→`smallint`) are
>   not restored, and a Full Load re-run recreates the table from the default conversion.
> - **With it** — the session resumes where you left off and the re-run reuses your
>   applied schema.
>
> Treat the value as a secret (see [`.env.example`](../.env.example)).

---

<br>

## Deploy on ECS Fargate

> [!TIP]
> **Recommended for production — real, large-scale migrations.** The whole data path
> stays inside AWS (source → Fargate → DSQL), not your laptop.

Two ways to deploy, no image build needed — the image is on **ECR Public** and
CloudFormation pulls it. Deploy the same `deploy/cloudformation.yaml` via:

- **AWS Console — recommended.** Upload the template; a guided form collects the values.
- **AWS CLI.** One `aws cloudformation deploy` with parameter overrides.

Both are detailed in [Deploy the app-stack](#2-deploy-the-app-stack) — gather the values
first in [Prerequisites](#1-prerequisites).

**Reaching the UI.** The ALB is `internal` by default, so browse
`https://<LoadBalancerDns>/` from inside the VPC — over VPN, Direct Connect, or an SSM
port-forward. There is no public endpoint by design — Well-Architected SEC05-BP02. To
expose it publicly, see the override note under [Deploy the app-stack](#2-deploy-the-app-stack).

<details>
<summary><b>Architecture diagram</b> — full topology (app-stack + optional CDC on MSK Connect)</summary>

<div align="center">
  <a href="../docs/images/architecture-aws.png"><img src="../docs/images/architecture-aws.png" alt="Full AWS architecture — the operator reaches the ECS Fargate control-plane app through an HTTPS ALB (optional Cognito); the app drives Full Load to Aurora DSQL and, for the optional CDC pipeline, deploys the cdc-stack whose Debezium source + custom DSQL sink connectors run on MSK Connect (plugins from S3), streaming through Amazon MSK to Aurora DSQL, with an in-VPC offset-seeder Lambda for a gapless handoff" width="900"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. Prerequisites

What to gather, then go straight to [Deploy the app-stack](#2-deploy-the-app-stack).
Full descriptions are in the collapsible below and in [Parameter reference](#parameter-reference).

| What | Parameter | Notes |
| --- | --- | --- |
| Access | — | AWS Console (recommended) or AWS CLI v2, able to create IAM roles, ECS, an ALB, security groups, and CloudWatch Logs. No image build — it's pulled from ECR Public. |
| A VPC | `VpcId` | Ideally your source MySQL's VPC, **same region as DSQL**. The subnets below are picked from it. |
| Two ALB + two task subnets | `AlbSubnetIds` / `ServiceSubnetIds` | Distinct AZs; task subnets need **egress on 443**. |
| An ACM certificate | `CertificateArn` | Same region. No domain? Run `AWS_REGION=<region> deploy/create_test_cert.sh` for a self-signed test cert. |
| The DSQL cluster ARN | `DsqlClusterArn` | The migration target. |
| Source-DB reachability | `SourceDbSecurityGroupId` (preferred) or `SourceDbCidr` | One of the two. |

Source-DB credentials go in the UI **after** deploy (no AWS secret unless you reuse one);
every other parameter keeps a sensible default.

<details>
<summary><b>Full parameter details</b> — VPC / subnet / certificate guidance, plus every optional value</summary>

#### Access

- **AWS Console** access (recommended path), **or** AWS CLI v2 authenticated to the
  target account (`aws sts get-caller-identity`).
- Permission to create the stack's resources: IAM roles, ECS, ELB (ALB), EC2
  security groups, CloudWatch Logs, and Cognito (optional — public ALB only).
- No image build needed — the image is pulled from ECR Public. (Building your own
  is only for a restricted network; see the Appendix.)

#### Good to know before you fill the form

> [!IMPORTANT]
> **Start with the VPC.** Use the one your source RDS/Aurora MySQL already lives
> in — same region as DSQL — and the subnets/certificate you pick from it are
> required by AWS itself (an ALB and a Fargate task must sit in subnets; an HTTPS
> listener must have a certificate). **The VPC must be owned by this account** —
> a RAM-shared (cross-account) VPC is not supported, because the CDC deploy role's
> EC2 permissions are scoped to the deploying account, so the connector's ENI
> create would fail with `AccessDenied`.

#### Optional values (sensible defaults otherwise)

| Optional | Parameter | When you need it |
| --- | --- | --- |
| **Source secret ARN** | `SourceSecretArn` | Only to **reuse an existing** Secrets Manager secret for the source creds. Leave empty to use username/password in the UI (the common case). |
| **Source DB reachability** | `SourceDbSecurityGroupId` (preferred) / `SourceDbCidr` | **Provide at least one** so the task gets egress to the source MySQL on `SourceDbPort`. `SourceDbSecurityGroupId` scopes egress to the source DB's SG; use `SourceDbCidr` if you have no SG id. With both empty the deploy is rejected (the task would have no route to the source). |
| **Custom domain** | `AppDomainName` | Only if you front the ALB with your own Route 53 domain. |
| **Public access / Cognito** | `AlbScheme`, `AllowedIngressCidr`, `EnableCognitoAuth`, `CognitoDomainPrefix` | Only to expose the UI publicly; defaults keep it `internal` (no login). |
| **AI assist** | `EnableAiAssist`, `BedrockModelId`, `BedrockRegion` | Only to enable Amazon Bedrock-assisted conversion (pick a model; IAM scope auto-derived). |
| **Custom image / sizing** | `ContainerImageUri`, `ContainerCpu`, `ContainerMemory` | Only for a private-ECR image or non-default task size. |

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. Deploy the app-stack

Two ways to deploy `deploy/cloudformation.yaml` — pick one. Both produce the same
stack; see **Parameter reference** below.

#### Recommended — AWS Console (guided form)

![CloudFormation — Create stack → Upload a template file](../docs/images/cfn-create-stack.png)

First confirm you're in the **right region** (top-right of the console — the same
region as your Aurora DSQL cluster), then:

**1. Open the Create stack wizard.** Go to the CloudFormation console:
<https://console.aws.amazon.com/cloudformation/home> → **Create stack** →
**With new resources (standard)**. (Direct link, swap your region:
`https://<region>.console.aws.amazon.com/cloudformation/home?region=<region>#/stacks/create`.)

**2. Prerequisite — Prepare template.** Choose **Template is ready**, then under
**Specify template** choose **Upload a template file** → **Choose file** →
select `deploy/cloudformation.yaml` from this repo → **Next**.

**3. Specify stack details.** Set the **Stack name** to `mysql-dsql-migrator`,
then fill the parameters. The form uses native pickers, so you **select from
your account** instead of typing ids.

**Fill these required fields** (everything else has a working default):

| Field | What to enter |
| --- | --- |
| `VpcId` | Dropdown — the VPC your source MySQL lives in. |
| `AlbSubnetIds` | Subnet multi-select — **2 subnets in distinct AZs** (see the subnet callout below). |
| `ServiceSubnetIds` | Subnet multi-select — **2 private subnets in distinct AZs** (or reuse the ALB subnets + set `AssignPublicIp=ENABLED` if you have no private/NAT subnets). |
| `CertificateArn` | ACM cert ARN for HTTPS — **no domain? see the command just below.** |
| `DsqlClusterArn` | The target Aurora DSQL cluster ARN. |
| `SourceDbSecurityGroupId` (or `SourceDbCidr`) | One of the two — scopes the task's egress to the source MySQL. Prefer the security-group id; use the CIDR only if you have none. |

> [!WARNING]
> The subnet dropdowns list **every subnet in the region**, not just your
> VpcId's. Picking one from another VPC fails the deploy — choose the right ones
> using the **"Which subnets to pick"** callout below.

**Optional:** leave `SourceSecretArn` empty unless reusing an existing source
secret — you'll enter the source host/username/password in the UI after deploy.

> [!TIP]
> **No ACM certificate yet?** Generate a self-signed **test** cert in one line, then
> paste the ARN it prints into `CertificateArn` (browsers warn; test only — for prod,
> request a real ACM cert for a domain you own instead):

```bash
AWS_REGION=<region> deploy/create_test_cert.sh
#  → prints:  CertificateArn=arn:aws:acm:<region>:<account>:certificate/xxxx
```

**Reaching the UI from your desktop browser?** The default is an `internal` ALB
(reachable only from inside the VPC/VPN). Two ways to open it from your own
machine — pick A or B:

**A. Recommended — sign in via Cognito** (works from anywhere, any number of users):

| Field | What to enter |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **public** subnets (not private) |
| `EnableCognitoAuth` | `true` — plus `CognitoDomainPrefix` and `CognitoAdminEmail` (see the note below) |
| `AllowedIngressCidr` | `0.0.0.0/0` is fine — the Cognito login is the access gate; narrow it further only if you also know your users' network CIDR |

**B. Alternative — just your machine, no login:**

| Field | What to enter |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **public** subnets (not private) |
| `AllowedIngressCidr` | your desktop public IP as `/32` — get it with `curl https://checkip.amazonaws.com` (e.g. `203.0.113.5/32`) |

Leave the remaining parameters at their defaults (e.g. the container image).
In particular **keep `HttpsEgressCidr` at `0.0.0.0/0`** — it's the task's
outbound CIDR for reaching AWS APIs (DSQL, Secrets Manager, ECR, CloudWatch) via
NAT/IGW; only tighten it if you front all of those with VPC endpoints (PrivateLink),
otherwise the task can't pull its image or reach DSQL and fails to start. → **Next**.

> [!TIP]
> **Which subnets to pick.** The dropdown lists **every subnet in the region**
> (across all your VPCs). **First narrow to your VpcId's subnets by their CIDR
> range** (e.g. a VPC on `172.31.0.0/16` → pick the `172.31.x` subnets; ignore
> other CIDRs, which belong to other VPCs), then use the **AZ column** for
> "distinct AZs" and the **Name tag** for public vs. private. Not sure which is
> which? Open the **VPC console → Subnets**, filter by your VPC, and check each
> subnet's route table (`0.0.0.0/0 → nat-…` = private with egress; `→ igw-…` =
> public) — a clear Name-tag convention (`…-private-a` / `…-public-a`) makes the
> dropdown self-explanatory going forward.

> [!IMPORTANT]
> If you picked **Option A** (Cognito) above, set `EnableCognitoAuth=true`,
> `CognitoDomainPrefix`, **and `CognitoAdminEmail`** together in step 3 (then
> continue through steps 4–5 as usual). Afterward, see
> [Point DNS at the ALB](#point-dns-at-the-alb--optional-custom-domain-only)
> (custom domain only) and
> [Create operator users](#create-operator-users-cognito--only-with-cognito) (sign
> in and add more Cognito users). `CognitoAdminEmail` is not optional here — the
> template rejects Cognito without it, because the user pool has no self sign-up
> and you would get an app with no way in. `AppDomainName` stays optional: leave
> it empty to use the ALB's own DNS name.

**4. Configure stack options.** Defaults are fine. Optionally add tags. → **Next**.

**5. Review and create.** Scroll to the bottom and **check the acknowledgement**
   "I acknowledge that AWS CloudFormation might create IAM resources with custom
   names" (`CAPABILITY_NAMED_IAM`). → **Submit**.

**6. Wait and get the URL.** The stack moves through `CREATE_IN_PROGRESS` →
   `CREATE_COMPLETE` (a few minutes; watch the **Events** tab). Then open the
   **Outputs** tab and copy **`AppUrl`** — that's your tool URL (reach it from
   inside the VPC; see "Reaching the UI" above).

**7. Open it — you should see the tool.** Browse `AppUrl` in a browser (from
   inside the VPC). The **MySQL → Aurora DSQL Migration Tool** UI loads — the
   guided workflow starting at **Connect** (Connect → Evaluation → Schema
   Conversion → Data Migration → Validation → Cut over). If it loads, the
   deployment is done; enter your source DB credentials at **Connect** to begin.

> [!NOTE]
> **▶ Next: run your first migration.** Deployment ends here — the UI is up. For
> what each step does and how to drive an actual migration, follow the
> [**User Manual**](../docs/manual/en/README.md) (start at
> [Set up](../docs/manual/en/01-setup.md) → Connect).

<details>
<summary><b>Alternative — deploy with the AWS CLI</b></summary>

#### AWS CLI

Set your environment as shell variables once; the command itself is identical for
every customer. The minimal (Dev/Test) deploy:

```bash
# --- Your environment (edit these) -------------------------------------------
export AWS_REGION=us-east-1
# VpcId: recommended -- the source DB's VPC
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# AlbSubnetIds: 2 subnets, distinct AZs
export ALB_SUBNET_IDS=subnet-0f1e2d3c4b5a69788,subnet-0a9b8c7d6e5f43210
# ServiceSubnetIds: 2 private subnets
export SERVICE_SUBNET_IDS=subnet-0123456789abcdef0,subnet-0fedcba987654321f
# CertificateArn: paste a real ACM cert ARN below, OR auto-fill a self-signed TEST
# cert (no domain needed) by capturing the script's output in one line instead:
#   export CERTIFICATE_ARN=$(deploy/create_test_cert.sh | sed -n 's/^CertificateArn=//p')
export CERTIFICATE_ARN=arn:aws:acm:us-east-1:123456789012:certificate/a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4a5b6c
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------
```

> [!WARNING]
> **Stack name: use lower case, 28 characters or fewer.** The stack provisions its
> ALB as `<stack-name>-alb`, which the ALB service caps at 32 characters — a longer
> stack name fails the deploy, after a ~2 minute rollback, with
> `The load balancer name '<stack-name>-alb' cannot be longer than '32' characters`.
> Lower case matters too:
> the ALB's DNS name inherits the casing of its name, and Cognito login (if you enable
> it) only works when that DNS name is lower case, because the ALB sends the OAuth
> `redirect_uri` with the host lower-cased and Cognito compares the two exactly.

This template exceeds CloudFormation's 51,200-byte inline-upload limit, so the CLI
needs an S3 bucket to stage it (the Console handles this invisibly — one reason
it's the recommended path). Create one once, or reuse a bucket you already have
in this account/region:

```bash
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export TEMPLATE_BUCKET=mysql-dsql-migrator-templates-$ACCOUNT-$AWS_REGION
aws s3 mb "s3://$TEMPLATE_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
```

```bash
aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" \
  --s3-bucket "$TEMPLATE_BUCKET" \
  --s3-prefix cfn-templates \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    AlbSubnetIds="$ALB_SUBNET_IDS" \
    ServiceSubnetIds="$SERVICE_SUBNET_IDS" \
    CertificateArn="$CERTIFICATE_ARN" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    EnableAiAssist=true \
    BedrockRegion="$AWS_REGION" \
    BedrockModelId=global.anthropic.claude-sonnet-5
    # BedrockModelId default shown; other model choices in §8
    # SourceSecretArn=...   # optional — only to reuse an existing source secret
```

> [!TIP]
> **AI assist (recommended).** `EnableAiAssist=true` + `BedrockRegion` turns on the
> AI DBA for Schema Conversion and the Query Converter — an opt-in, advisory-only
> feature scoped to `bedrock:InvokeModel` for the selected model. You **must still
> enable model access** for `BedrockModelId` (default
> `global.anthropic.claude-sonnet-5`) in the Bedrock console for that region, and the
> task needs egress to the Bedrock endpoint. Omit both to deploy without AI (the
> deterministic path is unchanged). Full details + model choices in §8.

For external access with Cognito sign-in, add these to
`--parameter-overrides`:

```bash
    AlbScheme=internet-facing \
    AllowedIngressCidr=0.0.0.0/0 \
    EnableCognitoAuth=true \
    CognitoDomainPrefix=<your-unique-prefix> \
    CognitoAdminEmail=<your-email>
```

All three Cognito fields are required together — the template enforces it.

> [!TIP]
> **Other overrides for `--parameter-overrides`**
>
> - **Public UI (from your desktop):** `AlbScheme=internet-facing` **and**
>   `AllowedIngressCidr=<your-public-ip>/32` (get it: `curl https://checkip.amazonaws.com`).
>   The default `10.0.0.0/8` is internal-only and blocks public browsers; never use
>   `0.0.0.0/0` (fully-open additionally requires `EnableCognitoAuth=true`).
> - **Custom domain:** add `AppDomainName=<your-domain>` — see
>   [Point DNS at the ALB](#point-dns-at-the-alb--optional-custom-domain-only).
> - **Custom image:** override `ContainerImageUri` — your own private ECR copy
>   ([pull-through cache](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html)
>   or build from `deploy/Dockerfile`, see the Appendix) for a restricted network, or
>   any other image you maintain.
> - **Review changes before applying (production):** add `--no-execute-changeset`
>   to the command above; it prints a change-set ARN instead of deploying. Inspect
>   it with `aws cloudformation describe-change-set --change-set-name <the ARN>
>   --region "$AWS_REGION" --query 'Changes[].ResourceChange.[Action,LogicalResourceId,ResourceType,Replacement]' --output table`,
>   then apply it with `aws cloudformation execute-change-set --change-set-name
>   <the ARN> --region "$AWS_REGION"` once it looks right.

Read the outputs after it completes:

```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```

Key outputs: `LoadBalancerDns`, `AppUrl`, `ClusterName`, `ServiceName`,
`TaskRoleArn`, `CognitoHostedUiDomain`.

Open **`AppUrl`** in a browser (from inside the VPC). The **MySQL → Aurora DSQL
Migration Tool** UI loads — the guided workflow starting at **Connect** (Connect
→ Evaluation → Schema Conversion → Data Migration → Validation → Cut
over). Seeing the UI means the deployment succeeded; enter your source DB
credentials at **Connect** to begin.

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### Reference and operations

Optional deep-dives — expand what you need; none of this is required for a first deploy.

<details>
<summary><b>Parameter reference and task sizing</b> — every parameter, plus how to size CPU / memory</summary>

### Parameter reference

| Parameter | Required | Default | Description |
| --- | --- | --- | --- |
| `VpcId` | yes | — | VPC that can reach the source DB privately. |
| `AlbSubnetIds` | yes | — | ≥2 subnets (distinct AZs) for the ALB. |
| `ServiceSubnetIds` | yes | — | Private subnets for the Fargate task. |
| `AlbScheme` | no | `internal` | `internal` or `internet-facing`. **Recommended: `internal`** (reach via VPN/Direct Connect/peering); use `internet-facing` only with Cognito on. |
| `CertificateArn` | yes | — | ACM cert ARN for the HTTPS (443) listener. |
| `ContainerImageUri` | no | published ECR Public image | Defaults to the image published on ECR Public — no build needed. Override only for a restricted network (your private ECR copy / pull-through cache) or a custom build; prefer an immutable tag or digest. |
| `ContainerCpu` | no | `512` | Fargate task CPU units. **Full Load is CPU-bound** (the source reader does per-row type conversion in Python), so raise this for a large migration — a measured payments+orders load ran **~3.8× faster at 4096 (4 vCPU) than at the 512 default** on the same data. Default `512` is fine for evaluation; use **4096 or higher** for a real large-scale Full Load. See [manual §7.2](../docs/manual/en/07-performance-and-tuning.md#72-tuning-parallelism). |
| `ContainerMemory` | no | `1024` | Fargate task memory (MiB) — a **hard limit**: exceed it and the kernel OOM-kills the task (no graceful shutdown, only a CloudWatch spike + ELB timeout). Memory is bounded by the buffered pipeline — roughly `table_parallelism × (prefetch + batch_parallelism) × per-batch-bytes` **summed across the worker processes**, not by table size — and **wide / oversized-LOB rows push per-batch bytes up**, so the `1024` default can be tight for a real load with concurrent source writes. **Sizing:** `1024` is fine for evaluation / small tables; use **≥ 2048** for a real Full Load, and **≥ 4096** (which also needs `ContainerCpu` ≥ 2048) when tables have large `TEXT`/`BLOB` values or you raise the parallelism settings. Must be valid for the CPU (Fargate pairs `512` CPU with 1–4 GB, `1024` with 2–8 GB, `2048` with 4–16 GB, `4096` with 8–30 GB — see the sizing table below). The app logs a memory high-water and an ~80% pressure warning (in the activity log too) so an approaching OOM is visible before the kill. |
| `AppPort` | no | `8080` | Container listen port. |
| `AssignPublicIp` | no | `DISABLED` | `ENABLED` to run the task in public subnets without a NAT (test); **recommended: keep `DISABLED`** for production (NAT gateway or VPC endpoints). |
| `AllowedIngressCidr` | no | `10.0.0.0/8` | CIDR allowed to reach the ALB on 443. **Recommended: scope to your network**, not `0.0.0.0/0`. |
| `DsqlClusterArn` | yes | — | Target DSQL cluster ARN (scopes `dsql:DbConnect`). |
| `SourceSecretArn` | no | `""` | **Optional.** Set only to **reuse an existing** Secrets Manager secret for the source creds (scopes `GetSecretValue`). Leave empty to enter username/password in the UI (the common case). |
| `SourceDbSecurityGroupId` | no* | `""` | Source DB SG; **preferred (recommended) egress target** over a raw CIDR. *One of this / `SourceDbCidr` is required. |
| `SourceDbCidr` | no* | `""` | Source DB CIDR (use if no SG id). *One of this / `SourceDbSecurityGroupId` is required. |
| `SourceDbPort` | no | `3306` | Source MySQL port. |
| `HttpsEgressCidr` | no | `0.0.0.0/0` | Destination CIDR for the task's outbound 443 (AWS APIs: DSQL token, Secrets Manager, ECR, CloudWatch, Bedrock) and 5432 (DSQL). **Recommended: leave the `0.0.0.0/0` default** — the task reaches public AWS endpoints via NAT/IGW. Only tighten (e.g. to your VPC CIDR) when you front *all* those services with interface VPC endpoints (PrivateLink); tightening without them blocks image pull / DSQL and the task fails to start. |
| `EnableCognitoAuth` | no | `false` | ALB authenticates via Cognito (OIDC). Defaults to `false`: an internal ALB (or one scoped to your CIDR) is the access gate and the operator already holds the IAM/DB permissions, so no login is needed. **Required (enforced) only when `AllowedIngressCidr=0.0.0.0/0`.** Needs **both** `CognitoDomainPrefix` and `CognitoAdminEmail` when `true`. |
| `AppDomainName` | no | `""` | DNS name fronting the ALB (must match the cert). Leave **empty** to use the ALB's own DNS name as the Cognito callback host — then no custom domain or Route 53 record is needed. |
| `CognitoDomainPrefix` | if Cognito | `""` | Globally-unique Cognito hosted-UI prefix (`https://<prefix>.auth.<region>.amazoncognito.com`). |
| `CognitoAdminEmail` | if Cognito | `""` | Email of the **first login user**, created by the stack. Cognito mails it a temporary password; the hosted UI asks for a new one on first sign-in. **Required with Cognito** — the user pool disables self sign-up, so without it the deploy succeeds and produces an app nobody can log in to (the template rejects that combination). Add more users later — see [§Create operator users](#create-operator-users-cognito--only-with-cognito). |
| `EnableAiAssist` | no | `false` | Opt-in; grants scoped `bedrock:InvokeModel`. |
| `BedrockModelArns` | no | `""` | **Optional override** of the invoke scope; blank = auto-derived from `BedrockModelId`. |
| `BedrockRegion` | no | `""` | `BEDROCK_REGION` for the app. |
| `BedrockModelId` | no | `global.anthropic.claude-sonnet-5` | Anthropic model (dropdown); IAM scope auto-derived from it. |

### Task sizing — `ContainerCpu` / `ContainerMemory`

Fargate does **not** let you pick CPU and memory independently: each CPU value allows
only a fixed memory range, and memory is a **hard limit** (over it → OOM kill, no app
shutdown). Pick a valid pair:

| `ContainerCpu` (vCPU) | Allowed `ContainerMemory` | Step |
| --- | --- | --- |
| `256` (0.25) | 512, 1024, 2048 MiB | fixed |
| `512` (0.5) | 1–4 GB | 1 GB |
| `1024` (1) | 2–8 GB | 1 GB |
| `2048` (2) | 4–16 GB | 1 GB |
| `4096` (4) | 8–30 GB | 1 GB |
| `8192` (8) | 16–60 GB | 4 GB |
| `16384` (16) | 32–120 GB | 8 GB |

**Recommended by workload:**

- **Evaluation / small tables:** the `512` / `1024` MiB default is fine.
- **Real Full Load:** **`1024` CPU / `2048` MiB** or higher. Full Load is CPU-bound
  (per-row type conversion) and memory grows with `table_parallelism × batch_parallelism`
  across worker processes — the `512`/`1024` default has OOM-killed a load that ran with
  concurrent source writes.
- **Large tables with big `TEXT`/`BLOB`, or raised parallelism:** **`4096` CPU /
  `8192`+ MiB** — wide rows enlarge each buffered batch. (To go above 4 GB memory you
  must also raise CPU: 4 GB needs CPU ≥ `1024`, 8 GB needs CPU ≥ `2048`.)

> [!TIP]
> Memory to raise it is a **redeploy** (the stack updates the task in place) — Fargate
> does not auto-scale a task's memory, and this single-task control plane does not scale
> horizontally. If unsure, size up: an over-provisioned task only costs a little more,
> an under-provisioned one OOM-kills mid-migration. The app logs a memory high-water and
> an ~80% pressure warning (also on the activity log) so you can right-size from evidence.

</details>

<details>
<summary><b>Custom domain and Cognito login</b> — optional; skip with the default internal ALB</summary>

### Point DNS at the ALB — optional (custom domain only)

Only when you set `AppDomainName` (your own domain). **Skip this with the default
setup** — you reach the app at the ALB DNS name (the `AppUrl` output) directly.

Create a Route 53 **alias A record** for `AppDomainName` targeting the ALB
(`LoadBalancerDns`). The name must match `CertificateArn`. Example
(alias to an internal ALB in your private zone):

```bash
aws elbv2 describe-load-balancers \
  --names "$(aws cloudformation describe-stack-resource \
    --stack-name mysql-dsql-migrator --logical-resource-id LoadBalancer \
    --query 'StackResourceDetail.PhysicalResourceId' --output text)" \
  --query 'LoadBalancers[0].[DNSName,CanonicalHostedZoneId]' --output text
```

Use the returned DNS name + hosted-zone id to create the alias record (console
or `aws route53 change-resource-record-sets`).

### Create operator users (Cognito) — only with Cognito

Only when you enabled Cognito (`EnableCognitoAuth=true`). Skip this with the default
`internal` ALB and no Cognito.

**The first user already exists** — the stack created it from `CognitoAdminEmail`, and
Cognito emailed a temporary password to that address. Check the **`CognitoFirstUser`**
stack output for which address it went to. To log in, open `AppUrl`, sign in with that
email + the temporary password, and set a new password when prompted.

To add **more** users, use the `CognitoUserPoolId` stack output:

```bash
POOL_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoUserPoolId'].OutputValue | [0]" \
  --output text)

aws cognito-idp admin-create-user \
  --user-pool-id "$POOL_ID" \
  --username operator@example.com \
  --user-attributes Name=email,Value=operator@example.com Name=email_verified,Value=true \
  --desired-delivery-mediums EMAIL
```

Each user receives a temporary password and is prompted to set a new one on first
sign-in via the Cognito hosted UI (triggered by the ALB). The pool has **self sign-up
disabled**, so every user must be created this way.

</details>

<details>
<summary><b>Verify, update, and AI assist</b> — post-deploy checks, new-image rollout, enabling Bedrock</summary>

### Verify

```bash
# ECS service should reach runningCount = desiredCount (1) and be ACTIVE.
aws ecs describe-services --cluster "$(... ClusterName ...)" \
  --services "$(... ServiceName ...)" \
  --query 'services[0].[status,desiredCount,runningCount]' --output text

# Tail application logs.
aws logs tail /ecs/mysql-dsql-migrator-mysql-dsql-migrator --follow --region "$AWS_REGION"
```

Then open `https://AppDomainName/` from a host inside the allowed network
(`AllowedIngressCidr`). You should be redirected to Cognito sign-in (if enabled)
and then to the migration workflow (Connect → Evaluation → Schema
Conversion → Data Migration → Validation → Cut over).

#### Observability & runtime diagnostics

Deployment is deliberately parameter-light: **log level and CloudWatch
mirroring of the activity log are not CloudFormation parameters** — adjust them
at runtime from **Settings → Diagnostics** in the app (the gear in the sidebar
footer), no redeploy:

- **Log level** — flip `INFO`/`DEBUG` while troubleshooting (DEBUG adds Python
  stacktraces to failure events; never row values or credentials).
- **Send to CloudWatch (stdout)** — toggle on to stream the activity log to
  stdout, which the container's `awslogs` driver forwards to this stack's
  CloudWatch log group (a durable audit copy that survives task replacement).
- **Download activity log** — pull the full UTC, one-line-per-event timeline
  (connection / assessment / schema apply / Full Load / CDC) from the
  **Activity log** tab of the same dialog. The file is size-capped and rotated
  on `/tmp`.

Changes apply app-wide (single task) and reset to startup defaults on restart;
advanced operators can set the startup defaults via the
`DSQL_MIGRATOR_LOG_LEVEL` / `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` environment
variables, but the Settings dialog is the intended path.

### Update to a new image version

Build and push a new tag, then redeploy with the new `ContainerImageUri`. ECS
performs a rolling replacement of the task:

```bash
export IMAGE_TAG=0.1.1
export IMAGE_URI="$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG"
docker build -f deploy/Dockerfile -t "$ECR_REPO:$IMAGE_TAG" .
docker tag "$ECR_REPO:$IMAGE_TAG" "$IMAGE_URI"
docker push "$IMAGE_URI"

aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator --region "$AWS_REGION" \
  --s3-bucket "$TEMPLATE_BUCKET" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ContainerImageUri=$IMAGE_URI
  # (re-supply the other parameters, or rely on previous values)
  # $TEMPLATE_BUCKET: the staging bucket from the AWS CLI deploy section above
  # (the template exceeds CloudFormation's 51,200-byte inline-upload limit)
```

> [!WARNING]
> The control plane runs as a **single task**, so expect a brief interruption
> during replacement. Your migrated data, the DSQL cluster, and a deployed
> cdc-stack are unaffected and recovered on reconnect. In-flight session state
> (workflow progress, an in-progress Full Load) lives on the task's ephemeral disk
> and does **not** survive — **finish or quiesce in-flight jobs before updating**,
> then reconnect and re-run the read-only Evaluation (minutes).

### Enable AI-assisted conversion (optional)

AI assist is opt-in and grants a **scoped** `bedrock:InvokeModel`:

```bash
aws cloudformation deploy ... \
  --parameter-overrides \
    EnableAiAssist=true \
    BedrockModelId=global.anthropic.claude-sonnet-5 \
    BedrockRegion=$AWS_REGION
```

**AI assist runs only on Amazon Bedrock.** Bedrock is the sole AI backend — the
tool has no field for a direct Anthropic/OpenAI (or any other) API key, so the
only model you can select is a Bedrock foundation model invoked with your AWS
credentials. Set the model with `BedrockModelId` (default
`global.anthropic.claude-sonnet-5`).

**Recommended models — the latest Anthropic Claude Opus or Sonnet:**

| Model | Bedrock model id (`BedrockModelId`) | When to use |
|---|---|---|
| Claude Sonnet 5 (default) | `global.anthropic.claude-sonnet-5` | Best balance of quality, speed, and cost for most schemas. |
| Claude Opus 5 | `global.anthropic.claude-opus-5` | Hardest `MANUAL` / `UNSUPPORTED` conversions; highest quality. |
| Claude Opus 4.8 | `global.anthropic.claude-opus-4-8` | High quality; a step below Opus 5. |
| Claude Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` | Previous-generation Sonnet. |

`BedrockModelId` is a **dropdown** of these `global.` cross-region inference profiles
(they resolve from every commercial region, so one list serves any deploy),
and the task role's `bedrock:InvokeModel` scope is **derived from it
automatically** — so you do **not** set `BedrockModelArns` (use it only to
override with a different model/ARNs). You **must still enable model access** for
the chosen model in the Bedrock console for `BedrockRegion`.

Ensure task egress can reach the Bedrock runtime endpoint (NAT or a Bedrock VPC
endpoint). Enable AI in the UI; use the **Verify AI access** preflight to
confirm reachability.

</details>

<details>
<summary><b>Teardown, troubleshooting, and security</b> — remove everything, common issues, security notes</summary>

### Teardown

> [!WARNING]
> **Complete teardown order (remove ALL resources / stop ALL cost).** The
> migration uses up to three stacks; remove them in this order so nothing — and no
> cost — is left behind:
>
> 1. **cdc-stack first (if you ever deployed CDC)** — this is the costly one
>    (Amazon MSK / MSK Connect / NAT). Remove it **while the app is still up**, from
>    the UI: **Start over (top right) → "Delete all CDC infrastructure"** (the app
>    drives the `cdc-stack` deletion, ~15–25 min). If the app is already gone, delete it
>    manually: `aws cloudformation delete-stack --stack-name mysql-dsql-cdc-stack --region "$AWS_REGION"`.
>    (CDC is the separate `cdc-stack`; see the CDC docs.)
> 2. **app-stack** — `deploy/teardown.sh` (below).
> 3. **build-stack** — only if you used Option B (CodeBuild) (below).
> 4. **Verify nothing is left** — no `mysql-dsql-*` CloudFormation stacks remain
>    (`aws cloudformation list-stacks --query "StackSummaries[?starts_with(StackName,\`mysql-dsql\`) && StackStatus!=\`DELETE_COMPLETE\`].StackName"`),
>    plus any **Route 53** records and the **CodeBuild source S3 bucket** you created.

Use the helper script (deletes the stack and waits; keeps the ECR repo by
default):

```bash
export AWS_REGION=us-east-1
deploy/teardown.sh mysql-dsql-migrator          # delete the stack only
DELETE_ECR=true deploy/teardown.sh mysql-dsql-migrator   # also remove the ECR repo + images
```

Route 53 records you created manually must be removed manually.

If you used **Option B (CodeBuild)**, also delete the build stack (its ECR repo
has `EmptyOnDelete`, so images are removed with it):

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-build --region "$AWS_REGION"
```

### Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| Service never reaches `runningCount=1` | Image pull failed (check `ContainerImageUri`, execution role, ECR egress/VPC endpoints) — see ECS service events. |
| Task stuck pulling image / no egress (private subnets, no NAT) | Either add a NAT gateway or VPC endpoints (ecr.api, ecr.dkr, S3 gateway, logs, secretsmanager, sts), or for a test set `AssignPublicIp=ENABLED` with `ServiceSubnetIds` in public subnets. |
| Task stops with `exec format error` | Image architecture mismatch. `build_and_push.sh` builds `linux/amd64` to match the task's default X86_64; only set `IMAGE_PLATFORM=linux/arm64` if the task runs on ARM64/Graviton. |
| `docker: command not found` when building | No local container runtime. Either install one (Option A: `brew install colima docker && colima start`) or use Option B (CodeBuild) to build in the cloud with no local Docker. |
| Target group unhealthy / 502 | App not listening on `0.0.0.0:AppPort`, or health check path `/` failing — check container logs. |
| 504 / timeout from ALB | Task SG does not allow inbound from the ALB SG, or task is in a subnet without egress. |
| Cognito redirect loop / 401 | `AppDomainName` must match the cert and the Cognito callback `https://AppDomainName/oauth2/idpresponse`; user not created/confirmed. |
| App cannot reach the source DB | Source DB SG must allow inbound on `SourceDbPort` from the task SG; verify `SourceDbSecurityGroupId`/`SourceDbCidr`. |
| DSQL auth errors | `DsqlClusterArn` scope, region (`DSQL_MIGRATOR_AWS_REGION`), and task-role `dsql:DbConnect`. |
| Bedrock errors when AI on | `BedrockModelArns` scope, model enabled in `BedrockRegion`, and egress to the Bedrock endpoint. |
| Need more detail when diagnosing a failure | Set log level to `DEBUG` under **Settings → Diagnostics** (the gear in the sidebar footer) to add Python stacktraces to activity-log failure events; toggle "Send to CloudWatch (stdout)" for a durable copy. No redeploy. |

### Security notes

- **Least privilege**: the task role grants only `dsql:DbConnect` +
  `dsql:DbConnectAdmin` (scoped to the cluster; the app connects as the DSQL
  `admin` role by default), the read-only `dsql:GetCluster` +
  `dsql:ListTagsForResource` (scoped to the cluster; used only to show the
  cluster's `Name` tag on the overview diagram), and
  `secretsmanager:GetSecretValue` (scoped to the
  source secret); `bedrock:InvokeModel` is added only when AI assist is enabled
  and is scoped to the allowed model ARN(s). A separate execution role handles
  ECR pull + logs and reads only the auto-generated session-cookie secret
  (scoped `secretsmanager:GetSecretValue`) to inject it at container start.
- **Auto-generated session-cookie secret**: the stack creates an
  `AWS::SecretsManager::Secret` (no operator input) that signs the browser
  session cookie (`DSQL_MIGRATOR_STORAGE_SECRET`), so the browser session id stays
  stable across restarts — the key under which the durable snapshot (next bullet)
  is found. It signs the cookie only — no DB/user credentials — and is never
  plaintext in the template.
- **Durable session resume**: each session's non-secret workbench snapshot
  (workflow progress, Evaluation result, schema choices, CDC start point) is
  written to the tool's managed plugin bucket
  (`mysql-dsql-migrator-plugins-<account>-<region>`, auto-provisioned — no operator
  input) under a `sessions/` prefix, so it survives a Fargate **task replacement**
  (a redeploy), not just an in-task restart. Combined with the stable cookie secret
  above, a reconnecting browser resumes its workbench instead of re-running Step 1
  (Evaluation). Non-secret only (Property 7) — the source DB password is re-entered
  on the Connect screen.
- **Audit trail**: the structured activity log (success + failure timeline,
  downloadable from the UI) records non-secret fields only — never row values,
  passwords, or IAM tokens. It is size-capped and rotated on the task's
  ephemeral disk; enable the CloudWatch mirror (see **Verify**) for a durable
  copy.
- **Network**: the ALB accepts 443 only from `AllowedIngressCidr`; the task
  accepts traffic only from the ALB; task egress is scoped to the source DB
  (`SourceDbPort`), outbound 443 (AWS endpoints), and 5432 (the Aurora DSQL
  endpoint). Prefer an `internal` ALB.
- **Credentials are never stored** in the template or image; the app reads the
  source secret at runtime and authenticates to DSQL with short-lived IAM
  tokens.
- **Container image CVEs (perl).** An ECR scan of the app image flags several
  `perl` CVEs (e.g. CVE-2026-12087, CVE-2026-489xx) against the base image's
  `perl 5.40.1-6`. `perl` is a **transitive package of the `python:3.12-slim`
  (Debian trixie) base**, not something the tool uses — the app is **pure Python
  and never invokes perl**, so the vulnerable code paths are **unreachable** in
  this container. The `Dockerfile` runtime stage runs `apt-get upgrade` at build
  time, so a rebuild adopts the fix automatically once Debian ships it; as of now
  these CVEs are still **open in Debian trixie/sid** (no fixed `perl` exists to
  upgrade to), so the scan cannot be cleared today by any image rebuild. If your
  compliance posture requires a clean scan before then, rebuild on a base without
  perl (e.g. a distroless Python image) — note this is a larger change that needs
  its own validation.
- This stack has **not** been deployed from this repository — validate it in
  your target account before production use.

</details>

---

<br>

## Run on a single EC2 host (from source, Lambda-free)

For accounts that **cannot use containers/ECR or AWS Lambda**. The same control-plane
app runs on **one in-VPC EC2 host, from source**, as a **`systemd` service** — no image
build, no ECR, no ALB. You reach the UI over an **SSM port-forward** (the host has no
public IP and no inbound rule). State (the Full Load job / session) lives on a
**retained EBS volume** — it survives a reboot and needs **no S3 bucket**. For CDC it
seeds Kafka **in-process**, so — unlike Fargate — it creates **no offset-seeder Lambda**
(`SeedMode=External`).

Template: **`deploy/cloudformation-ec2.yaml`**.

<details>
<summary><b>Architecture diagram</b> — single EC2 host, in-process CDC seed (SeedMode=External)</summary>

<div align="center">
  <a href="../docs/images/architecture-aws-ec2.png"><img src="../docs/images/architecture-aws-ec2.png" alt="Single EC2 host architecture — the migration tool runs from source on one in-VPC EC2 host reached over an SSM port-forward (no ALB), drives Full Load to Aurora DSQL and seeds CDC in-process to MSK, with the Debezium source + custom DSQL sink connectors on MSK Connect loading plugins from S3" width="820"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### When to use it

- ✅ Your account/policy **forbids running containers or pulling from ECR**, or
  **forbids AWS Lambda**.
- ✅ You still want the **private in-VPC data path** (source → host → DSQL) that
  Fargate gives — not routing data through a laptop.
- ❌ Otherwise prefer **[ECS Fargate](#deploy-on-ecs-fargate)**: it's the managed,
  load-balanced path with no host to patch.

> [!WARNING]
> **Single host = single point of failure.** There is no ALB, no Auto Scaling, no
> second task. State survives an instance reboot / replacement on the retained EBS
> volume, but the control plane itself is one box — fine for a migration you actively
> run, not a long-lived HA service.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. Prerequisites

- **A private subnet with internet egress (a NAT gateway).** The host has **no public
  IP**, and at **first boot it pulls `uv` / CPython / Python wheels and clones the repo
  from the public internet** (astral.sh · PyPI · GitHub) — then reaches the source DB,
  DSQL, and AWS APIs over the same egress. VPC endpoints alone won't work: those public
  sources aren't reachable over PrivateLink. Put it in **the same VPC as your source
  MySQL** (and, if you'll run CDC, the same VPC as the MSK).
- **The AWS CLI with the Session Manager plugin** on your machine — this is how you open
  the UI (there's no ALB or public endpoint; you port-forward over SSM).
  [Install guide](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
- **For CDC only — your host subnet's CIDR.** You pass it to the cdc-stack so MSK admits
  the host on port 9098 for the in-process seed (see
  [Admit the host to MSK](#5-admit-the-host-to-msk-on-9098-cdc-only) below).
- **A way for the host to fetch the app source.** By default (`SourceMode=git`) it clones
  the public repo over HTTPS — no credentials. If the host's network can't reach the
  repo, upload a tarball of your checkout to S3 and use `SourceMode=s3` (`SourceS3Uri`).

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. Required / key parameters

| Parameter | Required | Default | What it is |
| --- | --- | --- | --- |
| `VpcId` | yes | — | VPC of the source DB / MSK (same region as DSQL). |
| `HostSubnetId` | yes | — | A **NAT-egress private subnet** of `VpcId`, co-located with the MSK. |
| `DsqlClusterArn` | yes | — | Target DSQL cluster (scopes `dsql:DbConnect`). |
| `SourceDbSecurityGroupId` / `SourceDbCidr` | one required | `""` | Opens host egress to the source MySQL (SG preferred over a raw CIDR). |
| `SourceMode` | no | `git` | `git` (clone `SourceRepoUrl@SourceRepoRef` over public HTTPS) or `s3` (tarball from `SourceS3Uri`). |
| `SourceS3Uri` | if `s3` | `""` | `s3://…/source.tar.gz` of the repo root — the temporary "run my local copy" path. |
| `MskEgressCidr` | no | `0.0.0.0/0` | CIDR the host may reach MSK on 9098 for the in-process seed; narrow to the connector subnet CIDR for least privilege. |
| `InstanceType` | no | `t3.large` | Control-plane host size. |
| `StateVolumeSizeGiB` | no | `20` | Retained EBS state volume; size up for a large-table Full Load's local CSV spillover. |
| `SourceSecretArn` | no | `""` | Only to reuse an existing source-creds secret (else enter username/password in the UI). |
| `EnableAiAssist` / `BedrockModelId` / `BedrockRegion` | no | off / `global.anthropic.claude-sonnet-5` | Same opt-in Bedrock AI assist as Fargate (IAM scope auto-derived from the model). |
| `KeyName` | no | `""` | Optional SSH key; SSM is the primary access path, so usually left empty (the host has no inbound rule at all). |

> [!WARNING]
> The stack name **must not start with `mysql-dsql-cdc-`** (that prefix falls inside
> the CDC deploy role's scope). `mysql-dsql-migrator-ec2` is a good choice.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 3. Deploy

This is a CloudFormation stack (`deploy/cloudformation-ec2.yaml`), deployable two ways —
the same as Fargate:

- **AWS Console — recommended.** Upload the template and fill the guided form (native
  pickers for `VpcId` / `HostSubnetId`; the Console stages the template for you, so no
  S3 bucket is needed). The steps match the
  [Fargate Console walkthrough](#recommended--aws-console-guided-form) — just pick this
  template, enter the EC2 parameters above, and name the stack `mysql-dsql-migrator-ec2`.
- **AWS CLI** — one `aws cloudformation deploy`:

```bash
# --- Your environment (edit these) -------------------------------------------
export AWS_REGION=us-east-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# HostSubnetId: a NAT-egress private subnet in VPC_ID, co-located with the MSK
export HOST_SUBNET_ID=subnet-0123456789abcdef0
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------

# The host subnet's CIDR — scopes the host's egress to MSK (MskEgressCidr below) and,
# for CDC, admits the host on 9098 (see "Admit the host to MSK"):
export HOST_SUBNET_CIDR=$(aws ec2 describe-subnets --subnet-ids "$HOST_SUBNET_ID" \
  --region "$AWS_REGION" --query 'Subnets[0].CidrBlock' --output text)

# This template exceeds CloudFormation's 51,200-byte inline-upload limit, so the CLI
# needs an S3 bucket to stage it. Create one once, or reuse a bucket you already have:
export TEMPLATE_BUCKET=mysql-dsql-migrator-templates-$ACCOUNT-$AWS_REGION
aws s3 mb "s3://$TEMPLATE_BUCKET" --region "$AWS_REGION" 2>/dev/null || true

aws cloudformation deploy \
  --template-file deploy/cloudformation-ec2.yaml \
  --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" \
  --s3-bucket "$TEMPLATE_BUCKET" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    HostSubnetId="$HOST_SUBNET_ID" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    MskEgressCidr="$HOST_SUBNET_CIDR"
    # Default SourceMode=git clones the public repo (no credentials). If the host
    # can't reach it, add:  SourceMode=s3 SourceS3Uri=s3://$TEMPLATE_BUCKET/dsql-src.tar.gz
```

First boot takes ~3–4 min: the host installs Python 3.12 + wheels over 443, runs
`uv sync --extra cdc-external` (which brings in `kafka-python` + the MSK IAM signer for
the in-process seed), then starts the service. Progress is in
`/var/log/dsql-migrator-userdata.log` on the host.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 4. Reach the UI (SSM port-forward)

The stack outputs `HostInstanceId` and a ready-to-run `SsmPortForwardCommand`:

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region "$AWS_REGION"
```

Open `http://localhost:8080` → the tool UI loads (the same guided workflow). Check the
service health over SSM Run Command with `systemctl is-active dsql-migrator.service` and
`journalctl -u dsql-migrator`.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 5. Admit the host to MSK on 9098 (CDC only)

For CDC the host seeds Kafka in-process, so it must reach MSK Serverless on 9098 — the
cdc-stack admits it via its `HostSubnetCidr` parameter (which adds the connector-SG
ingress). **On this EC2 host that's automatic:** the host derives its own subnet CIDR at
boot and the tool passes it when you **Deploy CDC infrastructure** from the UI — you set
nothing. (You'd only pass `HostSubnetCidr` by hand if you deploy the cdc-stack yourself,
outside the tool.) Either way, if the host can't reach MSK, **Start CDC fails loudly
before creating any connector** (`CdcDeployError`) — never a silent gap.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 6. Teardown

If you deployed CDC, remove it **first, while the host is still running** — on the
**Data Migration** step, use **Delete all CDC infrastructure**. That teardown runs from
the app on the host, so deleting the host first leaves you to remove the cdc-stack by
hand (`aws cloudformation delete-stack`). Then tear down the EC2 host:

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
```

> [!WARNING]
> The state EBS volume has **`DeletionPolicy: Retain`** by design, so it **survives
> stack deletion** — delete it manually if you don't want to keep it (find it by the
> `aws:cloudformation:stack-name` tag).

---

<br>

## Appendix — Build your own image (ECS Fargate; restricted network only)

> [!NOTE]
> **This applies to the ECS Fargate deployment only** — the single-EC2-host mode runs
> from source and uses no container image, so it never needs this.
>
> **And most Fargate deployments skip it too.** The image is published to ECR Public and
> CloudFormation pulls it by default — you build nothing. Build your own image only
> if your network can't reach ECR Public (then pass the result as
> `ContainerImageUri`). Pick Option A or B below; both create the ECR repository,
> build `deploy/Dockerfile` for `linux/amd64`, push to ECR, and print the image URI.

### Option A — local build (requires a Docker-compatible runtime)

```bash
export AWS_REGION=us-east-1
deploy/build_and_push.sh            # tag defaults to the project version
# or pin an explicit tag:
deploy/build_and_push.sh 0.1.0
```

<!-- markdownlint-disable-next-line -->
Requires a running `docker` daemon (Docker Desktop, or `brew install colima
docker && colima start`).

### Option B — cloud build with AWS CodeBuild (no local Docker)

Deploy the build infrastructure once (ECR repo + S3 source bucket + CodeBuild
project), then run the helper to zip the source, upload it, and start a build:

```bash
export AWS_REGION=us-east-1

# One-time: provision the build infrastructure.
aws cloudformation deploy \
  --template-file deploy/codebuild.yaml \
  --stack-name mysql-dsql-migrator-build \
  --capabilities CAPABILITY_IAM \
  --region "$AWS_REGION"

# Each build: zip + upload source, run CodeBuild, wait, print the image URI.
deploy/build_in_codebuild.sh            # tag defaults to the project version
# or pin an explicit tag:
deploy/build_in_codebuild.sh 0.1.0
```

CodeBuild runs Docker in its managed (privileged) environment, so your machine
only needs the AWS CLI. The image is built for `linux/amd64` and pushed to the
same ECR repository.

> [!TIP]
> Use an immutable tag (or an image digest) per release so deployments are
> reproducible.
