# Handoff — make `CertificateArn` optional (keep HTTPS), auto self-signed when blank

> Context handoff so this task can be continued in **kiro-cli**. Self-contained:
> the decision, the hard constraints already researched (don't re-litigate), the
> exact implementation plan, tests, docs, and open decisions. Repo:
> `mysql-dsql-migration-tool`, branch `cdc-dlq-fix-and-topic-sizing`.

## Goal / decision (confirmed by the user)

- Make app-stack parameter **`CertificateArn` OPTIONAL** (currently required).
- **Keep HTTPS (443)** — no HTTP-only fallback. (User explicitly rejected HTTP-80.)
- **When `CertificateArn` is blank → the stack supplies a self-signed TEST cert
  itself** so the HTTPS listener can still be created with zero extra steps.
- Access model: customers reach the app via the **ALB DNS URL**, including a
  **public / internet-facing** ALB. Because the ALB URL is an `*.elb.amazonaws.com`
  name the customer doesn't own, a *publicly-trusted* cert is impossible there —
  so a **self-signed cert (browser shows "not trusted") is the only option** when
  no real cert is supplied. This is acceptable and must be documented, not hidden.
- Net effect: required params drop to **VpcId, AlbSubnetIds, ServiceSubnetIds,
  DsqlClusterArn** (cert no longer required).

## Hard constraints already researched — DO NOT re-litigate

1. **The managed Lambda Python runtime cannot generate an X.509 cert without a
   build artifact.** The runtime (python3.12+/3.13/3.14) is **Amazon Linux 2023,
   a <40 MB *minimal* image** and bundles **only boto3/botocore**. It does **not**
   include the `openssl` CLI, nor the `cryptography` / `pyOpenSSL` Python packages.
   (Sources: AWS docs "Building Lambda functions with Python" → Runtime-included
   SDK versions = boto3 only; "Using AL2023 in AWS Lambda" → minimal <40 MB image.)
   The Python **standard library has no X.509 generation** either.
   ⇒ You **cannot** mint a cert at runtime in an inline (`ZipFile`) Lambda. Doing
   so would require bundling a `cryptography` layer or a static `openssl` binary —
   a build artifact, which violates this repo's **deployment-convenience /
   no-build-artifacts** principle (`git clone` must deploy with no toolchain).
2. **`acm.import_certificate` needs only boto3** → importing a *pre-made* cert from
   a custom-resource Lambda IS artifact-free and fine.
3. **An ELBv2 HTTPS listener requires a certificate** — there is no "HTTPS without
   cert". So "optional cert + keep HTTPS" necessarily means *the stack provides one
   when the user doesn't*.

⇒ Only artifact-free way to deliver "auto cert when blank": **commit a TEST-ONLY
self-signed PEM + key into the repo and `acm.import_certificate` it via a
custom-resource Lambda**, then point the listener at the imported ARN.

## Implementation plan (recommended)

All edits in `deploy/cloudformation.yaml` unless noted.

1. **Parameter** `CertificateArn`: add `Default: ""`; change `AllowedPattern` to
   accept empty: `^$|^arn:aws[a-zA-Z-]*:acm:[a-z0-9-]+:[0-9]{12}:certificate/.+$`.
   (Mirror exactly how `SourceSecretArn` was made optional earlier this session.)
2. **Condition** (add near the others at ~line 318):
   `HasCertificateArn: Fn::Not: [ Fn::Equals: [ Ref: CertificateArn, "" ] ]`.
3. **Committed test cert** under `deploy/test-cert/`:
   - `cert.pem`, `key.pem` — self-signed, `CN=mysql-dsql-migrator.test`, long expiry
     (10y), with a SAN. Generate **once locally** using the same openssl flags as
     the existing `deploy/create_test_cert.sh` (which already does exactly this),
     but **save the files instead of importing**.
   - Add `deploy/test-cert/README.md` labeling it **TEST-ONLY — protects nothing,
     browser will warn; pass a real ACM `CertificateArn` for production.**
4. **Custom resource to import it** (app-stack has **no** Lambda today — this is the
   first). Mirror the CDC pattern at `deploy/cdc-stack/lambda/cfnresponse.py`:
   - Inline `AWS::Lambda::Function` (`Runtime: python3.12`, `Handler: index.handler`,
     code via `ZipFile`). On **Create/Update**: `acm.import_certificate(Certificate=...,
     PrivateKey=..., Tags=[{Key:'Name',Value:'<stack>-selfsigned-test'}])`, return the
     ARN as a response attribute. On **Delete**: `acm.delete_certificate(CertificateArn=...)`.
   - **How to get the PEM+key into the Lambda** — OPEN DECISION (see below). Default
     recommendation: pass them as **template content** (two `Parameters` or `Mappings`
     entries, `NoEcho: true` on the key) and read from env — simplest, no S3. They're
     committed anyway so "secrecy" is moot for a test cert.
   - **IAM role** for the Lambda: `acm:ImportCertificate`, `acm:AddTagsToCertificate`
     (cannot be resource-scoped — no ARN exists pre-create), `acm:DeleteCertificate`
     (scope by the Name tag condition if possible), and CloudWatch Logs.
   - **`Custom::SelfSignedCert` resource** wraps the function; only *materialized* when
     `HasCertificateArn` is false. Use `Condition: Fn::Not[HasCertificateArn]` style —
     i.e. add condition `NeedsSelfSignedCert` (`Fn::Equals: [Ref CertificateArn, ""]`)
     and put it on the Lambda, role, and custom resource.
5. **HTTPS listener** (`cloudformation.yaml:939-967`, `HttpsListener.Properties.Certificates`):
   ```yaml
   Certificates:
     - CertificateArn:
         Fn::If:
           - HasCertificateArn
           - Ref: CertificateArn
           - Fn::GetAtt: [SelfSignedCert, CertificateArn]   # custom resource output
   ```
6. **Delete ordering**: ACM refuses to delete a cert still in use by a listener.
   Add `DependsOn: HttpsListener` to the `Custom::SelfSignedCert` resource so CFN
   tears the listener down *before* invoking the cert-delete on stack delete.
7. **Console UX** in `Metadata.AWS::CloudFormation::Interface`:
   - `ParameterLabels.CertificateArn` → must start with **`[Optional]`** (the test
     enforces tag-matches-default) e.g.
     `"[Optional] ACM certificate ARN for HTTPS — blank = auto self-signed TEST cert (browser warns)"`.
   - Group label `"TLS & access — how operators reach the UI (cert required)"` →
     `"... (cert optional — self-signed if blank)"`.

## Tests — `tests/test_deployment_artifacts.py`

- `test_console_parameter_interface_groups_and_labels_every_param` already asserts
  **each label's `[Required]`/`[Optional]` tag matches whether the param has a
  `Default`**. So once `CertificateArn` gets `Default: ""`, its label MUST start
  with `[Optional]` or this test fails — update the label accordingly.
- If any test enumerates the required set, drop `CertificateArn` from it.
- **Add tests**: HTTPS listener is still `Protocol: HTTPS`, `Port: 443`; the
  `Certificates` entry is an `Fn::If` on `HasCertificateArn`; the
  `Custom::SelfSignedCert` + Lambda + role exist and are gated on the blank-cert
  condition; the custom resource has `DependsOn: HttpsListener`; Lambda IAM is
  scoped (no `"*"` beyond ImportCertificate's unavoidable create-time wildcard);
  `AllowedPattern` accepts `""`.
- Run: `.venv/bin/python -m pytest -q` — keep green (was **1706 passed, 2 skipped**).
- Also `aws cloudformation validate-template --template-body file://deploy/cloudformation.yaml`.

## Docs — after implementation

`deploy/DEPLOYMENT.md` (EN) and `deploy/DEPLOYMENT.ko.md` (KO), same pattern used
for SourceSecretArn earlier this session:
- Move `CertificateArn` from the **Required** table to **Optional**.
- §2 console step-3 "required values" list: remove CertificateArn; add note "blank =
  auto self-signed test cert (browser warns); pass a real ACM ARN for production".
- §2 CLI block: drop the `CertificateArn=...` required line (leave a commented hint).
- §3 parameter reference table: `CertificateArn | yes | —` → `no | ""` + the note.
- §1 "why these are required" paragraph currently cites the cert as an AWS
  requirement — revise to: the HTTPS listener still needs a cert, but the stack
  **auto-provides a test one when blank**, so the customer needn't create one.

## Security caveats — MUST be documented (non-negotiable)

- Committing `deploy/test-cert/key.pem` (a private key) conflicts with this repo's
  rule "never write credentials to disk/repo/logs". It is acceptable **only**
  because it's a **self-signed TEST cert that protects nothing** (ALB URL ⇒
  untrusted regardless) and exists purely so blank-cert deploys still get TLS.
  Label it loudly in `deploy/test-cert/README.md` and the deployment guide.
- It **may trip GitHub push-protection / gitleaks / trufflehog** for customers who
  fork+push. Note this caveat in DEPLOYMENT (and consider a `.gitleaks` allow note).
- Production guidance everywhere: **pass a real ACM `CertificateArn`.**
- Interaction with the existing Rule (`CognitoRequiredWhenIngressOpen`): a public
  ALB (`AllowedIngressCidr=0.0.0.0/0`) still forces Cognito on. Cognito OIDC needs
  HTTPS — which we have (self-signed). The browser warning doesn't block the OIDC
  flow (user accepts the warning), but mention it.

## Open decisions for kiro-cli

1. **PEM+key delivery to the import Lambda**: (a) embed as template content
   (`Parameters`/`Mappings`, `NoEcho` key) read from env — *simplest, recommended,
   no S3*; or (b) bundle the two PEMs into the Lambda zip via S3 staging — app-stack
   would gain an S3 dependency it doesn't have today (heavier). Lean (a).
2. **Re-confirm** the committed-test-key tradeoff is acceptable vs. the only
   alternative (a bundled `cryptography` Lambda layer = build artifact). User leaned
   artifact-free, so (committed PEM) is the working assumption.

## Current repo state (as of this handoff)

- Branch `cdc-dlq-fix-and-topic-sizing`. Untracked: `RESUME_CONTEXT.md`,
  `deploy/cdc-stack/lambda/`, and now this file.
- **Done earlier this session** (already committed-quality, suite green):
  - `SourceSecretArn` made optional across template + tests + EN/KO docs.
  - `[Required]`/`[Optional]` prefixes on all 24 `ParameterLabels` + fixed group
    labels; test now asserts the tag matches `Default` presence.
  - Required set is currently: VpcId, AlbSubnetIds, ServiceSubnetIds, CertificateArn,
    DsqlClusterArn. **This task removes CertificateArn from that set.**
- `deploy/cloudformation.yaml`: **46247 bytes** (< 51200, the inline `--template-body`
  CLI limit). Adding the cert PEM text may push it over 51200 → then only the inline
  CLI string form is affected; `--template-file` and console upload are unaffected.
  Watch this; if it crosses, prefer (a) compact PEM or note `--template-file` usage.
- Key locations: HTTPS listener `cloudformation.yaml:939-967`; `Conditions:` at
  `:318`; `Outputs:` at `:1099`; **no `Mappings:`**; **no existing Lambda/custom
  resource** in app-stack. CDC custom-resource reference: `deploy/cdc-stack/lambda/`
  (`cfnresponse.py`, `seeder.py`). Local self-signed helper: `deploy/create_test_cert.sh`.
- Verify after work: `.venv/bin/python -m pytest -q` green + `validate-template` ok.
