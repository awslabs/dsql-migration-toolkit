# デプロイガイド — MySQL → Aurora DSQL 移行ツール (app-stack)

_言語: [English](DEPLOYMENT.md) | [한국어](DEPLOYMENT.ko.md) | **日本語**_

このガイドでは、**コントロールプレーンアプリ**を、お客様自身の AWS アカウントおよび
VPC 内で（シングルテナント）、**Application Load Balancer (HTTPS)** の背後にある
単一タスクの **Amazon ECS Fargate** サービスとしてデプロイします。イメージは
**Amazon ECR** から取得します。デフォルトでは ALB は **`internal`** です（ログイン不要 —
ネットワークがアクセスゲートになります）。**Amazon Cognito (OIDC)** ログインは opt-in の
追加機能であり、UI を公開する場合にのみ必要です。オプションのストリーミング
**CDC パイプライン**（MSK + Debezium + シンク）は別の `cdc-stack` であり、本書では扱いません。

---

## クイックデプロイ (TL;DR)

急いでいますか。正常系（ハッピーパス）を順番に。各ステップの詳細は以下のセクションにあります。

1. 実行環境を選ぶ（テスト — Local、実際の移行 — Fargate 推奨）。
2. 必須の値を集める。
3. ACM 証明書を準備する。
4. CloudFormation テンプレートをアップロードする。
5. パラメータを入力する。
6. スタックを作成する。
7. ツールの URL（`AppUrl`）を開く。
8. （オプション）公開アクセス、Cognito ログイン、または AI アシストを有効化する。

---

## ステップ 1 — 実行場所を選ぶ

- **ローカル** — `uv run mysql-dsql-migrator ui`。UI はご自身のマシンで動作し
  （ブラウザ → `127.0.0.1:8080`）、**移行そのものもそこで実行されます**。ご自身の
  ワークステーションがソースを読み取り DSQL に書き込むエンジンとなるため、すべての
  データがご自身のマシンとそのネットワークを通過します。つまり、**ご自身のデスクトップが
  ソース MySQL _と_ ターゲット Aurora DSQL の _両方_ に到達できる必要があります** —
  プライベートなソースには SSM ポートフォワード / VPN が必要で、ご自身のマシンには
  DSQL リージョンへのアウトバウンド HTTPS + AWS 認証情報が必要です。インフラ不要 —
  評価 / 小規模な移行 / 開発に最適です。これはホスティングされたアーキテクチャ
  では *ありません*。実際の移行には Fargate を使用してください。

  > **ヒント — 再起動をまたいでセッション（と編集内容）を維持する。** 起動前に
  > `DSQL_MIGRATOR_STORAGE_SECRET` を固定のランダム文字列に設定してください。例:
  > `DSQL_MIGRATOR_STORAGE_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))") uv run mysql-dsql-migrator ui`。
  > 設定しないと、再起動のたびに新しいブラウザセッション ID が発行されるため、
  > ワークフローの進捗**および Schema Conversion の編集内容（カスタマイズした
  > ターゲット DDL — 例: `TINYINT(1)`→`smallint` の再マッピング）** が復元されず、
  > Full Load を再実行するとデフォルトの変換でテーブルが再作成されてしまいます。
  > 設定しておけば、セッションは中断したところから再開され、再実行では適用済みの
  > スキーマが再利用されます。（この値はシークレットとして扱ってください。
  > [`.env.example`](../.env.example) を参照。）
- **ECS Fargate — 推奨** — 同じエンジンが、**ご自身の VPC 内の**単一タスク Fargate
  サービス + HTTPS ALB として動作するため、データ経路はご自身のノート PC ではなく
  AWS 内にとどまります。実際のデプロイであり、本ガイドの残りが扱う対象です。

---

## ステップ 2 — ECS Fargate にデプロイ (推奨)

イメージのビルドは不要です — イメージは **ECR Public** にあり、CloudFormation が
取得します。同じ `deploy/cloudformation.yaml` をデプロイする 2 つの方法があります。

- **AWS Console — 推奨。** テンプレートをアップロードすると、ガイド付きフォームが
  値を受け取ってくれます。[セクション 2](#2-app-stack-のデプロイ) を参照。
- **AWS CLI。** パラメータのオーバーライドを伴う `aws cloudformation deploy` コマンド
  1 つで実行します。こちらも [セクション 2](#2-app-stack-のデプロイ) にあります。

まず、両方の経路で必要になる値を集めます（詳細は
[セクション 1](#1-前提条件) にあります）。**VPC から始めてください**（推奨: ソース DB が
存在する VPC）。次に、その VPC から ALB 用とタスク用の**サブネット**を選びます
（Console では VpcId を選ぶと、その VPC のサブネットがドロップダウンに表示されます）。
加えて **ACM 証明書**、**DSQL クラスター ARN**、ソース DB 用の
**Secrets Manager シークレット ARN** が必要です。残りはデフォルトで処理されます
（公開イメージ、`internal` ALB、Cognito オフ）。

**UI への到達 (internal ALB)。** ALB はデフォルトで internal なので、`https://<LoadBalancerDns>/`
には **VPC 内から**アクセスします — VPN / Direct Connect / SSM ポートフォワード。
設計上、公開エンドポイントはありません（Well-Architected SEC05-BP02）。公開する
には、セクション 2 のオーバーライドの注記を参照してください。

---

## 1. 前提条件

### アクセス

- **AWS Console** へのアクセス（推奨経路）、**または**対象アカウントに認証済みの
  AWS CLI v2（`aws sts get-caller-identity`）。
- スタックのリソースを作成する権限: IAM ロール、ECS、ELB (ALB)、EC2 セキュリティ
  グループ、CloudWatch Logs、Cognito（オプション — 公開 ALB の場合のみ）。
- イメージのビルドは不要です — イメージは ECR Public から取得されます。（自前ビルドは
  制限されたネットワーク向けのみ。付録を参照。）

### 必須の値

> 🔑 **VPC から始めてください — 残りはすべてそこから決まります。** **ソースの
> RDS/Aurora MySQL がすでに存在する VPC** を使用してください。同一 VPC が最も単純で
> 推奨される選択肢です（ツールはソースにプライベートに到達でき、ソースのセキュリティ
> グループをタスクに対してのみ開放すれば済みます）。DSQL ターゲットと**同一リージョン
> でなければなりません**。**以下の 2 つのサブネットフィールドは _この VPC の中から_
> 選びます** — AWS Console では VpcId を選ぶとその VPC のサブネットがドロップダウンで
> 表示されるので、入力するのではなく選択します。（ピアリングされた VPC / Transit
> Gateway / Direct Connect / VPN も、ルーティングと SG がタスクからソースに到達できる
> ようにすれば動作します。）

| 必須 | パラメータ | 内容 |
| --- | --- | --- |
| **VPC** | `VpcId` | 上記の VPC — 推奨: ソース DB の VPC、DSQL と同一リージョン。 |
| **ALB サブネット** | `AlbSubnetIds` | **その VPC の**サブネット 2 つ、異なる AZ — `internal` ALB（推奨）にはプライベート、internet-facing にはパブリック。 |
| **タスクサブネット** | `ServiceSubnetIds` | **その VPC の**プライベートサブネット 2 つ、異なる AZ、**443 の egress**（NAT ゲートウェイまたは VPC エンドポイント）を備え、DSQL / Secrets Manager / ECR / CloudWatch に到達できるもの。 |
| **ACM 証明書** | `CertificateArn` | HTTPS リスナー用の**同一リージョン**の ACM 証明書の **ARN**（`arn:aws:acm:<region>:<account>:certificate/<id>`）。**本番:** 保有するドメイン用に ACM パブリック証明書を発行してください。**手早いテスト（ドメインなし）:** `AWS_REGION=<region> deploy/create_test_cert.sh` を実行し、出力される `CertificateArn` を貼り付けます（自己署名。ブラウザは警告します）。既存の ARN は ACM コンソールからコピーします。 |
| **DSQL クラスター ARN** | `DsqlClusterArn` | ターゲットの Aurora DSQL クラスター。 |

> **ソースの認証情報**は、デプロイ**後**に UI（Connect ステップ）で入力します —
> 通常は**ユーザー名/パスワード**（RDS/Aurora MySQL の一般的なケース）で、メモリ上に
> 保持され、AWS シークレットは不要です。したがって `SourceSecretArn` は**オプション**です
> （次の表）: 既存の Secrets Manager シークレットを再利用する場合にのみ設定してください。

> **なぜ VPC 以外にこれらだけが必須なのか。** サブネットと証明書は AWS 自体が要求します —
> ALB と Fargate タスクは必ずサブネットに配置する必要があり、HTTPS リスナーには証明書が
> 必要で、CloudFormation は VPC だけからそれらを自動で選ぶことができません。DSQL
> クラスター ARN は移行の**ターゲット**です。残りはデフォルトがあります（次の表）。

### オプションの値 (それ以外の場合は妥当なデフォルト)

| オプション | パラメータ | 必要になる場合 |
| --- | --- | --- |
| **ソースシークレット ARN** | `SourceSecretArn` | 既存の Secrets Manager シークレットをソース認証情報として**再利用する場合のみ**。空のままにすると UI でユーザー名/パスワードを使用します（一般的なケース）。 |
| **ソース DB への到達性** | `SourceDbSecurityGroupId`（推奨） / `SourceDbCidr` | タスクが `SourceDbPort` でソース MySQL への egress を得られるよう、**少なくとも一方を指定**してください。`SourceDbSecurityGroupId` は egress をソース DB の SG に絞り込みます。SG id がない場合は `SourceDbCidr` を使用します。両方とも空だとデプロイは拒否されます（タスクにソースへの経路がなくなるため）。 |
| **カスタムドメイン** | `AppDomainName` | ご自身の Route 53 ドメインで ALB をフロントする場合のみ。 |
| **公開アクセス / Cognito** | `AlbScheme`, `AllowedIngressCidr`, `EnableCognitoAuth`, `CognitoDomainPrefix` | UI を公開する場合のみ。デフォルトは `internal`（ログインなし）のままです。 |
| **AI アシスト** | `EnableAiAssist`, `BedrockModelId`, `BedrockRegion` | Amazon Bedrock 支援の変換を有効化する場合のみ（モデルを選択。IAM スコープは自動的に導出）。 |
| **カスタムイメージ / サイジング** | `ContainerImageUri`, `ContainerCpu`, `ContainerMemory` | プライベート ECR イメージ、またはデフォルト以外のタスクサイズの場合のみ。 |

---

## 2. app-stack のデプロイ

`deploy/cloudformation.yaml` をデプロイする 2 つの方法があります — いずれか 1 つを
選んでください。どちらも同じスタックを作成します。パラメータのリファレンスは
セクション 3 です。

### 推奨 — AWS Console (ガイド付きフォーム)

まず、**正しいリージョン**（コンソール右上 — Aurora DSQL クラスターと同一リージョン）
にいることを確認し、次に:

> **開始前に — `CertificateArn` を用意しておく。** コンソールは HTTPS 証明書を
> 代わりに生成できません。保有ドメイン用の ACM 証明書がまだない場合は、先に
> ターミナルで `AWS_REGION=<region> deploy/create_test_cert.sh` を実行し、出力される
> `arn:aws:acm:…` を保管して step 3 で貼り付けられるようにしてください（自己署名の
> テスト証明書 — ブラウザは警告します。本番では、ご自身のドメイン用の実際の ACM
> 証明書を使用してください）。
>
> **デスクトップから到達しますか。パブリック IP も取得しておく。** `AlbScheme=internet-facing`
> を設定する場合は、今すぐ `curl https://checkip.amazonaws.com` で IP を取得し、
> step 3 で `AllowedIngressCidr=<その IP>/32` を入力して、あなただけが ALB に
> 到達できるようにしてください。デフォルトの `10.0.0.0/8` は internal ALB 用
> （VPC/VPN 内部から到達）であり、公開ブラウザをブロックします。

**1. Create stack ウィザードを開く。** CloudFormation コンソールへ移動:
<https://console.aws.amazon.com/cloudformation/home> → **Create stack** →
**With new resources (standard)**。（直接リンク、リージョンを置き換えてください:
`https://<region>.console.aws.amazon.com/cloudformation/home?region=<region>#/stacks/create`。）

**2. Prerequisite — Prepare template。** **Template is ready** を選択し、次に
**Specify template** の下で **Upload a template file** → **Choose file** →
このリポジトリの `deploy/cloudformation.yaml` を選択 → **Next**。

**3. Specify stack details。** **Stack name** を `mysql-dsql-migrator` に設定し、
続いてパラメータを入力します。フォームはグループ化されており（Network /
Migration endpoints / TLS & access / Authentication / Container image & sizing / AI）、
ネイティブのピッカーを備えているため、id を入力する代わりに**ご自身のアカウントから
選択**します。

**以下の必須フィールドを入力します**（それ以外はすべて動作するデフォルトがあります）:

| フィールド | 入力する値 |
| --- | --- |
| `VpcId` | ドロップダウン — ソース MySQL が存在する VPC。 |
| `AlbSubnetIds` | サブネットのマルチセレクト — **異なる AZ の 2 つのサブネット**（下のサブネットの注記を参照）。 |
| `ServiceSubnetIds` | サブネットのマルチセレクト — **異なる AZ の 2 つのプライベートサブネット**（プライベート/NAT サブネットがない場合は ALB サブネットを流用し `AssignPublicIp=ENABLED` を設定）。 |
| `CertificateArn` | HTTPS 用の ACM 証明書 ARN — **ドメインがない場合は、すぐ下のコマンドを参照。** |
| `DsqlClusterArn` | ターゲットの Aurora DSQL クラスター ARN。 |

> ⚠️ サブネットのドロップダウンは、ご自身の VpcId のものだけでなく、**リージョン内の
> すべてのサブネット**を一覧表示します。別の VPC のものを選ぶとデプロイが失敗します —
> 下の **「どのサブネットを選ぶか」** の注記を使って正しいものを選んでください。

**推奨:** `SourceDbSecurityGroupId`（または `SourceDbCidr`）を設定して、タスクが
ソースに到達できるようにします。既存のソースシークレットを再利用する場合を除き
`SourceSecretArn` は空のままにします — デプロイ後に UI でソースのホスト/ユーザー名/
パスワードを入力します。

**ACM 証明書がまだありませんか。** 自己署名の**テスト**証明書を 1 行で生成し、
出力される ARN を `CertificateArn` に貼り付けます（ブラウザは警告します。テスト専用）:

```bash
AWS_REGION=<region> deploy/create_test_cert.sh
#  → 出力:  CertificateArn=arn:aws:acm:<region>:<account>:certificate/xxxx
```

**デスクトップのブラウザから UI に到達しますか。** デフォルトは `internal` ALB
（VPC/VPN 内部からのみ到達可能）です。ご自身のマシンから開くには、以下の 3 つを
まとめて設定します:

| フィールド | 入力する値 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **パブリック**サブネット（プライベートではない） |
| `AllowedIngressCidr` | デスクトップのパブリック IP を `/32` で — `curl https://checkip.amazonaws.com` で取得（例: `203.0.113.5/32`） |

internet-facing ALB で `AllowedIngressCidr` をデフォルトの `10.0.0.0/8` のままにすると
ブラウザがブロックされます。`0.0.0.0/0`（インターネット全体）は追加で
`EnableCognitoAuth=true` が必要です。

それ以外はすべてデフォルトのままにします（公開イメージ、`internal` ALB、Cognito
オフ）。特に **`HttpsEgressCidr` は `0.0.0.0/0` のままにしてください** — これは
タスクが NAT/IGW 経由で AWS API（DSQL、Secrets Manager、ECR、CloudWatch）に到達する
ためのアウトバウンド CIDR です。これらすべてを VPC エンドポイント（PrivateLink）で
フロントする場合にのみ絞り込んでください。そうでないのに絞り込むと、タスクは
イメージを取得できず DSQL に到達できず、起動に失敗します。→ **Next**。

> **どのサブネットを選ぶか。** ドロップダウンは（すべての VPC にわたる）**リージョン内の
> すべてのサブネット**を `subnet-id | CIDR | アベイラビリティーゾーン | Name タグ` として
> 一覧表示します。**まず CIDR 範囲でご自身の VpcId のサブネットに絞り込んでください**
> （例: `172.31.0.0/16` の VPC → `172.31.x` のサブネットを選び、他の VPC に属する別の
> CIDR は無視）。次に **AZ 列**を使って「異なる AZ」を満たし、**Name タグ**でパブリックと
> プライベートを見分けます。次の表に従って選んでください（スタックは事前にフラグを
> 付けられません — ドロップダウンは AWS がご自身のアカウントから埋めます）:
>
> | フィールド | 推奨サブネット |
> | --- | --- |
> | `AlbSubnetIds` | **2 つの異なる AZ にある 2 つのサブネット。** デフォルトの `internal` ALB には**プライベート**サブネット、`internet-facing` には**パブリック**を使用。 |
> | `ServiceSubnetIds` | **2 つの異なる AZ にある 2 つのプライベートサブネット**。それぞれアウトバウンド 443（NAT ゲートウェイのルート、または VPC エンドポイント）を備え、タスクが DSQL / Secrets Manager / ECR に到達できるもの。 |
>
> どれがどれか分からない場合は、**VPC コンソール → Subnets** を開き、ご自身の VPC で
> フィルターして、各サブネットの AZ とルートテーブルを確認します（`0.0.0.0/0 → nat-…`
> のルート = egress を持つプライベート。`→ igw-…` = パブリック）。明確な Name タグの
> 規約（例: `…-private-a` / `…-public-a`）があれば、ドロップダウンが一目で分かります。

**4. Configure stack options。** デフォルトで問題ありません。必要ならタグを追加します。→ **Next**。

**5. Review and create。** 一番下までスクロールし、確認事項
   「I acknowledge that AWS CloudFormation might create IAM resources with custom
   names」（`CAPABILITY_NAMED_IAM`）を**チェック**します。→ **Submit**。

**6. 待機して URL を取得する。** スタックは `CREATE_IN_PROGRESS` →
   `CREATE_COMPLETE` へと進みます（数分。**Events** タブで観察してください）。その後、
   **Outputs** タブを開いて **`AppUrl`** をコピーします — これがツールの URL です
   （VPC 内から到達してください。上記の「UI への到達」を参照）。

**7. 開く — ツールが表示されるはずです。** ブラウザで `AppUrl` にアクセスします
   （VPC 内から）。**MySQL → Aurora DSQL Migration Tool** の UI が読み込まれます —
   **Connect** から始まるガイド付きワークフロー（Connect → Migration plan → Evaluation
   → Schema Conversion → Data Migration → Validation → Cut over）です。読み込まれれば
   デプロイは完了です。**Connect** でソース DB の認証情報を入力して開始します。

> **▶ 次: 最初の移行を実行する。** デプロイはここで終わりです — UI が起動しています。
> 各ステップが何をするか、実際の移行をどう進めるかは、
> [**ユーザーマニュアル**](../docs/manual/ja/README.md) に従ってください（[セットアップ](../docs/manual/ja/01-setup.md)
> → Connect から開始）。

**Prod プロファイル**の場合は、step 3 で追加で `EnableCognitoAuth=true`、
`CognitoDomainPrefix`、`AppDomainName` を設定してください（その後セクション 4〜5 を実施）。

### AWS CLI

環境をシェル変数として一度設定します。コマンド自体はどのお客様でも同一です。
最小構成（Dev/Test）のデプロイ:

```bash
# --- あなたの環境 (ここを編集) -----------------------------------------------
export AWS_REGION=us-east-1
export VPC_ID=vpc-xxxxxxxx                               # 推奨: ソース DB の VPC
export ALB_SUBNET_IDS=subnet-aaaaaaa,subnet-bbbbbbb      # サブネット 2 つ、異なる AZ
export SERVICE_SUBNET_IDS=subnet-ccccccc,subnet-ddddddd  # プライベートサブネット 2 つ
# CertificateArn: 下に実際の ACM 証明書 ARN を貼り付けるか、自己署名テスト証明書
# (ドメイン不要) をスクリプト出力の 1 行キャプチャで自動入力する:
#   export CERTIFICATE_ARN=$(deploy/create_test_cert.sh | sed -n 's/^CertificateArn=//p')
export CERTIFICATE_ARN=arn:aws:acm:us-east-1:<account>:certificate/xxxx
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:<account>:cluster/xxxx
export SOURCE_DB_SG=sg-source
# -----------------------------------------------------------------------------

aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" \
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
    BedrockModelId=us.anthropic.claude-sonnet-4-6
    # BedrockModelId はデフォルト値を表示; 他のモデル選択肢は §8 参照
    # SourceSecretArn=...   # オプション — 既存のソースシークレットを再利用する場合のみ
```

> **AI アシスト（推奨）。** `EnableAiAssist=true` + `BedrockRegion` で Schema
> Conversion と Query Playground の AI DBA を有効にします — オプトインかつ助言専用の
> 機能で、選択したモデルに対する `bedrock:InvokeModel` のみにスコープされます。
> `BedrockModelId`（デフォルト `us.anthropic.claude-sonnet-4-6`）について、その
> リージョンの Bedrock コンソールで**モデルアクセスを有効化**する必要があり、タスクが
> Bedrock エンドポイントへ egress できる必要があります。両方省略すれば AI なしで
> デプロイされます（決定論的パスは変わりません）。詳細とモデル選択は §8 を参照。

**Prod プロファイル**の場合は次を追加します: `EnableCognitoAuth=true`、
`CognitoDomainPrefix=...`、`AppDomainName=...`（および自前イメージの場合は任意で
`ContainerImageUri=...`）。

> **テストのショートカット / オーバーライド**
>
> - **ACM 証明書 / ドメインなし:** `AWS_REGION=us-east-1 deploy/create_test_cert.sh`
>   が自己署名証明書をインポートします。その `CertificateArn` を使用します（ブラウザは警告します。テスト専用）。
> - **NAT ゲートウェイなし:** `AssignPublicIp=ENABLED` を設定し、`ServiceSubnetIds` を
>   パブリックサブネットに配置します（タスクは依然として ALB SG 経由でのみ到達可能）。
> - **公開 UI（デスクトップから）:** `AlbScheme=internet-facing` **かつ**
>   `AllowedIngressCidr=<あなたのパブリック IP>/32`（取得方法: `curl https://checkip.amazonaws.com`）。
>   デフォルトの `10.0.0.0/8` は内部専用で公開ブラウザをブロックします。`0.0.0.0/0` は
>   決して使用しないでください（完全開放は追加で `EnableCognitoAuth=true` を必要とします）。
> - **制限されたネットワーク（ECR Public 不可）:** `ContainerImageUri` をご自身の
>   プライベート ECR コピーでオーバーライドします（[pull-through キャッシュ](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html)
>   または `deploy/Dockerfile` からビルド。付録を参照）。

よろしければ、まずテンプレートを検証します:

```bash
aws cloudformation validate-template \
  --template-body file://deploy/cloudformation.yaml --region "$AWS_REGION"
```

完了後、出力を読み取ります:

```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```

主な出力: `LoadBalancerDns`、`AppUrl`、`ClusterName`、`ServiceName`、
`TaskRoleArn`、`CognitoHostedUiDomain`。

ブラウザで **`AppUrl`** を開きます（VPC 内から）。**MySQL → Aurora DSQL
Migration Tool** の UI が読み込まれます — **Connect** から始まるガイド付き
ワークフロー（Connect → Migration plan → Evaluation → Schema Conversion →
Data Migration → Validation → Cut over）です。UI が表示されればデプロイは
成功です。**Connect** でソース DB の認証情報を入力して開始します。

---

## 3. パラメータリファレンス

| パラメータ | 必須 | デフォルト | 説明 |
| --- | --- | --- | --- |
| `VpcId` | yes | — | ソース DB にプライベートに到達できる VPC。 |
| `AlbSubnetIds` | yes | — | ALB 用のサブネット ≥2 つ（異なる AZ）。 |
| `ServiceSubnetIds` | yes | — | Fargate タスク用のプライベートサブネット。 |
| `AlbScheme` | no | `internal` | `internal` または `internet-facing`。**推奨: `internal`**（VPN/Direct Connect/ピアリング経由で到達）。`internet-facing` は Cognito オンの場合のみ使用。 |
| `CertificateArn` | yes | — | HTTPS (443) リスナー用の ACM 証明書 ARN。 |
| `ContainerImageUri` | no | 公開された ECR Public イメージ | デフォルトは ECR Public に公開されたイメージ — ビルド不要。制限されたネットワーク（ご自身のプライベート ECR コピー / pull-through キャッシュ）またはカスタムビルドの場合のみオーバーライド。イミュータブルなタグまたはダイジェストを推奨。 |
| `ContainerCpu` | no | `512` | Fargate タスクの CPU ユニット。 |
| `ContainerMemory` | no | `1024` | Fargate タスクのメモリ（MiB）。CPU に対して有効な値。 |
| `AppPort` | no | `8080` | コンテナのリッスンポート。 |
| `AssignPublicIp` | no | `DISABLED` | NAT なしでタスクをパブリックサブネットで実行するには `ENABLED`（テスト）。**推奨: 本番では `DISABLED` のまま**（NAT ゲートウェイまたは VPC エンドポイント）。 |
| `AllowedIngressCidr` | no | `10.0.0.0/8` | ALB の 443 に到達を許可する CIDR。**推奨: `0.0.0.0/0` ではなく、ご自身のネットワークに絞り込む**。 |
| `DsqlClusterArn` | yes | — | ターゲットの DSQL クラスター ARN（`dsql:DbConnect` のスコープを指定）。 |
| `SourceSecretArn` | no | `""` | **オプション。** 既存の Secrets Manager シークレットをソース認証情報として**再利用する**場合のみ設定します（`GetSecretValue` のスコープを指定）。空のままにすると UI でユーザー名/パスワードを入力します（一般的なケース）。 |
| `SourceDbSecurityGroupId` | no* | `""` | ソース DB の SG。生の CIDR よりも**推奨される（望ましい）egress ターゲット**。*これ / `SourceDbCidr` のいずれか一方が必須。 |
| `SourceDbCidr` | no* | `""` | ソース DB の CIDR（SG id がない場合に使用）。*これ / `SourceDbSecurityGroupId` のいずれか一方が必須。 |
| `SourceDbPort` | no | `3306` | ソース MySQL のポート。 |
| `HttpsEgressCidr` | no | `0.0.0.0/0` | タスクのアウトバウンド 443（AWS API: DSQL トークン、Secrets Manager、ECR、CloudWatch、Bedrock）および 5432（DSQL）の宛先 CIDR。**推奨: デフォルトの `0.0.0.0/0` のまま**にする — タスクは NAT/IGW 経由でパブリックな AWS エンドポイントに到達します。絞り込み（例: ご自身の VPC CIDR へ）は、それらのサービス*すべて*をインターフェース VPC エンドポイント（PrivateLink）でフロントする場合にのみ行ってください。エンドポイントなしで絞り込むとイメージの取得 / DSQL がブロックされ、タスクは起動に失敗します。 |
| `EnableCognitoAuth` | no | `false` | ALB が Cognito (OIDC) で認証します。デフォルトは `false`: internal ALB（またはご自身の CIDR に絞り込んだ ALB）がアクセスゲートであり、運用者はすでに IAM/DB の権限を保持しているため、ログインは不要です。**`AllowedIngressCidr=0.0.0.0/0` の場合にのみ必須（強制されます）。** `true` の場合は `CognitoDomainPrefix` が必要です。 |
| `AppDomainName` | Cognito 時 | `""` | ALB をフロントする DNS 名（証明書と一致）。 |
| `CognitoDomainPrefix` | Cognito 時 | `""` | グローバルに一意な Cognito hosted-UI プレフィックス。 |
| `EnableAiAssist` | no | `false` | opt-in。スコープが絞られた `bedrock:InvokeModel` を付与。 |
| `BedrockModelArns` | no | `""` | invoke スコープの**オプションのオーバーライド**。空欄 = `BedrockModelId` から自動導出。 |
| `BedrockRegion` | no | `""` | アプリの `BEDROCK_REGION`。 |
| `BedrockModelId` | no | `us.anthropic.claude-sonnet-4-6` | Anthropic モデル（ドロップダウン）。IAM スコープはこれから自動導出。 |

---

## 4. DNS を ALB に向ける — オプション (カスタムドメインのみ)

`AppDomainName`（ご自身のドメイン）を設定した場合のみです。**デフォルト設定では
これをスキップしてください** — アプリには ALB の DNS 名（`AppUrl` 出力）で直接
到達します。

`AppDomainName` 用の Route 53 **エイリアス A レコード**を作成し、ALB
（`LoadBalancerDns`）をターゲットにします。名前は `CertificateArn` と一致しなければ
なりません。例（プライベートゾーン内の internal ALB へのエイリアス）:

```bash
aws elbv2 describe-load-balancers \
  --names "$(aws cloudformation describe-stack-resource \
    --stack-name mysql-dsql-migrator --logical-resource-id LoadBalancer \
    --query 'StackResourceDetail.PhysicalResourceId' --output text)" \
  --query 'LoadBalancers[0].[DNSName,CanonicalHostedZoneId]' --output text
```

返された DNS 名 + ホストゾーン id を使ってエイリアスレコードを作成します
（コンソールまたは `aws route53 change-resource-record-sets`）。

---

## 5. 運用者ユーザーの作成 (Cognito) — オプション

Cognito を有効化した場合のみです（`EnableCognitoAuth=true`、すなわち公開 ALB）。
デフォルトの `internal` ALB ではこれをスキップします。スタックのユーザープールに
ユーザーを作成します:

```bash
POOL_ID=$(aws cognito-idp list-user-pools --max-results 60 \
  --query "UserPools[?Name=='mysql-dsql-migrator-users'].Id | [0]" --output text)

aws cognito-idp admin-create-user \
  --user-pool-id "$POOL_ID" \
  --username operator@example.com \
  --user-attributes Name=email,Value=operator@example.com Name=email_verified,Value=true
```

ユーザーは一時パスワードを受け取り、（ALB がトリガーする）Cognito hosted UI 経由の
初回サインイン時に新しいパスワードを設定するよう求められます。

---

## 6. 検証

```bash
# ECS サービスは runningCount = desiredCount (1) に達し、ACTIVE であるべきです。
aws ecs describe-services --cluster "$(... ClusterName ...)" \
  --services "$(... ServiceName ...)" \
  --query 'services[0].[status,desiredCount,runningCount]' --output text

# アプリケーションログを tail します。
aws logs tail /ecs/mysql-dsql-migrator-mysql-dsql-migrator --follow --region "$AWS_REGION"
```

次に、許可されたネットワーク（`AllowedIngressCidr`）内のホストから
`https://AppDomainName/` を開きます。（有効な場合は）Cognito サインインに
リダイレクトされ、その後、移行ワークフロー（Connect → Migration plan → Evaluation →
Schema Conversion → Data Migration → Validation → Cut over）に移動するはずです。

### 可観測性 & ランタイム診断

デプロイは意図的にパラメータを最小限にしています: **ログレベルとアクティビティログの
CloudWatch ミラーリングは CloudFormation パラメータではありません** — これらはアプリの
**Diagnostics** コントロール（サイドバーのフッター）からランタイムで調整します。
再デプロイは不要です:

- **ログレベル** — トラブルシューティング中に `INFO`/`DEBUG` を切り替えます（DEBUG は
  失敗イベントに Python のスタックトレースを追加します。行の値や認証情報は決して
  含みません）。
- **Send to CloudWatch (stdout)** — オンにするとアクティビティログを stdout に
  ストリーミングし、コンテナの `awslogs` ドライバーがそれをこのスタックの CloudWatch
  ロググループに転送します（タスクの置き換えを乗り越えて残る、耐久性のある監査コピー）。
- **Download activity log** — 同じフッターから、完全な UTC のイベントごとに 1 行の
  タイムライン（接続 / 評価 / スキーマ適用 / Full Load / CDC）を取得します。ファイルは
  `/tmp` 上でサイズ上限が設けられローテーションされます。

変更はアプリ全体（単一タスク）に適用され、再起動時に起動時のデフォルトにリセット
されます。上級の運用者は `DSQL_MIGRATOR_LOG_LEVEL` / `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT`
環境変数で起動時のデフォルトを設定できますが、Diagnostics コントロールが意図された
経路です。

---

## 7. 新しいイメージバージョンへの更新

新しいタグをビルドしてプッシュし、新しい `ContainerImageUri` で再デプロイします。
ECS はタスクのローリング置き換えを実行します:

```bash
export IMAGE_TAG=0.1.1
export IMAGE_URI="$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG"
docker build -f deploy/Dockerfile -t "$ECR_REPO:$IMAGE_TAG" .
docker tag "$ECR_REPO:$IMAGE_TAG" "$IMAGE_URI"
docker push "$IMAGE_URI"

aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator --region "$AWS_REGION" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ContainerImageUri=$IMAGE_URI
  # (他のパラメータを再度指定するか、以前の値に依存する)
```

> コントロールプレーンは**単一タスク**として実行されるため、置き換え中は短い中断が
> 予想されます。移行済みのデータ、DSQL クラスター、デプロイ済みの cdc-stack は影響を
> 受けず、再接続時に復旧します。進行中のセッション状態（ワークフローの進捗、進行中の
> Full Load）はタスクの一時ディスク上に存在し、**残りません** — **更新前に進行中の
> ジョブを完了または静止させて**から、再接続して読み取り専用の Evaluation を再実行
> してください（数分）。

---

## 8. AI 支援変換の有効化 (オプション)

AI アシストは opt-in であり、**スコープが絞られた** `bedrock:InvokeModel` を付与します:

```bash
aws cloudformation deploy ... \
  --parameter-overrides \
    EnableAiAssist=true \
    BedrockModelId=us.anthropic.claude-sonnet-4-6 \
    BedrockRegion=$AWS_REGION
```

**AI アシストは Amazon Bedrock 上でのみ動作します。** Bedrock が唯一の AI バックエンド
です — このツールには直接の Anthropic/OpenAI（またはその他）の API キーを入力する
欄がないため、選択できるモデルは、ご自身の AWS 認証情報で呼び出す Bedrock 基盤モデル
だけです。モデルは `BedrockModelId` で設定します（デフォルト
`us.anthropic.claude-sonnet-4-6`）。

**推奨モデル — 最新の Anthropic Claude Opus または Sonnet:**

| モデル | Bedrock モデル id (`BedrockModelId`) | 使用する場面 |
|---|---|---|
| Claude Opus 4.8 | `us.anthropic.claude-opus-4-8` | 最も難しい `MANUAL` / `UNSUPPORTED` の変換。最高品質。 |
| Claude Opus 4.6 | `us.anthropic.claude-opus-4-6-v1` | 高品質。4.8 より一段下。 |
| Claude Sonnet 4.6 (デフォルト) | `us.anthropic.claude-sonnet-4-6` | ほとんどのスキーマで品質・速度・コストの最良のバランス。 |

`BedrockModelId` はこれらの `us.` クロスリージョン推論プロファイルの**ドロップダウン**
であり、タスクロールの `bedrock:InvokeModel` スコープはそこから**自動的に導出**されます
— したがって `BedrockModelArns` は**設定する必要はありません**（別のモデル/ARN で
オーバーライドする場合にのみ使用）。ただし、選択したモデルについて `BedrockRegion` の
Bedrock コンソールで**モデルアクセスを有効化する必要は依然としてあります**。

タスクの egress が Bedrock ランタイムエンドポイントに到達できることを確認してください
（NAT または Bedrock VPC エンドポイント）。UI で AI を有効化し、**Verify AI access** の
事前チェックで到達性を確認してください。

---

## 9. Teardown

> **完全な teardown の順序 (すべてのリソースを削除 / すべてのコストを停止)。** 移行は
> 最大 3 つのスタックを使用します。何も — そしてコストも — 残らないよう、この順序で
> 削除してください:
>
> 1. **最初に cdc-stack（一度でも CDC をデプロイした場合）** — これはコストのかかるもの
>    です（Amazon MSK / MSK Connect / NAT）。**アプリがまだ起動している間に** UI から
>    削除します: **Start over（右上） → 「Delete all CDC infrastructure」**（アプリが
>    `cdc-stack` の削除を実行、~15〜25 分）。アプリがすでにない場合は手動で削除します:
>    `aws cloudformation delete-stack --stack-name mysql-dsql-cdc-stack --region "$AWS_REGION"`。
>    （CDC は別の `cdc-stack` です。CDC のドキュメントを参照。）
> 2. **app-stack** — `deploy/teardown.sh`（下記）。
> 3. **build-stack** — Option B（CodeBuild）を使用した場合のみ（下記）。
> 4. **何も残っていないことを確認** — `mysql-dsql-*` の CloudFormation スタックが残って
>    いないこと
>    （`aws cloudformation list-stacks --query "StackSummaries[?starts_with(StackName,\`mysql-dsql\`) && StackStatus!=\`DELETE_COMPLETE\`].StackName"`）、
>    加えて、作成したすべての **Route 53** レコードと **CodeBuild ソース S3 バケット**。

ヘルパースクリプトを使用します（スタックを削除して待機します。デフォルトでは ECR
リポジトリを保持します）:

```bash
export AWS_REGION=us-east-1
deploy/teardown.sh mysql-dsql-migrator          # スタックのみ削除
DELETE_ECR=true deploy/teardown.sh mysql-dsql-migrator   # ECR リポジトリ + イメージも削除
```

手動で作成した Route 53 レコードは手動で削除する必要があります。

**Option B（CodeBuild）**を使用した場合は、ビルドスタックも削除します（その ECR
リポジトリは `EmptyOnDelete` なので、イメージはそれと一緒に削除されます）:

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-build --region "$AWS_REGION"
```

---

## 10. トラブルシューティング

| 症状 | 考えられる原因 / 対処 |
| --- | --- |
| サービスが `runningCount=1` に達しない | イメージの取得に失敗（`ContainerImageUri`、実行ロール、ECR egress/VPC エンドポイントを確認）— ECS サービスイベントを参照。 |
| タスクがイメージの取得で停止 / egress なし（プライベートサブネット、NAT なし） | NAT ゲートウェイまたは VPC エンドポイント（ecr.api, ecr.dkr, S3 gateway, logs, secretsmanager, sts）を追加するか、テストではパブリックサブネットの `ServiceSubnetIds` に `AssignPublicIp=ENABLED` を設定。 |
| タスクが `exec format error` で停止 | イメージのアーキテクチャ不一致。`build_and_push.sh` はタスクのデフォルト X86_64 に合わせて `linux/amd64` をビルドします。タスクが ARM64/Graviton で動作する場合にのみ `IMAGE_PLATFORM=linux/arm64` を設定。 |
| ビルド時に `docker: command not found` | ローカルのコンテナランタイムがない。インストールする（Option A: `brew install colima docker && colima start`）か、Option B（CodeBuild）でローカル Docker なしにクラウドでビルド。 |
| ターゲットグループが unhealthy / 502 | アプリが `0.0.0.0:AppPort` でリッスンしていない、またはヘルスチェックパス `/` が失敗している — コンテナログを確認。 |
| ALB からの 504 / タイムアウト | タスク SG が ALB SG からのインバウンドを許可していない、またはタスクが egress のないサブネットにある。 |
| Cognito リダイレクトループ / 401 | `AppDomainName` は証明書および Cognito コールバック `https://AppDomainName/oauth2/idpresponse` と一致する必要がある。ユーザーが未作成/未確認。 |
| アプリがソース DB に到達できない | ソース DB の SG がタスク SG から `SourceDbPort` のインバウンドを許可する必要がある。`SourceDbSecurityGroupId`/`SourceDbCidr` を確認。 |
| DSQL 認証エラー | `DsqlClusterArn` のスコープ、リージョン（`DSQL_MIGRATOR_AWS_REGION`）、タスクロールの `dsql:DbConnect`。 |
| AI オン時の Bedrock エラー | `BedrockModelArns` のスコープ、`BedrockRegion` でのモデル有効化、Bedrock エンドポイントへの egress。 |
| 失敗の診断にさらに詳細が必要 | アプリの **Diagnostics** コントロール（サイドバーのフッター）でログレベルを `DEBUG` に設定して、アクティビティログの失敗イベントに Python スタックトレースを追加。「Send to CloudWatch (stdout)」をトグルして耐久性のあるコピーを取得。再デプロイ不要。 |

---

## 11. セキュリティに関する注記

- **最小権限**: タスクロールは、`dsql:DbConnect` + `dsql:DbConnectAdmin`（クラスターに
  スコープ限定。アプリはデフォルトで DSQL の `admin` ロールとして接続します）、読み取り
  専用の `dsql:GetCluster` + `dsql:ListTagsForResource`（クラスターにスコープ限定。
  概要図にクラスターの `Name` タグを表示するためだけに使用）、および
  `secretsmanager:GetSecretValue`（ソースシークレットにスコープ限定）のみを付与します。
  `bedrock:InvokeModel` は AI アシストを有効化した場合にのみ追加され、許可されたモデル
  ARN にスコープ限定されます。別の実行ロールが ECR の pull + ログを処理し、自動生成の
  セッションクッキーシークレットのみを読み取って（`secretsmanager:GetSecretValue` に
  スコープ限定）、コンテナ起動時に注入します。
- **自動生成のセッションクッキーシークレット**: スタックは、ブラウザセッションクッキー
  （`DSQL_MIGRATOR_STORAGE_SECRET`）に署名する `AWS::SecretsManager::Secret`（運用者の
  入力なし）を作成します。これにより、再接続したブラウザがタスクの再起動をまたいで
  ワークベンチの状態を再開できます。これはクッキーの署名のみを行い — DB/ユーザーの
  認証情報ではありません — テンプレート内に平文で存在することは決してありません。
- **監査証跡**: 構造化されたアクティビティログ（成功 + 失敗のタイムライン、UI から
  ダウンロード可能）は、シークレットではないフィールドのみを記録します — 行の値、
  パスワード、IAM トークンは決して記録しません。タスクの一時ディスク上でサイズ上限が
  設けられローテーションされます。耐久性のあるコピーには CloudWatch ミラー（セクション
  6 を参照）を有効化してください。
- **ネットワーク**: ALB は `AllowedIngressCidr` からのみ 443 を受け付けます。タスクは
  ALB からのトラフィックのみを受け付けます。タスクの egress は、ソース DB
  （`SourceDbPort`）、アウトバウンド 443（AWS エンドポイント）、および 5432
  （Aurora DSQL エンドポイント）にスコープ限定されます。`internal` ALB を推奨します。
- **認証情報は決して保存されません** — テンプレートやイメージに保存されません。アプリは
  ランタイムでソースシークレットを読み取り、短命な IAM トークンで DSQL に認証します。
- **コンテナイメージの CVE (perl)。** アプリイメージの ECR スキャンは、ベースイメージの
  `perl 5.40.1-6` に対していくつかの `perl` CVE（例: CVE-2026-12087、CVE-2026-489xx）を
  フラグします。`perl` は **`python:3.12-slim`（Debian trixie）ベースの推移的
  （transitive）パッケージ**であり、ツールが使用するものではありません — アプリは
  **純粋な Python であり、perl を決して呼び出さない**ため、このコンテナでは脆弱な
  コードパスは**到達不能**です。`Dockerfile` のランタイムステージはビルド時に
  `apt-get upgrade` を実行するので、Debian が修正版を出荷すれば再ビルドが自動的に
  それを取り込みます。現時点ではこれらの CVE は依然として **Debian trixie/sid で open**
  （アップグレードできる修正済みの `perl` が存在しない）であるため、今日どのイメージの
  再ビルドでもスキャンをクリアにすることはできません。それまでの間、コンプライアンス上の
  姿勢としてクリーンなスキャンが必要な場合は、perl を含まないベース（例: distroless
  Python イメージ）で再ビルドしてください — ただしこれは独自の検証を要する、より大きな
  変更である点に注意してください。
- このスタックはこのリポジトリからデプロイされたことが**ありません** — 本番利用の前に
  対象アカウントで検証してください。

---


## 付録 — 自前のイメージをビルドする (制限されたネットワークのみ)

> **ほとんどのデプロイはこのセクションをスキップします。** イメージは ECR Public に
> 公開されており、CloudFormation がデフォルトでそれを取得するため、何もビルドしません。
> ネットワークが ECR Public に到達できない場合にのみ、自前のイメージをビルドしてください
> （その結果を `ContainerImageUri` として渡します）。以下の Option A または B を選んで
> ください。どちらも ECR リポジトリを作成し、`deploy/Dockerfile` を `linux/amd64` 向けに
> ビルドし、ECR にプッシュして、イメージ URI を出力します。

### Option A — ローカルビルド (Docker 互換のランタイムが必要)

```bash
export AWS_REGION=us-east-1
deploy/build_and_push.sh            # タグはデフォルトでプロジェクトバージョン
# または明示的なタグを固定:
deploy/build_and_push.sh 0.1.0
```

<!-- markdownlint-disable-next-line -->
実行中の `docker` デーモンが必要です（Docker Desktop、または `brew install colima
docker && colima start`）。

### Option B — AWS CodeBuild でのクラウドビルド (ローカル Docker 不要)

ビルドインフラを一度デプロイし（ECR リポジトリ + S3 ソースバケット + CodeBuild
プロジェクト）、次にヘルパーを実行してソースを zip し、アップロードし、ビルドを開始します:

```bash
export AWS_REGION=us-east-1

# 一度だけ: ビルドインフラをプロビジョニング。
aws cloudformation deploy \
  --template-file deploy/codebuild.yaml \
  --stack-name mysql-dsql-migrator-build \
  --capabilities CAPABILITY_IAM \
  --region "$AWS_REGION"

# 各ビルド: ソースを zip + アップロードし、CodeBuild を実行し、待機し、イメージ URI を出力。
deploy/build_in_codebuild.sh            # タグはデフォルトでプロジェクトバージョン
# または明示的なタグを固定:
deploy/build_in_codebuild.sh 0.1.0
```

CodeBuild はマネージドな（特権付きの）環境で Docker を実行するため、ご自身のマシンには
AWS CLI だけがあれば済みます。イメージは `linux/amd64` 向けにビルドされ、同じ ECR
リポジトリにプッシュされます。

> デプロイの再現性のため、リリースごとにイミュータブルなタグ（またはイメージダイジェスト）
> を使用してください。
