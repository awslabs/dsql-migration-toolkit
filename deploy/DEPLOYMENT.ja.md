# デプロイガイド — MySQL → Aurora DSQL 移行ツール (app-stack)

_言語: [English](DEPLOYMENT.md) | [한국어](DEPLOYMENT.ko.md) | **日本語**_

本ツールは**お客様自身の AWS アカウント内（シングルテナント）**にデプロイします — どこでも
同じツール・同じ UI で、**どこで実行するか**だけが異なります。オプションのストリーミング
**CDC パイプライン**（MSK + Debezium + シンク）は別の `cdc-stack` であり、本書では扱いません。

---

## 実行場所を選ぶ

1 つを選んでください — 各モードには以下に専用セクションがあります。

- **[ローカルで実行](#ローカルで実行)** — `uv run …`、**インフラ不要。** ご自身のマシンがエンジンに
  なるため、ソース MySQL と DSQL の**両方**に到達できる必要があります。評価・小規模な移行に最適。
  **👉 まずこちらを試してください。**
- **[ECS Fargate にデプロイ](#ecs-fargate-にデプロイ)** — **実運用・大規模な移行に推奨。** ご自身の
  VPC 内の単一タスク **ECS Fargate** サービスが **HTTPS ALB** の背後で動作し、イメージは **ECR**
  から取得するため、データ経路は AWS 内にとどまります（ノート PC を経由しません）。ALB は既定で
  **`internal`** で、**Cognito**（OIDC）ログインは公開時のみ使う opt-in の追加機能です。
- **[単一 EC2 ホストで実行](#単一-ec2-ホストで実行-ソースからlambda-free)** — アプリを
  **ソースから**（`git` + `uv` + **systemd** サービス）実行し、**SSM ポートフォワード**で接続します
  （ALB もパブリック IP もなし）。状態は保持型 EBS ボリューム、CDC は**インプロセスで**シード
  （オフセットシーダー Lambda なし）。アカウントが**コンテナ/ECR** や **AWS Lambda** を使えない場合。

---

<br>

## ローカルで実行

**ECS Fargate のデプロイを決める前に、まずローカルで試してください** — **コマンド 1 つで
UI が起動します。それだけです。**

```console
$ uv run mysql-dsql-migrator ui
NiceGUI ready to go on http://127.0.0.1:8080
```

この URL をブラウザで開けばすぐ使えます — **インフラも、ビルドも、作成する AWS リソースも
ありません。** 最初の確認・評価・小規模な移行に最適で、Fargate に進むか決める前に試すのに
向いています。

<details>
<summary><b>スクリーンショット</b> — ツールの UI（ガイド付き 5 ステップのワークフロー）</summary>

<div align="center">
  <a href="../docs/images/demo-ui.png"><img src="../docs/images/demo-ui.png" alt="ツールの UI — ガイド付き 5 ステップの移行ワークフロー" width="900"></a>
</div>

</details>

UI はご自身のマシンで動作し（ブラウザ → `127.0.0.1:8080`）、**移行そのものもそこで実行
されます**。ご自身のワークステーションがソースを読み取り DSQL に書き込むエンジンとなるため、
すべてのデータがご自身のマシンとそのネットワークを通過します。つまり、**ご自身のデスクトップが
ソース MySQL _と_ ターゲット Aurora DSQL の _両方_ に到達できる必要があります** —
プライベートなソースには SSM ポートフォワード / VPN が必要で、ご自身のマシンには
DSQL リージョンへのアウトバウンド HTTPS + AWS 認証情報が必要です。インフラ不要 —
評価 / 小規模な移行 / 開発に最適です。これはホスティングされたアーキテクチャ
では *ありません*。実際の移行には **[ECS Fargate](#ecs-fargate-にデプロイ)** を使用してください。

> [!TIP]
> **再起動をまたいでセッション（と編集内容）を維持する。** ブラウザセッション ID（保存された
> ワークベンチが格納されるキー）が固定されるよう、`DSQL_MIGRATOR_STORAGE_SECRET` を固定の
> ランダム文字列に設定して起動してください:
>
> ```bash
> DSQL_MIGRATOR_STORAGE_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))") \
>   uv run mysql-dsql-migrator ui
> ```
>
> - **設定しないと** — 再起動のたびに新しいセッション ID が発行され、ワークフローの進捗**および
>   Schema Conversion の編集内容**（カスタマイズしたターゲット DDL、例: `TINYINT(1)`→`smallint`）が
>   復元されず、Full Load を再実行するとデフォルトの変換でテーブルが再作成されます。
> - **設定すると** — セッションは中断したところから再開され、再実行では適用済みのスキーマが
>   再利用されます。
>
> この値はシークレットとして扱ってください（[`.env.example`](../.env.example) を参照）。

---

<br>

## ECS Fargate にデプロイ

> [!TIP]
> **実運用・大規模な移行に推奨。** データ経路全体がノート PC ではなく AWS 内にとどまります
> （ソース → Fargate → DSQL）。

イメージのビルド不要で 2 つの方法でデプロイできます — イメージは **ECR Public** にあり、
CloudFormation が取得します。同じ `deploy/cloudformation.yaml` を次のいずれかで:

- **AWS Console — 推奨。** テンプレートをアップロードすると、ガイド付きフォームが値を集めます。
- **AWS CLI。** `aws cloudformation deploy` 1 回でパラメータを渡します。

いずれも [app-stack のデプロイ](#2-app-stack-のデプロイ) に詳細があります — 必要な値はまず
[前提条件](#1-前提条件) で集めてください。

**UI への到達。** ALB はデフォルトで internal なので、`https://<LoadBalancerDns>/` には
VPC 内から — VPN、Direct Connect、または SSM ポートフォワードで — アクセスします。設計上、
公開エンドポイントはありません — Well-Architected SEC05-BP02。公開するには、
[app-stack のデプロイ](#2-app-stack-のデプロイ) のオーバーライドの注記を参照してください。

<details>
<summary><b>アーキテクチャ図</b> — 全体トポロジ（app-stack + オプションの CDC on MSK Connect）</summary>

<div align="center">
  <a href="../docs/images/architecture-aws.png"><img src="../docs/images/architecture-aws.png" alt="AWS アーキテクチャ全体 — オペレーターが HTTPS ALB（オプションで Cognito）経由で ECS Fargate コントロールプレーンアプリに接続し、アプリが Aurora DSQL への Full Load を行い、オプションの CDC パイプラインでは cdc-stack をデプロイして Debezium ソース + カスタム DSQL シンクコネクタが MSK Connect で（S3 からプラグインをロード）実行され、Amazon MSK を経由して Aurora DSQL へストリーミングし、gapless ハンドオフのための VPC 内 offset-seeder Lambda を備える" width="900"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. 前提条件

何を集めるか。その後すぐ [app-stack のデプロイ](#2-app-stack-のデプロイ) へ進んでください。
詳細な説明は下記の折りたたみと [パラメータリファレンス](#パラメータリファレンス) にあります。

| 何 | パラメータ | 補足 |
| --- | --- | --- |
| アクセス | — | AWS Console（推奨）または AWS CLI v2 — IAM ロール、ECS、ALB、セキュリティグループ、CloudWatch Logs を作成できること。イメージのビルドは不要 — ECR Public から取得します。 |
| VPC | `VpcId` | ソース MySQL の VPC が理想的、**DSQL と同一リージョン**。以下のサブネットはここから選びます。 |
| ALB サブネット 2 つ + タスクサブネット 2 つ | `AlbSubnetIds` / `ServiceSubnetIds` | 異なる AZ。タスクサブネットは**443 の egress**が必要。 |
| ACM 証明書 | `CertificateArn` | 同一リージョン。ドメインがない場合は `AWS_REGION=<region> deploy/create_test_cert.sh` で自己署名のテスト証明書を作成してください。 |
| DSQL クラスター ARN | `DsqlClusterArn` | 移行のターゲット。 |
| ソース DB への到達性 | `SourceDbSecurityGroupId`（推奨）または `SourceDbCidr` | いずれか一方。 |

ソース DB の認証情報はデプロイ**後**に UI で入力します（再利用する場合を除き AWS シークレットは
不要）。それ以外のパラメータはすべて妥当なデフォルトのままです。

<details>
<summary><b>パラメータの詳細</b> — VPC / サブネット / 証明書のガイダンスと、すべてのオプション値</summary>

#### アクセス

- **AWS Console** へのアクセス（推奨経路）、**または**対象アカウントに認証済みの
  AWS CLI v2（`aws sts get-caller-identity`）。
- スタックのリソースを作成する権限: IAM ロール、ECS、ELB (ALB)、EC2 セキュリティ
  グループ、CloudWatch Logs、Cognito（オプション — 公開 ALB の場合のみ）。
- イメージのビルドは不要です — イメージは ECR Public から取得されます。（自前ビルドは
  制限されたネットワーク向けのみ。付録を参照。）

#### フォームを入力する前に知っておくこと

> [!IMPORTANT]
> **VPC から始めてください。** ソースの RDS/Aurora MySQL がすでに存在する VPC を使用してください
> — DSQL と同一リージョン — ここで選ぶサブネット/証明書は AWS 自体が要求するものです（ALB と
> Fargate タスクは必ずサブネットに配置する必要があり、HTTPS リスナーには証明書が必要です）。
> **この VPC はこのアカウントが所有している必要があります** — RAM 共有（クロスアカウント）の
> VPC はサポートされません。CDC デプロイロールの EC2 権限はデプロイ先アカウントにスコープ
> 限定されているため、コネクタの ENI 作成が `AccessDenied` で失敗します。

#### オプションの値 (それ以外の場合は妥当なデフォルト)

| オプション | パラメータ | 必要になる場合 |
| --- | --- | --- |
| **ソースシークレット ARN** | `SourceSecretArn` | 既存の Secrets Manager シークレットをソース認証情報として**再利用する場合のみ**。空のままにすると UI でユーザー名/パスワードを使用します（一般的なケース）。 |
| **ソース DB への到達性** | `SourceDbSecurityGroupId`（推奨） / `SourceDbCidr` | タスクが `SourceDbPort` でソース MySQL への egress を得られるよう、**少なくとも一方を指定**してください。`SourceDbSecurityGroupId` は egress をソース DB の SG に絞り込みます。SG id がない場合は `SourceDbCidr` を使用します。両方とも空だとデプロイは拒否されます（タスクにソースへの経路がなくなるため）。 |
| **カスタムドメイン** | `AppDomainName` | ご自身の Route 53 ドメインで ALB をフロントする場合のみ。 |
| **公開アクセス / Cognito** | `AlbScheme`, `AllowedIngressCidr`, `EnableCognitoAuth`, `CognitoDomainPrefix` | UI を公開する場合のみ。デフォルトは `internal`（ログインなし）のままです。 |
| **AI アシスト** | `EnableAiAssist`, `BedrockModelId`, `BedrockRegion` | Amazon Bedrock 支援の変換を有効化する場合のみ（モデルを選択。IAM スコープは自動的に導出）。 |
| **カスタムイメージ / サイジング** | `ContainerImageUri`, `ContainerCpu`, `ContainerMemory` | プライベート ECR イメージ、またはデフォルト以外のタスクサイズの場合のみ。 |

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. app-stack のデプロイ

`deploy/cloudformation.yaml` をデプロイする 2 つの方法があります — いずれか 1 つを
選んでください。どちらも同じスタックを作成します。パラメータのリファレンスは
**パラメータリファレンス** セクションにあります。

#### 推奨 — AWS Console (ガイド付きフォーム)

![CloudFormation — Create stack → Upload a template file](../docs/images/cfn-create-stack.png)

まず、**正しいリージョン**（コンソール右上 — Aurora DSQL クラスターと同一リージョン）
にいることを確認し、次に:

**1. Create stack ウィザードを開く。** CloudFormation コンソールへ移動:
<https://console.aws.amazon.com/cloudformation/home> → **Create stack** →
**With new resources (standard)**。（直接リンク、リージョンを置き換えてください:
`https://<region>.console.aws.amazon.com/cloudformation/home?region=<region>#/stacks/create`。）

**2. Prerequisite — Prepare template。** **Template is ready** を選択し、次に
**Specify template** の下で **Upload a template file** → **Choose file** →
このリポジトリの `deploy/cloudformation.yaml` を選択 → **Next**。

**3. Specify stack details。** **Stack name** を `mysql-dsql-migrator` に設定し、
続いてパラメータを入力します。フォームはネイティブのピッカーを備えているため、
id を入力する代わりに**ご自身のアカウントから選択**します。

**以下の必須フィールドを入力します**（それ以外はすべて動作するデフォルトがあります）:

| フィールド | 入力する値 |
| --- | --- |
| `VpcId` | ドロップダウン — ソース MySQL が存在する VPC。 |
| `AlbSubnetIds` | サブネットのマルチセレクト — **異なる AZ の 2 つのサブネット**（下のサブネットの注記を参照）。 |
| `ServiceSubnetIds` | サブネットのマルチセレクト — **異なる AZ の 2 つのプライベートサブネット**（プライベート/NAT サブネットがない場合は ALB サブネットを流用し `AssignPublicIp=ENABLED` を設定）。 |
| `CertificateArn` | HTTPS 用の ACM 証明書 ARN — **ドメインがない場合は、すぐ下のコマンドを参照。** |
| `DsqlClusterArn` | ターゲットの Aurora DSQL クラスター ARN。 |
| `SourceDbSecurityGroupId`（または `SourceDbCidr`） | いずれか一方 — タスクのソース MySQL への egress の範囲を指定します。セキュリティグループ id を優先し、ない場合のみ CIDR を使用してください。 |

> [!WARNING]
> サブネットのドロップダウンは、ご自身の VpcId のものだけでなく、**リージョン内の
> すべてのサブネット**を一覧表示します。別の VPC のものを選ぶとデプロイが失敗します —
> 下の **「どのサブネットを選ぶか」** の注記を使って正しいものを選んでください。

**オプション:** 既存のソースシークレットを再利用する場合を除き `SourceSecretArn` は空のまま
にします — デプロイ後に UI でソースのホスト/ユーザー名/パスワードを入力します。

> [!TIP]
> **ACM 証明書がまだありませんか。** 自己署名の**テスト**証明書を 1 行で生成し、
> 出力される ARN を `CertificateArn` に貼り付けます（ブラウザは警告します。テスト専用 —
> 本番では、ご自身のドメイン用の実際の ACM 証明書を取得してください）:

```bash
AWS_REGION=<region> deploy/create_test_cert.sh
#  → 出力:  CertificateArn=arn:aws:acm:<region>:<account>:certificate/xxxx
```

**デスクトップのブラウザから UI に到達しますか。** デフォルトは `internal` ALB
（VPC/VPN 内部からのみ到達可能）です。ご自身のマシンから開く方法は 2 つ —
A か B のいずれかを選んでください:

**A. 推奨 — Cognito でサインインする**（どこからでも、複数人でアクセス可能）:

| フィールド | 入力する値 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **パブリック**サブネット（プライベートではない） |
| `EnableCognitoAuth` | `true` — さらに `CognitoDomainPrefix` と `CognitoAdminEmail`（下の注記を参照） |
| `AllowedIngressCidr` | `0.0.0.0/0` で問題ありません — Cognito のログインがアクセスゲートになります。ユーザーのネットワーク CIDR が分かる場合はさらに絞り込んでも構いません |

**B. 代替 — ご自身のマシンのみ、ログインなし:**

| フィールド | 入力する値 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **パブリック**サブネット（プライベートではない） |
| `AllowedIngressCidr` | デスクトップのパブリック IP を `/32` で — `curl https://checkip.amazonaws.com` で取得（例: `203.0.113.5/32`） |

残りのパラメータはデフォルトのままにします（例: コンテナイメージ）。特に
**`HttpsEgressCidr` は `0.0.0.0/0` のままにしてください** — これは
タスクが NAT/IGW 経由で AWS API（DSQL、Secrets Manager、ECR、CloudWatch）に到達する
ためのアウトバウンド CIDR です。これらすべてを VPC エンドポイント（PrivateLink）で
フロントする場合にのみ絞り込んでください。そうでないのに絞り込むと、タスクは
イメージを取得できず DSQL に到達できず、起動に失敗します。→ **Next**。

> [!TIP]
> **どのサブネットを選ぶか。** ドロップダウンは（すべての VPC にわたる）**リージョン内の
> すべてのサブネット**を一覧表示します。**まず CIDR 範囲でご自身の VpcId のサブネットに
> 絞り込んでください**（例: `172.31.0.0/16` の VPC → `172.31.x` のサブネットを選び、他の
> VPC に属する別の CIDR は無視）。次に **AZ 列**を使って「異なる AZ」を満たし、**Name
> タグ**でパブリックとプライベートを見分けます。どれがどれか分からない場合は、**VPC
> コンソール → Subnets** を開き、ご自身の VPC でフィルターして、各サブネットのルート
> テーブルを確認します（`0.0.0.0/0 → nat-…` のルート = egress を持つプライベート。
> `→ igw-…` = パブリック）— 明確な Name タグの規約（`…-private-a` / `…-public-a`）が
> あれば、以降はドロップダウンが一目で分かります。

> [!IMPORTANT]
> 上記で **A**（Cognito）を選んだ場合は、step 3 で `EnableCognitoAuth=true`、
> `CognitoDomainPrefix`、**および `CognitoAdminEmail`** を併せて設定してください（その後
> step 4〜5 はそのまま進めます）。続いて
> [DNS を ALB に向ける](#dns-を-alb-に向ける--オプション-カスタムドメインのみ)
> （カスタムドメインの場合のみ）と
> [運用者ユーザーの作成](#運用者ユーザーの作成-cognito--cognito-を有効にした場合のみ)
> （サインインと追加ユーザーの作成）を参照してください。`CognitoAdminEmail` は任意では
> ありません — ユーザープールにセルフサインアップがないため、これなしで Cognito を有効に
> するとログイン手段のないアプリになり、テンプレートが拒否します。`AppDomainName` は
> 任意で、空のままにすると ALB 自身の DNS 名を使用します。

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
   **Connect** から始まるガイド付きワークフロー（Connect → Evaluation → Schema
   Conversion → Data Migration → Validation → Cut over）です。読み込まれれば
   デプロイは完了です。**Connect** でソース DB の認証情報を入力して開始します。

> [!NOTE]
> **▶ 次: 最初の移行を実行する。** デプロイはここで終わりです — UI が起動しています。
> 各ステップが何をするか、実際の移行をどう進めるかは、
> [**ユーザーマニュアル**](../docs/manual/ja/README.md) に従ってください（[セットアップ](../docs/manual/ja/01-setup.md)
> → Connect から開始）。

<details>
<summary><b>代替 — AWS CLI でデプロイ</b></summary>

#### AWS CLI

環境をシェル変数として一度設定します。コマンド自体はどのお客様でも同一です。
最小構成（Dev/Test）のデプロイ:

```bash
# --- あなたの環境 (ここを編集) -----------------------------------------------
export AWS_REGION=us-east-1
# VpcId: 推奨 -- ソース DB の VPC
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# AlbSubnetIds: サブネット 2 つ、異なる AZ
export ALB_SUBNET_IDS=subnet-0f1e2d3c4b5a69788,subnet-0a9b8c7d6e5f43210
# ServiceSubnetIds: プライベートサブネット 2 つ
export SERVICE_SUBNET_IDS=subnet-0123456789abcdef0,subnet-0fedcba987654321f
# CertificateArn: 下に実際の ACM 証明書 ARN を貼り付けるか、自己署名テスト証明書
# (ドメイン不要) をスクリプト出力の 1 行キャプチャで自動入力する:
#   export CERTIFICATE_ARN=$(deploy/create_test_cert.sh | sed -n 's/^CertificateArn=//p')
export CERTIFICATE_ARN=arn:aws:acm:us-east-1:123456789012:certificate/a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4a5b6c
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------
```

> [!WARNING]
> **スタック名は小文字で、28 文字以内にしてください。** スタックは ALB を
> `<スタック名>-alb` として作成し、ALB サービスはこの名前を 32 文字に制限します。
> これを超えると約 2 分のロールバックの後、`The load balancer name '<スタック名>-alb'
> cannot be longer than '32' characters` でデプロイが失敗します。小文字であることも
> 重要です。ALB の DNS 名は ALB 名の大文字小文字をそのまま継承し、Cognito ログインは
> この DNS 名が小文字のときのみ動作します。ALB が OAuth `redirect_uri` のホストを
> 小文字に変換して送信する一方、Cognito は 2 つの文字列を厳密に比較するためです。

このテンプレートは CloudFormation のインライン アップロード上限(51,200 バイト)を超えて
いるため、CLI がステージング用の S3 バケットを必要とします(Console はこれを裏側で
自動的に処理するため、推奨パスになっています)。一度だけ作成するか、この
アカウント/リージョンに既存のバケットがあれば再利用してください:

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
    # BedrockModelId はデフォルト値を表示; 他のモデル選択肢は §8 参照
    # SourceSecretArn=...   # オプション — 既存のソースシークレットを再利用する場合のみ
```

> [!TIP]
> **AI アシスト（推奨）。** `EnableAiAssist=true` + `BedrockRegion` で Schema
> Conversion と Query Converter の AI DBA を有効にします — オプトインかつ助言専用の
> 機能で、選択したモデルに対する `bedrock:InvokeModel` のみにスコープされます。
> `BedrockModelId`（デフォルト `global.anthropic.claude-sonnet-5`）について、その
> リージョンの Bedrock コンソールで**モデルアクセスを有効化**する必要があり、タスクが
> Bedrock エンドポイントへ egress できる必要があります。両方省略すれば AI なしで
> デプロイされます（決定論的パスは変わりません）。詳細とモデル選択は §8 を参照。

外部アクセス + Cognito ログインを使う場合は、`--parameter-overrides` に
次を追加してください:

```bash
    AlbScheme=internet-facing \
    AllowedIngressCidr=0.0.0.0/0 \
    EnableCognitoAuth=true \
    CognitoDomainPrefix=<一意なprefix> \
    CognitoAdminEmail=<あなたのメール>
```

Cognito の 3 つのフィールドは常にセットで必要です — テンプレートが強制します。

> [!TIP]
> **`--parameter-overrides` に追加できるその他のオーバーライド**
>
> - **公開 UI（デスクトップから）:** `AlbScheme=internet-facing` **かつ**
>   `AllowedIngressCidr=<あなたのパブリック IP>/32`（取得方法: `curl https://checkip.amazonaws.com`）。
>   デフォルトの `10.0.0.0/8` は内部専用で公開ブラウザをブロックします。`0.0.0.0/0` は
>   決して使用しないでください（完全開放は追加で `EnableCognitoAuth=true` を必要とします）。
> - **カスタムドメイン:** `AppDomainName=<あなたのドメイン>` を追加 —
>   [DNS を ALB に向ける](#dns-を-alb-に向ける--オプション-カスタムドメインのみ) を参照。
> - **カスタムイメージ:** `ContainerImageUri` をオーバーライド — 制限されたネットワークなら
>   ご自身のプライベート ECR コピー（[pull-through キャッシュ](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html)
>   または `deploy/Dockerfile` からビルド。付録を参照）で、それ以外なら管理している
>   任意のイメージで。
> - **適用前に変更内容を確認する（本番）:** 上のコマンドに `--no-execute-changeset` を
>   追加すると、デプロイの代わりに change-set の ARN が出力されます。`aws cloudformation
>   describe-change-set --change-set-name <その ARN> --region "$AWS_REGION" --query
>   'Changes[].ResourceChange.[Action,LogicalResourceId,ResourceType,Replacement]'
>   --output table` で確認し、問題なければ `aws cloudformation execute-change-set
>   --change-set-name <その ARN> --region "$AWS_REGION"` で適用してください。

完了後、出力を読み取ります:

```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```

主な出力: `LoadBalancerDns`、`AppUrl`、`ClusterName`、`ServiceName`、
`TaskRoleArn`、`CognitoHostedUiDomain`。

ブラウザで **`AppUrl`** を開きます（VPC 内から）。**MySQL → Aurora DSQL
Migration Tool** の UI が読み込まれます — **Connect** から始まるガイド付き
ワークフロー（Connect → Evaluation → Schema Conversion →
Data Migration → Validation → Cut over）です。UI が表示されればデプロイは
成功です。**Connect** でソース DB の認証情報を入力して開始します。

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 参考資料と運用

任意の詳細情報 — 必要なものだけ展開してください。初回デプロイに必須ではありません。

<details>
<summary><b>パラメータリファレンスとタスクのサイジング</b> — すべてのパラメータ、CPU/メモリのサイジング方法</summary>

### パラメータリファレンス

| パラメータ | 必須 | デフォルト | 説明 |
| --- | --- | --- | --- |
| `VpcId` | yes | — | ソース DB にプライベートに到達できる VPC。 |
| `AlbSubnetIds` | yes | — | ALB 用のサブネット ≥2 つ（異なる AZ）。 |
| `ServiceSubnetIds` | yes | — | Fargate タスク用のプライベートサブネット。 |
| `AlbScheme` | no | `internal` | `internal` または `internet-facing`。**推奨: `internal`**（VPN/Direct Connect/ピアリング経由で到達）。`internet-facing` は Cognito オンの場合のみ使用。 |
| `CertificateArn` | yes | — | HTTPS (443) リスナー用の ACM 証明書 ARN。 |
| `ContainerImageUri` | no | 公開された ECR Public イメージ | デフォルトは ECR Public に公開されたイメージ — ビルド不要。制限されたネットワーク（ご自身のプライベート ECR コピー / pull-through キャッシュ）またはカスタムビルドの場合のみオーバーライド。イミュータブルなタグまたはダイジェストを推奨。 |
| `ContainerCpu` | no | `512` | Fargate タスクの CPU ユニット。**Full Load は CPU バウンド**（ソースリーダーが行ごとに Python で型変換を行う）なので、大規模移行では引き上げること — 同一データの payments+orders ロード実測で **デフォルトの 512 より 4096（4 vCPU）で約 3.8 倍高速**だった。評価用途はデフォルト `512` で十分、実際の TB 級 Full Load には **4096 以上** を推奨。[マニュアル §7.2](../docs/manual/ja/07-performance-and-tuning.md#72-並列度のチューニング) 参照。 |
| `ContainerMemory` | no | `1024` | Fargate タスクのメモリ（MiB）— **ハード制限**: 超えるとカーネルがタスクを OOM-kill します（graceful shutdown なし、CloudWatch のスパイクと ELB タイムアウトのみ）。メモリはテーブルサイズではなくバッファされたパイプライン — おおよそ `table_parallelism × (prefetch + batch_parallelism) × バッチあたりバイト` を**ワーカープロセスにわたって合算** — で制限され、**広い / 大きな LOB 行がバッチあたりバイトを増やす**ため、同時にソース書き込みがある実運用ロードでは `1024` デフォルトが厳しいことがあります。**サイジング:** 評価/小規模は `1024` で十分、実運用の Full Load には **≥ 2048**、大きな `TEXT`/`BLOB` や並列度を上げる場合は **≥ 4096**（この場合 `ContainerCpu` ≥ 2048 が必要）。CPU に対して有効な値であること（下のサイジング表を参照）。アプリはメモリの high-water と約 80% の圧迫警告をログ（+アクティビティログ）に残すので、kill の前に OOM の接近が見えます。 |
| `AppPort` | no | `8080` | コンテナのリッスンポート。 |
| `AssignPublicIp` | no | `DISABLED` | NAT なしでタスクをパブリックサブネットで実行するには `ENABLED`（テスト）。**推奨: 本番では `DISABLED` のまま**（NAT ゲートウェイまたは VPC エンドポイント）。 |
| `AllowedIngressCidr` | no | `10.0.0.0/8` | ALB の 443 に到達を許可する CIDR。**推奨: `0.0.0.0/0` ではなく、ご自身のネットワークに絞り込む**。 |
| `DsqlClusterArn` | yes | — | ターゲットの DSQL クラスター ARN（`dsql:DbConnect` のスコープを指定）。 |
| `SourceSecretArn` | no | `""` | **オプション。** 既存の Secrets Manager シークレットをソース認証情報として**再利用する**場合のみ設定します（`GetSecretValue` のスコープを指定）。空のままにすると UI でユーザー名/パスワードを入力します（一般的なケース）。 |
| `SourceDbSecurityGroupId` | no* | `""` | ソース DB の SG。生の CIDR よりも**推奨される（望ましい）egress ターゲット**。*これ / `SourceDbCidr` のいずれか一方が必須。 |
| `SourceDbCidr` | no* | `""` | ソース DB の CIDR（SG id がない場合に使用）。*これ / `SourceDbSecurityGroupId` のいずれか一方が必須。 |
| `SourceDbPort` | no | `3306` | ソース MySQL のポート。 |
| `HttpsEgressCidr` | no | `0.0.0.0/0` | タスクのアウトバウンド 443（AWS API: DSQL トークン、Secrets Manager、ECR、CloudWatch、Bedrock）および 5432（DSQL）の宛先 CIDR。**推奨: デフォルトの `0.0.0.0/0` のまま**にする — タスクは NAT/IGW 経由でパブリックな AWS エンドポイントに到達します。絞り込み（例: ご自身の VPC CIDR へ）は、それらのサービス*すべて*をインターフェース VPC エンドポイント（PrivateLink）でフロントする場合にのみ行ってください。エンドポイントなしで絞り込むとイメージの取得 / DSQL がブロックされ、タスクは起動に失敗します。 |
| `EnableCognitoAuth` | no | `false` | ALB が Cognito (OIDC) で認証します。デフォルトは `false`: internal ALB（またはご自身の CIDR に絞り込んだ ALB）がアクセスゲートであり、運用者はすでに IAM/DB の権限を保持しているため、ログインは不要です。**`AllowedIngressCidr=0.0.0.0/0` の場合にのみ必須（強制されます）。** `true` の場合は `CognitoDomainPrefix` と `CognitoAdminEmail` の **両方**が必要です。 |
| `AppDomainName` | no | `""` | ALB をフロントする DNS 名（証明書と一致する必要があります）。**空のままにすると** ALB 自身の DNS 名を Cognito のコールバックホストとして使用します — カスタムドメインや Route 53 レコードは不要です。 |
| `CognitoDomainPrefix` | Cognito 時 | `""` | グローバルに一意な Cognito hosted-UI プレフィックス（`https://<prefix>.auth.<region>.amazoncognito.com`）。 |
| `CognitoAdminEmail` | Cognito 時 | `""` | スタックが作成する**最初のログインユーザー**のメールアドレス。Cognito がこのアドレスに一時パスワードを送信し、hosted UI が初回サインイン時に新しいパスワードを求めます。**Cognito を有効にする場合は必須** — ユーザープールはセルフサインアップを無効にしているため、この値がないとデプロイは成功しても誰もログインできないアプリになります（テンプレートがその組み合わせを拒否します）。ユーザーの追加は [§運用者ユーザーの作成](#運用者ユーザーの作成-cognito--cognito-を有効にした場合のみ) を参照。 |
| `EnableAiAssist` | no | `false` | opt-in。スコープが絞られた `bedrock:InvokeModel` を付与。 |
| `BedrockModelArns` | no | `""` | invoke スコープの**オプションのオーバーライド**。空欄 = `BedrockModelId` から自動導出。 |
| `BedrockRegion` | no | `""` | アプリの `BEDROCK_REGION`。 |
| `BedrockModelId` | no | `global.anthropic.claude-sonnet-5` | Anthropic モデル（ドロップダウン）。IAM スコープはこれから自動導出。 |

### タスクのサイジング — `ContainerCpu` / `ContainerMemory`

Fargate では CPU とメモリを**独立に選べません**: CPU 値ごとに許容メモリ範囲が固定で、メモリは
**ハード制限**（超過で OOM kill、アプリの終了ログなし）です。有効な組み合わせから選びます:

| `ContainerCpu` (vCPU) | 許容 `ContainerMemory` | 刻み |
| --- | --- | --- |
| `256` (0.25) | 512, 1024, 2048 MiB | 固定 |
| `512` (0.5) | 1–4 GB | 1 GB |
| `1024` (1) | 2–8 GB | 1 GB |
| `2048` (2) | 4–16 GB | 1 GB |
| `4096` (4) | 8–30 GB | 1 GB |
| `8192` (8) | 16–60 GB | 4 GB |
| `16384` (16) | 32–120 GB | 8 GB |

**ワークロード別の推奨:**

- **評価 / 小規模テーブル:** `512` / `1024` MiB のデフォルトで十分。
- **実運用の Full Load:** **`1024` CPU / `2048` MiB** 以上。Full Load は CPU バウンド（行ごとの型変換）
  で、メモリはワーカープロセスにわたって `table_parallelism × batch_parallelism` で増加します —
  同時にソース書き込みがあるロードで `512`/`1024` デフォルトが OOM-kill された事例があります。
- **大きな `TEXT`/`BLOB` テーブル、または並列度を上げる場合:** **`4096` CPU / `8192`+ MiB** — 広い行が
  バッチを大きくします。（メモリ 4 GB 超は CPU も上げる必要: 4 GB は CPU ≥ `1024`、8 GB は CPU ≥ `2048`。）

> [!TIP]
> メモリの引き上げは**再デプロイ**（スタックがタスクをインプレース更新）です — Fargate はタスクの
> メモリを自動スケールせず、単一タスクのコントロールプレーンなので水平スケールもしません。迷ったら
> 大きめに: 過剰プロビジョニングはコストがわずかに増えるだけですが、不足すると移行の途中で OOM-kill
> されます。アプリはメモリの high-water と約 80% の圧迫警告をログ（+アクティビティログ）に残すので、
> 根拠を見て適切なサイズを決められます。

</details>

<details>
<summary><b>カスタムドメインと Cognito ログイン</b> — オプション。デフォルトの internal ALB ならスキップ</summary>

### DNS を ALB に向ける — オプション (カスタムドメインのみ)

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

### 運用者ユーザーの作成 (Cognito) — Cognito を有効にした場合のみ

Cognito を有効化した場合のみです（`EnableCognitoAuth=true`）。デフォルトの `internal` ALB で
Cognito を使わない場合はスキップします。

**最初のユーザーはすでに存在します** — スタックが `CognitoAdminEmail` から作成し、Cognito が
そのアドレスに一時パスワードを送信しています。どのアドレスに送られたかはスタック出力
**`CognitoFirstUser`** で確認できます。ログインは `AppUrl` を開き、そのメールアドレスと一時
パスワードでサインインし、求められたら新しいパスワードを設定します。

**追加**のユーザーを作成するには、スタック出力 `CognitoUserPoolId` を使います:

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

各ユーザーは一時パスワードを受け取り、（ALB がトリガーする）Cognito hosted UI 経由の
初回サインイン時に新しいパスワードを設定するよう求められます。ユーザープールは**セルフ
サインアップが無効**なため、すべてのユーザーをこの方法で作成する必要があります。

</details>

<details>
<summary><b>検証、更新、AI アシスト</b> — デプロイ後の確認、新イメージのロールアウト、Bedrock の有効化</summary>

### 検証

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
リダイレクトされ、その後、移行ワークフロー（Connect → Evaluation →
Schema Conversion → Data Migration → Validation → Cut over）に移動するはずです。

#### 可観測性 & ランタイム診断

デプロイは意図的にパラメータを最小限にしています: **ログレベルとアクティビティログの
CloudWatch ミラーリングは CloudFormation パラメータではありません** — これらはアプリの
**Settings → Diagnostics**（サイドバーのフッターの歯車）からランタイムで調整します。
再デプロイは不要です:

- **ログレベル** — トラブルシューティング中に `INFO`/`DEBUG` を切り替えます（DEBUG は
  失敗イベントに Python のスタックトレースを追加します。行の値や認証情報は決して
  含みません）。
- **Send to CloudWatch (stdout)** — オンにするとアクティビティログを stdout に
  ストリーミングし、コンテナの `awslogs` ドライバーがそれをこのスタックの CloudWatch
  ロググループに転送します（タスクの置き換えを乗り越えて残る、耐久性のある監査コピー）。
- **Download activity log** — 同じダイアログの **Activity log** タブから、完全な UTC のイベントごとに 1 行の
  タイムライン（接続 / 評価 / スキーマ適用 / Full Load / CDC）を取得します。ファイルは
  `/tmp` 上でサイズ上限が設けられローテーションされます。

変更はアプリ全体（単一タスク）に適用され、再起動時に起動時のデフォルトにリセット
されます。上級の運用者は `DSQL_MIGRATOR_LOG_LEVEL` / `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT`
環境変数で起動時のデフォルトを設定できますが、Settings ダイアログが意図された
経路です。

### 新しいイメージバージョンへの更新

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
  --s3-bucket "$TEMPLATE_BUCKET" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ContainerImageUri=$IMAGE_URI
  # (他のパラメータを再度指定するか、以前の値に依存する)
  # $TEMPLATE_BUCKET: 上の AWS CLI デプロイのセクションで作成したステージング用バケット
  # (このテンプレートは CloudFormation のインラインアップロード上限 51,200 バイトを超えています)
```

> [!WARNING]
> コントロールプレーンは**単一タスク**として実行されるため、置き換え中は短い中断が
> 予想されます。移行済みのデータ、DSQL クラスター、デプロイ済みの cdc-stack は影響を
> 受けず、再接続時に復旧します。進行中のセッション状態（ワークフローの進捗、進行中の
> Full Load）はタスクの一時ディスク上に存在し、**残りません** — **更新前に進行中の
> ジョブを完了または静止させて**から、再接続して読み取り専用の Evaluation を再実行
> してください（数分）。

### AI 支援変換の有効化 (オプション)

AI アシストは opt-in であり、**スコープが絞られた** `bedrock:InvokeModel` を付与します:

```bash
aws cloudformation deploy ... \
  --parameter-overrides \
    EnableAiAssist=true \
    BedrockModelId=global.anthropic.claude-sonnet-5 \
    BedrockRegion=$AWS_REGION
```

**AI アシストは Amazon Bedrock 上でのみ動作します。** Bedrock が唯一の AI バックエンド
です — このツールには直接の Anthropic/OpenAI（またはその他）の API キーを入力する
欄がないため、選択できるモデルは、ご自身の AWS 認証情報で呼び出す Bedrock 基盤モデル
だけです。モデルは `BedrockModelId` で設定します（デフォルト
`global.anthropic.claude-sonnet-5`）。

**推奨モデル — 最新の Anthropic Claude Opus または Sonnet:**

| モデル | Bedrock モデル id (`BedrockModelId`) | 使用する場面 |
|---|---|---|
| Claude Sonnet 5 (デフォルト) | `global.anthropic.claude-sonnet-5` | ほとんどのスキーマで品質・速度・コストの最良のバランス。 |
| Claude Opus 5 | `global.anthropic.claude-opus-5` | 最も難しい `MANUAL` / `UNSUPPORTED` の変換。最高品質。 |
| Claude Opus 4.8 | `global.anthropic.claude-opus-4-8` | 高品質。Opus 5 より一段下。 |
| Claude Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` | 前世代の Sonnet。 |

`BedrockModelId` はこれらの `global.` クロスリージョン推論プロファイルの**ドロップダウン**
であり（すべての商用リージョンから解決できるため、1 つのリストでどのデプロイにも対応します）、
タスクロールの `bedrock:InvokeModel` スコープはそこから**自動的に導出**されます
— したがって `BedrockModelArns` は**設定する必要はありません**（別のモデル/ARN で
オーバーライドする場合にのみ使用）。ただし、選択したモデルについて `BedrockRegion` の
Bedrock コンソールで**モデルアクセスを有効化する必要は依然としてあります**。

タスクの egress が Bedrock ランタイムエンドポイントに到達できることを確認してください
（NAT または Bedrock VPC エンドポイント）。UI で AI を有効化し、**Verify AI access** の
事前チェックで到達性を確認してください。

</details>

<details>
<summary><b>Teardown、トラブルシューティング、セキュリティ</b> — すべて削除、よくある問題、セキュリティに関する注記</summary>

### Teardown

> [!WARNING]
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

### トラブルシューティング

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
| 失敗の診断にさらに詳細が必要 | **Settings → Diagnostics**（サイドバーのフッターの歯車）でログレベルを `DEBUG` に設定して、アクティビティログの失敗イベントに Python スタックトレースを追加。「Send to CloudWatch (stdout)」をトグルして耐久性のあるコピーを取得。再デプロイ不要。 |

### セキュリティに関する注記

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
  入力なし）を作成します。これにより再起動をまたいでブラウザセッション id が安定して保たれ
  — 次項の durable なスナップショットを見つける鍵になります。これはクッキーの署名のみを行い —
  DB/ユーザーの認証情報ではありません — テンプレート内に平文で存在することは決してありません。
- **Durable なセッション再開**: 各セッションの非機密のワークベンチスナップショット（ワークフロー
  進捗・Evaluation 結果・スキーマ選択・CDC 開始点）を、ツールが管理するプラグインバケット
  （`mysql-dsql-migrator-plugins-<account>-<region>`、自動プロビジョニング — 運用者の入力なし）の
  `sessions/` プレフィックスに書き込むため、プロセス内再起動だけでなく Fargate の**タスク置換**
  （再デプロイ）を越えて保持されます。上記の安定したクッキーシークレットと合わせて、再接続した
  ブラウザは Step 1（Evaluation）を再実行せずにワークベンチを再開します。非機密のみ（Property 7）—
  ソース DB のパスワードは Connect 画面で再入力します。
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

</details>

---

<br>

## 単一 EC2 ホストで実行 (ソースから、Lambda-free)

**コンテナ/ECR や AWS Lambda を使えないアカウント**向けです。同じコントロールプレーンアプリが、
VPC 内の **EC2 ホスト 1 台でソースのまま**、**systemd サービス**として動作します — イメージビルドも
ECR も ALB もありません。UI には **SSM ポートフォワード**で接続します（ホストにはパブリック IP も
インバウンドルールもありません）。状態（Full Load ジョブ / セッション）は**保持型 EBS ボリューム**
にあり、再起動後も保持され、**S3 バケットは不要です。** CDC では Kafka を**インプロセスで**シード
するため、Fargate と異なり**オフセットシーダー Lambda を作成しません**（`SeedMode=External`）。

テンプレート: **`deploy/cloudformation-ec2.yaml`**。

<details>
<summary><b>アーキテクチャ図</b> — 単一 EC2 ホスト、インプロセス CDC シード（SeedMode=External）</summary>

<div align="center">
  <a href="../docs/images/architecture-aws-ec2.png"><img src="../docs/images/architecture-aws-ec2.png" alt="単一 EC2 ホストのアーキテクチャ — 移行ツールが VPC 内の EC2 ホスト 1 台でソースのまま動作し、SSM ポートフォワードで接続（ALB なし）、Aurora DSQL への Full Load とインプロセスの CDC シードを MSK に対して行い、Debezium ソース + カスタム DSQL シンクコネクタが MSK Connect で S3 からプラグインをロード" width="820"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### いつ使うか

- ✅ アカウント/ポリシーが**コンテナ実行や ECR からの pull を禁止**している、または **AWS Lambda を
  禁止**している場合。
- ✅ それでも Fargate と同じ **VPC 内のプライベートなデータ経路**（ソース → ホスト → DSQL）が欲しく、
  データをノート PC 経由にしたくない場合。
- ❌ それ以外は **[ECS Fargate](#ecs-fargate-にデプロイ)** が適しています — マネージド・ロード
  バランス経路で、パッチすべきホストがありません。

> [!WARNING]
> **単一ホスト = 単一障害点（SPOF）。** ALB も Auto Scaling も 2 つ目のタスクもありません。状態は
> インスタンスの再起動 / 置き換え後も保持型 EBS ボリュームで残りますが、コントロールプレーン自体は
> 1 台です — 能動的に進める移行には適していますが、長期常設の HA サービス用ではありません。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. 前提条件

- **インターネット egress ができるプライベートサブネット（NAT ゲートウェイ）。** ホストには
  **パブリック IP がなく**、**初回起動時に `uv`・CPython・Python ホイールの取得とリポジトリの
  クローンをすべて公開インターネット**（astral.sh · PyPI · GitHub）から行い、その後もソース
  DB・DSQL・AWS API へは同じ egress で到達します。VPC エンドポイントだけでは不十分です
  （これらの公開ソースは PrivateLink では取得できません）。**ソース MySQL と同じ VPC**
  （CDC を使う場合は MSK とも同じ VPC）に配置してください。
- **AWS CLI と Session Manager プラグイン**（ご自身のマシンに）— UI を開く手段です（ALB や
  公開エンドポイントはなく、SSM でポートフォワードします）。
  [インストール手順](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html)。
- **CDC を使う場合のみ — ホストサブネットの CIDR。** これを cdc-stack に渡すと、MSK が
  インプロセスシードのためにホストをポート 9098 で許可します（下記
  [ホストを MSK 9098 に許可](#5-ホストを-msk-9098-に許可-cdc-のみ) を参照）。
- **ホストがアプリソースを取得する方法。** デフォルト（`SourceMode=git`）は公開リポジトリを
  HTTPS でクローンします — 認証情報は不要。ホストのネットワークがリポジトリに到達できない
  場合は、チェックアウトの tarball を S3 にアップロードして `SourceMode=s3`（`SourceS3Uri`）を
  使用してください。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. 必須 / 主要パラメータ

| パラメータ | 必須 | デフォルト | 内容 |
| --- | --- | --- | --- |
| `VpcId` | はい | — | ソース DB / MSK の VPC（DSQL と同一リージョン）。 |
| `HostSubnetId` | はい | — | `VpcId` の **NAT-egress プライベートサブネット**、MSK と同じ場所。 |
| `DsqlClusterArn` | はい | — | ターゲット DSQL クラスター（`dsql:DbConnect` のスコープ）。 |
| `SourceDbSecurityGroupId` / `SourceDbCidr` | いずれか必須 | `""` | ソース MySQL へのホスト egress を開放（生の CIDR より SG を推奨）。 |
| `SourceMode` | いいえ | `git` | `git`（公開 HTTPS で `SourceRepoUrl@SourceRepoRef` をクローン）または `s3`（`SourceS3Uri` の tarball）。 |
| `SourceS3Uri` | `s3` の場合必須 | `""` | リポジトリルートの `s3://…/source.tar.gz` — 一時的な「ローカルコピーを実行」パス。 |
| `MskEgressCidr` | いいえ | `0.0.0.0/0` | インプロセスシードのためにホストが MSK 9098 に到達する CIDR。最小権限のためコネクタサブネット CIDR に絞る。 |
| `InstanceType` | いいえ | `t3.large` | コントロールプレーンホストのサイズ。 |
| `StateVolumeSizeGiB` | いいえ | `20` | 保持型 EBS 状態ボリューム。大きなテーブルの Full Load でローカル CSV がスピルする場合は大きく。 |
| `SourceSecretArn` | いいえ | `""` | 既存のソース資格情報シークレットを再利用する場合のみ（それ以外は UI でユーザー名/パスワードを入力）。 |
| `EnableAiAssist` / `BedrockModelId` / `BedrockRegion` | いいえ | off / `global.anthropic.claude-sonnet-5` | Fargate と同じ opt-in の Bedrock AI アシスト（IAM スコープはモデルから自動導出）。 |
| `KeyName` | いいえ | `""` | オプションの SSH キー。SSM が主要なアクセス経路なので通常は空（ホストにはインバウンドルールが一切なし）。 |

> [!WARNING]
> スタック名は **`mysql-dsql-cdc-` で始めてはいけません**（その接頭辞は CDC デプロイロールのスコープに
> 入ります）。`mysql-dsql-migrator-ec2` が適切です。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 3. デプロイ

これは CloudFormation スタック（`deploy/cloudformation-ec2.yaml`）で、Fargate と同様に 2 通りで
デプロイできます:

- **AWS Console — 推奨。** テンプレートをアップロードしてガイド付きフォームを入力します
  （`VpcId` / `HostSubnetId` はネイティブピッカー。Console がテンプレートをステージングするため、
  S3 バケットは不要）。手順は [Fargate の Console ウォークスルー](#推奨--aws-console-ガイド付きフォーム)
  と同じです — このテンプレートを選び、上記の EC2 パラメータを入力し、スタック名を
  `mysql-dsql-migrator-ec2` にするだけです。
- **AWS CLI** — `aws cloudformation deploy` 1 回:

```bash
# --- ご自身の環境 (ここを編集) -----------------------------------------------
export AWS_REGION=us-east-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# HostSubnetId: VPC_ID 内の NAT-egress プライベートサブネット、MSK と同じ場所
export HOST_SUBNET_ID=subnet-0123456789abcdef0
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------

# ホストサブネットの CIDR — ホストの MSK への egress 範囲（下記 MskEgressCidr）と、
# CDC の場合はホストを 9098 で許可するのに使用（「ホストを MSK 9098 に許可」を参照）:
export HOST_SUBNET_CIDR=$(aws ec2 describe-subnets --subnet-ids "$HOST_SUBNET_ID" \
  --region "$AWS_REGION" --query 'Subnets[0].CidrBlock' --output text)

# このテンプレートは CloudFormation のインラインアップロード上限(51,200 バイト)を超えて
# いるため、CLI がステージング用の S3 バケットを必要とします。一度だけ作成するか、既存の
# バケットがあれば再利用してください:
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
    # デフォルトの SourceMode=git は公開リポジトリをクローンします（認証情報不要）。ホストが
    # リポジトリに到達できない場合は追加:  SourceMode=s3 SourceS3Uri=s3://$TEMPLATE_BUCKET/dsql-src.tar.gz
```

初回起動は約 3〜4 分: ホストが 443 経由で Python 3.12 + wheel をインストールし、
`uv sync --extra cdc-external`（インプロセスシードに必要な `kafka-python` + MSK IAM 署名機を含む）
を実行してからサービスを開始します。進捗はホストの `/var/log/dsql-migrator-userdata.log` にあります。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 4. UI に接続する (SSM ポートフォワード)

スタックは `HostInstanceId` とすぐ実行できる `SsmPortForwardCommand` を出力します:

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region "$AWS_REGION"
```

`http://localhost:8080` を開くとツールの UI が表示されます（同じガイド付きワークフロー）。SSM Run
Command で `systemctl is-active dsql-migrator.service` と `journalctl -u dsql-migrator` を使って
サービスの状態を確認してください。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 5. ホストを MSK 9098 に許可 (CDC のみ)

CDC ではホストが Kafka をインプロセスでシードするため、MSK Serverless 9098 に到達する必要があります
— cdc-stack の `HostSubnetCidr` パラメータがホストを許可します（コネクタ SG のイングレスを追加）。
**この EC2 ホストでは自動です:** ホストが起動時に自分のサブネット CIDR を導出し、UI で **CDC
インフラをデプロイ**するとツールがその値を渡します — 設定するものはありません。（cdc-stack をツール
外で自分でデプロイする場合のみ `HostSubnetCidr` を手動で渡します。）いずれの場合も、ホストが MSK に
到達できないと **Start CDC はコネクタを作成する前に明確に失敗**します（`CdcDeployError`）— 静かな
ギャップは発生しません。

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 6. Teardown

CDC をデプロイした場合は、**ホストが稼働しているうちに先に**削除してください — **Data Migration**
ステップで **Delete all CDC infrastructure** を使います。この削除はホスト上のアプリから実行される
ため、先にホストを削除すると cdc-stack を手動（`aws cloudformation delete-stack`）で削除することに
なります。その後、EC2 ホストを解体します:

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
```

> [!WARNING]
> 状態 EBS ボリュームは設計上 **`DeletionPolicy: Retain`** のため、**スタック削除後も残ります** —
> 保持したくない場合は手動で削除してください（`aws:cloudformation:stack-name` タグで検索）。

---

<br>

## 付録 — 自前のイメージをビルドする (ECS Fargate 専用; 制限されたネットワークのみ)

> [!NOTE]
> **これは ECS Fargate デプロイにのみ該当します** — 単一 EC2 ホストモードはソースから実行され、
> コンテナイメージを使用しないため、このセクションは一切不要です。
>
> **さらに Fargate デプロイでもほとんどはスキップします。** イメージは ECR Public に
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

> [!TIP]
> デプロイの再現性のため、リリースごとにイミュータブルなタグ（またはイメージダイジェスト）
> を使用してください。
