# mysql-dsql-migrator

_言語: [English](README.md) | [한국어](README.ko.md) | **日本語**_

Amazon RDS MySQL / Aurora MySQL データベースを **Amazon Aurora DSQL** へ移行するための、
Web ベースのオールインワンツールです。

Aurora DSQL は MySQL ではなく PostgreSQL 16 互換の分散データベースであるため、
これは 2 つの変換が重なり合う **異種間移行（heterogeneous migration）** になります。

1. MySQL → PostgreSQL 方言
2. PostgreSQL → DSQL の制約（外部キー非対応、楽観的並行性制御（OCC）、
   トランザクションごとの行数/時間制限、非同期インデックス、`C` コレーションなど）

本ツールの目標は、完全に自動化されたゼロダウンタイム移行ではありません。目指すのは、
**移行可能性を評価し、確定的に変換できるものは自動化し、人手が必要な箇所を明確に浮き彫りにすること**
です。変換は確定的な手法を最優先し（`sqlglot`）、ソースデータベースへのアクセスは常に読み取り専用です。

> **開始する前に、[カスタマー FAQ](docs/manual/ja/11-customer-faq.md) をお読みください。**
> 異種間の MySQL → Aurora DSQL 移行は、バージョンアップグレードよりも可動部が多くなります。
> FAQ は、計画すべき事項をあらかじめ答えています。すなわち、Full Load と CDC の比較、
> スキーマが適合しなければならない DSQL の制限、型マッピング、正しさの検証方法、
> カットオーバー/ロールバック、コスト面と運用面の考慮事項です。先にお読みいただくことで、
> 後々の驚きを避けられます。
>
> **初めての方へ** [**ユーザーマニュアル**](docs/manual/README.md) は、Aurora MySQL から来る
> エンジニア向けのタスク指向のウォークスルーです。セットアップ、Evaluation、Schema
> Conversion、Full Load、CDC + DSQL 制約、Validation、そして制限事項を扱います。

## 概要

2 つのデータ経路が Aurora DSQL に収束します。ツールが駆動する 1 回限りの **Full Load** と、
マネージド MSK Connect 上で動作する任意の継続的な **CDC** ストリームです。

![アーキテクチャ図](deploy/architecture-aws-simple.png)

> 詳細なトポロジーは [アーキテクチャ](#アーキテクチャ) にあります。

## できること / できないこと

ツールが代わりに行うこと、ご自身で行うこと、そして対象外の事項を、ひと目で確認できます。

**✅ できること**

- MySQL スキーマをイントロスペクトし、**DSQL 互換性評価**
  （`AUTO` / `MANUAL` / `UNSUPPORTED` + 工数見積もり）を生成します。
- MySQL → DSQL の **スキーマ（DDL）を変換して適用**します。型マッピング、FK 削除、
  非同期インデックス、PK 戦略などです。
- **Full Load** — 一貫性のあるスナップショットをストリーミングで一括ロードします（再開可能、TB スケール）。
- **CDC**（任意）— ニアゼロダウンタイムのカットオーバー（Cut over）のための継続的な変更レプリケーション。
  Full Load からギャップのない引き継ぎを実現します。
- **Validation** — 行数、チェックサム、PK 突き合わせにより、ソース ↔ ターゲットの一致を証明し、
  ドリフトを報告します。
- **AI アシスト**（任意、既定はオフ）— 難しい項目に対する変換の提案。ユーザーによるレビューと
  承認の後にのみ適用されます。

**❌ できないこと / 対象外**

- **完全に自動化されたゼロダウンタイム移行ではありません** — 難しい変換と最終的な
  **Cut over** は、ご自身で判断し実行します。
- **ソースには決して書き込みません** — ソースは常に読み取り専用です（ロールバックのアンカーとして保持されます）。
- **DDL は CDC でレプリケートされません** — スキーマ変更は Schema Conversion を通じてご自身で適用する必要があります。
- **クロスリージョン非対応** — ソースとターゲットは同一リージョンになければなりません。
- **DSQL が省いている機能は制約のまま残ります** — 外部キー、トリガー、ストアドプロシージャは非対応、
  トランザクションごとの行数制限、1 値あたり 1 MiB の制限などです（ツールは回避策を案内しますが、
  DSQL 自身の制限を変えることはできません）。

> 適用される制限とその回避策の完全な一覧は、ユーザーマニュアル
> [第 6 章 — 制限事項](docs/manual/ja/06-limitations.md) にあります。

## ワークフロー

Web UI は、**Connect** を予備ステップとして、6 ステップのワークフローを案内します。

`Connect → Migration plan → Evaluation → Schema Conversion → Data Migration → Validation → Cut over`

| ステップ | 内容 |
| --- | --- |
| Connect | ソース（RDS/Aurora MySQL）とターゲット（Aurora DSQL）の接続情報を入力します。認証情報はセッションごとのメモリに保持され、セッション終了時に破棄されます。 |
| 1. Migration plan | **この移行で CDC を使うか（はい/いいえ）** だけを決めます。この選択の唯一の永続的な効果は、ストリーミング（CDC）インフラを早期にプロビジョニングするかどうかです（はい → プロビジョニングする、いいえ → Full Load のみ）。より細かい分岐 — Full load + CDC か CDC のみか — は後の Data Migration ステップで選択し、その選択は取り消し可能です（Full-load のみで開始して後から CDC を追加できます）。 |
| 2. Evaluation | ソース **と** ターゲットの両方をイントロスペクトし、変換工数の見積もりとターゲット名の競合検出を含む互換性評価レポート（`AUTO` / `MANUAL` / `UNSUPPORTED`）を生成します。任意で AI アシストによる戦略も付加します。 |
| 3. Schema Conversion | ソース/ターゲットのオブジェクトを閲覧し、ソースと変換後の DDL を並べて表示し、変換後の DDL をターゲットに適用します（SKIP / REPLACE）。 |
| 4. Data Migration | 前提条件チェックを実行してテーブルを選択し、次に **Full Load** を行います。一貫性ウォーターマーク（watermark）を取得し、スナップショットをエクスポートし、テーブルごとの進捗とダウンロード可能なエラーログとともにターゲットへロードします。任意で、ストリーミングの **CDC**（別途の cdc-stack）へ拡張できます。 |
| 5. Validation | 移行済みのターゲットを、ウォーターマーク時点のソースと比較し、行数/チェックサムの結果とスナップショット以降のドリフトを報告し、レポートをエクスポートします。 |
| 6. Cut over | Validation が通過した後にアプリケーションを MySQL から DSQL へ切り替えるための運用ランブックです。ツールが代わりに実行しない唯一のステップです。ご自身のパターン（CDC ドレイン か Full-Load フリーズか）に合わせて調整され、MySQL ソースはロールバックのアンカーとして保持されます。 |

各ステップはその状態（未開始 / 進行中 / 完了 / 失敗）を表示し、独立して実行または再実行できます。
前提となるステップが未完了の場合は UI が案内します。

## 機能

- テーブル、カラム、型、主キー、インデックス、外部キー、ビュー、トリガー、ルーチン、
  `AUTO_INCREMENT`、文字セット/コレーションの **読み取り専用ソースイントロスペクション**。
- すべてのオブジェクトを分類し、DSQL の制約（FK、トリガー、手続き型ルーチン、PK なし、
  大文字小文字を区別しないコレーション、パーティショニング、非対応の型）を理由と推奨事項とともに
  フラグ付けする **互換性評価**。
- `sqlglot` による **スキーマ（DDL）変換**。型マッピング、アプリ層の整合性に関する注記を伴う FK 削除、
  `CREATE INDEX ASYNC`、PK 戦略、DDL/DML をトランザクションあたり単一 DDL の単位に分割する処理。
- **インタラクティブな適用**（SCT ライク）。オブジェクトツリー、DDL の差分、競合処理、
  適用時の `40001`/OC001 冪等リトライ。
- ロックのアンチパターン検出（例: DSQL の制約に反する `SELECT ... FOR UPDATE`）を備えた
  **クエリ（DML）変換**。
- ウォーターマーク取得（binlog 座標 / GTID / スナップショットのタイムスタンプ）、
  一貫性スナップショットのエクスポート、OCC リトライを伴うバッチ処理された
  `INSERT ... ON CONFLICT` インポート（主経路としての Aurora DSQL Loader）、
  トランザクションごとの制限を尊重する再開可能かつチャンク化された設計を備えた **データ移行**。
- 行数とサンプリング/チェックサムによる **Validation**。ライブソースに対してはウォーターマークベースの
  ドリフト報告を行います。
- `FOR UPDATE`、FK 依存、`AUTO_INCREMENT` 依存、トリガー/SP 呼び出し、非対応関数を対象とした
  **アプリケーションアンチパターンリンター**。
- **任意の AI アシスト変換**（Amazon Bedrock）。既定はオフ。有効化すると、`MANUAL`/`UNSUPPORTED`
  項目に対するレビュー専用の提案を生成します。提案は、明示的な人手によるレビューと承認なしに適用されることはありません。
- **任意の大規模ストリーミング CDC**（別途の `cdc-stack`）。マネージド MSK Connect 上の Debezium
  → Amazon MSK → **カスタム Aurora DSQL シンクコネクタ**（当社の Java プラグイン）で、
  統合モニタリングと単一のダウンロード可能なエラーログを備えます。ツールはコントロールプレーンであり、
  コネクタはマネージド MSK Connect 上で動作します（シンクのコンピューティングは保有しません）。

## アーキテクチャ

本ツールは、オペレーターが顧客環境内で実行する **Python アプリ**（NiceGUI UI + インポート可能なエンジン）
であり、確定的優先の移行を実行します。すなわち、評価 → 変換 → 一貫性のあるスナップショットの一括ロード
→ 検証です。デプロイ時には、**HTTPS ALB** の背後の **単一タスクの Amazon ECS Fargate サービス**
（既定は `internal`、任意で Cognito）として動作し、**Amazon ECR** からコンテナイメージを取得します
（既定では公開されている ECR Public イメージ）。全経路をひと目で見るには、上記の [概要](#概要) を参照してください。

[![完全な AWS アーキテクチャトポロジー](deploy/architecture-aws.png)](deploy/architecture-aws.png)

> 図は詳細です — **クリックすると原寸解像度で開きます。**

- **AI アシストはコントロールプレーンのみ** — 有効化すると、**Amazon Bedrock** が変換の提案、
  CDC 準備状況の評価、DLQ トリアージを追加しますが、**CDC のデータ経路には決して配置されません**
  （既定はオフ）。
- **CDC は任意の別経路** — ニアゼロダウンタイムのカットオーバーのために、**Amazon MSK + Debezium**
  のストリーミングパイプライン（別途の `cdc-stack`）を立ち上げることができます。実際に DSQL へ書き込む
  シンクは、マネージド MSK Connect 上の **カスタム DSQL シンクコネクタ**
  （[`connectors/dsql-sink/`](connectors/dsql-sink)）です。既製の JDBC シンクでは DSQL の短命な
  IAM トークン、ステートメントレベルの OCC リトライ、≤3,000 行のバッチに対応できないため、当社が独自に構築しました。
  ツールはコントロールプレーン（設定、一括ロード、ウォーターマーク、モニタリング）にとどまり、
  自前のシンクコンピューティングは一切動かしません。

> **詳しくは:**
> - 完全な AWS アイコントポロジー: [`deploy/architecture-aws.png`](deploy/architecture-aws.png)
>   （簡略版の概要: [`deploy/architecture-aws-simple.png`](deploy/architecture-aws-simple.png)）。
> - サービスごとの役割: 下記の [使用する AWS サービス](#使用する-aws-サービス)。
> - CDC と DSQL 制約がデータ経路でどう振る舞うか: ユーザーマニュアル
>   [第 4 章 — CDC と DSQL 制約](docs/manual/ja/04-cdc-and-dsql-constraints.md)。
> - パフォーマンスとスケーリング（カスタムシンクが存在する理由、並列性のチューニング）: マニュアル
>   [第 7 章 — パフォーマンスとチューニング](docs/manual/ja/07-performance-and-tuning.md)。

## 使用する AWS サービス

コントロールプレーン（app-stack）は常に使用されます。ストリーミング CDC のデータプレーン（cdc-stack）は任意です。
移行の **ソース**（Amazon RDS / Aurora MySQL）は顧客が所有し、両スタックの外部にあります。Debezium は
MSK Connect **上で** 動作するオープンソースソフトウェアであり、別個の AWS サービスではありません。

**コントロールプレーンと共有（app-stack）**

| サービス | 役割 |
| --- | --- |
| Amazon ECS (Fargate) | 単一タスクのコントロールプレーンアプリ（NiceGUI + エンジン）を実行します。 |
| Amazon ECR | Fargate が取得するアプリのコンテナイメージを保管します（既定では公開されている ECR Public イメージ）。 |
| Elastic Load Balancing (ALB) | アプリへ転送する HTTPS のエントリポイント（既定は `internal`）。 |
| Amazon Route 53 | アプリドメインの DNS（パブリックドメインを使用する場合のみ。オペレーターが提供）。 |
| AWS WAF | ALB の前段の Web 保護（パブリック公開時に推奨）。 |
| Amazon Cognito | ALB での OIDC 認証ゲート（任意 — パブリックインターネットに公開する場合に必須）。 |
| AWS Certificate Manager (ACM) | ALB HTTPS リスナー用の TLS 証明書。 |
| Amazon VPC | プライベートサブネット、セキュリティグループ、NAT / VPC エンドポイント（app-stack と cdc-stack）。 |
| AWS IAM | 最小権限のタスク / 実行 / コネクタロールと、DSQL の IAM トークン認証。 |
| AWS Secrets Manager | UI セッションクッキー署名シークレット（スタックが自動作成）。ソース MySQL の認証情報は既定では UI で入力され、既存のシークレットを再利用する場合にのみここで使用されます（実行時に読み取り。テンプレートには決して保存されません）。 |
| Amazon Aurora DSQL | 移行のターゲット（PostgreSQL 互換、IAM 認証、OCC）。 |
| Amazon S3 | Full Load のステージング（大きなテーブルのストリーミングエクスポート）、コネクタプラグインのアーティファクト、CodeBuild のソース。 |
| Amazon CloudWatch (Logs) | アプリとコネクタのログ。CDC のラグ / メトリクス。 |
| Amazon Bedrock | 任意の AI アシスト変換 / CDC 準備状況 / DLQ トリアージ（コントロールプレーンのみ）。 |
| AWS CloudFormation | app-stack と cdc-stack のための Infrastructure-as-Code。 |

> 通常のデプロイでは、公開されている **ECR Public イメージ** をそのまま取得するため、**イメージのビルドはありません**。
> **AWS CodeBuild** はランタイムコンポーネントではありません。ローカルの Docker がない制限されたネットワークで
> 自前のイメージをビルドする必要がある場合にのみ、別のビルドスタック（`deploy/codebuild.yaml`）経由で
> 一度だけ使用する任意のビルドツールです。

**任意の CDC データプレーン（cdc-stack）**

| サービス | 役割 |
| --- | --- |
| Amazon MSK (Serverless) | Kafka のバックボーン。PK でパーティション分割されたテーブルごとのトピックと、DLQ トピック。 |
| Amazon MSK Connect | Debezium MySQL ソースコネクタと当社のカスタム DSQL シンクコネクタをホストするマネージド Kafka Connect ランタイム。スキーマはランタイム組み込みの **JSON コンバータ**（`schemas.enable=true`）で運ばれます — 別個のスキーマレジストリは不要です。 |
| AWS Lambda | VPC 内の **オフセットシーダー** — ギャップのない Full Load → CDC の引き継ぎのために、Debezium のオフセット（GTID ウォーターマーク）を connect-offsets トピックへ自動投入する CloudFormation カスタムリソース。 |
| Amazon VPC (専用) | CDC は独自の VPC（プライベートサブネット、NAT、VPC エンドポイント）にデプロイされ、ソース MySQL へプライベートに到達します。 |

## 前提条件

移行を開始する前に、以下が必要です。

**共通（どちらの実行方法でも）**

- スキーマとデータを読み取れるユーザーを持つソース **Amazon RDS / Aurora MySQL**
  （読み取り専用で十分です — ツールはソースに決して書き込みません）。
- ソースと **同一リージョン** にあるターゲット **Amazon Aurora DSQL** クラスター。
  （パスワードなし — IAM トークン認証。）
- 標準的な認証情報チェーン（環境、`~/.aws`、または名前付きプロファイル）を通じて到達可能で、
  Aurora DSQL の IAM トークンを生成する権限（`dsql:DbConnect`）を持つ **AWS 認証情報**。
  任意で `secretsmanager:GetSecretValue`（Secrets Manager 内のソース認証情報）と
  `bedrock:InvokeModel`（AI アシスト） — どちらも任意です。

**ローカルで実行する場合のみ、加えて:**

- Python 3.10 以降（本プロジェクトは `.python-version` で 3.12 に固定しています）
- 依存関係管理のための [`uv`](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`

> ソース DB のセットアップと CDC 要件（binlog など）を含む完全なチェックリストは、
> [ユーザーマニュアル §1.1](docs/manual/ja/01-setup.md) にあります。

## クイックスタート（clone → 実行）

### オプション A — ローカルで実行（最速）

追加のインフラなしで、ご自身のマシン上に UI を立ち上げます。評価、小規模な移行、開発に最適です。

```bash
# 1. Clone the repo
git clone <repo-url> mysql-dsql-migrator
cd mysql-dsql-migrator

# 2. Install dependencies (uv creates and fills a .venv virtualenv)
uv sync

# 3. (Optional) pre-fill connection details — the Connect screen picks these up
cp .env.example .env
#   Edit .env with your source/target connection values. .env is git-ignored.

# 4. Launch the web UI
uv run mysql-dsql-migrator ui
```

既定では `http://127.0.0.1:8080` にバインドされます。表示された URL をブラウザで開き、
**Connect** ステップから始めてください。[**ユーザーマニュアル**](docs/manual/README.md)
は、そこからの各ステップを案内します。

> ここでは **ご自身のマシンが移行エンジン** になります。すべてのデータがそれを通過するため、
> ご自身のマシンは **ソース MySQL と DSQL の両方** に到達できなければなりません
> （プライベートなソースには VPN / SSM フォワードが必要です）。AWS 認証情報は、これを実行する
> シェルで使用可能でありさえすればよいです（`aws sso login`、`AWS_PROFILE=...`、環境変数）。

### オプション B — ECS Fargate（実運用の移行）

実運用の移行では、同じツールを AWS にデプロイします。クローン後、**CloudFormation で
app-stack をデプロイ**します（イメージのビルドなし — 公開されている ECR Public イメージを使用します）。
ツールは VPC 内の単一タスクの Fargate サービスとして立ち上がり、出力される **ALB URL** で UI に到達できます。

```bash
git clone <repo-url> mysql-dsql-migrator
cd mysql-dsql-migrator
# then deploy with CloudFormation — exact command + parameters in the deploy guide
```

**完全な手順は [`deploy/DEPLOYMENT.ja.md`](deploy/DEPLOYMENT.ja.md) にあります**（下記の
[デプロイ](#デプロイ) にも要約されています）。ローカルとは異なり、**すべての移行トラフィック
（ソース読み取り → 変換 → DSQL 書き込み）は AWS 内で行われ、ローカルマシンを一切通過しません** —
ブラウザは ALB URL 経由で UI を開くだけであり、データ経路上にはありません。これが、大規模/TB スケールの
移行やプライベートなソースに適している理由です。

> 対比での比較は、下記の [実行方法: ローカル vs ECS Fargate](#実行方法-ローカル-vs-ecs-fargate) の表を参照してください。

## 実行方法: ローカル vs ECS Fargate

同じツール、同じ UI、同じ移行ステップ — 変わるのは **どこで実行するか** だけです。評価/小規模な移行には
ローカルを、実運用の移行には Fargate を使用してください。

| | **ローカル（ご自身のマシン）** | **ECS Fargate（AWS にデプロイ）** |
|---|---|---|
| 最適な用途 | 評価、小規模な移行、開発 | 実運用の移行、大規模/TB スケール |
| 実行場所 | ご自身のラップトップ/ワークステーション | VPC 内の単一タスクの Fargate サービス |
| **データ経路** | ソース → **ご自身のマシン** → DSQL（すべてのデータがご自身のマシン/ネットワークを通過） | ソース → **VPC 内の Fargate** → DSQL（データは AWS 内にとどまる） |
| ネットワーク到達性 | ご自身のマシンが **ソース MySQL と DSQL の両方** に到達できる必要がある（プライベートなソースには VPN / SSM フォワードが必要） | Fargate は VPC 内部からソースへプライベートに到達 |
| 開き方 | ブラウザ → `127.0.0.1:8080` | ブラウザ → ALB（既定は `internal`。VPN / Direct Connect / SSM 経由で到達） |
| 認証 | 不要（ローカルのため） | ネットワークがゲート（既定）。パブリック公開する場合は Cognito が必須 |
| 大きなテーブルのステージング | ローカルの一時 CSV（小さなテーブルのみ） | S3 ステージング（ストリーミング、大きなテーブル） |
| セットアップ方法 | 上記の [クイックスタート](#クイックスタートclone--実行) | CloudFormation — [デプロイ](#デプロイ) / [`deploy/DEPLOYMENT.ja.md`](deploy/DEPLOYMENT.ja.md) |
| インフラ | なし | ECS · ALB · IAM など（CloudFormation でプロビジョニング） |

> **要点:** ローカルでは *ご自身のマシンが移行エンジン* であるため、ソースとターゲットの両方の
> ネットワークに到達することが難関になります。Fargate はそのエンジンを VPC 内に移し、データ経路が
> AWS 内にとどまるようにします — ホスト型の形態です。

## 設定（上級者向け — 通常は触る必要なし）

> ほとんどのユーザーは **このセクションをスキップできます** — すべては UI で行われ、
> 妥当な既定値が適用されます。以下は、自動化、チューニング、トラブルシューティングのための
> オペレーター向けの環境変数リファレンスです（パフォーマンスの調整項目の背景については、
> マニュアルの [パフォーマンスとチューニング](docs/manual/ja/07-performance-and-tuning.md) の章を参照してください）。

設定は環境変数から読み取られます。認証情報の値が設定に永続化されることは一切ありません。

| 変数 | 既定値 | 説明 |
| --- | --- | --- |
| `DSQL_MIGRATOR_APP_HOST` | `127.0.0.1` | UI がバインドするホスト/インターフェース。 |
| `DSQL_MIGRATOR_APP_PORT` | `8080` | UI がリッスンするポート。 |
| `DSQL_MIGRATOR_AWS_REGION` | _(未設定)_ | boto3 クライアント（例: DSQL トークン生成）用の AWS リージョン。 |
| `DSQL_MIGRATOR_AWS_PROFILE` | _(未設定)_ | すべての AWS クライアントに適用される任意の単一グローバル AWS 名前付きプロファイル。未設定の場合は標準的な認証情報チェーンにフォールバックします。プロファイル名（非機密）のみが保存されます。 |
| `DSQL_MIGRATOR_JOB_STATE_PATH` | `job_state.sqlite` | ローカルのジョブ状態ストアへのパス。Full Load ジョブのスナップショット（状態、テーブルごとの進捗、ウォーターマーク）はここに永続化され、再起動時に再読み込みされるため、中断されたジョブを再開できます（中断されて処理中だったテーブルは、部分的なリトライのために失敗として表示されます）。 |
| `DSQL_MIGRATOR_ACTIVITY_LOG_PATH` | `migration_activity.log` | 構造化されたアクティビティログファイルへのパス。すべての移行イベント — 接続テスト、評価の実行、オブジェクトごとのスキーマ適用（CREATED/SKIPPED/FAILED）、テーブルごとの Full Load の結果（成功/失敗と詳細）、CDC コントロールプレーンのアクション — が、UTC タイムスタンプ付きの 1 行の JSON として追記されます。UI からダウンロード可能で（サイドバーの「アクティビティログをダウンロード」）、タイムライン全体を時系列で読み取り/並べ替えできます。成功と失敗の両方が記録されます（ジョブごとのエラーログは、失敗のみ・行レベルのアーティファクトのままです）。ファイルはサイズ上限が設けられローテーションされるため（セグメントあたり約 20 MB、バックアップ 4 個、合計約 100 MB）、際限なく増大することはなく、ダウンロードでは保持されているセグメントを時系列順に連結します。`DSQL_MIGRATOR_LOG_LEVEL=DEBUG` の場合、失敗イベントには追加でデバッグ用の完全な Python の `stacktrace`（コールスタックのみ — 行の値や認証情報は決して含まない）が付加されます。既定の `INFO` レベルでは、通常のログを整然と保つために省略されます。 |
| `DSQL_MIGRATOR_SESSION_STATE_PATH` | `session_state.sqlite` | ローカルのセッションごとの状態ストアへのパス。各セッションの非機密のワークベンチ状態（ワークフローの進捗、評価結果、生成されたオブジェクト、移行ジョブの紐付け）を永続化するため、再接続したブラウザは再起動後も中断した箇所から再開できます。ブラウザのセッション ID が再起動をまたいで安定するように、`DSQL_MIGRATOR_STORAGE_SECRET` と併用してください。 |
| `DSQL_MIGRATOR_STAGING_BUCKET` | _(未設定)_ | Full Load のステージング用の任意の S3 バケット。設定すると、各テーブルはストリーミングのマルチパートアップロードでこのバケットにエクスポートされ、`s3://` URI からロードされるため、テーブル全体の CSV がコンテナのエフェメラルディスクに置かれることはありません（大規模/TB テーブル向けのスケーラブルな経路）。未設定の場合は、上限付きのローカル一時 CSV が使用されます（ローカル開発 / 小さなテーブルのみ）。 |
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | `4`（≤16） | Full Load: いくつのテーブルを並行してロードするか。DSQL への同時接続数の合計 ≈ テーブル × バッチの並列性。クラスターの接続クォータ内に収めてください。マニュアルの [パフォーマンスとチューニング](docs/manual/ja/07-performance-and-tuning.md) の章を参照してください。 |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | `8`（≤32） | Full Load: テーブルあたりの処理中の `INSERT ... ON CONFLICT` バッチ数。値を大きくするほどスループットは上がりますが、ホットなキー範囲での OCC（40001）衝突が増えます。 |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | `2000`（≤3000） | Full Load: バッチ書き込みあたりの行数。DSQL のトランザクションごとの 3000 行制限で厳密に上限が設けられます。 |
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | `4`（≤32） | Validation: いくつのテーブルを並行して比較するか（それぞれが独自の読み取り専用のソース + ターゲット接続を持つ）。`1` = 逐次。 |
| `DSQL_MIGRATOR_LOG_LEVEL` | `INFO` | 起動時のログレベル。`DEBUG` に設定すると、アクティビティログの失敗イベントで完全な Python の `stacktrace`（コールスタックのみ — 行の値や認証情報は決して含まない）を追加で取得します。これは初期値のみです — トラブルシューティング中は、再デプロイなしにアプリの **Diagnostics** コントロール（サイドバーのフッター）から実行時に変更してください。 |
| `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` | `false` | 各アクティビティログイベントを（ローテーションするファイルに加えて）JSON 行として標準出力にミラーリングするかの起動時の既定。ECS では、コンテナの `awslogs` ドライバーが標準出力を CloudWatch Logs に転送するため、タスクの置き換えを生き延びる、耐久性がありクエリ可能な監査証跡のコピーが得られます（ローテーションするファイルはエフェメラルストレージ上に存在します）。これは初期値のみです — トラブルシューティング中は、再デプロイなしにアプリの **Diagnostics** コントロール（サイドバーのフッター）から実行時に切り替えてください。 |
| `BEDROCK_MODEL_ID` | `us.anthropic.claude-sonnet-4-6` | AI アシスト変換に使用される Bedrock モデル / 推論プロファイル ID（オプトイン）。 |
| `BEDROCK_REGION` | _(未設定)_ | Amazon Bedrock 呼び出し用のリージョン。 |

AI アシスト変換は既定では無効で、UI でオンにします。Connect/設定画面では、
設定された Bedrock モデル/リージョンが到達可能かをチェックし、実行可能な失敗理由
（アクセス拒否、モデルが有効化されていない、スロットリング）を認証情報を露出させずに報告する
**Verify AI access** のプリフライトも提供します。

## プロジェクト構成

コードを読む際に知っておくべきトップレベルのディレクトリ:

| パス | 内容 |
|---|---|
| `src/dsql_migrator/core/` | インポート可能な移行エンジン（UI 依存なし）。 |
| `src/dsql_migrator/ui/` | NiceGUI Web アプリケーション — **主要インターフェース**。 |
| `src/dsql_migrator/cli/` | 自動化のためのコマンドラインエントリポイント。 |
| `connectors/dsql-sink/` | カスタム Aurora DSQL Kafka Connect **シンクコネクタ**（Java。任意の CDC データプレーンプラグイン）。 |
| `deploy/` | デプロイ資産 — `Dockerfile`、CloudFormation テンプレート（app-stack と cdc-stack）、ビルド/ティアダウンスクリプト、アーキテクチャ図。詳細は [`deploy/DEPLOYMENT.ja.md`](deploy/DEPLOYMENT.ja.md)。 |
| `docs/manual/` | ステップバイステップのユーザーマニュアル（EN と KO）。 |

## デプロイ

本ツールは、顧客の IAM コンテキストで顧客のプライベートな RDS/Aurora と DSQL に接続するため、
中央集権的な SaaS としてではなく、**顧客環境内（シングルテナント）** で動作します — 本番では
`deploy/cloudformation.yaml`（app-stack）からデプロイされる単一タスクの **Amazon ECS Fargate**
サービスとして、イメージのビルドなしで動作します（公開されている ECR Public イメージ）。

**▶ 完全なステップバイステップの手順は [`deploy/DEPLOYMENT.ja.md`](deploy/DEPLOYMENT.ja.md) にあります** —
クイックデプロイ、CloudFormation パラメータ、Dev/Test vs Prod プロファイル、DNS と Cognito、
検証、更新、ティアダウン、トラブルシューティングです。（任意の大規模ストリーミング CDC パイプラインは
別途の **cdc-stack** であり、ガイドで扱われます。）

> [!IMPORTANT]
> **リージョン制約 — 単一リージョンのみ。クロスリージョン移行はサポートされません。** 本ツールは
> **Aurora DSQL を提供する任意の AWS リージョン** で動作しますが、**ソース（RDS / Aurora MySQL）と
> ターゲット（Aurora DSQL）は同一リージョンになければならず**、ツールがプロビジョニングするすべての
> インフラはその 1 つのリージョンにデプロイされます（DSQL ターゲットエンドポイントから導出されます — 例:
> `…dsql.ap-northeast-2.on.aws` → `ap-northeast-2`）。特に任意の CDC データプレーンは
> **DSQL リージョンの VPC 内** で動作し、ソース MySQL へプライベートに到達しなければならないため、
> クロスリージョンのソース/ターゲットの組み合わせはサポートされません。

> **ドキュメント全体の流れ:** この README がオリエンテーションを行い（何であるか、アーキテクチャ）→
> [`deploy/DEPLOYMENT.ja.md`](deploy/DEPLOYMENT.ja.md) がデプロイして UI を立ち上げ →
> [**ユーザーマニュアル**](docs/manual/README.md) がその UI で実際の移行を実行する手順を案内します。
> 完全なランタイムトポロジーは、上記の [アーキテクチャ](#アーキテクチャ) 図を参照してください。

## バージョン / 変更履歴

現在のバージョンは [`pyproject.toml`](pyproject.toml) で宣言されています。各バージョンが何を追加/変更するかは
[**変更履歴（CHANGELOG.md）**](CHANGELOG.ja.md) に記録されています — 更新後、新しい内容を確認するにはそこをチェックしてください。

## ライセンス

**Apache License 2.0** の下でライセンスされています — [`LICENSE`](LICENSE) と
[`NOTICE`](NOTICE) を参照してください。本プロジェクトは `connectors/plugins/` の下に、事前ビルドされた
サードパーティのコネクタアーティファクト（Debezium とそのランタイム依存関係）をバンドルしています。
それらのライセンスは [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md) に列挙されています。
バンドルされている依存関係の 1 つ、MySQL Connector/J は Universal FOSS Exception 付きの GPL-2.0 の下にあるため、
再配布する前に確認してください。
