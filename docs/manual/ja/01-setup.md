# 1. セットアップ

_言語: [English](../en/01-setup.md) | [한국어](../ko/01-setup.md) | **日本語**_

> **前へ:** [0. はじめる前に](00-before-you-begin.md)

この章では、「Amazon RDS または Aurora データベース(MySQL または PostgreSQL)がある」状態から
「ツールがブラウザで開いており、ソースと Aurora DSQL ターゲットの両方に接続できている」状態までを
案内します。

> **すでに AWS へデプロイ済みですか?** [`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md)
> に従って UI がすでに `AppUrl` で開いている場合は、[§1.5 接続](#15-ソースとターゲットへの接続)まで
> 読み飛ばしてください。この章では、デプロイガイドが扱わないローカル実行についても説明します。

ツールを実行する方法は 3 つあります。

- **ローカル** — 評価や比較的小規模なマイグレーションのために、ノートPC/ワークステーション上で
  実行します。最も速く始められます。
- **AWS 上 (ECS Fargate)** — 実際のマイグレーションで多くのチームが利用するデプロイ形態で、
  Application Load Balancer の背後にある Web エンドポイント経由でアクセスします。
- **AWS 上 (単一 EC2 ホスト、ソースから)** — コンテナ/ECR や AWS Lambda を使えないアカウント向け:
  VPC 内の単一 EC2 ホスト上でソースから直接実行し(`git clone` + `uv sync` + **systemd** サービス)、
  SSM ポートフォワード経由でアクセスします(ECS/ALB/イメージなし)。→ §1.4 を参照。

どの方法でも接続する先は **同じ** ソースとターゲットです。異なるのはツールの *プロセス* が
どこで実行されるか、その 1 点だけです。

---

## 1.1 前提条件

**データベース**

- ネットワーク経由で到達できるソースデータベース — **Amazon RDS または Aurora MySQL**、あるいは
  **Amazon RDS または Aurora PostgreSQL**。スキーマとデータを読み取れるユーザーがあれば十分です。
  Evaluation、Schema Conversion、Full Load、Validation には読み取り専用で十分で、MySQL ソース
  (および Full Load のみの PostgreSQL マイグレーション)には一切書き込みません。唯一の例外は
  **PostgreSQL CDC** です。Full Load の整合点でツールが、移行対象テーブルだけに厳密に限定した論理
  レプリケーションスロットと publication を作成し(AUTOCOMMIT、小さな許可リストに限定、監査記録
  あり)、teardown(撤去)時に削除します。これがツールがいずれのソースに対しても行う唯一の書き込み
  です。
  - **サポートされるソースエンジンとバージョン** — 両方のソース経路(Schema Conversion + Full
    Load + CDC + カットオーバー)を実インフラ上でエンドツーエンドに検証済み:
    - **RDS for MySQL** 5.7 / 8.0 / 8.4、**Aurora MySQL** 5.7(v2) / 8.0(v3) / 8.4。MySQL 5.7 は
      標準サポートが終了しています(RDS/Aurora の Extended Support が適用される場合あり)が、
      ツールは移行ソースとして 5.7 を完全にサポートします。
    - **RDS for PostgreSQL / Aurora PostgreSQL**、PG **13–16**(エンドツーエンドでテスト済み)。
      ツールに厳格なバージョンゲートはありません — サーバーバージョンは表示用に読むだけで、どの
      メジャーバージョンも拒否しません。PG 13–16 が検証済みの範囲です。
- ツールを実行するのと **同じ AWS リージョン** にあるターゲットの **Amazon Aurora DSQL**
  クラスター。(DSQL は IAM トークン認証を使うため、管理すべきパスワードはありません。)

**ローカル実行の場合**

- Python 3.10+ (プロジェクトは `.python-version` で 3.12 に固定しています)。
- 依存関係管理のための [`uv`](https://docs.astral.sh/uv/)。
- 標準の認証情報チェーン(環境変数、`~/.aws`、または名前付きプロファイル)経由で到達でき、
  **Aurora DSQL IAM トークンを生成** できる(`dsql:DbConnect` / `dsql:DbConnectAdmin`)AWS
  認証情報。任意で `secretsmanager:GetSecretValue`(ソース認証情報が Secrets Manager にある場合)
  および `bedrock:InvokeModel`(AI アシストを有効にする場合のみ)も必要です。

**AWS へデプロイする場合** — 完全版は [`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md)
を参照してください。要約は §1.3 にあります。

> **DSQL ターゲットに関する注意:** 設定ファイルにコピーして貼り付けるような DSQL の
> 「エンドポイント + パスワード」はありません。ツールには DSQL の **クラスターエンドポイント** と
> AWS アイデンティティを渡します。ツールは接続ごとに短命の IAM トークンを発行します。実行に使う
> アイデンティティが、その DSQL クラスターへの接続を許可されていることを確認してください。

**CDC (任意のストリーミングパイプライン) を使う場合** — ほぼ無停止のカットオーバーに CDC を
使う場合にのみ該当します。**Full Load のみ** のマイグレーションでは、これらは一切不要です。
以下は、CDC がソースの変更ストリーム — **MySQL のバイナリログ**、または PostgreSQL ソースの場合は
レプリケーションスロット + publication 経由の **論理デコーディング WAL** — を読めるようにするための
ソース側の要件です。ツールの前提条件ゲートは CDC 開始前に各項目を検査し、何が不足しているかを
正確に知らせますが、設定自体は一度行っておくソース側の作業です。

> **マネージド RDS/Aurora は自己管理型(community)サーバーとは設定方法が異なります。** 自己管理型
> の MySQL や PostgreSQL サーバーであれば `my.cnf` / `postgresql.conf` を編集し `SET GLOBAL` を
> 実行しますが、**Amazon RDS / Aurora ではそのどちらもできません** — サーバー変数は
> **パラメータグループ** で設定し(MySQL: `binlog_format` など、**PostgreSQL:
> `rds.logical_replication`**)、binlog 保持などの MySQL 運用設定は **RDS ストアドプロシージャ**
> (`mysql.rds_*`)で変更します。PostgreSQL の有効化パラメータは **静的(static)なので再起動が
> 必要です。** 以下の手順は、このツールが対象とするマネージド(RDS/Aurora)方式です。

- **MySQL ソース — バイナリログが ROW 形式・完全な行イメージで有効になっている必要があります** —
  `log_bin=ON`、`binlog_format=ROW`、`binlog_row_image=FULL`。これは CDC の **必須要件** です
  (満たされていないとゲートが CDC を失敗扱いにします)。マネージド MySQL での設定方法:
  - **RDS for MySQL:** `log_bin` を **直接オンにすることはできず**、`my.cnf` も **編集できません**。
    代わりに **自動バックアップ** を有効にすると(バックアップ保持期間を > 0 に設定)バイナリ
    ロギングがオンになり、その後インスタンスにアタッチした **カスタム DB パラメータグループ** で
    `binlog_format=ROW` と `binlog_row_image=FULL` を設定します。(`binlog_row_image` の既定値は
    `FULL` ですが、確実にするため明示的に設定してください。)
  - **Aurora MySQL:** `binlog_format` は **クラスターレベル** のパラメータです — **カスタム DB
    *クラスター* パラメータグループ**(既定のグループは変更できません)で `ROW` に設定し、`OFF`
    から変更した場合はクラスターを **再起動** してください。既定値は `OFF` なので、この作業を
    行うまではバイナリログはオフです。
  - **Community / 自己管理型 MySQL(対比のため):** その場合は
    `log_bin`/`binlog_format`/`binlog_row_image` を `my.cnf`(またはランタイムで `SET GLOBAL`)に
    設定して再起動しますが — **RDS/Aurora にはそのいずれも当てはまりません。**
- **PostgreSQL ソース — 論理デコーディングが有効になっている必要があります** —
  **`wal_level=logical`**(**必須要件**。満たされていないとゲートが CDC を失敗扱いにします)。
  **RDS for PostgreSQL / Aurora PostgreSQL** では、カスタム DB(クラスター)パラメータグループで
  静的パラメータ **`rds.logical_replication=1`** を設定し、その後 **再起動** してください
  (Aurora: ライターを再起動)。自己管理型 PostgreSQL では `postgresql.conf` に `wal_level=logical`
  を設定して再起動します。(PostgreSQL には `binlog_format`/`binlog_row_image` に相当するものは
  ありません。)
- **レプリケーション権限を持つソースユーザー。** **MySQL:** `SELECT`、`REPLICATION CLIENT`、
  `REPLICATION SLAVE`(加えて初期スナップショットの記録管理に使う `RELOAD` と `LOCK TABLES`)。
  **PostgreSQL:** CDC ユーザーは論理レプリケーションスロットを作成・読み取りできる必要があります —
  **superuser** であるか、**REPLICATION ロール属性**(`pg_roles.rolreplication`)を持つか、
  **`rds_replication`** のメンバー(RDS/Aurora では REPLICATION 属性を直接付与できない)であれば
  通過します。スナップショットのために移行対象テーブルへの **SELECT** も必要です。(これは
  community の `REPLICATION` オブジェクト権限ではありません。)admin アカウントではなく、専用の
  最小権限 CDC ユーザーを使ってください。
- **MySQL ソース — CDC が追いつく前にログが削除されないよう、binlog 保持期間を延ばしてください。**
  RDS/Aurora は既定でバイナリログを積極的に削除します — **Aurora MySQL は 24 時間しか保持せず**、
  RDS for MySQL ではバックアップ保持期間に従います。CDC は Full Load 中にキャプチャした
  **ウォーターマーク(watermark)** から再開するため、その位置の binlog が **CDC 開始時点でまだ存在
  している必要があります** — さらに、CDC スタック(MSK + MSK Connect)のデプロイだけでも
  **約 15〜20 分** かかった後にようやくストリーミングが始まります。RDS ストアドプロシージャで余裕の
  ある期間を設定してください(単位:時間、RDS for MySQL・Aurora MySQL の両方で動作します):

  ```sql
  CALL mysql.rds_set_configuration('binlog retention hours', 168);  -- 例: 7日
  ```

  Full Load と CDC 開始の間のギャップに、想定される追いつき時間を加えた分を余裕をもってカバー
  できる期間を選んでください(7 日が安全な既定値です)。Aurora MySQL の最大値は **2160(90 日)**
  で、カットオーバー後に再び短くしても構いません。ゲートは保持期間が短すぎる(24 時間未満)場合や
  未設定の場合に **警告(WARN、非ブロッキング)** を出すので、binlog が削除される前に気づけます —
  ただし Full Load にかかる時間はユーザーしか分からないため、ブロックはしません。
- **PostgreSQL ソース — 変更すべき binlog 保持設定はありません。** 代わりに、ツールが Full Load の
  整合点で作成する **論理レプリケーションスロット** が、その LSN から必要な WAL を **自動的に固定
  (pin)** するため、設定するものはありません。スロットが開始位置を保持するので、約 15〜20 分の CDC
  スタックのプロビジョニングも問題になりません。トレードオフは逆方向のリスクです:**非アクティブ/
  未消費のスロットが WAL を固定し続け、ソースのディスクを満杯にする恐れがある** ため、WAL/スロットの
  健全性を監視してください — ツールは `wal_status` を提示し、`wal_status='lost'` はスロットが無効化
  されたことを意味します → gapless な再開が壊れる → Full Load を再実行してください。
- **MySQL ソース — GTID は推奨ですが必須ではありません。** `gtid_mode=ON` にすると、ソースの
  フェイルオーバー(failover)やレプリカ昇格の後でも CDC の再開が堅牢になります。設定していない場合、
  ツールは binlog の `file:position` ウォーターマークにフォールバックします — 動作はしますが、
  フェイルオーバーをまたぐ堅牢性は劣ります。ゲートは GTID の欠如をブロッカーとしてではなく、情報と
  して報告します。(PostgreSQL には GTID / `file:position` の概念はありません。)
- **PostgreSQL ソース — CDC はクラスターのライターに対して実行する必要があります。** ソースは
  **スタンバイではなくライター** でなければなりません(`pg_is_in_recovery()` が false であること)—
  スタンバイはレプリケーションスロットをホストできないため、CDC はライターのエンドポイントに向けて
  ください。
- **PostgreSQL ソース — レプリケーション対象の各テーブルには使用可能な REPLICA IDENTITY が必要
  です。** 主キーがあれば既定で十分で、なければ `ALTER TABLE … REPLICA IDENTITY FULL` を設定します
  (FULL またはインデックスアイデンティティでも動作)。`REPLICA IDENTITY NOTHING` のままのテーブルは
  publisher 側で UPDATE/DELETE がエラーになるため、拒否されます。レプリケーションスロット /
  `max_wal_senders`(walsender)の **余裕** は非ブロッキングの **警告(WARN)** として検査されます
  (スロットのエントリに空きがあっても、walsender プールが満杯だと新しいスロットはブロックされます)。

---

## 1.2 ツールをローカルで実行する

クローンした直後の状態から:

```bash
# ローカルの仮想環境に依存関係をインストール
uv sync

# Web UI を起動 (既定では 127.0.0.1:8080 にバインド)
uv run mysql-dsql-migrator ui
```

その後、ブラウザで **http://127.0.0.1:8080** を開きます。

任意の便利機能: `.env.example` を `.env` にコピーして接続項目を記入すると、**Connect** 画面が
これらの値をフォームに事前入力するため、セッションごとに再入力する必要がなくなります。`.env` は
git 管理から除外されており、ローカル開発専用です。

```bash
cp .env.example .env
# .env を編集: ソース DB の host/port/user、ターゲット DSQL エンドポイント、リージョンなど
```

> アプリは `reload=False` で実行されるため、コードの変更を **ホットリロードしません** — 編集を
> 反映するには再起動してください。これはツール自体を変更している場合にのみ関係します。

---

## 1.3 ツールを AWS 上で実行する (ECS Fargate)

実際のマイグレーションでは、多くのチームがツールを **シングルタスクの ECS Fargate サービス** として
Application Load Balancer の背後に(任意で Amazon Cognito OIDC 認証によるゲート付きで)デプロイし、
コンテナイメージは Amazon ECR に置きます。パラメータ化された完全な CloudFormation フローは
[`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) にあります。要点は次のとおりです。

```bash
# 1. イメージをビルド + プッシュ。ローカルに Docker がない場合は AWS CodeBuild を使用:
AWS_REGION=us-east-1 deploy/build_in_codebuild.sh      # イメージ URI を出力

# 2. app-stack (ECS Fargate + ALB + IAM) をデプロイし、そのイメージ URI と
#    VPC/サブネット/証明書/DSQL/ソースの詳細をパラメータとして渡す。
#    正確な `aws cloudformation deploy` コマンドは deploy/DEPLOYMENT.ja.md を参照。
```

**デプロイの利便性を重視した設計:** 新規の `git clone` から最小限のセットアップでデプロイできます —
コネクタプラグインのアーティファクトはコミット済みで(Java/Maven ツールチェーンは不要)、ツールは
自身の S3 バケットをプロビジョニングしてアーティファクトを自らアップロードし、任意の CDC
インフラは自動検出されます(VpcId のように本当に推論できないものだけを入力します)。

> **VPC とサブネットは、デプロイ先のアカウントが所有している必要があります。** RAM で共有
> された (クロスアカウントの) サブネットはサポートしません — CDC デプロイロールの EC2 権限は
> このアカウントのリソースにスコープされているため、共有サブネットにコネクタのネットワーク
> インターフェイスを作成しようとすると `AccessDenied` で失敗します。

> **セキュリティ上の注意:** デプロイされたアプリは **自身では一切認証を行わず**、ALB の任意の
> Cognito ゲートに依存します。`0.0.0.0/0` に全開放したインターネット向け ALB を Cognito **なし**
> のままにすると、デプロイテンプレートの `Rules`(`CognitoRequiredWhenIngressOpen`)がこれを
> ブロックします。Cognito(`EnableCognitoAuth=true`)を有効にするか、`AllowedIngressCidr` を
> 自分のネットワークに限定してください。

### 顧客向けデプロイの推奨設定

スタックはパラメータ化されているため、簡単なテストであれば近道を選ぶことも *できます* が、実際の
デプロイでは以下がより安全で耐久性の高い選択です。各項目は
[`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) の CloudFormation パラメータに対応します。

| 設定 | 推奨値 | 理由 |
|---|---|---|
| `AlbScheme` | **`internal`**(推奨) | ツールをパブリックインターネットから切り離す — VPN / Direct Connect / VPC ピアリング経由でアクセスします。`internet-facing` は `AllowedIngressCidr` を自分のネットワークに限定した場合のみ使用してください(`0.0.0.0/0` に開放した ALB は Cognito なしではブロックされます)。 |
| `EnableCognitoAuth` | **`true`**(推奨。`AllowedIngressCidr=0.0.0.0/0` の場合のみ **必須**) | アプリは自身の認証を持たないため、Cognito が唯一のゲートです。`CognitoDomainPrefix` と **`CognitoAdminEmail`** を併せて設定してください — テンプレートは 3 つをセットで要求します。ユーザープールにセルフサインアップがなく、最初のユーザーがいないと誰もログインできないアプリになるためです。 |
| `AllowedIngressCidr` | **自分のネットワークに限定**(推奨) | ALB に到達できる範囲を制限します。`0.0.0.0/0` のように全開放しないでください。 |
| `AssignPublicIp` | **`DISABLED` + NAT ゲートウェイまたは VPC エンドポイント**(本番環境で推奨) | パブリックサブネットでの `ENABLED` は NAT を省くための **テスト専用** の近道です。 |
| タスクの egress | **VPC エンドポイント**(現実的な場合は推奨) | DSQL / Secrets Manager / ECR / Logs (/ Bedrock) にパブリック経路なしでプライベートに到達します。そうでなければ NAT ゲートウェイを使用します。 |
| イメージ参照 | **イミュータブルなタグまたは digest**(推奨) | 再現可能なデプロイ — 動く `:latest` は避けてください。 |
| アクティビティログの CloudWatch ミラー | **オン**(推奨) | 永続的な監査証跡になります — タスク上の `/tmp` のコピーはタスク置き換え時に失われます。 |
| ジョブ/セッションの状態 | **S3 バックエンド** — 管理対象バケット(無損失の再開に推奨) | タスク置き換え後も残るため、進行中の Full Load が再開できます。既定の `/tmp` はタスクごとで揮発性です。 |

> *動作を最も速く確認する* 近道は **Dev/Test プロファイル**(`internal` ALB、
> `EnableCognitoAuth=false`、自己署名証明書)です — これでも本物の Fargate であり、構成要素が
> 少ないだけです。評価を超える用途には **Prod プロファイル**(Cognito + 実際のドメイン/証明書)へ
> 昇格させてください。どちらのプロファイルも `deploy/DEPLOYMENT.ja.md` にあります。

---

## 1.4 単一 EC2 ホストで実行する (ソースから)

**コンテナ/ECR や AWS Lambda を使えないアカウント**では、同じツールを **VPC 内の単一 EC2 ホスト上で
ソースのまま**実行します — ビルドや取得するイメージはありません。パラメータ化された完全な
CloudFormation フロー(`deploy/cloudformation-ec2.yaml`)は
[`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md#単一-ec2-ホストで実行-ソースからlambda-free)
にあり、要点は次のとおりです。

- ホストはソースからブートストラップします(`git clone` + `uv sync` + **systemd** サービス)—
  **Docker・ECR なし**。
- UI には **SSM ポートフォワード**(Session Manager)で接続するため、**ALB・パブリック IP・インバウンド
  ルールは不要**で、ACM 証明書や Cognito もありません。
- アプリの状態は **保持型 EBS ボリューム**(S3 ではなく)にあり、インスタンス置換を越えて保持されます。
- CDC は Kafka を**インプロセスで**シードするため、**オフセットシーダー Lambda は作成されません**
  (CDC はコネクタアーティファクトのために S3 プラグインバケットは引き続き自動プロビジョニングします)。

VPC 内のプライベートなデータ経路(ソース → EC2 → DSQL)は Fargate と同じで、構成要素はずっと少なく
なります。パラメータと SSM ポートフォワードのコマンドは
[デプロイガイド](../../../deploy/DEPLOYMENT.ja.md#単一-ec2-ホストで実行-ソースからlambda-free)を
参照してください。

---

## 1.5 ソースとターゲットへの接続

ツールを開き、**Connect** ステップから始めます。入力する項目は次のとおりです。

| 項目 | 入力内容 |
|---|---|
| **Source** | **ソースエンジン**(MySQL または PostgreSQL)を選び、host、port、user、password — **または** それらを保持する Secrets Manager シークレットの ARN/名前 — を入力します。既定のポートはエンジンに従います(MySQL は **3306**、PostgreSQL は **5432**)。PostgreSQL では接続する単一の **データベース** も指定します。両エンジンとも認証は password または Secrets Manager です(IAM トークン認証はターゲット DSQL 専用)。 |
| **Target** | Aurora DSQL の **クラスターエンドポイント**、リージョン、データベース(`postgres` に固定、読み取り専用で表示)、ユーザー名(既定は `admin`)。**パスワードなし** — ツールが IAM トークンを生成します。 |

次に、各接続を **テスト** するためにクリックします。ツールは:

- ソースを **読み取り専用** で読み、到達性と権限を確認し、
- DSQL IAM トークンを生成してターゲットへ接続できることを確認します。

**認証情報はセッションごとのプロセスメモリ内にのみ存在します。** ディスク・ログ・レポート・
ジョブ状態には一切書き込まれず、セッションが終了すると破棄されます(これは本ツールの厳格で妥協しない
ルールです)。再起動後は再入力します。

> **単一リージョン。** このツールは Aurora DSQL が利用可能などのリージョンでも動作しますが、
> **ソースとターゲットは同じリージョンになければなりません** — クロスリージョンのマイグレーションは
> **サポートされていません**。ツールもそのリージョンで実行してください。

両方の接続テストが緑色になったら、そのまま **Evaluation** へ進みます — ツールが
両方のデータベースをイントロスペクトして互換性レポートを生成します。ここからガイド付きフローは
Schema Conversion、Data Migration(ここで Full Load のみか CDC を追加するかを選んで実行)、
Validation、そして最後に **Cut over**(アプリケーションを DSQL へ切り替えるためのランブック)へと
続き、それぞれ以降の章で扱います。

---

**次へ:** [2. Evaluation と Schema Conversion →](02-evaluation-and-schema-conversion.md)
