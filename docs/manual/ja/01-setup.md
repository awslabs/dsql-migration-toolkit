# 1. セットアップ

_言語: [English](../en/01-setup.md) | [한국어](../ko/01-setup.md) | **日本語**_

> **前へ:** [0. はじめる前に](00-before-you-begin.md)

この章では、「Amazon RDS または Aurora データベース(MySQL または PostgreSQL)がある」状態から
「ツールがブラウザで開いており、ソースと Aurora DSQL ターゲットの両方に接続できている」状態までを
案内します。

**最短ルート:** 評価だけなら、ツールをローカルで実行([§1.2](#12-ツールをローカルで実行する))して
接続([§1.5](#15-ソースとターゲットへの接続))するだけです。
[`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) に従って UI がすでに `AppUrl` で開いている
場合は、そのまま §1.5 へ進んでください。[§1.6](#16-cdc-のためのソース準備) は Full load + CDC を使う
場合にのみ必要です。

ツールは次の 3 つの方法のいずれかで実行します。どの方法でも接続する先は **同じ** ソースとターゲットで、
異なるのはツールの *プロセス* がどこで実行されるか、その 1 点だけです。

| 方式 | 向いている用途 | UI へのアクセス方法 | 状態の保存先 | 詳細 |
|---|---|---|---|---|
| **ローカル** | 評価、比較的小規模なマイグレーション。最も速く始められる | `http://127.0.0.1:8080` | ローカルファイル | [§1.2](#12-ツールをローカルで実行する) |
| **ECS Fargate** | 実際のマイグレーション — 多くのチームが利用する形態 | Application Load Balancer のエンドポイント(`AppUrl`)。任意で Cognito の背後 | S3 — タスク置き換え後も残る | [§1.3](#13-ツールを-aws-上で実行する-ecs-fargate) |
| **単一 EC2 ホスト**(ソースから) | コンテナ/ECR や AWS Lambda を使えないアカウント | SSM ポートフォワード — ALB・パブリック IP・インバウンドルールは不要 | 保持型 EBS ボリューム — インスタンス置換後も残る | [§1.4](#14-単一-ec2-ホストで実行する-ソースから) |

---

## 1.1 前提条件

**データベース**

- ネットワーク経由で到達できる **ソースデータベース** — **Amazon RDS または Aurora MySQL**、あるいは
  **Amazon RDS または Aurora PostgreSQL**。スキーマとデータを読み取れるユーザーがあれば十分です。
  Evaluation、Schema Conversion、Full Load、Validation には読み取り専用で十分で、MySQL ソース
  (および Full Load のみの PostgreSQL マイグレーション)には一切書き込みません。例外は CDC を使う
  場合だけです: `gtid_mode=ON` でないソースでの MySQL の **Full load + CDC** 実行は
  `FLUSH BINARY LOGS` を 1 回実行し(そのユーザーには `RELOAD` が必要です。データは書き込まれず、
  テーブルもロックされません)、**PostgreSQL CDC** は Full Load の整合点で、移行対象テーブルだけに
  厳密に限定した論理レプリケーションスロットと publication を作成し、CDC キーが変わったテーブルに
  `REPLICA IDENTITY FULL` を設定し、Full Load の後に選択したテーブルをその publication に追加します
  (AUTOCOMMIT、小さな許可リストに限定、監査記録あり)。スロットと publication は teardown(撤去)時に
  削除します。
- **サポートされるバージョン** — 両方のソース経路(Schema Conversion、Full Load、CDC、カットオーバー)を
  実インフラ上でエンドツーエンドに検証済み:
  - **RDS for MySQL** 5.7 / 8.0 / 8.4、**Aurora MySQL** 5.7(v2) / 8.0(v3) / 8.4。MySQL 5.7 は
    標準サポートが終了しています(RDS/Aurora の Extended Support が適用される場合あり)が、
    移行ソースとして完全にサポートします。
  - **RDS for PostgreSQL / Aurora PostgreSQL**、PG **13–18**(エンドツーエンドでテスト済み)。
    厳格なバージョンゲートはありません — サーバーバージョンは表示用に読むだけで、どのメジャー
    バージョンも拒否しません。
- ツールを実行するのと **同じ AWS リージョン** にあるターゲットの **Amazon Aurora DSQL** クラスター。
  DSQL にパスワードはありません。ツールにクラスターの **エンドポイント** と AWS アイデンティティ(その
  クラスターへの接続を許可されている必要があります)を渡すと、ツールが短命の IAM トークンを自分で
  発行し、それぞれを期限が切れる前に更新します。

**ローカル実行の場合**

- Python 3.10+ (`.python-version` で 3.12 に固定)と [`uv`](https://docs.astral.sh/uv/)。
- 標準の認証情報チェーン(環境変数、`~/.aws`、または名前付きプロファイル)の AWS 認証情報で、
  **Aurora DSQL IAM トークンを生成** できる(`dsql:DbConnect` / `dsql:DbConnectAdmin`)もの。任意:
  ソース認証情報が Secrets Manager にある場合は `secretsmanager:GetSecretValue`、AI アシストを有効に
  する場合は `bedrock:InvokeModel` と `bedrock:InvokeModelWithResponseStream` の両方(チャットは応答を
  ストリーミングするため、前者だけでは足りません)。

**AWS へデプロイする場合** — [`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) を
参照してください。要約は [§1.3](#13-ツールを-aws-上で実行する-ecs-fargate) にあります。

**CDC (任意) を使う場合** — Full load + CDC を使う場合にのみ該当し、**Full Load のみ** のマイグレー
ションではこれらは一切不要です。ソースにいくつかの設定(バイナリログまたは論理デコーディング、
レプリケーションユーザー、binlog 保持期間)が必要で、ツールの前提条件ゲートが CDC 開始前に検査します:
[§1.6](#16-cdc-のためのソース準備)。

---

## 1.2 ツールをローカルで実行する

クローンした直後の状態から:

```bash
uv sync                          # ローカルの仮想環境に依存関係をインストール
uv run mysql-dsql-migrator ui    # Web UI を起動 (既定では 127.0.0.1:8080 にバインド)
```

その後、ブラウザで **http://127.0.0.1:8080** を開きます。

任意: `.env.example` を `.env` にコピーして接続項目(ソース DB の host/port/user、ターゲット DSQL
エンドポイント、リージョンなど)を記入すると、**Connect** 画面がこれらをフォームに事前入力します。
`.env` は git 管理から除外されており、ローカル開発専用です。

```bash
cp .env.example .env
```

アプリは `reload=False` で実行されるため、コードの変更をホットリロードしません — 編集を反映するには
再起動してください。これはツール自体を変更している場合にのみ関係します。

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

新規の `git clone` から最小限のセットアップでデプロイできます — コネクタプラグインのアーティファクトは
コミット済みで(Java/Maven ツールチェーンは不要)、ツールは自身の S3 バケットをプロビジョニングして
アーティファクトを自らアップロードし、任意の CDC インフラは自動検出されます。VpcId のように推論
できないものだけを入力します。

> **VPC とサブネットは、デプロイ先のアカウントが所有している必要があります。** RAM で共有
> された (クロスアカウントの) サブネットはサポートしません — CDC デプロイロールの EC2 権限は
> このアカウントのリソースにスコープされているため、共有サブネットにコネクタのネットワーク
> インターフェイスを作成しようとすると `AccessDenied` で失敗します。

> **セキュリティ上の注意:** デプロイされたアプリは **自身では一切認証を行わず**、ALB の任意の
> Cognito ゲートに依存します。`0.0.0.0/0` に全開放したインターネット向け ALB を Cognito **なし**
> のままにすると、デプロイテンプレートの `Rules`(`CognitoRequiredWhenIngressOpen`)がこれを
> ブロックします。Cognito(`EnableCognitoAuth=true`)を有効にするか、`AllowedIngressCidr` を
> 自分のネットワークに限定してください。

*動作を最も速く確認する* 近道は **Dev/Test プロファイル**(`internal` ALB、`EnableCognitoAuth=false`、
自己署名証明書)です — これでも本物の Fargate であり、構成要素が少ないだけです。評価を超える用途には
**Prod プロファイル**を使用してください。すべてのパラメータは
[`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) で説明しています。主な設定は次のとおりです。

- Cognito を有効にする(`EnableCognitoAuth=true`。`CognitoDomainPrefix` と `CognitoAdminEmail` も併せて
  設定 — テンプレートは 3 つをセットで要求します)。実際のドメイン/証明書も用意します。
- `internal` ALB。`internet-facing` は `AllowedIngressCidr` を自分のネットワークに限定した場合のみ。
- `AssignPublicIp=DISABLED` + NAT ゲートウェイまたは VPC エンドポイント。
- イミュータブルなイメージタグまたは digest。
- アクティビティログの CloudWatch ミラーをオンにする(**Settings → Diagnostics**)— 永続的な監査証跡に
  なります。

状態は S3 に保存されるため、タスクが置き換えられてもワークベンチの状態と Full Load の記録は保持されます。
その時点で実行中のロードは中断され、**Retry unfinished tables** が完了していない各テーブルを最初から
ロードし直します
([§6.3](06-limitations.md#63-デプロイの制限aws-ホスト形態))。

---

## 1.4 単一 EC2 ホストで実行する (ソースから)

**コンテナ/ECR や AWS Lambda を使えないアカウント**では、ツールを **VPC 内の単一 EC2 ホスト上で
ソースのまま**実行します — ビルドや取得するイメージはありません。パラメータ化された CloudFormation
フロー(`deploy/cloudformation-ec2.yaml`)、パラメータ、SSM ポートフォワードのコマンドは
[デプロイガイド](../../../deploy/DEPLOYMENT.ja.md#単一-ec2-ホストで実行-ソースからlambda-free)に
あり、要点は次のとおりです。

- ホストはソースからブートストラップします(`git clone` + `uv sync` + **systemd** サービス)—
  **Docker・ECR なし**。
- UI には SSM ポートフォワード(Session Manager)で接続するため、ACM 証明書や Cognito もありません。
- CDC は Kafka を**インプロセスで**シードするため、**オフセットシーダー Lambda は作成されません**
  (CDC はコネクタアーティファクトのために S3 プラグインバケットは引き続き自動プロビジョニングします)。

データ経路(ソース → EC2 → DSQL)は Fargate と同様に VPC 内でプライベートに保たれ、構成要素はずっと
少なくなります。

---

## 1.5 ソースとターゲットへの接続

ツールを開き、**Connect** ステップから始めます。入力する項目は次のとおりです。

| 項目 | 入力内容 |
|---|---|
| **Source** | **ソースエンジン**(MySQL または PostgreSQL)を選び、host、port、user、password — **または** それらを保持する Secrets Manager シークレットの ARN/名前 — を入力します。既定のポートはエンジンに従います(MySQL は **3306**、PostgreSQL は **5432**)。PostgreSQL では接続する単一の **データベース** も指定します。両エンジンとも認証は password または Secrets Manager です(IAM トークン認証はターゲット DSQL 専用)。 |
| **Target** | Aurora DSQL の **クラスターエンドポイント**、リージョン、データベース(`postgres` に固定、読み取り専用で表示)、ユーザー名(既定は `admin`)。**パスワードなし** — ツールが IAM トークンを生成します。 |

次に、各接続を **テスト** します。ツールはソースを **読み取り専用** で読んで到達性と権限を確認し、
DSQL IAM トークンを生成してターゲットへ接続できることを確認します。

**認証情報はセッションごとのプロセスメモリ内にのみ存在します。** ディスク・ログ・レポート・
ジョブ状態には一切書き込まれず、セッションが終了すると破棄されます(これは本ツールの厳格で妥協しない
ルールです)。再起動後は再入力します。

> **単一リージョン。** このツールは Aurora DSQL が利用可能などのリージョンでも動作しますが、
> **ソースとターゲットは同じリージョンになければなりません** — クロスリージョンのマイグレーションは
> **サポートされていません**。ツールもそのリージョンで実行してください。

両方の接続テストが緑色になったら、**Evaluation** へ進みます — ツールが両方のデータベースを
イントロスペクトして互換性レポートを生成します。ここからガイド付きフローは Schema Conversion、
Data Migration(Full Load のみ、または CDC を追加)、Validation、そして最後に **Cut over**
(アプリケーションを DSQL へ切り替えるためのランブック)へと続き、それぞれ以降の章で扱います。
**Full load + CDC を計画している場合** は、先にソースを準備してください
([§1.6](#16-cdc-のためのソース準備)): ソースのバイナリログ(MySQL)または論理デコーディング
(PostgreSQL)の設定は、Full Load を開始する前に整っている必要があります。

---

## 1.6 CDC のためのソース準備

*Full load + CDC(ほぼ無停止のカットオーバー)を使う場合にのみ該当します。Full Load のみの
マイグレーションではこの節を飛ばして構いません。* CDC はソースの変更ストリーム — **MySQL のバイナリ
ログ**、または PostgreSQL ではレプリケーションスロット + publication 経由の **論理デコーディング WAL**
— を読むため、以下の設定を Full Load を開始する前に一度行っておいてください。ツールの前提条件ゲートは
CDC 開始前に各項目を検査し、何が不足しているかを正確に知らせます。

**RDS/Aurora では `my.cnf` / `postgresql.conf` の編集も `SET GLOBAL` の実行もできません。**
サーバー変数は **パラメータグループ** で設定し(PostgreSQL の `rds.logical_replication` は静的
(static)なので **再起動が必要です**)、binlog 保持などの MySQL 運用設定は **RDS ストアドプロシージャ**
(`mysql.rds_*`)で変更します。以下の表はこのマネージド方式です。

### MySQL ソース

| 設定 | 値 | 設定方法 |
|---|---|---|
| バイナリログ(`log_bin`) | `ON` | **RDS for MySQL:** 直接オンにできません — **自動バックアップ** を有効にします(バックアップ保持期間 > 0)。**Aurora MySQL:** `binlog_format` を設定するまではオフです。 |
| `binlog_format` | `ROW` | **RDS:** インスタンスにアタッチしたカスタム DB パラメータグループ。**Aurora:** **クラスターレベル** のパラメータ — カスタム DB *クラスター* パラメータグループ(既定のグループは変更できません)で設定し、`OFF`(既定値)だった場合はクラスターを **再起動** します。 |
| `binlog_row_image` | `FULL` | 同じパラメータグループ。既定値ですが、明示的に設定してください。 |
| `binlog_row_value_options`(MySQL 8.0 以降) | 空 | 同じパラメータグループ。`PARTIAL_JSON` は JSON の更新を CDC が適用できない差分として記録します。空にした後はソースを再起動するか、アプリケーションの接続プールをリサイクルしてください — 変更前に開いた接続は差分を記録し続けます。 |
| binlog 保持期間 | 安全な既定値は **168 時間(7 日)** | RDS for MySQL・Aurora MySQL の両方で `CALL mysql.rds_set_configuration('binlog retention hours', 168);`。Aurora MySQL の最大値は **2160** 時間(90 日)で、カットオーバー後に再び短くしても構いません。 |
| レプリケーションユーザー | `SELECT`、`REPLICATION CLIENT`、`REPLICATION SLAVE`、`RELOAD`、`LOCK TABLES`。MySQL 8.0.19 以降でアプリケーションが XA トランザクションを使う場合は `XA_RECOVER_ADMIN` も | admin アカウントではなく、専用の最小権限ユーザーを使ってください。`RELOAD` と `LOCK TABLES` は初期スナップショットの記録管理用です。 |
| `gtid_mode` | `ON` — 推奨(必須ではありません) | パラメータグループ。Full Load の前に決め、カットオーバーまで変えないでください。 |

最初の 4 行は **必須要件** です — 満たされていないとゲートが CDC を失敗扱いにします。

- **binlog 保持期間。** RDS/Aurora はバイナリログを積極的に削除します — **Aurora MySQL は 24 時間しか
  保持しません**。CDC は Full Load 中にキャプチャした **ウォーターマーク(watermark)** から再開する
  ため、その位置の binlog が **CDC 開始時点でまだ存在している必要があり**、CDC スタック(MSK + MSK
  Connect)のデプロイだけでも **約 15〜20 分** かかった後にようやくストリーミングが始まります。
  Full Load と CDC 開始の間のギャップに追いつき時間を加えた分をカバーできる期間を選んでください。
  ゲートは保持期間が未設定または 24 時間未満の場合に **警告(WARN)** を出すだけで、ブロックはしません。
  Full Load にかかる時間はユーザーしか分からないためです。
- **GTID。** `gtid_mode=ON` では CDC が GTID で位置を決めるため、バイナリログファイルの異なる
  レプリカへのフェイルオーバー(failover)の後でも続けられます。`OFF`、`OFF_PERMISSIVE`、
  `ON_PERMISSIVE` では binlog の `file:position` を使います。`gtid_mode` が `ON` でないときに記録された
  開始位置では — また `ON` でないときに最後に開始した CDC スタックでも — `ON` になった後に CDC を
  開始できません([§6.2](06-limitations.md#62-移行プロセスの制限))。
- **GTID がない場合**、Full load + CDC の実行は `file:position` の開始位置を `FLUSH BINARY LOGS` で
  正確にし、これには `RELOAD` が必要です(データは書き込まれず、テーブルもロックされません。binlog は
  新しいファイルに移ります)。enhanced binlog を使う Aurora MySQL ではツールはこれに依存しないため、
  そのような実行には `gtid_mode=ON` が必要です(または enhanced binlog を無効にします。どちらも
  ライターの再起動が必要です)
  ([§3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し))。

### PostgreSQL ソース

| 設定 | 値 | 設定方法 |
|---|---|---|
| 論理デコーディング(`wal_level`) | `logical` — **必須要件**(満たされていないとゲートが CDC を失敗扱いにします) | **RDS for PostgreSQL / Aurora PostgreSQL** では、カスタム DB(クラスター)パラメータグループで静的パラメータ **`rds.logical_replication=1`** を設定し、その後 **再起動** してください(Aurora: ライターを再起動)。 |
| エンドポイント | スタンバイではなくクラスターの **ライター**(`pg_is_in_recovery()` が false であること) | スタンバイはレプリケーションスロットをホストできません。 |
| レプリケーションユーザー | 論理レプリケーションスロットを作成・読み取りできること — **superuser**、**REPLICATION ロール属性**(`pg_roles.rolreplication`)を持つロール、または **`rds_replication`** のメンバー — に加え、スナップショットのために移行対象テーブルへの **SELECT** | admin アカウントではなく、専用の最小権限ユーザーを使ってください。RDS/Aurora では REPLICATION 属性を直接付与できません。(これは community の `REPLICATION` オブジェクト権限ではありません。) |
| レプリケーション対象の各テーブルの `REPLICA IDENTITY`(パーティション化されたテーブルではすべてのパーティション) | `DEFAULT` は `DEFERRABLE` でない有効な主キー、`USING INDEX` は有効な一意インデックス、`FULL`(`ALTER TABLE … REPLICA IDENTITY FULL`)はどのテーブルでも動作しますが、PostgreSQL 18 で生成列を持つテーブルは例外 | ない場合、テーブルが publication に含まれた後はそのテーブルのすべての UPDATE/DELETE がソースで失敗するため、前提条件チェックが失敗と判定し、Full Load はソースで何も変更する前に停止します。 |
| レプリケーションスロット / `max_wal_senders` の余裕 | 空きスロットと walsender | 非ブロッキングの **警告(WARN)**: スロットのエントリに空きがあっても、walsender プールが満杯だと新しいスロットはブロックされます。 |

- **変更すべき保持設定はありません。** ツールが Full Load の整合点で作成する
  **論理レプリケーションスロット** が、その LSN から必要な WAL を **固定(pin)** するため、約 15〜20 分
  の CDC スタックのプロビジョニングも問題になりません。リスクは逆方向にあります:**非アクティブ/
  未消費のスロットが WAL を固定し続け、ソースのディスクを満杯にする恐れがある** ため、WAL/スロットの
  健全性を監視してください。ツールは `wal_status` を提示し、`wal_status='lost'` はスロットが無効化
  されたことを意味します → gapless な再開が壊れる → Start CDC がブロックして解決方法を示します
  ([§4.3](04-cdc-and-dsql-constraints.md)): Full load + CDC では **Drop & reload** で Full Load を
  再実行します。スタックのコネクタを以前に開始していた場合は、その前に Delete CDC infrastructure して
  再デプロイします。

---

**次へ:** [2. Evaluation と Schema Conversion →](02-evaluation-and-schema-conversion.md)
