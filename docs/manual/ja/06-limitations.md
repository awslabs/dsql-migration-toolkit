# 6. 制限事項

_言語: [English](../en/06-limitations.md) | [한국어](../ko/06-limitations.md) | **日本語**_

> **前へ:** [5. Validation](05-validation.md)

計画時に必ず考慮すべき、**実際に強制される**制限です。ほとんどは Aurora DSQL
自体に由来します(分散型であり、水平方向にスケールしない機能を意図的に除外しているため)。
一部はツールのデプロイ方法に由来します。いずれも実行時に突然現れるものではありません。
ツールが **Evaluation** でこれらをフラグ付けし、処理できるものは処理し、処理できない
ものは**明示的に**(loudly)失敗させます。

---

## 6.1 Aurora DSQL の機能制限(スキーマがこれらに適合している必要があります)

| 制限 | 影響 | ツールの動作 |
|---|---|---|
| **外部キー(強制される。ランタイム上の注意点あり)** | DSQL は **強制される** 外部キーをサポートしますが、参照される側/参照する側のテーブルへの DML は **追加の読み取り** を発生させ、並行する競合は再試行可能な `40001` になり、`CASCADE`/`SET NULL`/`SET DEFAULT` アクションは **3000 行/トランザクション** 制限に算入されます(3000 行を超えるカスケードは失敗します)。 | 保持され、post-load の `ALTER TABLE … ADD CONSTRAINT` として再作成されます(Full Load はデータ着地後に適用し、CDC マイグレーションではカットオーバーまで延期します)。子側のカーディナリティが無制限になりうる場合は `NO ACTION`/`RESTRICT` を優先するか、これらを取り除いて(`preserve_foreign_keys=False`)整合性をアプリで強制してください。 |
| **ソースの `CHECK` 制約** | ソースの `CHECK` 制約はターゲットに変換されません(例: MySQL の `CHECK`、8.0.16+)。 | DDL から削除され **MANUAL** としてフラグ付け — DSQL 互換の `CHECK` を手動で再追加するか、アプリで強制してください。(ツールが MySQL の `ENUM` や PostgreSQL の enum (どちらも `text` に変換されます) 用に生成する `CHECK … IN (...)` は影響を受けません。後からソースの型に追加された値は、その `CHECK` を置き換えるまで `23514` でデッドレターされます。[§4.2](04-cdc-and-dsql-constraints.md#cdc-中のソーススキーマドリフトの検出と対処) を参照してください。) |
| **主キー必須** | PK のないテーブルは移行できません。 | **UNSUPPORTED** としてフラグ付け。Full Load もブロックされます(keyset エクスポートには PK が必要)。 |
| **トリガー / ストアドプロシージャ / 関数 / スケジュールイベントなし** | サーバー側ロジックは移行されません。 | **UNSUPPORTED** としてフラグ付け — アプリケーションで再実装してください(イベント → EventBridge/Lambda)。 |
| **ネイティブパーティショニングなし** | DSQL がデータを自動的に分散します。 | パーティション化されたテーブルは **MANUAL** としてフラグ付け(パーティショニングを削除)。 |
| **列あたり 1 MiB の上限** | 1 つの列の値が **1 MiB** を超えると保存できません — 型に関係ありません（MySQL の `TEXT`/`BLOB`、PostgreSQL の `text`/`bytea`）: `54000 ERROR: maximum column size exceeded`。**行**全体は **2 MiB** が上限（`maximum row size exceeded`）で、1 つの書き込みトランザクションは 10 MiB です。（[Aurora DSQL のクォータ](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)） | 上限を超える値は行単位でエラーログ（Full Load）/ **DLQ**（CDC）に**隔離**（quarantine）されます。Kafka のメッセージ上限を超えうる列は**キャプチャ時に除外**する必要があります。超過値を保持しうる列は `OVERSIZED_LOB` としてフラグ付けされます: バイナリ/ドキュメント列(MySQL の `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`、PostgreSQL の `bytea`/`json`/`jsonb`)は LOSS — その型を選ぶこと自体が大きな値を格納する意図だからです。長さ制限のない PostgreSQL の `text`/`varchar` は RECOMMENDATION — それが普通の短い文字列の慣用的な書き方でもあるためです。CHECK でリテラル集合に限定された列はフラグ付けされません。 |
| **`DECIMAL` の精度 > 38** | これより高い精度は未対応です。 | Evaluation は **UNSUPPORTED**(`NUMERIC_PRECISION`)としてフラグしますが、変換された DDL を適用すると Schema Conversion が **`numeric(38,37)` にクランプ**(非可逆)し警告します — スケールも 37 に制限されます。 |
| **空間 / geometry 型** | MySQL ソースでは `bytea` に置換されます(元の WKB が Full Load・CDC の全区間でそのまま保存されます)。PostgreSQL ソースは自動置換**されません**。 | MySQL ソース: コンバーターが各列を `bytea` に自動置換し、アセッサーがそのテーブルを要レビューの **MANUAL** としてフラグ付け。PostgreSQL ソース: geometric 型(point/line/lseg/box/path/polygon/circle)は Evaluation(`PG_UNSUPPORTED_TYPE`)と Schema Conversion の両方で **UNSUPPORTED** としてフラグ付けされ、`text` への再モデリングが提案されます — DDL 適用前に自分で再モデリングする必要があります。(自動置換ではなく手動での再モデリングが必要なその他の PG 専用型: 配列、network `inet`/`cidr`/`macaddr`、`xml`、`money`、`bit`/`varbit`、`tsvector`/`tsquery`、range/multirange、composite、pgvector。) |
| **FULLTEXT / SPATIAL インデックス** | 未対応です。 | **UNSUPPORTED** としてフラグ付け。 |
| **テーブルあたり ≤ 255 列、データベースあたり ≤ 1000 テーブル** | これらを超えると未対応です。 | **UNSUPPORTED**(`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT`)としてフラグ付け。 |
| **テーブルあたり ≤ 24 インデックス**(PK も数に含まれるため、セカンダリは ≤ 23。MySQL ソースは 64) | 超過分の `CREATE INDEX ASYNC` は、Full Load が全行を書き込んだ**後に**失敗します。 | 計画段階で **MANUAL**(`TOO_MANY_INDEXES`)としてフラグ付けし、Schema Conversion にも同じ注記を表示。 |
| **主キー・インデックスの列数 ≤ 8**(MySQL ソースは 16) | これを超えるキーは error 54011 で失敗します。 | 主キーが超過する場合は **UNSUPPORTED**(`CREATE TABLE` 自体が拒否されるため、データは一切ロードされません)、インデックスが超過する場合は **MANUAL**(`TOO_MANY_KEY_COLUMNS`)。変換はロード後に必ず失敗する DDL を出力せず、そのインデックスを**省略**し、どのインデックスかを注記に明示します。 |
| **キーの合計サイズ ≤ 1 KiB**(主キーごと、インデックスごと) | DDL ではなく **`INSERT`/`UPDATE` 時の値**に対して検査されます。したがって実際のキーが長すぎる行のみが失敗します(error 54000 `key size too large`)。 | *宣言された*幅がこれを超えうる場合、変換が警告(**MANUAL** の推奨)し、MySQL ソースの utf8mb4 を想定して 1 文字 4 バイトで計算した該当キーと最悪ケースのサイズを明示します。ブロックはしません — 宣言が広くても実際の値が短ければ問題なく移行できるためです。 |
| **クラスターあたり 1 データベース** | DSQL は複数データベースではなくスキーマで構成します。 | MySQL ソースの場合、複数データベースのソースは **MANUAL** としてフラグ付け(MySQL の 1 データベースが DSQL の 1 スキーマにマップされる — スキーマに統合するか、クラスターを分割)。PostgreSQL ソースは単一のデータベースに接続し、その非システムスキーマがすでに DSQL のスキーマに直接マップされる(修飾された `schema.table`)ため、複数データベースの統合ステップはありません。 |
| **`TRUNCATE` なし、トランザクションあたり 1 DDL、楽観的並行性制御** | 従来のシングルノード RDBMS(MySQL または PostgreSQL ソース)とは異なる書き込み / DDL のセマンティクスです。 | 透過的に処理: TRUNCATE の代わりに DROP+再作成、単一 DDL 単位、必要な箇所すべてで `40001` リトライ。 |
| **IAM トークン認証(パスワードなし)、短命トークン** | 静的な DB パスワードはありません。 | ツール(および CDC シンク)が IAM トークンを自動的に発行・更新します。 |

> **スキーマ設計における結論:** 大きな blob、サーバー側ロジック、非常に高精度な数値は、
> DSQL に依存する*前に*、データベースの外(アプリケーション)へ移してください。また、
> カスケードのファンアウトは DSQL のトランザクションあたりの制限内に収めてください。
> どのオブジェクトにこの対応が必要かは、Evaluation が正確に教えてくれます。

---

## 6.2 移行プロセスの制限

- **単一リージョン — クロスリージョン移行はできません。** ツールは DSQL が
  利用可能などのリージョンでも動作しますが、**ソースとターゲットは同一リージョンに
  なければならず**、オプションの CDC パイプラインは単一のリージョン / VPC で実行され
  ます。クロスリージョンは未対応です。
- **CDC にはソース側の前提条件があります(エンジン別)。** 前提条件ゲートは、これらが
  満たされるまで **CDC をブロック**します。**Full Load のみの場合はこれらは一切不要です。**
  - **MySQL ソース:** バイナリログが **ROW 形式かつ完全な行イメージ**(full row image)で
    有効になっており(`binlog_format=ROW`、`binlog_row_image=FULL`)、**レプリケーション権限**
    を持つユーザーが必要です。また、Full Load のウォーターマーク(watermark)時点のログが
    CDC 開始時まで残るように、**binlog の保持期間を延長**する必要があります(Aurora MySQL は
    デフォルトで 24 時間しか binlog を保持しません)。RDS/Aurora ではこれらは `my.cnf` ではなく、
    パラメータグループと `mysql.rds_set_configuration` で設定します —
    [§1.1](01-setup.md#11-前提条件) を参照してください。
  - **PostgreSQL ソース:** `wal_level=logical`(RDS/Aurora: カスタム DB / クラスターパラメータ
    グループで静的パラメータ `rds.logical_replication=1` を設定してから再起動; 自己管理:
    `wal_level=logical` を設定して再起動);**レプリケーション権限を持つユーザー**(superuser、
    `REPLICATION` ロール属性、または RDS/Aurora の `rds_replication` メンバーシップ);ソースは
    standby/reader ではなくクラスターの **WRITER** でなければなりません(`pg_is_in_recovery()=false`);
    そして、複製対象の各テーブルには使用可能な **`REPLICA IDENTITY`** が必要です(PK があれば
    デフォルト、なければ `ALTER TABLE … REPLICA IDENTITY FULL` またはインデックス identity —
    `NOTHING` は拒否され、publisher で `UPDATE`/`DELETE` がエラーになります)。binlog の保持期間に
    相当する概念はありません: ツールが Full Load の整合点で**論理レプリケーションスロット**と
    **移行対象テーブルだけを正確に範囲とする publication** を作成するため、WAL は保持期間ではなく
    スロットによって固定されます(`wal_status` を監視してください — 非アクティブなスロットはソースの
    ディスクを埋め尽くす可能性があります)。レプリケーションスロット / `max_wal_senders` の余裕は
    チェックされます(ブロックはしません)。
  - **PostgreSQL ソース、リキーされたテーブル:** CDC がリキーするテーブル — Schema Conversion の
    **Composite key**、またはソースの主キー外の列を加えるように編集したターゲットキー — には、Start 時点
    だけでなく **CDC の実行中ずっと、テーブルとすべてのパーティションに `REPLICA IDENTITY FULL`** が必要
    です(後から作成したパーティションは `DEFAULT` で始まります)。FULL 未満では、そのテーブルの `DELETE`
    が Aurora DSQL 上の行を削除しません — エラーもデッドレターもなく、静かに — そしてキー列を変更する
    `UPDATE` も影響を受けます。前提条件チェックが Start をブロックします。Data Migration、Validation
    または Cut over ステップが開いている間、ツールは最大 2 分ごとにこれを読み直します: CDC モニターに赤い
    通知が表示され、再び FULL になりテーブルが CHECKSUM で一致するまで Cut over は保留されます
    ([§5.6](05-validation.md#56-リキーされた-postgresql-テーブルとカットオーバー) を参照)。アプリの再起動後
    は、再確認にソースのパスワードが再び必要です(Connect ステップで再接続)。2 回の再確認の間に下がって元に
    戻った場合は検出されないため、FULL を維持し続けてください。
- **CDC が複製するのはデータであり、DDL ではありません。** CDC 中にソースで発生した
  スキーマ変更は DSQL に**伝播されません** — 同等の DDL を DSQL に自分で適用する
  必要があります
  ([第 4 章 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-は-スキーマ-ではなく-データ-を複製する--重要) を参照)。
  その一部はデッドレターなしに CDC が書き込む値を変えるため(nullable 列の削除、型の拡張、テーブルの名前変更など)、
  CDC モニター・Validation・Cut over のステップが開いている間、ツールはキャプチャ対象テーブルの定義を最大 5 分に
  1 回読み直し、Step 1 のインベントリと異なれば Cut over を保留します
  ([§5.7](05-validation.md#57-ソーススキーマの変更とカットオーバー) を参照)。この再確認で見えないもの:
  - 2 回の読み取りの間に元に戻された変更、または CDC モニター・Validation・Cut over のページがどれも開いていない
    間の変更(次に開いたときに見えます);
  - 形が同じ `DROP TABLE` + `CREATE TABLE`(MySQL には比較できる安定したテーブル識別子がありません)、または値が
    追加された PostgreSQL の enum(`ALTER TYPE … ADD VALUE`);
  - デフォルト・コメント・照合順序・インデックス・生成列の式(比較しません);
  - ツールがそのテーブルを初めて確認した時点ですでに Step 1 のインベントリに入っていた変更(CDC 実行前の Step 1
    再実行は基準になります);
  - アプリの再起動後、Connect ステップでソースを再接続するまでのすべて(パスワードはメモリ上にしかありません);
  - 範囲の限られた読み取り(サーバー 8 秒、ソケット 10 秒)で終わらない非常に大きな MySQL 5.7 のカタログ: その場合は
    CDC モニターと Cut over が読み取りの失敗を報告します。
- **ソースの `TRUNCATE` は CDC で適用されません。** Aurora DSQL には `TRUNCATE` がないため、CDC はその
  イベントをデッドレターにし、CDC ステップにテーブル名を表示します。余分な行を削除し（Full Load + CDC は
  Drop & reload、CDC only は手動削除）、そのテーブルが CHECKSUM で一致するまで Cut over は保留されます
  （[第 4 章 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-は-スキーマ-ではなく-データ-を複製する--重要) を参照）。
  MySQL の `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` は変更イベントを一切生成せず、余分な行は
  Validation でしか確認できません。以前のバージョンのツールからアップグレードした時点ですでにストリーミング中
  だった MySQL パイプラインは、Stop CDC → Start CDC を 1 回行った後から `TRUNCATE` を検出します。
- **MySQL: CDC 開始位置と、その位置を使う Start との間のスキーマ変更。** 新しい開始位置をシードする
  MySQL の Start は Debezium を `snapshot.mode=recovery` で実行します: Full Load のウォーターマークまたは
  Manual の binlog 開始位置からの初回 Start(セッションに Full Load のウォーターマークがない MySQL の CDC
  only の初回開始には Manual の位置が必要)、**Re-run Full Load** やより新しい Manual 位置の後の Start、
  そして **Delete CDC infrastructure** と再デプロイ後の初回 Start — この場合は**元の**ウォーターマークから
  読み直します。`recovery` では、Debezium はテーブルの**現在の**定義からスキーマ履歴を再構築し、その以前の
  位置から binlog を読み、各行の値を位置の順に列へ対応付けます。そのため、その間にキャプチャ対象テーブルで
  コミットされたスキーマ変更(`ALTER TABLE` など)はストリームを壊します: 列の追加・削除は Start のたびに
  コネクタを停止させ、列名の変更もコネクタを停止させ、列数が変わらない変更(長さや DECIMAL の精度以外の型の
  変更、列順の変更、削除した列を別の列で置き換えること、文字セットの変換)は**エラーを一切出さずに**ずれた値や
  NULL を書き込みます。
  - **事前チェック。** Full Load のウォーターマークと、Manual 位置の **Fetch current position** は、位置を
    読む直前にキャプチャ対象テーブルごとの短いフィンガープリント(列の順序、型、NULL 規則、文字セット、
    主キー)を記録します。そのような Start の前に、Start CDC はテーブルをこれと比較し、変更されたテーブルを
    1 つずつ挙げて**拒否**します。チェックはソースを読むため、ソース接続が必要です(Connect ステップでソース
    接続を再テストするまで拒否します)。開始位置にフィンガープリントがない場合 — 以前のバージョンのツールが
    記録した位置、手入力した位置、定義を読めなかった位置 — は**警告**のみです。`CHAR`、`VARCHAR`、
    `VARBINARY` 列の長さの変更や、同じスケールでの `DECIMAL` の精度の変更は同じようにデコードされるため、
    拒否しません。ソーススキーマの再確認
    ([§4.2](04-cdc-and-dsql-constraints.md#デッドレターにならない変更-ソーススキーマの再確認))は引き続き
    これを報告し、Aurora DSQL の列を広げる必要がある場合があります。整数型の拡張や `ENUM` 値の追加は引き続き
    拒否されます: フィンガープリントでは、実際に誤ってデコードされる縮小や列順の変更と区別できないためです。
  - **チェックで分からないもの。** CDC がキャプチャ**しない**テーブルの列名変更(Debezium はすべての
    テーブルの定義を追跡するため、その再生で失敗します)、コネクタ作成中の変更、そしてその初回実行の
    自動再起動の直前の変更。列名の変更や列の追加・削除はコネクタを停止させ、CDC ステップが原因を示します —
    Start CDC のエラーで、または開始後に停止した場合はそのコネクタの Pipeline health 行で
    ([§4.6](04-cdc-and-dsql-constraints.md#パイプラインの健全性と-change-flow))。列数が変わらない変更は
    コネクタを停止させません: それ以前の行がエラーなしで、ずれた値や NULL で書き込まれ
    ます。ソーススキーマの再確認
    ([§4.2](04-cdc-and-dsql-constraints.md#デッドレターにならない変更-ソーススキーマの再確認))は型の変更と
    置き換えられた列を報告し、Cut over を保留します。列順の変更と文字セットの変換はどちらのチェックでも見えない
    ため、そのような変更の後は CHECKSUM で検証してください。
  - **再度の Start、Retry CDC、Debezium の「recovery mode」は決して役に立ちません** — 同じ行を再生する
    だけです。開始位置を変更の後へ移してください: CDC を Stop し、ソースのインベントリを読み直して(ステップ
    1 の Evaluation を再実行)Schema Conversion で変更されたテーブルを更新し、再ロードが新しい定義でテーブルを
    作成・読み取りするようにしてから(DDL を編集した、またはキー戦略を選んだテーブルはその編集が残るため、
    やり直すかリセットしてください。削除・名前変更された列は、先に Aurora DSQL で削除するか、そのテーブルを
    Replace で再適用する必要があります。Drop & reload は新しい定義にないターゲット列を消さないよう、その
    テーブルを拒否します)、**「Drop & reload」で Re-run Full Load** を実行します(既定の *Append* は既存の
    行を残すため、その間の UPDATE/DELETE が失われます。*Reload specific tables* と *Retry* は元の
    ウォーターマークを維持します)。新しい実行のウォーターマークは変更より後なので、Start CDC がオフセットを
    そこまで進めます。CDC only の実行には再実行する Full Load がないため、変更されたテーブルを Aurora DSQL で
    更新してから、同じスタックで **Manual**、**Fetch current position**、**Use this start point** を選んで
    ください(Start は停止中のスタックのオフセットを巻き戻しませんが、前には進めます。停止中のスタックでは、
    開始位置カードの **Manual — start from a later binlog position instead** の下にあります)。その位置より
    前にキャプチャ対象テーブルで起きた変更は複製されないため、「Drop & reload」で再ロードするか(Full load +
    CDC)Aurora DSQL 上で修正し(CDC only)、CHECKSUM で検証してください。再ロードすると CDC の開始位置が
    Automatic に戻るため、Start CDC の前に Manual を選び直してください(取得した位置は保持されます)。
  - **Stop CDC 後の再起動は別です。** 記録された位置から続行し、**記録されたスキーマ履歴を再生**するため
    (`snapshot.mode=schema_only`)、CDC 停止中の DDL はストリーミング中の変更と同じく順番に適用され、同じ
    フォローアップが必要です
    ([第 4 章 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-は-スキーマ-ではなく-データ-を複製する--重要))。
    その履歴が失われていれば Start はそう示して失敗します。コネクタがまだ残っていれば Stop CDC(または
    **Clean up leftover connector**)で先に削除してください — 残っているコネクタへの Start や Retry CDC は
    そのスナップショットモードを維持するため、同じように失敗します。その後、次の Start が同じチェックの後に
    履歴を再構築します(`recovery`)。このチェックはテーブルを元の開始位置と比較するため、Stop の前に行った
    変更でもその Start を拒否します。
- **MySQL: Debezium が解析できない DDL 文は、ソースサーバーのどこで実行されても CDC を停止させます。**
  Debezium は binary log のすべての DDL 文を自身の MySQL 文法(Debezium 2.7)で解析します — キャプチャ対象
  だけでなくサーバー上のすべてのデータベースとテーブルについて、ストアドルーチン・トリガー・ビュー・
  アカウントの文も含めて — 何かをフィルタリングする前にです。そしてツールの設定では、解析できない文を
  スキップせずに停止します。ソースコネクタはその文の直前で停止し、その位置は文を越えて進めません:
  Retry CDC も、Stop CDC 後の Start CDC も、再びその文で停止します。CDC ステップが原因を示します —
  Start CDC のエラーで、または開始後であればそのコネクタの Pipeline health 行とエラー通知で
  ([§4.6](04-cdc-and-dsql-constraints.md#パイプラインの健全性と-change-flow)) — そして引用符で囲まれた値を
  隠したうえで、その文の先頭部分を表示します。
  - **解決方法。** CDC を Stop してから、その文より後の開始位置を CDC に与えてください: すべてのテーブルに
    ついて **「Drop & reload」で Re-run Full Load** を実行する(Full load + CDC。その文がキャプチャ対象
    テーブルを変更した場合は、先にステップ 1 の Evaluation と Schema Conversion を再実行)か、**Manual** →
    **Fetch current position** → **Use this start point** を選びます。その場合、CDC が停止した位置とその
    位置の間の変更は複製されないため、それらのテーブルを「Drop & reload」で再ロードし(CDC only: Aurora
    DSQL 上で修正)、CHECKSUM で検証してください。再ロードすると開始位置が Automatic に戻るため、Start CDC
    の前に Manual を選び直してください。コネクタログの *binlog reader near position* を開始位置として
    使わないでください: それは失敗した文が始まる位置です。
  - **開始位置を移しても解決しない場合。** ソースから Debezium のスキーマを構築する Start — 初回の
    Start、新しい開始位置からの Start、失われたスキーマ履歴を再構築する Start — は、サーバー上のすべての
    テーブルの現在の定義を読みます。あるテーブルの定義自体が文法のサポートしない構文を使っていると、そのような
    Start は開始位置にかかわらず毎回その定義を読む途中で停止します: ソースでその定義を変更してから Stop CDC
    と Start CDC を行うか、そのサーバーは Full load only で移行してください。繰り返し実行される文(スキーマ
    移行ツール、スケジュールされたジョブ)は、実行されるたびに CDC を停止させます。
  - **スキップのオプションはありません。** ツールは Debezium の
    `schema.history.internal.skip.unparseable.ddl` を提供しません: スキップした文は Debezium のテーブル定義
    を黙って誤ったままにすることがあり、その後の行がエラーなしで誤った列に書き込まれるおそれがあります。
    また同じ設定は、Debezium が再生できない列名の変更(上記)もスキップし、その停止を誤った値に変えて
    しまいます。PostgreSQL ソースは影響を受けません: その変更ストリームには DDL が含まれません。
- **カスケードする外部キー動作は CDC で複製されません。** サーバー側の `ON DELETE/UPDATE CASCADE`
  (および `SET NULL`/`SET DEFAULT`)アクションはソースエンジンの*内部*で発生し、変更ストリームに
  捕捉されない場合があるため、CDC は適用できません — ソースがカスケードした子行がターゲットに
  残されます(MySQL ソースの場合、InnoDB が binlog に書き込まないまま実行します)。ツールは外部キーを
  DSQL 上で **カットオーバー時**
  に(レプリケーション中は決してしません)再作成するため、それらの孤立した行は `ADD CONSTRAINT` を
  **ブロック** し、暗黙のうちに乖離するのではなく **Validation の孤立チェック** によって報告されます。
  Evaluation はそうしたテーブルを **MANUAL** としてフラグします。自動アクションはアプリケーション内の
  明示的な子行の文で置き換え、乖離を確実に捕捉できるよう **最終カットオーバー比較の前にソースの書き込みを
  静止(quiesce)させてください**。
- **CDC はデプロイされている間は課金対象です。** ストリーミングパイプライン
  (MSK Serverless + MSK Connect、および作成した場合は NAT ゲートウェイ)は、
  実行されている間ずっとコストが発生します。Cut over 後は cdc-stack を削除して
  ください。Full Load のみを実行する場合、ストリーミングインフラはプロビジョニング
  されません。
- **ターゲット列が正確に保持できない値は、変換せずに拒否します。** 対象は次のとおりです:
  - `boolean` 列に入る 0 または 1 以外の数値(`2` を保持する MySQL の `TINYINT(1)`、または CDC で
    整数型に変更された PostgreSQL の `boolean`);
  - `numeric(p,s)` 列が保持するより多い小数桁;
  - 整数列に入る小数。

  通常は、Step 1 の後でソースの型が広げられたか、Schema Conversion でより狭い型を選んだときに起こります。
  Full Load はそのテーブルを停止します(テーブル全体の失敗)。CDC はその行をデッドレターにしてバナーを
  表示します。以前のバージョンのツールでデプロイした CDC インフラは、削除して再デプロイするまで変わった値を
  書き込み続けます。Schema Conversion で値を保持できる型を列に指定して(**Replace** で再適用)リロードするか、
  ソースデータをクリーンアップしてください。精度なしの `numeric` ターゲット(Schema Conversion が警告する
  Aurora DSQL の既定値 `numeric(18,6)`)は、設計どおり PostgreSQL の制約なし `numeric` を引き続き小数 6 桁に
  丸めます。確認しない対象: `numeric` 列に入る `FLOAT`/`DOUBLE` の値。
- **PostgreSQL 17/18 ソース — バージョン固有のエッジ 3 点（いずれも移行を止めません）。** PG 13–18
  をサポートします。17・18 では次の 3 点に注意してください:
  - **`interval` の `infinity`/`-infinity`（PG17 で新規）。** Aurora DSQL は PostgreSQL-16 互換で
    あり、その `interval` 入力パーサはこれらの値を拒否します。そのため *無限* の `interval` を持つ行は
    Full Load 中に **隔離（quarantine）** され（隔離カウントに表示）、Validation で count/checksum の
    不一致として表面化します — 暗黙のうちに変更されることはありません。有限の interval と
    `timestamp`/`timestamptz` の `infinity` は影響を受けません。その列を移行する前に、該当行を
    リモデリング（例: 有限のセンチネル値）してください。
  - **VIRTUAL 生成列（PG18 の既定の種類）。** Aurora DSQL は
    `GENERATED ALWAYS AS (expr) **STORED**` をサポートし値を自ら維持するため、STORED 列は
    **保持**されます — 再計算もドリフトもありません（書き込みでその列を省けばよく、Full Load と CDC
    シンクはそうします。DSQL は値を明示すると拒否します）。制約は **VIRTUAL** 側です: DSQL が受け付け
    ないため通常の列として作成され **MANUAL/LOSS** として表示されます。Full Load がソースの計算値を
    実体化するのでターゲットは最初は正しいものの、その後は何も維持せず、VIRTUAL 列は論理レプリケーション
    自体ができないため CDC も運びません。カットオーバー前にアプリケーションで再計算してください。
    どちらの場合も **apply の前に** 決めてください: DSQL は列から式を DROP できますが ADD はできません。
  - **CDC コネクタのカバレッジ。** 同梱の Debezium PostgreSQL コネクタのテスト対象は PG16 までです。
    `pgoutput` ストリーミングは 17/18 でも動作しますが（論理レプリケーションプロトコルは変わらず、
    ドライバは安定したワイヤプロトコルで接続）、17/18 の CDC は **ベストエフォート** として扱い、
    本番で依存する前にエンドツーエンドで検証（Full Load ウォーターマーク → スロット再開 → DSQL
    シンク → Validation チェックサム）してください。PG18 では `idle_replication_slot_timeout` が
    `0`（無効）であることも確認し、アイドル期間に CDC スロットが自動的に無効化されないようにして
    ください。

---

## 6.3 デプロイの制限(AWS ホスト形態)

- **単一タスクのコントロールプレーン。** ツールは**単一の** ECS Fargate タスクとして
  デプロイされ、1 を超えて増やすべきではありません。AWS ホスト形態のデプロイでは、
  **ジョブ / セッションの状態はどちらもマネージド S3 バケット**(`jobs/` + `sessions/`
  プレフィックス)に durable に保持されるため、タスクが置き換わっても(デプロイ、クラッシュ)
  進行を失わず**再開**します — 再開は最後に記録された状態遷移地点に戻り、進行中だった
  正確なバッチではありません。単一タスクの上限は状態の durability ではなく、**単一ライターの
  オーケストレーション**(CDC デプロイの調整とインメモリのセッション状態)によるものです。
  エフェメラルストレージ上のローカル SQLite はローカル開発時のフォールバックにすぎません。
- **組み込みのアプリ認証はありません。** アプリは ALB の**オプションの Cognito**
  ゲートに依存します。インターネットに公開するデプロイでは Cognito を有効にして
  ください — デプロイテンプレートは、Cognito なしでインターネットに公開する
  安全でない組み合わせをブロックします。
- **認証情報はセッションごと、かつメモリ内のみです。** 決して永続化されず、
  再起動するとソース / ターゲットの接続情報を再入力する必要があります。

---

**次へ:** [7. Performance and tuning →](07-performance-and-tuning.md)
