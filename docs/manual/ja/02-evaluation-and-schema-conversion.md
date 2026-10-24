# 2. Evaluation と Schema Conversion

_言語: [English](../en/02-evaluation-and-schema-conversion.md) | [한국어](../ko/02-evaluation-and-schema-conversion.md) | **日本語**_

> **前へ:** [1. Set up](01-setup.md)

接続とデータ移行の間には、マイグレーションを試行錯誤ではなく **決定論的で予測可能** にする 2 つの
ステップがあります。

```
Connect → [ 1. Evaluation → 2. Schema Conversion ] → 3. Data Migration → 4. Validation → 5. Cut over
```

- **Evaluation** は *「自分のソースデータベース(MySQL または PostgreSQL)の中で何が DSQL に移り、
  何が移らないのか、そしてそれぞれの作業量はどれくらいか?」* に答えます。
- **Schema Conversion** はソース DDL を DSQL 互換の DDL に変換し、ターゲットに適用します。

ソースエンジンが何であれ(Aurora MySQL または Aurora PostgreSQL)、**この段階は DSQL が受け入れない
ものを — たった 1 行も移す前に — 知る地点です。** スキップしないでください。

---

## 2.1 Evaluation — 互換性の評価

**Evaluation** を実行すると、ツールが **ソース**(MySQL ソースの場合: テーブル、カラム、型、主キー、
インデックス、外部キー、ビュー、トリガー、ルーチン、`AUTO_INCREMENT`、charset/collation。PostgreSQL
ソースの場合: **すべての非システムスキーマ** にわたる類似オブジェクト — スキーマ修飾されたテーブル名、
生成/identity カラムとシーケンス、`REPLICA IDENTITY` など。システムスキーマ
`pg_catalog`/`information_schema`/`pg_toast` は除外)**と** **ターゲット** の両方を
イントロスペクトし、**互換性評価レポート** を生成します。

読み取りは読み取り専用・autocommit で行われ、他のセッションが保持するロック (実行中の `ALTER TABLE`) は最大 5 秒しか待たず、テーブルごとに進捗を報告し、**Cancel evaluation** で取り消せます。再実行すると、成功するまで前回のレポートが残ります。MySQL の移行ユーザーは、ビュー定義を読むには `SHOW VIEW`、トリガー・イベント・ルーチンを一覧するには `TRIGGER`、`EVENT`、`SHOW_ROUTINE` (または `*.*` に対する `SELECT`) が必要です。権限がない場合、レポートはそれらを省略せず、一覧できなかったものを示します (`SOURCE_VISIBILITY`)。

PostgreSQL ソースでは、Evaluation はインベントリにない 2 つのものも読み取ります。

- **行レベルセキュリティ (row-level security)。** Aurora DSQL にはこの機能がないため、行レベルセキュリティが有効なテーブルは **UNSUPPORTED** (`PG_ROW_LEVEL_SECURITY`) と判定され、アプリケーションでルールを作り直せるよう各ポリシーの `USING` / `WITH CHECK` の式が示されます。この検出結果は、ソースユーザーがすべての行を読めるかどうかも示します: スーパーユーザー、`BYPASSRLS` を持つロール、またはテーブルが `FORCE ROW LEVEL SECURITY` でない場合のテーブル所有者です。すべての PostgreSQL ソースのセッションは `pg_dump` と同様に `row_security = off` で実行されるため、それ以外のユーザーでは、Full Load と Validation はポリシーが見せる行だけをコピー・比較するのではなく、そのテーブルでエラーとして停止します。Data Migration の前提条件チェックがロード前にこれを示します (`ROW_SECURITY`)。
- **スタンドアロンのシーケンス** (どの列にも所有されていないシーケンス)。Schema Conversion はこれを作成しないため、各シーケンスは、ソースの現在の値の次から続く Aurora DSQL の `CREATE SEQUENCE` 文を持つ **MANUAL** 項目 (`PG_STANDALONE_SEQUENCE`) になります: `CACHE 1`、またはソースがすでにキャッシュしている場合は `CACHE 65536` (Aurora DSQL が受け付けるのはこの 2 つだけ) で、ソースの範囲が bigint の既定値と異なる場合はその範囲を保ちます。カットオーバー前に作成し、ソースへの書き込みが止まったら `setval` でソースの最終値の次へ進めてください: ツールはこれを作成も同期もせず、Cut over ステップがそれぞれを実行する文とともに一覧します ([§10.3](10-conclusion.md#103-カットオーバー-アプリケーションを-dsql-へ切り替える))。

どちらかの読み取りに失敗した場合、レポートは存在しないかのように見せず、そのことを示します (`PG_CATALOG_UNREAD`)。

Evaluation はソースの **サーバー設定** も 1 回だけ読み取ります (設定のみで、テーブルはスキャンしません)。読み取った設定は、レポートのソースオブジェクト数の下 (**Source server settings**、折りたたみ表示) に一覧されます。これらは、テーブル定義には現れない、アプリケーションが動作する条件を示します。

- **MySQL:** `sql_mode`、`time_zone`、`system_time_zone`、`explicit_defaults_for_timestamp`、`auto_increment_increment`、`auto_increment_offset`、`lower_case_table_names`、`character_set_server`、`collation_server`、`default_collation_for_utf8mb4`、`div_precision_increment`、`group_concat_max_len` の GLOBAL 値です。アプリケーションのセッションはこのグローバル値から始まり、ツール自身のセッションは独自の値を設定するためです (下記)。4 つはそれぞれデータベースレベルの **MANUAL** 項目として判定されます。`SQL_MODE`: モードに `ONLY_FULL_GROUP_BY` がない (ビューやクエリが `GROUP BY` で決まらない列を選択でき、Aurora DSQL はこれを `42803` で拒否します。ビューの場合は Schema Conversion が適用するときです)、または `ANSI_QUOTES` / `PIPES_AS_CONCAT` がある (Query Converter に貼り付けるアプリケーションの SQL はそのモード向けに書かれていますが、変換は SQL を MySQL の既定のモードと同じように読み取ります) 場合です。`ZERO_DATES`: 日付列を持つテーブルがあるのにモードがゼロや不正な日付の保存を許す (`NO_ZERO_DATE` または `NO_ZERO_IN_DATE` がない、strict モードがない、または `ALLOW_INVALID_DATES`)、または列の `DEFAULT` がゼロの日付である場合です。Aurora DSQL にはそのような値がないため、項目は `DATE`・`DATETIME`・`TIMESTAMP` 列を一覧し、Full Load と CDC がそのような値を何として書き込むかを示し ([§6.2](06-limitations.md#62-移行プロセスの制限))、それを持つ行を数えるクエリをテーブルごとに示します。Evaluation は行を読み取りません。`EXPLICIT_DEFAULTS_FOR_TIMESTAMP`: 設定が `OFF` の場合、MySQL は `NOT NULL` と宣言された `TIMESTAMP` 列に NULL が書き込まれると現在時刻を保存しますが、Aurora DSQL はその書き込みを拒否します。項目はそれらの列を一覧します。`AUTO_INCREMENT_STEP`: `auto_increment_increment` が 1 より大きく、`AUTO_INCREMENT` キーを持つテーブルがある場合です。Aurora DSQL ではそのキーの identity は 1 ずつ増えます。タイムゾーンも PostgreSQL と同様に判定されます (下記)。その他の設定 (名前の大文字小文字の扱い、文字セットと照合順序の既定値、`div_precision_increment`、`group_concat_max_len`) は記録のみで、判定しません。
- **PostgreSQL:** 移行ユーザーの新しいセッションが接続先データベースで受け取る `TimeZone` です (DB または DB クラスターパラメータグループの値。`ALTER DATABASE` や `ALTER ROLE` の既定値があればその値)。ツールのセッションは UTC に固定されるため、専用の短い接続で読み取ります。あわせて、`ALTER DATABASE` や `ALTER ROLE` がそのデータベースに設定した各 `TimeZone` の既定値も読み取ります (アプリケーションが接続するロールは独自の値を持つことがあります)。タイムゾーンは判定されます (下記)。

**ソースのタイムゾーン** (`SOURCE_TIME_ZONE`)。時計関数 — MySQL の `NOW()`、`CURRENT_TIMESTAMP`、`CURDATE()`、および `timestamp without time zone` や `date` 列に書き込まれる PostgreSQL の `now()`、`LOCALTIMESTAMP`、`CURRENT_DATE` — は セッションのタイムゾーンの壁時計を返し、`DATETIME` / `timestamp` 列はその壁時計をそのまま格納します。Full Load と CDC は、その値がどのタイムゾーンで書かれたものでも変えずにコピーします。Aurora DSQL のセッションは、別のタイムゾーンを設定しない限り UTC で動作します。そのため Evaluation が読み取ったタイムゾーン — MySQL の `time_zone` (`SYSTEM` はサーバー自身のタイムゾーンで `system_time_zone`)、PostgreSQL の `TimeZone` — が UTC でない場合、Schema Conversion は時計を読む箇所にそのタイムゾーンを書き込みます: `DATETIME` (`timestamp without time zone`) のデフォルトは `DEFAULT (now() AT TIME ZONE 'Asia/Seoul')` になり、`DATE` や `TIME` のデフォルトはその日付や時刻になり、MySQL ビューの `NOW()`、`SYSDATE()`、`CURDATE()`、`CURTIME()` はそのタイムゾーンの時計を読みます。MySQL がセッションのタイムゾーンで読むその他の値も MySQL ビューはそのタイムゾーンで読み — `FROM_UNIXTIME()`、および `TIMESTAMP` 列の日付・時刻・テキスト (`DATE()`、`HOUR()`、`DATE_FORMAT()` ...) — そのタイムゾーンの時計を `TIMESTAMP` 列と比較するときは、そのタイムゾーンで指す時点として比較するため (両方を含む `UNION` の列は、MySQL の `DATETIME` と同じくそのタイムゾーンの壁時計です)、`ts > NOW() - INTERVAL 1 DAY` は Aurora DSQL のセッションがどのタイムゾーンで動作していても MySQL と同じ行を返します。`TIMESTAMP` を `DATETIME` 列や日付文字列と比較する場合は、引き続きセッションのタイムゾーンで比較されます。`+09:00` のような固定オフセットは、UTC の時計にオフセットを足した `(now() AT TIME ZONE 'UTC') + INTERVAL '+09:00'` として書きます: タイムゾーン名として書くと `'+09:00'` は POSIX の符号で読まれ、UTC より 9 時間西になります。`TIMESTAMP` (`timestamptz`) 列は時点 (instant) を保持するため `now()` のままで、`UTC_TIMESTAMP()` / `UTC_DATE()` は UTC のままです。PostgreSQL のビューは PostgreSQL が deparse したとおりに引き継ぐため、項目は時計を読むビューを確認できるよう一覧します。項目 (**MANUAL**) は該当する列とビュー、そしてアプリケーションがすべきことを示します: アプリケーションがセッションのタイムゾーンに依存している場合 — そのような列に `NOW()` や `CURRENT_DATE` を書き込む、またはそのような列と比較する場合 — Aurora DSQL への新しい接続ごとに `SET TIME ZONE 'Asia/Seoul'` を実行する (Aurora DSQL で許可されています) か、SQL にタイムゾーンを明示します。判別できないタイムゾーン — `KST` のような略称でしか報告されない `SYSTEM`、夏時間の規則を持つ PostgreSQL の `TimeZone`、`ALTER DATABASE` / `ALTER ROLE` の既定値は読み取れたものの短い接続では読み取れなかった `TimeZone` — はアドバイザリー項目でその旨を伝え、変換は UTC のソースと同じく UTC を前提とします。何も読み取れなかった場合は項目は表示されず、その場合も変換は UTC を前提とします。

どちらの読み取りも、アプリケーションが自分で設定する値は見えません: セッションの `SET` (`SET sql_mode`、`SET time_zone`、`SET TIME ZONE`)、またはドライバーや接続文字列が接続時に送るオプションです。読み取りに失敗した場合は一覧が空になるだけで、Evaluation は失敗しません。

ツール自身の MySQL ソースのセッションは独自の `sql_mode` で読み取ります: 各セッションは接続時に `SET time_zone = '+00:00', sql_mode = 'ALLOW_INVALID_DATES'` を実行します。これはセッションの設定であり、ソースには何も書き込みません。これにより、パラメータグループの `sql_mode` がツールの読み取る内容を変えることはありません: たとえば `ANSI_QUOTES` では、`SHOW CREATE` がビュー本体の識別子を二重引用符で出力し、変換はそれを文字列として読んでしまいます。`ALLOW_INVALID_DATES` は SQL の解釈を変えません。ツールの読み取りが、その月にない日として保存された日付 (`2024-02-31`) を指定できるようにするだけです。MySQL 8.0 以降はこの設定がないとそうした日付を拒否するため (エラー `1525`)、主キーにそうした日付を持つテーブルもページ単位で読み取れます。

### すべてのオブジェクトが 1 つの分類を受け取る

| 分類 | 意味 | 例 |
|---|---|---|
| **AUTO** | 自動変換され、人手の作業は不要。 | マッピング可能な型と PK を持つ通常のテーブル/カラム。**外部キー**(自動変換され、`RECOMMENDED` としてフラグ付け — 下記参照)。複数データベースの MySQL ソース(各データベースが独自のスキーマになり、`RECOMMENDED` としてフラグ付け)。同じスキーマの別のテーブル・ビュー・インデックスも使うインデックス名(インデックスは自動的に名前が変更され、`RECOMMENDED` としてフラグ付け)。 |
| **MANUAL** | 変換はされるが、判断またはアプリ側の変更が必要。 | 大文字小文字を区別しない collation、FULLTEXT/SPATIAL インデックス(出力されない: その検索はデータベースの外へ)、過大な LOB/JSON カラム、`ENUM`/`SET`、生成カラム、`ON UPDATE` タイムスタンプ、空間/geometry 型、`CASCADE`/`SET NULL` アクションを持つ外部キー(CDC が複製できません)、`bytea` カラム、または Aurora DSQL がインデックスを作れない PostgreSQL の `json`/`jsonb`/`interval`/`time with time zone` カラム上のセカンダリインデックス(出力されず、UNIQUE インデックスは一意性を失います)、PostgreSQL の btree 以外のインデックス(btree として作成)と、述語が型の変わるカラムを読む部分インデックス(それ以外の部分インデックスは `WHERE` を保持)、PostgreSQL の `EXCLUDE` 制約(作成されず、それを強制していたインデックスも作成されません: `PG_EXCLUSION_CONSTRAINT`)と、`WITHOUT OVERLAPS` で宣言された PostgreSQL 18 のキー(通常のキーとして作成: `PG_TEMPORAL_KEY`)、`DEFERRABLE` で宣言された PostgreSQL の主キーまたは `UNIQUE` 制約(Aurora DSQL が即時に検査する通常のキーとして作成: `PG_DEFERRABLE_KEY`)、精度を宣言していない PostgreSQL の `numeric`(`numeric(18,6)` として格納)、ソースが定義を返さなかったビュー、ソースユーザーが一覧できないオブジェクト(`SOURCE_VISIBILITY`)、どの列にも所有されていない PostgreSQL のシーケンス(`PG_STANDALONE_SEQUENCE`)。 |
| **UNSUPPORTED** | 自動変換不可 — 再設計が必要。 | トリガー、ストアドプロシージャ/関数、スケジュールイベント、PK のないテーブル、`bytea` カラム(`BINARY`/`VARBINARY`/`BLOB`、PostgreSQL の `bytea`)または PostgreSQL の `json`/`jsonb`/`interval`/`time with time zone` カラムの主キー、63 バイトに切り詰めると衝突する名前(1 つのテーブルの 2 つのカラム、または 1 つのスキーマの 2 つのテーブル/ビュー)、`DECIMAL` 精度 > 1000、テーブルあたりカラム > 255、ターゲットデータベースのテーブル 1,000 個・スキーマ 10 個・ビュー 5,000 個超(クラスターに既にあるものを含む)、PostgreSQL テーブルの行レベルセキュリティ(`PG_ROW_LEVEL_SECURITY`)。 |

上記の例は主に MySQL ソースを基準にしています。**PostgreSQL ソース** でも同じ尺度が適用されます: サポートされ
ない PG カラム型 — 配列(`[]`)、geometric(`point`/`line`/`box`/…)、network(`inet`/`cidr`/`macaddr`/`macaddr8`)、
`xml`、`money`、`bit`/`bit varying`/`varbit`、`tsvector`/`tsquery`、range + multirange、`pgvector`、`enum`、
composite — が黙って移行されることはありません: 値を保持する代替型がある場合(配列は `jsonb`、range・network・
`xml`・geometric・bit・テキスト検索型は `text`、`money` は `numeric(23,4)`、enum は `CHECK` 付きの `text`)は
Schema Conversion が警告付きで出力し、テーブルは **MANUAL**、代替型のない型(composite、domain、`pgvector`)は
**UNSUPPORTED** です。構造上の
ゲート(PK なし、`numeric` 精度 > 1000、キー列 > 8 個 など)も PostgreSQL ソースに等しく適用されます。

分類されないオブジェクトはありません — どのルールにも一致しないオブジェクトはデフォルトで **AUTO** に
なります。1 つのオブジェクトに複数のルールが一致した場合は、**最も厳しい** 分類が優先され
(`UNSUPPORTED` > `MANUAL` > `AUTO`)、一致したすべてのルールの理由・推奨事項が統合されるため、
どの検出結果も隠れることはありません。

### ギャップ(gap)と推奨事項(recommendation)

すべての検出結果が問題であるわけではありません。検出結果は次の 2 種類のいずれかであり、レポートは
どちらであるかを示します。

- **ギャップ** — 移行できなかった、または意味が変わったもの(失われた collation、再現できない
  `ON UPDATE` タイムスタンプなど)。どう対処するかを判断する必要があります。
- **推奨事項**(`RECOMMENDED`、info-blue) — 変換は完全かつ正確です。DSQL で *うまく動かす* ための
  助言であり、無視した場合に失われるのは性能であって正確性ではありません。

代表的な推奨事項は生成されるキー — MySQL の `AUTO_INCREMENT`、または PostgreSQL の `serial`/`IDENTITY`
キー — で、Schema Conversion は既定でこのキーが値を生成し続けるようにします: 単一カラムの整数キーはキャッシュ付き
identity (`bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)`。Aurora DSQL は identity を `bigint` にしか
許可しません) になります。Full Load はソースの id をそのまま書き込み、シーケンスは投入後とカットオーバー時にその
値の次へ進められます(後戻りはしません)。値には欠番が生じ、順序は緩くなります。identity が再現しない MySQL の
挿入の慣用句が 3 つあります: キーへの明示的な `NULL` や `0`、`LAST_INSERT_ID()` (`RETURNING` を使用)、
アプリケーションが自分で指定した id (シーケンスを進めません)。ターゲットで何も生成しないキーは推奨事項ではなく
**ギャップ** で、工数に算入されます: MySQL の `bigint unsigned` キー (`bigint` より広く既定で維持)、複合主キーに
含まれるキーカラム、整数でないキーは通常の値のままで、アプリケーションが挿入のたびに値を指定する必要があります。
それ以外に、UUID/ランダムキーは **挿入スループット** を得るための選択です —
DSQL は行を主キー順に格納するため、単調増加キーは書き込みを 1 つのパーティションに集中させます
([第 7 章 §7.1](07-performance-and-tuning.md#プライマリキー戦略--ホットパーティションの回避) を参照)。

**外部キー** はもう 1 つの advisory な検出結果です。Aurora DSQL は外部キーを **強制** するため、各外部キーは
クリーンに変換されて保持されます — ギャップではなく、`RECOMMENDED` としてフラグが立ち、**必要な作業は
ありません**。注記には検討に値するランタイム上の注意点が添えられます: 参照される側/参照する側のテーブルへの
DML は **追加の読み取り** を発生させます(AWS は追加前にベンチマークすることを推奨しています)。並行する
競合は再試行可能なシリアライゼーションエラー(`SQLSTATE 40001`)になります。そして
`CASCADE`/`SET NULL`/`SET DEFAULT` アクションは DSQL の 3000 行/トランザクション制限に算入されます
(3000 行を超えるカスケードは失敗します) — そのため、子側のカーディナリティが無制限になりうる場合は
`NO ACTION`/`RESTRICT` を優先してください。カスケードアクションは CDC を越えて存続することもできません
([第 6 章 §6.2](06-limitations.md#62-移行プロセスの制限) を参照)。これが Evaluation が今なお **MANUAL** として
フラグを立てる唯一のケースです。

PostgreSQL の外部キーは `MATCH FULL` と `DEFERRABLE` / `INITIALLY DEFERRED` を保持します(Aurora DSQL は
どちらも受け付け、遅延されたキーをソースと同じくコミット時に検査します)。検出結果には、それを宣言している
キーが 1 つずつ示されます。`NOT ENFORCED` で宣言された PostgreSQL 18 のキーは作成されません: ソースはその
キーを検査しませんが、`NOT ENFORCED` を持たない Aurora DSQL はソースが受け付ける書き込みを拒否してしまう
ためです。これも推奨事項です(`PG_FK_NOT_ENFORCED`、`RECOMMENDED`)。そのようなキーを Aurora DSQL に強制
させるには、そのキーの `ALTER TABLE … ADD CONSTRAINT … NOT VALID` をテーブルのターゲットスクリプトに追加
してください。一方、`DEFERRABLE` で宣言された主キーまたは `UNIQUE` 制約はギャップです
(`PG_DEFERRABLE_KEY`、**MANUAL**): Aurora DSQL は遅延可能な一意キーをサポートしないため、即時に検査される
通常のキーとして作成されます([§2.3](#23-mysql--dsql-の型と制約の処理-リファレンス) を参照)。

推奨事項は任意であるため、**オブジェクトの推定工数(effort)には算入されません** — そうしないと、
値を生成し続けるキーを持つほぼすべてのテーブルが、そのキーだけを理由に `MEDIUM` に膨らんでしまいます。
助言を採用した場合のコストは項目自体に表示される("effort if you take it")ため、判断に必要な情報は
保たれます。

> **Identity キーと投入済みの行。** キーがキャッシュ付き identity (`GENERATED BY DEFAULT AS IDENTITY`)
> であるとき — `bigint` に収まる MySQL の `AUTO_INCREMENT` または PostgreSQL の `serial`/`IDENTITY` キーの
> 既定 — Full Load 完了時にツールがそのシーケンスを
> 投入済みの行より先へ進め、アクティビティログに記録します(`identity sequences synced … RESTART WITH
> <max(pk)+1>`)。
>
> これは付随的な処理ではありません。`BY DEFAULT` はロードがソース自身のキー値を書き込めるようにするもので
> すが、**明示的に指定された値はシーケンスを進めません。** そのためこの手順がないとシーケンスは開始位置に
> 留まったまま、その値はすでに使われている状態になり、**カットオーバー後のアプリケーションの最初の
> INSERT** が重複キーで失敗します。その状態でも行数とチェックサムは一致するため **Validation はクリーンに
> 通過し**、ソースを停止させた後になって初めて問題が現れます。
>
> **完全に終了した**ロードの後にのみ実行されます: `MAX(pk)` がシーケンスが超えるべき値であるため、部分的な
> ロードで同期すると、まだ到着していない行との衝突が残る可能性があります。ロードを完了させる再試行は同期を
> 実行します。プレーンな整数キー(Keep source PK、複合キー、`bigint unsigned` キー)のテーブルにはシーケンスがない
> ため、変更されません。シーケンスはソース自身のカウンターの先へも進められるため、ソースが発行して削除した id が
> 再び発行されることはありません。


### 各レポート項目が伝えること

評価されたオブジェクトごとに、次の内容が提供されます。

- その **分類**(AUTO / MANUAL / UNSUPPORTED)、
- **リスクの説明** — *なぜ* フラグが立ったのか(例:「Aurora DSQL には `ON UPDATE CURRENT_TIMESTAMP` 句がありません」)、
- **推奨される対応** — *何をすべきか*(例:「更新時のタイムスタンプをアプリケーションで明示的に設定します」)、そして
- 自動でない項目については、**作業量の見積もり**(SCT スタイルの区分、例:simple / medium /
  significant)。これにより作業規模を把握できます。

レポートはこれらを **サマリー** にも集計します。分類ごと・作業量レベルごとの件数 —
スキーマがどれだけマイグレーションしやすいかを一目で把握できる図です。

画面の一覧は、オブジェクトを種類ごとに、重要度の高い順に表示します。対応が必要な種類は開いた状態で
表示され、種類ごとに 50 件ずつ、ページ切り替えとともに表示されます。オブジェクトのリスクと推奨される
対応は、その行を展開すると表示されます。**Classification**・**Estimated manual effort** フィルターと
検索ボックスで一覧を絞り込めます。検索ボックスはオブジェクト名または指摘のルールに一致します(`BYTEA_KEY` と入力
すると、1 つの修正が当てはまるすべてのテーブルが表示されます)。HTML・JSON・Text のエクスポートには、
常にすべてのオブジェクトが含まれ、冒頭に何を記述したレポートかが示されます: ソース(エンジンとアドレス。
認証情報は決して含みません)、ターゲットのエンドポイント、エクスポート時刻(アプリの
最後の起動以降に実行した評価なら実行時刻も)、ツールの
バージョン、準備度の区分とその件数、工数レベルの意味。ターゲットに既に存在する名前や、別のオブジェクトが
保持している名前も記載されます。ファイル名にはデータベース(またはホスト)とエクスポート時刻が入るため、
2 回エクスポートしても上書きされません。

### マイグレーション準備度スコア

レポート上部のカードは、オブジェクトごとの作業を 0〜100 で評価します。AUTO オブジェクトのコストは 0、
それ以外のオブジェクトは作業量の重み(simple 1、medium 3、significant 7。見積もりのない UNSUPPORTED
オブジェクトは significant として計算)がコストになり、推奨事項はコストになりません。スコアは、最悪の
場合(すべてのオブジェクトが significant)に対するそのコストの割合を 100 から引いた値で、作業が必要な
オブジェクトが 1 つでもある間は 100 になりません。データベース単位の制限項目はスコアに含めず、バンドの
決定にだけ使います。

バンドはまずブロッカーで決まるため、大きなソースでもブロッカーが平均に埋もれることはありません:

| バンド | 条件 |
|---|---|
| **Blocked**(赤) | マイグレーションが Aurora DSQL のデータベース制限を超える: 1 つのデータベースにテーブル 1,000 個、スキーマ 10 個、またはビュー 5,000 個を超える(ターゲットクラスターに既にあるものを含む)。解決するまでは他のことに意味がないため、スコアに関係なくこのバンドになります。 |
| **Unsupported objects**(アンバー) | UNSUPPORTED のオブジェクトが 1 つ以上あり、スコアだけなら Ready または Low effort になる場合。(スコアがそれより低い場合は、下の作業量バンドがそのまま使われます。) |
| **Ready** / **Low effort**(緑) | スコア ≥ 90 / ≥ 70 で、UNSUPPORTED がない。 |
| **Moderate effort** / **Significant effort**(アンバー) | スコア ≥ 45 / ≥ 1。 |
| **High effort**(赤) | スコア 0。 |

### ターゲットの名前衝突検出(何かを適用する前に)

Evaluation は **ターゲット** も検査します。作成しようとしているオブジェクトが DSQL に既に存在する場合、
その衝突を **今** 表面化させるため、Schema Conversion の適用ステップが予期しない衝突で途中失敗する
ことがありません。それを **skip** するか **replace** するかを、あらかじめ決定します。

### オプションの AI アシスト(Amazon Bedrock) — 何であり、何ができ、どう有効化するか

AI アシストは **オプトイン・デフォルト無効** の補強機能で、**Amazon Bedrock** を使ってスキーマ変換の
*人による判断* が必要な部分を支援します。これはツールが LLM を呼び出せる唯一の場所なので、それが
何をするのか、何が許可されているのか、そして何には決して触れないのかを正確に理解しておく価値があります。
そのため、ここですべてを説明します。

**有効にしたときに行うこと:**

- **Evaluation** では、**戦略ナラティブ** を追加します(スキーマがどれだけマイグレーション可能か、
  作業がどこにあるのかを平易な言葉で説明したもの)。
- **Schema Conversion** では、決定論的コンバーターがフラグを立てた難しい `MANUAL` / `UNSUPPORTED`
  項目に対して **変換の提案** を提示します(例:サポートされない構造をどう作り直すか)。

**決して行わないこと(安全モデル):**

- **決定論的な経路を置き換えることはありません。** 決定論的なソース→DSQL コンバーターが常に先に実行され
  (MySQL ソースは `sqlglot`、PostgreSQL ソースは PG→DSQL コンバーター)、AI はそれを *補強* するだけです。
  AI を無効にした状態(デフォルト)では、ワークフローはまったく同じように動作します。
- **すべての提案はレビュー専用です。** 提案がターゲットスキーマに反映されるのは、**あなたが明示的に
  承認した後だけ** です — 保留中の提案、編集したが未承認の提案、拒否した提案は、いずれも適用経路から
  除外されます(厳格な人によるレビューのゲート)。承認する前に提案を編集することもできます。
- **AI がデータ経路に入ることは決してありません。** 設計時のスキーマ/戦略を支援するだけで、Full Load や
  CDC の **行データ** を見ることも触れることもありません。
- **ソース/ターゲットのデータは送信されません。** 提案は行の値ではなく、スキーマ/DDL のメタデータから
  作成されます。

**権限 / デプロイモデル:**

- **AI バックエンドは Amazon Bedrock のみで、直接の API キー入力はありません。** このツールには
  **Anthropic/OpenAI(またはその他)の API キー入力欄がありません。** AI は *Bedrock 経由でのみ*、
  AWS 認証情報で認証して動作します。これにより、AI も他のすべての機能と同じ IAM/認証情報モデルに
  そのまま従うため、別途保存するシークレットがありません。
- AI アシストは **`bedrock:InvokeModel`** と **`bedrock:InvokeModelWithResponseStream`** (チャットが回答をストリーミング)
  だけで Bedrock を呼び出します — それ以外はありません。
  **ツールの他の部分と同じ認証情報**(Connect で選択した AWS プロファイル、または環境/ロールの認証情報
  チェーン)を使用します。
- **ローカルで実行する場合:** 認証情報チェーンが選択したモデルに対して両方のアクションを許可して
  いる必要があり、そのモデルが **AWS アカウントで有効化** されている必要があります(下記の *モデルアクセス*
  を参照)。
- **Fargate にデプロイする場合:** AI アシストは **`EnableAiAssist=true` でデプロイしない限り無効です。**
  この値を有効にすると、タスクロールに両方のアクションが **`anthropic.` プロバイダーにスコープを限定して** 付与されます:
  アカウントのすべての Anthropic 基盤モデルとクロスリージョン推論プロファイルなので、再デプロイせずに Model ID を
  Anthropic モデル間で切り替えられます (`*` 全体や他のプロバイダーには決してなりません)。`BedrockModelArns` は
  **任意** で、そのスコープを特定の ARN に絞るか、Anthropic 以外のモデルを許可する場合に使用します。あわせて
  `BedrockRegion` も設定し、タスクの送信(egress)トラフィックが Bedrock ランタイムエンドポイントに
  到達できる必要があります(NAT または Bedrock VPC エンドポイント)。
  [`deploy/DEPLOYMENT.md`: 「AI 支援変換の有効化 (オプション)」](../../../deploy/DEPLOYMENT.ja.md) を参照してください。
- **モデル:** デフォルトは `global.anthropic.claude-sonnet-5`(汎用的でコスト効率の良い選択)です。
  `BedrockModelId`(デプロイ時)または Connect 画面の Bedrock 設定で変更します。
- **モデルアクセス(AWS アカウントごとに 1 回):** Bedrock の Anthropic モデルは、アカウントごとに AWS
  Marketplace サブスクリプション — つまりモデルの *契約(agreement)* — を通じて有効化されます。アカウントで
  モデルを初めて呼び出すと、そのサブスクリプションがバックグラウンドで開始され、最終的に失敗する場合でも
  最大約 15 分間は呼び出しが成功することがあります。呼び出し元に `aws-marketplace:Subscribe` /
  `aws-marketplace:Unsubscribe` / `aws-marketplace:ViewSubscriptions` の権限がない場合、アカウント
  (またはその AWS Organization)で Anthropic の 1 回限りの初回利用(first-time use)フォームが提出されて
  いない場合、またはアカウントに有効な支払い方法がない場合、サブスクリプションは失敗します。Bedrock の
  拒否がモデルの AWS Marketplace サブスクリプションに関するものである場合、ツールは *the configured model
  is not enabled for this AWS account yet* と表示し、以下の対処方法を示します。初回利用フォームが未提出の
  場合は、代わりに Verify AI access がモデルを *not enabled or not available* と報告することがあります。
  その場合は Amazon Bedrock コンソールでフォームを 1 回提出してから、再度チェックしてください。
  ツールが使用する ID がサブスクライブできない場合(Fargate のタスクロールは意図的にできません)、それが
  できる人が Amazon Bedrock コンソールで、または次のコマンドで、モデルの契約を **1 回** 承諾します:

  ```bash
  aws bedrock list-foundation-model-agreement-offers --model-id <foundation-model-id>
  aws bedrock create-foundation-model-agreement --model-id <foundation-model-id> --offer-token <offer-token>
  ```

  `<foundation-model-id>` はモデル id から `global.` プレフィックスを除いた値で
  (`aws bedrock list-foundation-models --by-provider anthropic` で一覧表示できます)、`<offer-token>` は
  1 つ目のコマンドの出力から取得します。約 2 分待ってから **Verify AI access** を再実行してください。
  それ以降、どの呼び出し元にも AWS Marketplace の権限は不要で、ツールの IAM を変更する必要もありません。

**有効化する方法:**

1. **Connect** 画面で **AI Assist** を展開し、**Enable AI Assist** をトグルします(必要に応じて
   *Bedrock settings* でモデル id / リージョンを設定します)。
2. **Verify AI access** のプリフライトを使用してください。これはこの認証情報で設定済みのモデルに最小限の
   呼び出しを行う非ブロッキングのチェックです(有効な AWS プロファイルだけでは `bedrock:InvokeModel` は
   **保証されません**)。権限の不足を実行の途中ではなく、今のうちに見つけられます。Bedrock がそのモデルを
   利用不可として拒否した場合、またはモデルの契約以外の理由で拒否した場合、チェックはフォールバックモデル
   も試します。フォールバックモデルが応答すると Model ID がそのモデルに切り替わり、結果は問題のない合格では
   なく両方のモデルを示す警告になります(元のモデルに戻すには、そのモデルが有効化された後に再度選択して
   からチェックしてください)。モデルの契約(AWS Marketplace)による拒否、期限切れまたは無効な認証情報、
   スロットリング、ネットワークエラーでは最初の呼び出しで止まります。合格は、設定済みのモデルが
   **たった今応答した** ことを意味します。新しい AWS アカウントでは合格が一時的な場合があります(上記の
   *モデルアクセス* を参照)。その後 AI の応答が拒否されると、AI DBA パネルと Connect 画面の AI Assist の
   ステータスが *the configured model is not enabled for this AWS account yet* と表示し、対処方法 — モデルの
   契約を 1 回承諾し、約 2 分待ってから再度チェックする — を示します。

> **コストとレイテンシー:** AI を有効にすると Bedrock を呼び出すため、**コストと若干のレイテンシー** が
> 追加されます。これは `MANUAL`/`UNSUPPORTED` のロングテールのための利便性であって、必須ではありません
> — 決定論的な経路が、できることはすべて自力でマイグレーションします。

### レポートの活用方法

重大度の高い順に、上から順にリストを処理します。

1. **すべての UNSUPPORTED 項目を解決します。** これらはクリーンなマイグレーションを妨げます。PK が
   ない場合は追加し、トリガー/ルーチン/イベントはアプリケーション(または EventBridge/Lambda)へ移し、
   `DECIMAL` 精度は ≤ 1000 に縮小します。
2. **すべての MANUAL 項目を判断します。** デフォルト collation の受け入れ、
   `ENUM`/`SET` の扱い、置換された型(`bytea` になった空間列、`jsonb` になった PostgreSQL の配列)への対応、`ON UPDATE` タイムスタンプの判断、CDC が複製できないカスケードアクションの置き換え
   などを決めます。過大な LOB/JSON カラム(`OVERSIZED_LOB`)は、まず Data Migration の前提条件チェック
   (または **Check sizes**、[§13.5](13-excluding-tables-and-columns.md#135-特大-lob-の候補と-check-sizes))
   を実行します。値がすでに上限を超えている場合にのみ、その内容を外部ストレージ(例: Amazon S3)へ移すか、
   カラムを除外します(除外したカラムは NULL として移行されます)。
3. **AUTO 項目** については、あなたが行うことは何もありません。

> **PostgreSQL ソース。** 同じトップダウンのトリアージが適用されます。変換の前に、サポートされない PG 型を
> 作り直してください — 配列 → `jsonb` または子テーブル、`network`/`xml`/`bit`/range → `text`、
> `money` → `numeric(23,4)`、`enum` → `text`、composite → 個別のカラムまたは `jsonb`。各複製テーブルに
> 使用可能な `REPLICA IDENTITY` が必要という PG **CDC** の前提条件は、ここではなく Data Migration
> ステップに属します([第 4 章](04-cdc-and-dsql-constraints.md) を参照)。

> **DSQL で特に重要な理由:** DSQL はトリガー、ストアドプロシージャ、いくつかの型を意図的に除外し、
> 外部キーはランタイム上の注意点を伴って強制します。Evaluation は、その事実を — 読み取り専用で、
> データ移行の前に、低コストで — 知る瞬間です。後でロードの失敗として発見するのとは違います。

---

## 2.2 Schema Conversion — DSQL DDL の生成・適用

評価結果を手にしたら、**Schema Conversion** がソーススキーマを DSQL 互換の DDL に変換し、ターゲットに
適用します。SCT に相当するステップです。

- ソース/ターゲットのオブジェクトツリーを **参照** します。
- オブジェクトごとに、ソース DDL と変換された DSQL DDL を **並べて比較** します。
- 変換した DDL をターゲットに **適用** します — **Apply all** ですべてのオブジェクトを一括で適用するか、各オブジェクトの **Apply to target** ボタンで 1 つずつ適用するかを選べます。ターゲットに既に存在するオブジェクトについては、**SKIP**(そのまま残す)または **REPLACE**(削除して再作成)を選択します。単一の既存オブジェクトを適用する際は、その場で **Replace / Skip / Cancel** を選ぶダイアログが表示されるため、選択が明示的になります — これがテーブルの DDL を変更した後に再適用する方法で、例えば複合キーを整数キーに戻すといった場合に使います(SKIP では古いテーブルがそのまま残り、REPLACE は新しいキーで再作成されます)。Aurora DSQL が一時的な `XX000` ("server unavailable") を返した `CREATE` はリトライします: リトライで既に存在すると分かったテーブル・ビュー・スキーマは先の試行が作成したものですが、そうして見つかったインデックスは、名前が別のテーブルのインデックスである可能性があるため、作成済みとは報告しません。

### 変換が代わりに行ってくれること

変換は方言と制約のギャップを自動的に橋渡しします。**MySQL ソース** の場合は `sqlglot` を経由し
(MySQL → PostgreSQL 方言、その後 DSQL の制約へ)、**PostgreSQL ソース** の場合はソースから取得した正確な
カラム型(`format_type` 文字列)から構成する near-identity PG-16 → DSQL の再構築であり、`sqlglot` の
MySQL→PostgreSQL トランスパイルではありません。

- **型マッピング** — **MySQL ソース** の場合、完全な MySQL → DSQL 型テーブル(`TINYINT(1)` → `boolean`、
  `BIT(n)` → 整数、`ENUM` → `text` + `CHECK`、`BLOB`/`BINARY` → `bytea` など。
  下記 [§2.3](#23-mysql--dsql-の型と制約の処理-リファレンス) を参照)。**PostgreSQL ソース** には
  このような変換テーブルはありません — DSQL がサポートするカラム型はそのまま通過します(§2.3 のスコープ
  注記を参照)。
- **列のデフォルト値(MySQL ソース)** — ソースの `DEFAULT` を引き継ぎます(Aurora DSQL がサポートしており、
  `CURRENT_TIMESTAMP` **および小数秒付きの `CURRENT_TIMESTAMP(n)`**、`NOW()`、`CURRENT_DATE`/`CURRENT_TIME`、
  `LOCALTIME`/`LOCALTIMESTAMP` を含みます)。2 つは自動的に変換されます: `TINYINT(1)` のデフォルトは
  `boolean` ターゲット向けに `TRUE`/`FALSE` へ、時計を読む `DATETIME` のデフォルトはソースのタイムゾーンを
  明示します(UTC のソースなら `now() AT TIME ZONE 'UTC'`、そうでなければ Evaluation が読み取ったタイムゾーン:
  `SOURCE_TIME_ZONE`、§2.1。`CURRENT_TIMESTAMP + INTERVAL 30 DAY` のような 1 つの単位の整数による時計の演算はその上に
  そのまま残り、それ以外の時計の演算は下記のとおり破棄して報告されます)。
  列はソースの壁時計を保持し、ローダーはその値をそのままコピーするためです。
  これが最も重要なのは **デフォルト値を持つ `NOT NULL`
  列** です: MySQL はその列を省略した `INSERT` を受け付けますが、デフォルトがなければターゲットは
  同じ文を拒否します — カットオーバー後に初めて現れるアプリケーション障害です。DSQL に本当に対応が
  ないデフォルト(MySQL の `UUID()` は変換されますが、他の列を参照する式は不可)は、黙って失われる
  のではなく破棄して **報告** されます。`ON UPDATE CURRENT_TIMESTAMP` はまったく再現できません —
  DSQL には `ON UPDATE` 句もトリガーもありません — そのため、アプリケーション側で処理するよう
  **MANUAL** としてフラグ付けされます。**PostgreSQL ソース** では、列 DEFAULT を PostgreSQL が保存している
  テキスト(`pg_get_expr`)の **まま** 引き継ぎます。再レンダリングしないため、`CURRENT_TIME`、
  `CURRENT_TIMESTAMP(0)`、PostgreSQL 14 より前に保存された `date_part(...)`、3 引数の
  `date_trunc(..., 'Asia/Seoul')` も変わらずに Aurora DSQL に届きます(ただしソースのタイムゾーンが UTC でなければ、
  タイムゾーンのない列の時計のデフォルトはそのタイムゾーンを明示します: `SOURCE_TIME_ZONE`)。主キーの `serial`/`IDENTITY` `nextval` は選択した主キー戦略が代わりに
  決定し、キーでない列の `nextval`、パースできないデフォルト、DSQL にない型へキャストするデフォルト、
  DSQL にない関数(他の uuid-ossp 生成関数や `pg_catalog` 外のスキーマ修飾関数)を呼ぶデフォルトは
  注記付きで除外され Evaluation も報告します(`DEFAULT_DROPPED`)。uuid-ossp の `uuid_generate_v4()` は
  組み込みの `gen_random_uuid()` になります(enum のデフォルトは列が `text` になるため、プレーンな
  テキストリテラルとして保持されます)。キーでない列の `nextval` の注記は、データベースに値を生成させ続ける方法を示します: identity
  ですが、Aurora DSQL は identity を `bigint` 列にしか許可せず、適用後に列の型を変更できないため、`serial`
  (`integer`)列は適用前にターゲット DDL で `bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)` と宣言し、
  カットオーバー前に identity をロード済みの値の次から再開する必要があります(Evaluation の `NON_KEY_SEQUENCE`
  も同じ指示をします)。`CACHE 1` にすると挿入スループットは下がりますが、ソースと同じく値を 1 つずつ順番に
  発行します(中断または再試行されたトランザクションも値を 1 つ消費するため、欠番は引き続き発生し得ます)。複数のテーブルが共有するシーケンスを呼ぶ `DEFAULT` には identity を使いません:
  そのシーケンスの Step 1 の項目どおりにシーケンスを作成し、`DEFAULT` を付け直してください。`STORED` 生成カラムの式はそのまま再出力され、`VIRTUAL` 生成カラムは通常のカラムになります。
- **外部キーの保持** — Aurora DSQL は外部キーを **強制** するため、各ソースの FK は **`CREATE TABLE` の外**
  に保たれ、独立した post-load の `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` 文としてレンダリングされます。
  Full Load のみの場合は、データのロード **後** に **Apply foreign keys** 操作でこれらを再作成します(各キーを
  すぐに追加し、その後 Aurora DSQL が検証します)。**CDC** マイグレーション
  では、この適用は **カットオーバーまで延期** されます(sink が親より先に子の行を out-of-order でストリーム
  している間は制約が存在してはなりません — FK 違反はデッドレターに送られます、`SQLSTATE 23503`)。
  レンダリングされた DDL はここに表示されるため、カットオーバー時に適用できます。代わりに外部キーを
  **取り除くことを選ぶ** こともでき(`SchemaConvertOptions.preserve_foreign_keys=False`)、その場合は参照整合性を
  アプリケーション層で強制します。
- **主キー戦略** — キャッシュ付きの identity カラム(`bigint` に収まる単一カラムの生成キーの既定)、
  または整数 PK をそのまま維持する。各テーブルのカードには **primary-key picker** もあり、**複合キー**(高カーディナリティの列を元のキーの先頭に追加したもの、例: `(customer_id, id)`)への切り替えができます — DSQL パーティションへの書き込みを分散させ、書き込みホットパーティションの壁を移動させる唯一の戦略です。選択内容は DSQL のキー制限に対して検証され、元のキーの一意性は `CREATE UNIQUE INDEX ASYNC` によって維持されます。いつ・なぜ使うべきかは [第 7 章 §7.1](07-performance-and-tuning.md#プライマリキー戦略--ホットパーティションの回避) を参照してください。
- **`CREATE INDEX ASYNC` としてのインデックス** — DSQL はセカンダリインデックスをデータの後に非同期で
  構築します。
- **トランザクションあたり 1 つの DDL** — 変換は各 DDL 文をそれ自身の実行単位として出力し(DSQL は
  トランザクションあたり 1 つの DDL を許可)、適用経路は楽観的並行性の衝突(`40001`)を冪等に
  リトライします。
- **ビュー** — MySQL のビューは PostgreSQL の `CREATE VIEW` にトランスパイルされます。パースできない定義は、
  黙って捨てられるのではなく、手作業のフラグが付いた明示的なプレースホルダーとして表示されます。
  トランスパイラが変換しない関数(`FIND_IN_SET`、`UNIX_TIMESTAMP`、ユーザー関数)を呼ぶビューは、両方のステップで
  MANUAL とされます(`VIEW_NOT_CONVERTED`): その関数が Aurora DSQL にない限り `CREATE VIEW` は失敗します。
  忠実な PostgreSQL の形がある MySQL の呼び出しはその形に書き換えます(`YEAR()`/`HOUR()`/`DAYOFWEEK()`/`WEEKOFYEAR()` は
  `EXTRACT` に、`DATEDIFF`/`TO_DAYS`、`NOW()`/`CURRENT_TIMESTAMP`/`LOCALTIMESTAMP` と `CURTIME()` はセッションの壁時計に
  (Evaluation が読み取ったタイムゾーンが UTC でなければ、`SYSDATE()`、`CURDATE()` とともにソースのタイムゾーンの壁時計に:
  `SOURCE_TIME_ZONE`)、
  `UTC_DATE()`/`UTC_TIMESTAMP()`/`UTC_TIME()` は UTC の壁時計に(MySQL と同じく精度に合わせて丸めずに切り捨てて)、`STDDEV`/`VARIANCE` は
  MySQL が計算する母集団の形に、`VAR_SAMP`/`STDDEV_SAMP` は標本の形に、`CAST(x AS CHAR [CHARSET ...])` は text に — 長さが
  あれば切り詰めるだけで埋めない `varchar(n)` に — バイナリ列はそのバイトをデコードして(`CONVERT_FROM`)、`BINARY x` は
  そのバイトに(比較では x のテキストを、Aurora DSQL がすでにするとおりバイト単位で — 数値や日付は MySQL のそのテキストで、
  `'010'` は `10` ではない、型がわからない値は値そのもので、数値ならそのとき数値として比較 — `GROUP BY`・`PARTITION BY`・`COUNT(DISTINCT ...)` のキーは値そのものに、`ORDER BY` のキーは
  テキスト・日付・バイトならそのまま、数値や型がわからない値はそのテキストのバイト順に — 出力名はそれが指す式で、`FROM` の列にも
  ある名前は MySQL が読むとおりその列で — 選ぶもの
  でしか並べられない `UNION` と `SELECT DISTINCT` のキーは x そのままにしてフラグを付け)、`CAST(x AS UNSIGNED)` は
  MySQL と同じく先に丸めてから(文字列は先頭の整数として読んで)負の数を循環させて、整数と decimal の `/` は MySQL の正確な
  除算として(MySQL が保つ小数桁で切り捨てて計算し — 整数なら 9 桁で、`2 / 3 * 1000000` は `666666.6660` — 被除数のスケールに
  4 を足した decimal で表示し — `10 / 3` は `3.3333` — 0 で割ると `%`・`MOD`・`DIV` と同じく NULL)`AVG` はその `SUM` を
  `COUNT` で割る同じ除算として、`FLOAT` は MySQL と同じく倍精度で計算して(その `SUM`、`f * f`)、`FLOAT`/`DOUBLE` の
  `ROUND`/`TRUNCATE(x, d)` は MySQL と同じく倍精度で計算して(実行時に読む桁数でも。整数はその整数のまま)、数値として読まれる
  `TINYINT(1)`(Aurora DSQL では boolean)と比較式 — 算術、ビット演算子、`ROUND`/`SIGN`/`POWER` や数値へのキャスト、集計
  (`SUM(qty > 2)` は行を数えます)、または数値の隣(`= 1`、`<=> 1`、`IN`、単純 `CASE`)— はその整数として、`TINYINT(1)` に
  入れる数値(`SET paid = 1`、`INSERT ... VALUES (..., 1)` や `INSERT ... SELECT` — 埋める列で判断)は boolean として、
  `SET col = a = b` は MySQL が読むとおり(`col = (a = b)`)、`GROUP_CONCAT` は MySQL のテキスト上の集計に(`TINYINT(1)` や
  比較はその数字で)、
  `CONCAT_WS` は NULL を飛ばし `GREATEST`/`LEAST` は MySQL と同じく NULL を返すように、`LENGTH` はテキストのバイト数に、
  `CHAR_LENGTH` はその文字数に(`CHAR(n)` 列は埋め草の空白なしで、JSON 値はそのテキストで)、MySQL が文字列を求める
  ところ(`CONCAT`/`CONCAT_WS` — テキストと並ぶバイトはデコードしてフラグ付き — `LPAD`/`RPAD`、`LEFT`/`RIGHT`、
  `SUBSTRING`、`REPLACE`、`UPPER`/`LOWER`、`TRIM`、`REVERSE`、`REPEAT`、`MD5`、`ASCII`、`LOCATE`/`INSTR`、`LIKE`、
  `REGEXP`、文字列と並ぶ `GREATEST`/`LEAST` — ただし文字列の順序が数値の順序と一致する 2 未満の数値の文字列(下限を 0 にする
  `GREATEST(x, '0')`)は別 — 数値や日付とテキスト列や別の種類の文字列 `'N/A'` からなる `UNION` の列)の数値・日付・`TINYINT(1)`・比較は
  MySQL のテキストに(`DATETIME(6)` は小数 6 桁すべて)、MySQL が文字列にする `COALESCE`/`CASE`/`IF` — 文字列やテキスト列と
  並ぶ日付・時刻、文字列や日付と並ぶ数値、そのどちらかと並ぶ JSON 値 — はその場所で MySQL がその文字列を読むとおりに(PostgreSQL
  がその値として同じテキストで読む文字列 — `DATE` と並ぶ `IFNULL(end_date, '9999-12-31')`、`IFNULL(qty, '0')` — と並ぶなら
  日付は日付のまま、表示される数値は数値のままにし、派生テーブル・CTE・ビューの列を読むクエリや `INSERT ... SELECT` は MySQL と
  同じくそれで計算します。数値
  として読む場所 — 算術、`SUM`/`AVG`、数値関数、数値との比較 — では MySQL が読む倍精度に。`IFNULL(qty, '0') + 1` は
  足し算になり `''` は 0。日付として読む場所 — 日付関数、日付の算術、日付との比較 — では、文字列がその種類の日付なら日付の
  まま、時刻付きの文字列と並ぶ `DATE` はタイムスタンプに、数字のない文字列は日付関数では NULL に、日付との比較では MySQL の
  ゼロ日付(すべての日付より前の `'-infinity'`)に。それ以外 — 表示、テキスト化、JSON、
  並べ替え — では MySQL の文字列: 各値のテキストとリテラルは書かれたとおり)、日付と並ぶ `TIME` は MySQL の `DATETIME` に、
  正確な数値の `COALESCE` は最も広いスケールに(`IFNULL(price, 0)` は `0.00`)、`DATE` に日・月を足しても `DATE`
  のままに(列や式から読む量や小数は MySQL と同じく整数単位で数え — 文字列は先頭の整数として — `'1 2' DAY_HOUR` のような
  複合単位はその各部分として、2 桁の年や `yyyymmdd` の日付文字列は MySQL と同じく解釈)、`INTERVAL x unit + d` は `d + INTERVAL x unit` として、`TIMESTAMP '...'`
  リテラルはそれが表す `DATETIME` として、`+hh:mm` オフセットの `CONVERT_TZ`
  (リテラルでも列から読む値でも)、NULL を保ち `DATETIME`、`TINYINT(1)`(`1`/`0`)、比較(`true`/`false`)を MySQL と同じく書く `JSON_ARRAY`/`JSON_ARRAYAGG`、
  メンバー名と配列の添字からなるパスの `JSON_EXTRACT`/`->` と MySQL の `->>`(MySQL はビューに
  `json_unquote(json_extract(...))` として保存します)は MySQL と同じくパスをたどって(配列でない値の `[0]` はその値自体、`[0]` はドキュメントの中で入れ子になった呼び出しも合わせて 3 つまで。型がわからない
  ドキュメントは `json` にキャスト)— `->>` の JSON null はテキスト `'null'` に(日付関数が読む場所では MySQL と同じく
  NULL に。グループ化した `->>` はクエリが読むどこでもその `GROUP BY` キーの形に)、数値と比較・算術・`SUM`/`AVG`・`ROUND`・
  数値関数で使うそのテキストや JSON 値は MySQL と同じく倍精度に(フラグ付き: 数値でないテキストはそこで失敗します)— MySQL と
  同じく書式に応じて `TIME`/`DATE`/`DATETIME` を返す `STR_TO_DATE`(空文字列は NULL、2 桁の年は MySQL と同じく解釈、書式を
  読み終えて残った入力は無視)、`ON` のない結合(`CROSS JOIN`、`LATERAL`、`INNER JOIN`、Query Converter の文の最上位の
  `JOIN` — カンマはカンマのまま)は `CROSS JOIN` に、`CROSS JOIN ... ON` と `STRAIGHT_JOIN` は `JOIN` に、再帰 CTE の
  再帰部分の値は最初の SELECT の型に修飾子があればその型にキャストし(列を広げる MySQL の方法 `CAST(x AS CHAR(n))`、
  `VARCHAR(n)` や `DECIMAL(p,s)` の列。再帰部分の小数と並ぶ整数にはフラグ)、そしてすべての `DATE_FORMAT`/`STR_TO_DATE`/`FROM_UNIXTIME` の書式を `%%` や未知の指定子まで
  MySQL 自身の指定子から)。その他(ISO 以外のモードの `WEEK`、`SUBSTRING_INDEX`、`SHA2`、`JSON_SET` など)は同じように
  フラグが付き、PostgreSQL に同じ名前の関数があってもストアド関数(`date_trunc`)、Aurora DSQL がたどれない JSON パス
  (`'$'`、ワイルドカード、`last`、範囲、実行時に計算するパス、4 つ以上の `[0]`、複数のパス)、比較・並べ替え・グループ化・
  重複除去される JSON 値(Aurora DSQL には json 値の比較がありません)にはフラグが付き、Aurora DSQL にパターンの
  ない書式トークン(MySQL の `%U`/`%u`/`%V`/`%X` の週番号、`%w`)もフラグが付きます: ビューは作成されますが、異なるテキストを
  返します。列に対する `STR_TO_DATE`(書式で読めない値に MySQL は NULL を返しますが、Aurora DSQL はそうした値にはエラーを返し、
  区切り文字だけが違う値は日付として読みます。また夏時間のあるセッションのタイムゾーンでは、時計が飛ばす 1 時間の中の時刻が
  1 時間後として読まれます)、バイトのデコード(不正なバイトは MySQL では NULL、Aurora DSQL ではエラー)、列から読む書式、
  `FLOAT`/`DOUBLE` の `MOD`(MySQL のものは C の fmod で、Aurora DSQL にはその演算子も正確な形もありません)、
  `GROUP_CONCAT(DISTINCT x ORDER BY y)` にもフラグが付きます。各値はその内容に合わせて書きます — 列はその型で(MySQL が保存した
  結合を経ても、MySQL が束縛するとおりに — `db`.`table`.`col` はそのデータベースのテーブルに、その列を持たないテーブルの
  別名は越えて、派生テーブルや CTE には決して束縛せず)、派生テーブルや CTE の列はその本体が選ぶもので(読むテーブルと同じ名前の CTE も)、式はその計算結果で — そして
  Query Converter も同じ MySQL の式を同じように書き(`UPDATE`、`DELETE`、`ON DUPLICATE KEY UPDATE` でも)、ステップ 1 を
  実行していれば Evaluation のインベントリの型を使います(シングルデータベースモードで読んだビューは、保存されたテキストが自身の
  データベースのテーブルを修飾なしで書くため、自身のスキーマで読みます)。型がわからない値 — 別のビューの列、派生テーブルや CTE が計算する列、
  ステップ 1 の前の Query Converter の列 — の `LENGTH` と `BINARY` には
  フラグが付きます: `LENGTH` は `x || ''` のテキストを数え、テキスト・バイト・整数・decimal・日付には MySQL と同じ数になります
  (`TINYINT(1)`、`FLOAT`、`TIMESTAMP`、小数部のある `DATETIME` は別)。`BINARY` は `CAST(x AS BYTEA)` で、バイナリの列にだけ
  正しい形です。文字列関数の中では `x || ''` にし、テキスト・数値・日付の MySQL のテキストになります(`LEFT(created_at, 7)` は
  その月)。`LIKE`/`REGEXP` では `CAST(x AS TEXT)` にし、テキストの列のインデックスは `LIKE 'prefix%'` にも使われます(別のビューを
  通して読むバイナリの列はそこで 16 進のテキストとして比較されます)。`UPPER`/`LOWER` と `BINARY` の比較ではそのままにします。商は MySQL が表示する箇所でそのスケールで表示します: 文の出力と
  テキスト、そして MySQL が実体化(materialize)するビュー・派生テーブル・CTE — `ALGORITHM=TEMPTABLE` で宣言したビュー、
  または集計、ウィンドウ、`DISTINCT`、`GROUP BY`、`HAVING`、`LIMIT`、`UNION` を持つもの、テーブルのないもの、自身の行を読む
  サブクエリを含む出力を持つもの — の行で、それぞれその列のスケールで保存されます(ウィンドウの値も、クエリがそれでグループ化
  する `CASE`/`IF` も: `0.0000` — MySQL がソートでグループ化する `WITH ROLLUP` では各行の値そのまま)。`FLOAT`/`DOUBLE` の
  分岐を持つ `UNION` の列の商はすべての桁を保ち、`FLOAT` の分岐を持つ列は MySQL が型付けるとおりになります: `DOUBLE`、
  `DECIMAL`(商、正確な数の `SUM`)または `INT` 列と並ぶとき、あるいは別の整数(リテラル、`BIGINT`、`COUNT(*)`)が `FLOAT` より前にあるときは
  `DOUBLE`、`TINYINT`/`SMALLINT`/`MEDIUMINT`/`YEAR` 列と並ぶとき、またはそうした整数が後にあるときは `FLOAT`(`SELECT f ...
  UNION ALL SELECT 0` は `FLOAT` の `0.1` を表示します)。MySQL が読み手のクエリにマージするビューはそこで商が保つすべての桁で計算されるため、ビューは丸めずに
  残し、読み手のクエリは MySQL と同じ結果を返します。その代償: そのビューを直接選択すると、商の列は MySQL が計算に使う桁を
  表示します — MySQL が `0.3333` を表示するところで `0.333333333` を、`10.0000` を表示する `(x / 3) * 3` は `9.999999999` を
  — これは AUTO と評価され、ビューごとの注記はありません。またそのビューを読むビューはその列が商だと知り得ないため(PostgreSQL
  のビューは読み手にマージされません)、そこから計算した値もその桁で表示し、求められた桁数どおりに丸め・切り捨てます(MySQL は
  商のスケールより多い桁を残さず、`1 / 3` の `TRUNCATE(v.q, 6)` は `0.3333`)。表示される値が重要なら `ROUND(q, 4)` として
  読んでください。再現しないもの: MySQL は商の `GROUP BY`、ウィンドウの `PARTITION BY` / `ORDER BY`、`COUNT(DISTINCT ...)` を
  表示スケールでキーにしますが(`1 / 3` と `3333 / 10000` は同じキー)、変換は保った桁でキーにします — MANUAL のフラグが付き
  ます(`GROUP BY 1` や `GROUP BY` の別名は、両エンジンとも表示される出力でキーにします)。一時テーブルでグループ化するとき
  (グループ化列にインデックスがないとき)、MySQL は商の `SUM`、`MAX`、`MIN`、`AVG` も、キーでない `CASE`/`IF` の出力も
  (`0.0000`、ここでは `0`)そのスケールで保存し、これは実行計画によって変わります — それが重要なら `ROUND(q, 4)` で
  グループ化・集計してください。MySQL 8.0.22 以降(Aurora MySQL 3)は、
  実体化するビューや派生テーブルの商に対するフィルタ(`WHERE v.q = 0.3333`)をその中へ押し込んですべての桁で比較しますが、
  返す行は保存されたスケールを持ちます。Aurora DSQL は保存された値で比較するため、それが重要なら `ROUND(q, s)` や範囲で比較
  してください。引数にウィンドウ関数を含む `JSON_ARRAY`/`JSON_OBJECT` はウィンドウの後で計算され、その商の引数をスケール
  どおりに読みますが(`[7.1429, 2.6667]`)、変換は保ったすべての桁を書きます(`7.142857142`)。`IFNULL`、`IF`、`CASE` などの
  関数の奥に 3 段より深く入れ子になった `GREATEST`/`LEAST`、列から読む複合単位の interval、200 段より深く入れ子になった式
  (変換が型を読む範囲の外)には MANUAL のフラグが付きます。 フラグ付きでまだ異なるもの: `FLOAT` のテキスト(MySQL は
  有効数字 6 桁で `1234.57`)と JSON の中の `FLOAT`、もう一方の種類の日付文字列と並ぶ日付の算術
  (`IFNULL(d, '2024-06-01 10:00:00') + INTERVAL 1 DAY`: MySQL の結果は行ごとに形が変わる文字列で、日付の行は `2024-03-06`)。
  フラグなしでまだ異なるもの: 数値の文字列と並ぶ数値 — 表示される `COALESCE`/`CASE`/`IF`、`UNION` の列 — は、それを読む
  場所で数値として並べ替え(またはテキストと比較)られます(MySQL は文字列として並べ、`'10'` が `'9'` より前)。数値の文字列と並ぶ `DECIMAL` を別の
  ビューが算術で読むとそのスケールを保ちます(MySQL の倍精度が `20` を表示するところで `20.00`)。クエリがグループ化する
  `COALESCE` の JSON(`ONLY_FULL_GROUP_BY` のない `sql_mode`)は最も広いスケールで書かれます(`[0.00]`、MySQL は `[0]`)。
  `DECIMAL` と整数の `UNION` は、整数の分岐の行を列のスケールなしで表示します(MySQL が `5.00` を表示するところで `5`)。
  **PostgreSQL** のビュー定義(`pg_get_viewdef`)はすでに PostgreSQL なので、引用符付きのビュー名の下に
  **そのまま** 移され、ソース側のペインにも同じテキストが表示されます。再レンダリングするとビューが返す結果が
  変わります — たとえば jsonb キーに付いた `::text` キャストが `->` 全体にかかり、jsonb 列が text になります。
  唯一の書き換えは、変換が text にする enum 型へのキャストです (`'paid'::order_status` は `'paid'::text` に
  なります)。その型は Aurora DSQL に存在しないためです。変換が型を変える列 (enum、jsonb になる配列、text に
  なる範囲) を読むビューは、両方のステップで **MANUAL** (`VIEW_REMODELLED_COLUMN`) として示され、各列の旧型と
  新型を伝えます: その中の配列添字や範囲演算子は適用時に失敗するか、別の結果を返します。

### 外部キー — DSQL では強制されるが、データ適載の *後* に適用される

Aurora DSQL は外部キーを **強制(enforce)** するため、ツールは既定で外部キーを **保持** します
(DDL 上の **Preserve foreign keys** トグル)。ただし外部キーは `CREATE TABLE` の一部ではなく、
**Schema Apply では適用されません** — 独立した **適載後(post-load)** の文としてレンダリングされます:

```sql
ALTER TABLE "orders" ADD CONSTRAINT "fk_customer"
  FOREIGN KEY ("customer_id") REFERENCES "customers" ("id") NOT VALID;
```

PostgreSQL のキーは `REFERENCES` の後に宣言した内容を、PostgreSQL の順序で `NOT VALID` の前に保持します:
`MATCH FULL`、`ON DELETE` / `ON UPDATE` のアクション、そして `DEFERRABLE INITIALLY DEFERRED`(または
`DEFERRABLE INITIALLY IMMEDIATE`)です。Aurora DSQL はそれぞれを `NOT VALID` とともに受け付け、遅延された
キーをコミット時に検査します。遅延可能なキーは `ON DELETE` / `ON UPDATE` の `RESTRICT` も保持します。
PostgreSQL は、キーが遅延されている間もこれを即時に検査するためです。`NOT ENFORCED` で宣言された
PostgreSQL 18 のキーはそもそも作成されません。

**なぜ遅延するのか。** バルク適載は親テーブルと子テーブルを **並行して、テーブルごとの主キー順で、
親→子の順序保証なしに** 書き込むため、子の行が親より先に書き込まれることがあります。外部キーが
すでに強制されていれば、その書き込みは失敗し(`SQLSTATE 23503`)、**CDC** マイグレーションでは
シンクが行変更を **テーブルをまたいで順不同に** 適用するため、ストリーミング中に外部キーがあると
その行は **dead-letter** されます。したがってツールは、データが一貫した状態になってから制約を
追加します。

**いつ適用されるか:**

- **Full Load のみ** — 適載が終わった後、Data Migration ステップの明示的な **Apply foreign
  keys** 操作で適用します(適載の最後に自動では適用されません)。アクティビティログに
  `apply foreign keys` ステップ(`N applied, S skipped, F failed`)が記録されます。
- **CDC** — **カットオーバーに遅延**されます。最終ドレインの後、アプリケーションを再ポイント
  (repoint)する **前** に、カットオーバー Runbook(Step 4)で **Apply foreign keys** をクリック
  します。冪等なので再実行しても安全です。

**先に追加し、後で検証する。** `NOT VALID` は DSQL が受け入れる唯一の `ADD CONSTRAINT` 形式で
あり、このステップを速くしているのもこれです: Aurora DSQL は **行を 1 行も読まずに** 制約を追加し
— 子テーブルのサイズにかかわらず、孤児行があっても 1 秒もかかりません — その時点から **すべての
新規書き込みを強制** します: 存在しない親を指す挿入やキーを変える更新は拒否され(`23503`)、まだ
参照されている親行の削除やキー変更も拒否されます。そのためキーは書き込み凍結の中で数秒のうちに
揃い、大きすぎて扱えない子テーブルはありません。続けてツールは各キーに
`ALTER TABLE ASYNC … VALIDATE CONSTRAINT` を投入します: 適載済みの行をバックグラウンドで検査
する Aurora DSQL のジョブです。判定は Aurora DSQL 自身のものです — その文が返したジョブ ID で
読む `sys.jobs` のジョブ状態(`submitted`、`processing`、`completed`、`failed` とその詳細)と、
検査が成功したときにだけ true になる `pg_constraint.convalidated` であり、キーはそのときにだけ
**検証済み(validated)** です。ツールは各ジョブ ID をセッションに保持し、状態をバックグラウンドで
Aurora DSQL から読み直すため、画面の再描画、Validation の再実行、アプリの再起動でも失われません
(Aurora DSQL は終了したジョブを約 30 分保持し、ツールは最後に見たものを保持します)。

**検証が失敗したとき。** キーは `NOT VALID` のまま残り、それを破る新規書き込みを拒否し続けますが、
適載済みの行の一部が Aurora DSQL に存在しない親行を指しています。画面とアクティビティログには
Aurora DSQL のエラーと — このときにだけ数える — キーを破る行の数、そしてその行を一覧するクエリ
が表示されます。このクエリは、カウントが最初のそうした行を見つけたキーの範囲(カウントが読んだ
そのページ)だけを読むため、子テーブル全体を一度にスキャンしません。画面にはそのまま実行できる形で
表示され、アクティビティログとセッションには範囲の開始が値ではなくプレースホルダー
(`%(after_0)s`…)として残ります。Aurora DSQL 上でトランザクション
あたり最大 3,000 行ずつ修正してください: 不足している親行を追加するか、各行が既存の親か NULL を
指すようにするか、削除してから **Validate again** をクリックします。主キーのある子テーブルは、
複合キーも含めて、Aurora DSQL が持つキーの keyset ページで数えます: 接続の切断、サーバーによるセッションの終了(`57P01`–`57P03`)、
競合(`40001`)、Aurora DSQL の 60 分の接続上限のいずれが起きても、中断されたそのページだけを再実行するため、
テーブルのサイズにかかわらず最後まで数えます。主キーのないテーブルは 1 つのクエリで数え、その
クエリは DSQL の ~300 秒トランザクション上限内に終わる必要があります。

**追加する前に数えるキー。** 親側が設計上 Aurora DSQL にそろっていないキー — 参照列がマイグレー
ションから除外されている、または親か子のテーブルがマイグレーションに含まれていないキー — は先に
数えます。子行が Aurora DSQL にない親行を指していれば、そのキーは **実行可能な注記(テーブル /
外部キー / 行数 / 理由)とともにスキップ** されます: 追加すると、そうした親を指すアプリケーションの
書き込みを拒否してしまうためです。

**自分で追加・名前変更したキー。** ターゲットスクリプトで追加または名前を変更した外部キーも、ほかの
キーと同じように適用・検証され、待たれます。名前は Aurora DSQL が保存するとおりに読みます: 引用符
なしの名前は小文字で(`FK_Orders_Cust` は `fk_orders_cust`)、引用符付きの名前は書いたとおりです。
名前なしで書いたキー(`ADD FOREIGN KEY`)は、PostgreSQL がそうしたキーに付ける名前
`<table>_<columns>_fkey`(たとえば `orders_cid_fkey`、同じ列の二つ目のキーは `orders_cid_fkey1`)で
追加されるため、もう一度適用しても二つ目のコピーは作られません。

**プレビューでの表示。** FK は後から独立して実行されるため、生成されるターゲットスクリプトは
それらを編集可能な `CREATE` ボックスではなく、独立した読み取り専用の
**"Foreign keys — applied after Full Load"** セクションに表示し、Apply 結果も外部キーが遅延された
ことを示します。**Schema Apply 直後にターゲットへ外部キーが無いのは想定どおりで、失敗ではありません**
— 適用したとき(Full Load のみ)またはカットオーバー時(CDC)に現れます。

**Cascade アクションは CDC のギャップです。** MySQL は `ON DELETE`/`ON UPDATE CASCADE`(および
`SET NULL`/`SET DEFAULT`)を *エンジン内部で* 実行し、その cascade された子の変更はバイナリログに
現れません — そのため CDC は親の変更のみ複製し **cascade は複製しない** ので、孤児化・陳腐化した
子行が残ることがあります(親のキーが変わっても行数はすべてそのままです)。こうした FK は Evaluation
で **MANUAL** としてフラグ付けされます。自動アクションを明示的な子行の文に置き換えるか、最終比較の
前にソースの書き込みを停止(quiesce)してください(詳細は [第 4 章](04-cdc-and-dsql-constraints.md))。
カットオーバーではこうしたキーもすぐに追加されますが、それらの行で Aurora DSQL の検証が失敗します:
MySQL ソースでは **I've cut over** は、こうしたキー — そして MySQL が Aurora DSQL のバイト比較
より緩く一致させるテキスト列のキー、つまり大文字小文字を区別しない(case-insensitive)照合順序や、
`VARCHAR` で末尾の空白を無視する PAD SPACE の照合順序(`utf8mb4_bin` など)のキー — が検証される
まで、またはその検査なしでカットオーバーすると選ぶまで待ちます。

**整合性をアプリケーション側で強制したい場合は?** **Preserve foreign keys** をオフにします。
するとツールはすべての経路から FK DDL を取り除きます(関係は Validation の孤児チェック用に
メタデータとしては保持)。この選択は再接続 / インスタンス再起動後も記憶され、遅延された FK DDL 自体
は読み取り専用ソースから再導出されるため、クラッシュで失われることはありません。なお、強制された
外部キーは書き込みごとに追加の読み取りコストがかかり、コミット時の楽観的並行性(再試行可能な
`40001`)を使用し、`CASCADE`/`SET NULL`/`SET DEFAULT` の行は DSQL のトランザクションあたり
3,000 行の上限に算入されます。

### クエリ(DML)変換とアンチパターンの Lint

スキーマ以外にも、ツールはクエリを変換し、DSQL が受け入れない、または規模の拡大に耐えられない
パターンについて **アプリケーションの SQL を Lint** できます — 例えば `SELECT ... FOR UPDATE`
(DSQL の楽観的並行性に反する悲観的ロック)、`AUTO_INCREMENT` の前提、
トリガー/ストアドプロシージャの呼び出し、サポートされない関数などです。これを使って、Cut over
**の前に** 変更が必要なコードを見つけてください。

### 最終状態

Schema Conversion の後は、**DSQL ターゲットスキーマが確定** します。これが Full Load の書き込み先で
あり、CDC のストリーミング先となるスキーマです。そして重要な点として、CDC は後続のソース DDL を
**伝播しません**。したがって CDC の実行中にソーススキーマを変更した場合は、対応する変更を Aurora DSQL に
自分で行う必要があります — CDC のストリーミング中にここで再適用するのではありません(その間は **Replace** を
含めて Apply がブロックされ、Replace はテーブルを削除します)。CDC を停止した状態では、型・キーの変更は作り直し
です: ここで **Replace**、次に **Drop & reload**
([第 4 章 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-は-スキーマ-ではなく-データ-を複製する--重要) を参照)。

## 2.3 MySQL → DSQL の型と制約の処理 (リファレンス)

これは、Schema Conversion とデータパスが SQL 方言の違いを橋渡しするために行う処理です。Full Load の値コンバーターと CDC sink の両方が従う同一のマッピングであり、共有の「write contract」がこの 2 つを一致させます。

> **スコープ: このリファレンスは MySQL ソースを説明します。** **PostgreSQL ソース** では、変換とデータパスは
> near-identity PG-16 → DSQL の **pass-through** です — DSQL がサポートする型はそのまま引き継がれ、
> MySQL 式の型変換は **一切** 行われません。DSQL がサポートする PG 型はそのまま通過します(整数系、
> `numeric`/`decimal`、`real`/`double precision`、`char`/`varchar`/`text`/`bpchar`、
> `date`/`time`/`timetz`/`timestamp`/`timestamptz`、`interval`、`boolean`、`bytea`、`uuid`、`json`、
> `jsonb`)。DSQL に対応する型のない別の集合は、Evaluation と Schema Conversion の両方でフラグ付け
> されます: 値を保持する代替型は警告付きで出力されて **MANUAL**(配列は `jsonb`、`money` は
> `numeric(23,4)`、enum は `CHECK` 付きの `text`、その他は `text`)、代替型のない型(composite、domain、
> `pgvector`)は **UNSUPPORTED** です: 配列(`[]`)、
> geometric(`point`/`line`/`lseg`/`box`/`path`/`polygon`/`circle`)、
> network(`inet`/`cidr`/`macaddr`/`macaddr8`)、`xml`、`money`、`bit`/`bit varying`/`varbit`、
> `tsvector`/`tsquery`、range + PG14 multirange、`pgvector`、`enum`、composite。PG の numeric ルールは
> 下記のテーブルと同じです: `p > 1000`/`s > 1000` の `numeric(p,s)` は警告付きでクランプされ、精度指定のない
> `numeric`/`decimal` は `numeric(18,6)` として格納されます: 小数 6 桁を超える値はロード時に丸められ、
> Validation もその列をスケール 6 で比較するため、Evaluation がフラグ付けします(`PG_UNCONSTRAINED_NUMERIC`)。
> 列 DEFAULT は **出力され**、`serial`/`IDENTITY` `nextval` だけが除去されます(identity は PK 戦略が決定)。
> `STORED` 生成カラムの式は再出力され(Aurora DSQL がサポート・維持するため)、`VIRTUAL` 生成カラムは通常の
> カラムになります。
>
> **Aurora DSQL が強制できない制約。** PostgreSQL の `EXCLUDE` 制約は作成されず (Aurora DSQL が拒否します)、ソースでそれを強制していたインデックスも作成されないため、カットオーバー後は規則に反する行 (1 つの部屋の重なる予約など) が受け付けられます: Evaluation と Schema Conversion は各定義とともにテーブルを **MANUAL** としてフラグ付けします (`PG_EXCLUSION_CONSTRAINT`)。`WITHOUT OVERLAPS` で宣言された PostgreSQL 18 の `PRIMARY KEY` または `UNIQUE` キーは同じカラムの通常のキーとして作成され、このキーはすべてのキーカラムが他の行と等しい行だけを拒否するため、1 つのキーで期間が重なる行は受け付けられます (`PG_TEMPORAL_KEY`、**MANUAL**)。このような規則はアプリケーションで強制する (書き込むトランザクションが競合する行を確認し、同じ範囲を書く同時の書き込みが共有の行 1 つも更新するようにして、Aurora DSQL の楽観的同時実行制御がそのうち 1 つを `40001` で失敗させる) か、設計を変更してください。ソースでの `WITHOUT OVERLAPS` 主キーのインデックス (GiST) はキーの順に行を返せず、Full Load と Validation はその順にテーブルを読むため、そのような大きなテーブルをロードする前にソースでそのキーカラムに btree インデックスを追加してください。`UNIQUE ... NULLS NOT DISTINCT` のインデックスまたは制約 (PostgreSQL 15+) は `NULLS NOT DISTINCT` を保持し、Aurora DSQL はそれを受け付けます。
>
> `DEFERRABLE` で宣言された PostgreSQL の `PRIMARY KEY` または `UNIQUE` 制約は通常のキーとして作成されます: Aurora DSQL は遅延可能な一意キーをサポートしないため、ソースが各文の終わりに (`INITIALLY DEFERRED` で宣言されたキーはコミット時に) 検査するキーを即時に検査します。カットオーバー後は、2 つの行に一時的に同じキー値を持たせる文やトランザクション (2 行の値の入れ替え、番号の振り直し) が失敗することがあります: Evaluation と Schema Conversion はテーブルを **MANUAL** としてフラグ付けします (`PG_DEFERRABLE_KEY`)。外部キーは `DEFERRABLE` と `MATCH FULL` を保持します (§2.2)。
>
> **MySQL** の生成カラムは通常のカラムとして作成されます (ツールは MySQL の式を読み取って指摘とソース DDL に表示しますが、変換はしません)。Full Load は計算済みの値をコピーし、CDC は `STORED` カラムの値を運びます。式 (`GENERATED ALWAYS AS (...) STORED`) は、移行タイプにかかわらず適用前に Schema Conversion で再追加できます。そうすると Full Load と CDC は、適用 DDL が `GENERATED` にしたカラムをすべての書き込みから外し (Aurora DSQL はそのようなカラムへの値を拒否します、SQLSTATE `428C9`)、Aurora DSQL が値を計算します。主キーに含まれる生成カラムは通常のカラムのままにします。Full Load と CDC はすべてのキーカラムを書き込むため、`GENERATED` のキーカラムではすべての書き込みが拒否されます。

### 型マッピング (完全なリファレンス)

以下のすべての MySQL データ型について、Schema Conversion がターゲット DDL 型として出力するもの、**および** その値が Aurora DSQL 上でどのように格納されるかを示します。両方のマイグレーション経路 — Full Load バルクローダー (Python) と CDC sink (Java) — が同一のマッピングに従い、共有の **write-contract** パリティテストによって強制されるため、どちらの経路でマイグレーションしても同じソース行が同一に着地します。分類: **AUTO** = 自動・ロスレス、**MANUAL** = 変換されるが検討・判断が必要、**UNSUPPORTED** = 自動変換なし (再設計)。

#### 整数型

| MySQL 型 | Aurora DSQL 型 | 格納される値の形式 | 分類 | 備考 |
|---|---|---|---|---|
| `TINYINT` | `smallint` | `smallint` | AUTO | 符号付き 8 ビット。 |
| `TINYINT(1)` | `boolean` | `boolean` (`true`/`false`) | MANUAL | MySQL の boolean 慣例。`0/1`→`false/true`。`{0,1}` **の範囲外の値は拒否** します (サイレントな平坦化はありません): Full Load はテーブルを停止し、CDC はその行をデッドレターにします。列にほかの値がある場合は、ここで型を `smallint` に編集してください。 |
| `SMALLINT` | `smallint` | `smallint` | AUTO | 符号付き 16 ビット。 |
| `MEDIUMINT` | `integer` | `integer` | AUTO | PostgreSQL には 3 バイト整数がありません。`integer` が符号付き 24 ビット範囲をカバーします。 |
| `INT` / `INTEGER` | `integer` | `integer` | AUTO | 符号付き 32 ビット。 |
| `BIGINT` | `bigint` | `bigint` | AUTO | 符号付き 64 ビット。 |
| `TINYINT UNSIGNED` | `smallint` | `smallint` | AUTO | `0..255` を保持するために拡幅。 |
| `SMALLINT UNSIGNED` | `integer` | `integer` | AUTO | `0..65535` を保持するために拡幅。 |
| `MEDIUMINT UNSIGNED` | `integer` | `integer` | AUTO | `0..16M` を保持するために拡幅。 |
| `INT UNSIGNED` | `bigint` | `bigint` | AUTO | `0..4.29B` を保持するために拡幅。 |
| `BIGINT UNSIGNED` | `numeric(20,0)` | `numeric(20,0)` | AUTO | より幅の広い整数型は存在しません。`2^64-1` の全範囲を保持します。(CDC は `bigint.unsigned.handling.mode=precise` が必要です。) |
| `INT(11)`、`BIGINT(20)`、… (表示幅) | bare `smallint`/`integer`/`bigint` | `smallint`/`integer`/`bigint` | AUTO | `(N)` 表示幅は **ドロップ** されます (MySQL では表示用。PostgreSQL の整数は幅を取りません)。 |
| `BIT(n)` | `smallint` (n≤15) / `integer` (≤31) / `bigint` (≤63) / `numeric(20,0)` (64) | `smallint`/`integer`/`bigint`/`numeric(20,0)` | MANUAL | DSQL には **`BIT` 型がありません**。ビットパターンはそれが表す符号なし整数として格納されます。 |

#### 固定小数点 & 浮動小数点

| MySQL 型 | Aurora DSQL 型 | 格納される値の形式 | 分類 | 備考 |
|---|---|---|---|---|
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric(p,s)` | `numeric(p,s)` | AUTO | 精度/スケールを保持。**精度 > 1000** は Evaluation で **UNSUPPORTED** と表示されます (DSQL のドキュメント上、最大精度 1000、スケール -1000..1000、デフォルト `numeric(18,6)`) が、Schema Conversion は **最大値にクランプ**し、データ損失の警告とともに DDL を出力します。MySQL の `DECIMAL` (最大 65,30) とすべての PostgreSQL `numeric` (自身の上限も同じ 1000) はこの範囲内なので、このフラグとクランプは安全装置にすぎません。ターゲットが保持するより小数桁の多い値 (Step 1 の後で広げられたソース、またはここで指定したより狭い型) は、Full Load と CDC のどちらでも丸めずに拒否します。 |
| `DECIMAL(p,s) UNSIGNED` | `numeric(p,s)` | `numeric(p,s)` | AUTO | 符号なしは表現できず、格納上の意味を持ちません。 |
| `FLOAT` | `real` | `real` | AUTO | 単精度 float。 |
| `FLOAT(M,D)` | `real` | `real` | AUTO | `(M,D)` 表示仕様はドロップされます (PostgreSQL の `float` はスケールではなく 1 つの精度を取ります)。 |
| `DOUBLE` / `DOUBLE UNSIGNED` | `double precision` | `double precision` | AUTO | 倍精度 float。 |

#### 日付 & 時刻

| MySQL 型 | Aurora DSQL 型 | 格納される値の形式 | 分類 | 備考 |
|---|---|---|---|---|
| `DATE` | `date` | `date` | AUTO | |
| `DATETIME` | `timestamp` (タイムゾーンなし) | `timestamp` (ソースの wall-clock) | AUTO | どのタイムゾーンで書かれた値でもそのままコピーされます: タイムゾーンを適用したりずらしたりしません。時計を読むデフォルトはソースのタイムゾーンを明示します(`SOURCE_TIME_ZONE`、§2.1)。 |
| `DATETIME(6)` | `timestamp` | `timestamp` (wall-clock, microsecond precision) | AUTO | 小数秒はマイクロ秒まで保持されます。 |
| `TIMESTAMP` | `timestamptz` | `timestamptz` (UTC instant) | AUTO | 絶対的な UTC 時点として格納されます。 |
| `TIME` | `time` (タイムゾーンなし) | `time` | MANUAL | 範囲内は `00:00:00..23:59:59`。**範囲外** の MySQL `TIME` (負の値または `> 24h`、MySQL の範囲は `-838:59:59..838:59:59`) は `time` として表現できないため **明示的に失敗** します (代わりに `interval` 列が必要です)。Evaluation もこれを示します (`TIME_TYPE`)。CDC (シンクプラグイン v54 以降) ではこのような値を interval として書き込みます: `interval` にリマップした列には格納され、`time` 列ではその理由とともにデッドレターになります。`interval` またはテキストにリマップすると、Full Load と CDC はどちらも `[-]HH:MM:SS[.f]` (時間は 24 で打ち切らない) で書き込み、Validation は期間の値を比較します。 |
| `YEAR` | `smallint` | `smallint` (integer year) | MANUAL | DSQL には `YEAR` 型がありません。`1901–2155` は `smallint` に収まり、整数の年として格納されます (`YEAR` の表示セマンティクスは保持されません)。 |

#### 文字列、バイナリ、構造化

| MySQL 型 | Aurora DSQL 型 | 格納される値の形式 | 分類 | 備考 |
|---|---|---|---|---|
| `CHAR(n)` | `char(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`/`TEXT`/`MEDIUMTEXT`/`LONGTEXT` | `text` | `text` | AUTO | 単一の値が **> 約 1 MiB** の場合は DSQL に拒否されます → 行単位の隔離 (Full Load) / DLQ (CDC)。特大サイズの LOB 列は Evaluation でフラグ付けされます。 |
| `COLLATE` 付きの `CHAR`/`VARCHAR`/`TEXT` (例: `utf8mb4_*_ci`) | 同一、**照合順序はドロップ** | `text` (collation dropped) | MANUAL | DSQL はデフォルトの照合順序を使用します。大文字小文字を区別しない照合順序は保持されないため MANUAL としてフラグ付けされます。 |
| `BINARY(n)` / `VARBINARY(n)` | `bytea` | `bytea` (raw bytes) | AUTO | 長さ修飾子はドロップされます (PostgreSQL の `bytea` は取りません)。 |
| `TINYBLOB`/`BLOB`/`MEDIUMBLOB`/`LONGBLOB` | `bytea` | `bytea` (raw bytes) | AUTO | バイナリペイロードをバイト単位で保持します。 |
| `ENUM('a','b',…)` | `text` + `CHECK (col IN ('a','b',…))` | `text` | MANUAL | DSQL には `ENUM` がありません。順序のセマンティクスは保持されません。 |
| `SET('x','y',…)` | `text` | `text` (comma-joined) | MANUAL | ロスレスなマッピングはありません。複数値の set セマンティクスはアプリケーションで処理します。 |
| `JSON` | `json` | `json` | AUTO | 単一の値が **> 約 1 MiB** の場合は DSQL に拒否されます(`json` では圧縮後のサイズに上限が適用されます)→ 行単位の隔離 (Full Load) / DLQ (CDC)。Evaluation は `OVERSIZED_LOB` としてフラグ付けします。(CDC は JSON テキストを `PGobject(type=json)` でラップし、`json` 列をターゲットにします。) |
| 空間型 (`GEOMETRY`/`POINT`/`LINESTRING`/…) | `bytea` | `bytea` (raw WKB bytes) | MANUAL | DSQL には空間型がありません。データは生の WKB バイトとして **保持** されます (Full Load は `ST_AsBinary(col)` を読み取り、CDC は Debezium の geometry の `.wkb` を抽出します。**SRID はドロップ** されます)。`geometry` の *列型* 自体は MANUAL としてフラグ付けされ(自動的に `bytea` へ置換され、WKB は保持)、値は失われません。 |

> **DSQL では `bytea` 列はキーになれません。** `bytea` にマッピングされる列
> (`BINARY`/`VARBINARY`、`*BLOB`、空間型)は主キーやインデックスの一部に **できません**:
> そうした列を PK にするとテーブルは **UNSUPPORTED**、セカンダリインデックスならインデックスは
> **破棄 (MANUAL)** されます。`text`/`uuid` やハッシュ列に re-key してください。下の構造上の制約を参照。

### 構造上の制約

| DSQL のルール | ツールの動作 |
|---|---|
| **外部キーは強制されます** | Aurora DSQL は **強制される** 外部キーをサポートします。各ソースの FK は `CREATE TABLE` の外に保たれ、post-load の `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` として再作成されます(Full Load はデータ着地後に適用し、CDC マイグレーションではカットオーバーまで延期します)。ランタイム上の注意点: 参照される側/参照する側のテーブルへの DML は **追加の読み取り** を発生させ、並行する競合は再試行可能な `40001` になり、`CASCADE`/`SET NULL`/`SET DEFAULT` アクションは 3000 行/トランザクション制限に算入されます — 子側のカーディナリティが無制限になりうる場合は `NO ACTION`/`RESTRICT` を優先してください。代わりにこれらを取り除いて(`preserve_foreign_keys=False`)、整合性をアプリで強制することもできます。 |
| **主キー必須** | PK のないテーブルは **UNSUPPORTED** としてフラグ付けされます (ロードできません)。 |
| **`TRUNCATE` なし** | 「置換」ロードでは `TRUNCATE` ではなく **DROP + 再作成** を使用します。 |
| **トランザクションあたり 1 つの DDL** | Schema Conversion は実行単位ごとに正確に 1 つの DDL 文を出力します。 |
| **`CREATE INDEX ASYNC`** | セカンダリインデックスはデータの後に非同期で作成されます。 |
| **楽観的並行性制御** | すべてのバッチと DDL は `40001` リトライでラップされます。 |
| **列のデフォルト値はサポートされます** | **MySQL ソース:** ソースの `DEFAULT` を引き継ぎます(`CURRENT_TIMESTAMP[(n)]`、`NOW()`、`CURRENT_DATE`/`CURRENT_TIME`、`LOCALTIME`/`LOCALTIMESTAMP` を含む)。`TINYINT(1)` のデフォルトは `TRUE`/`FALSE` に変換され(`boolean` 列は `DEFAULT 1` を拒否します)、時計を読む `DATETIME` のデフォルトはソースのタイムゾーンを明示し(UTC のソースなら `now() AT TIME ZONE 'UTC'`。`SOURCE_TIME_ZONE`、ローダーが列の壁時計をそのままコピーするため)、関数デフォルトは変換されます(`UUID()`→`gen_random_uuid()`、`CURDATE()`→`CURRENT_DATE`、`TIME` 列の `CURTIME()`→`LOCALTIME`、`UTC_TIMESTAMP()`/`UTC_DATE()`→UTC 式)。DSQL に対応がないデフォルト(例: 他の列を参照する式)は黙って失われるのではなく、破棄して **報告** します。デフォルトの保持が最も重要なのは `NOT NULL` 列です: MySQL はその列を省略した `INSERT` を受け付けますが、ターゲットは拒否します。 **PostgreSQL ソース:** ソースの `DEFAULT` を PostgreSQL が保存しているテキストの **まま** 引き継ぎます(再レンダリングしません。ソースのタイムゾーンが UTC でなければ、タイムゾーンのない列の時計のデフォルトはそのタイムゾーンを明示します) — 除去されるのは `serial`/`IDENTITY` `nextval` と生成カラムの式だけで、identity はターゲット側で選択した PK 戦略が決定します。変換された enum 列はデフォルトをプレーンなテキストリテラルとして保持し、uuid-ossp の `uuid_generate_v4()` は `gen_random_uuid()` になります。DSQL にない別の型へキャストする、または DSQL にない関数を呼ぶデフォルトは注記付きで除外されます。 |
| **`ON UPDATE CURRENT_TIMESTAMP` なし** | 再現不可: DSQL には `ON UPDATE` 句も、(PostgreSQL で通常用いる回避策の)トリガーもありません。列は挿入時のデフォルトを保持し **MANUAL** としてフラグ付けされます — 更新時のタイムスタンプはアプリケーションで明示的に設定してください。 |
| **トリガー / ストアドプロシージャ / イベントなし** | **UNSUPPORTED** としてフラグ付け — アプリケーションで再実装します (スケジュールイベントは EventBridge/Lambda で)。 |
| **ネイティブパーティショニングなし** | DSQL が自動分散します。Schema Conversion はパーティショニングを削除し、パーティション化されたテーブルにはギャップではなく推奨事項 (パーティションに依存していたもの、CDC 中のパーティション保守の停止) が付きます。 |
| **クラスターあたり 1 つのデータベース** | 複数データベースの MySQL ソースの各データベースは、自動的に独自のスキーマになります(`RECOMMENDED` としてフラグ付け)。いくつまで収まるかは次の行を参照してください。 |
| **データベースあたりスキーマ ≤ 10、テーブル ≤ 1,000、ビュー ≤ 5,000** | Evaluation はマイグレーション後にターゲットデータベースが保持する数(ソースのオブジェクト + クラスターに既にあるもの。組み込みの `public` スキーマは数えず、空のスキーマは数え、名前は 63 バイトに切り詰めた格納形で比較します)を数え、超過分を **UNSUPPORTED**(`SCHEMA_COUNT_LIMIT` / `TABLE_COUNT_LIMIT` / `VIEW_COUNT_LIMIT`)としてフラグ付けし、準備度スコアは **Blocked** になります。そうしないと、適用は最初のオブジェクトを作成した後に `54000` で失敗します。制限に達すると Aurora DSQL は既存スキーマに対する `IF NOT EXISTS` も含めてすべての `CREATE SCHEMA` を拒否するため(ライブで確認)、適用はカタログを確認し、既存スキーマは作成済みとして扱います。 |
| **ソースの `CHECK` 制約は破棄** | ソースの `CHECK` 制約(MySQL の `CHECK` は 8.0.16+ が必要)はターゲットに引き継がれず — 関数/演算子が DSQL と異なる場合がある — **MANUAL** としてフラグ付けされます: DSQL 互換の `CHECK` を手動で再追加するか、アプリで強制してください。(ツールが `ENUM` 用に *生成* する `CHECK … IN (...)` は影響を受けません。) |
| **`bytea` はキーになれない** | `bytea` 列(`BINARY`/`VARBINARY`、`*BLOB`、空間型由来)は **主キー** 列(テーブル → **UNSUPPORTED**)や **インデックス** 列(インデックス破棄 → **MANUAL**。破棄された UNIQUE インデックスは一意性を保証しなくなり、注記にそう明示されます)にできません。Evaluation も同じテーブルをフラグ付けします(`BYTEA_KEY`)。`text`/`uuid`/ハッシュに re-key してください。 |
| **キー列は 8 個以下** | 主キーまたはインデックスが 8 列を超えると DSQL の上限超過: PK は **UNSUPPORTED**、過剰なセカンダリインデックスは **破棄 (MANUAL)**。 |
| **テーブルあたりインデックス 24 個以下** | DSQL はテーブルあたり最大 24 個のインデックスを許可(PK を含む → セカンダリは 23 個以下)。超過分は表示され、一部は生成されません。 |
| **キー値は 1 KiB 以下** | 結合キー値は ~1 KiB 未満である必要があります。大きいキーは **推奨(recommendation)** として表示されます(実データで挿入時に失敗する可能性)。 |
| **識別子 63 バイト** | DSQL/PostgreSQL は識別子を (文字境界で) 63 バイトに切り詰めます。1 つのテーブルの 2 つのカラム名が同じ名前に切り詰められるとテーブルは **UNSUPPORTED** (`IDENTIFIER_LENGTH`) になります。1 つのスキーマの 2 つのテーブルやビューが同じ名前に切り詰められる場合も **UNSUPPORTED** (`NAME_COLLISION`) で、先に作成された方がもう一方の行を受け取ってしまうため、Schema Conversion はどちらも作成しません (Data Migration も対象として提示しません)。先頭 63 バイトが異なるようソースで名前を変更してください。スキーマ名も同じように切り詰められるため、名前の先頭 63 バイトが同じ 2 つのソースデータベースは 1 つのターゲットスキーマを共有します。何とも衝突しない長い名前は **推奨事項** です: DSQL は短縮した名前で格納します。 |
| **インデックス名はスキーマ単位で一意** | Aurora DSQL は 1 つのスキーマのテーブル・ビュー・インデックスを 1 つの名前空間に置きますが、MySQL はインデックス名をテーブル単位で区別します。そのため Schema Conversion は、同じスキーマの別の relation も使う名前のインデックスを **すべて** `<table>_<index>` に自動的に変更します(必要なら 63 バイトに切り詰めてハッシュ接尾辞を付けます)。テーブルとビューは名前を保持し、ビュー名もテーブル名と同様に引用符で囲んで大文字小文字を保持します。これにより UNIQUE を含むすべてのインデックスが作成されます。Evaluation と Schema Conversion は名前を変更したインデックスを `RECOMMENDED`(`NAME_COLLISION`)としてフラグ付けし、主キー戦略を変更しても同じ名前が保たれます。以前の適用が残した relation が別のオブジェクトに必要な名前を占めている場合(テーブル名と同じインデックスなど)、適用は "already exists" としてスキップせず、名前を占めている relation を示して **FAILED** と報告し、Evaluation のターゲット分析はそれを *Names held on the target by another object* に表示します。以前のバージョンが作成したターゲットでも、適用は名前を変えたインデックスと引用符付きのビューを新たに作成し、以前の名前で残った古いコピーを結果で知らせます。古いインデックスのコピーは Aurora DSQL が書き込みのたびに維持し続ける重複インデックスで (UNIQUE なら一意性も強制し続けます)、古いビューのコピーはもうソースと揃えられません。このコピーは削除してください (**Replace** は一緒に削除します)。 |
| **大文字と小文字が混在する名前** | Schema Conversion はすべての名前を引用符で囲んで綴りどおりに保ち、Aurora DSQL は **引用符のない** 名前を小文字に変換します。MySQL は列名を大文字小文字を区別せずに照合するため、`CustomerId` や `Orders` を引用符なしで書くアプリケーションの SQL はカットオーバー後に何も見つけられません。Evaluation はそのような名前と、大文字小文字だけが異なるテーブル名を **MANUAL** (`MIXED_CASE_IDENTIFIERS`、MySQL ソース) として示します: アプリケーションで綴りどおりに引用符で囲むか、Schema Conversion の前に **ソース** で小文字に名前を変えてください。カットオーバー前にターゲットで名前を変えてはいけません: Full Load、CDC、Validation、カットオーバーの DDL はすべてソースの綴りを使います。対象は ASCII の大文字だけです — Aurora DSQL はそれ以外を小文字にしません。 |

上の表の **クラス** 列は、Evaluation がすべてのオブジェクトに適用するのと同じ
AUTO / MANUAL / UNSUPPORTED の尺度です — 各クラスの意味と、複数のルールが一致した場合に
最も厳しいクラスが優先される仕組みについては
[§2.1](#すべてのオブジェクトが-1-つの分類を受け取る) を参照してください。
---

**次へ:** [3. Full Load →](03-full-load.md)
