# 2. Evaluation と Schema Conversion

_言語: [English](../en/02-evaluation-and-schema-conversion.md) | [한국어](../ko/02-evaluation-and-schema-conversion.md) | **日本語**_

> **前へ:** [1. Set up](01-setup.md)

接続とデータ移行の間には、マイグレーションを試行錯誤ではなく **決定論的で予測可能** にする
2 つのステップがあります。

```
Connect → [ 1. Evaluation → 2. Schema Conversion ] → 3. Data Migration → 4. Validation → 5. Cut over
```

- **Evaluation** は *「自分のソースデータベースの中で何が Aurora DSQL
  に移り、何が移らないのか、そしてそれぞれの作業量はどれくらいか?」* に答えます。
- **Schema Conversion** はソーススキーマを Aurora DSQL の DDL
  に変換し、それを確認・編集できるようにしたうえで、ターゲットに適用します。

**最短ルート:** **Run evaluation** → 準備度のバンドと **Unsupported** 項目を確認
([§2.1](#21-evaluation--互換性の評価)) → 各検出結果を修正または判断
([§2.4](#24-検出結果リファレンス) に対処) → **Generate DDL for selected** → 確認して
**Apply** ([§2.2](#22-schema-conversion--dsql-ddl-の生成適用))。

どちらのステップもソースを読み取るだけで、Schema Conversion
がターゲットに書き込むのは適用したときだけです。ソースエンジンが何であれ (MySQL または
PostgreSQL)、**この段階は、1 行も移す前に Aurora DSQL が受け入れないものを知る地点です。**
スキップしないでください。

手順はどちらのソースエンジンでも同じです。異なる箇所では短い注記や表がその違いを示し、
[§2.3](#23-型マッピング-リファレンス) が各エンジンのすべての列型のマッピングを、
[§2.4](#24-検出結果リファレンス) が検出結果と対処をまとめます。

---

## 2.1 Evaluation — 互換性の評価

**Run evaluation** をクリックします。ツールはソースのスキーマ (MySQL では Connect
で指定したデータベース、空欄にした場合はすべてのデータベース。PostgreSQL
では接続先データベースのすべての非システムスキーマ) と、
ターゲットがすでに保持しているものを読み取り、互換性レポートを表示します。
どちら側も何も変更しません。実行中はテーブルごとに進捗が表示され、**Cancel evaluation**
は読み取り中のテーブルの後で実行を止めます。再実行すると、
成功するまで前回のレポートが画面に残ります。移行ユーザーに必要な権限は
[§1.1](01-setup.md#11-前提条件) にあります。

Evaluation はソースの **サーバー設定** も 1 回だけ読み取ります
(設定のみで、テーブルはスキャンしません)。読み取った設定は、Evaluation
画面のソースオブジェクト数の下 (**Source server settings**、折りたたみ表示) に一覧されます
(ダウンロードするレポートにはこの一覧は含まれませんが、設定を判定する各項目がその値を引用します)。
これらは、テーブル定義には現れない、アプリケーションが動作する条件を示します:
時計関数が読むタイムゾーンと、MySQL では、たとえば Aurora DSQL
に値のないゼロの日付をソースが保持できるかどうかを決める `sql_mode` です
(`ZERO_DATES`、[§6.2](06-limitations.md#62-移行プロセスの制限))。この読み取りでは、
アプリケーションが自分で設定する値 — セッションの `SET` (`SET sql_mode`、`SET TIME ZONE`)
や、ドライバーが接続時に送るオプション — は見えないため、それらは自分で確認してください。
判定される設定があなたにとって何を意味するかは、下の
[ソースのタイムゾーンとサーバー設定](#ソースのタイムゾーンとサーバー設定) で説明します。

### すべてのオブジェクトが 1 つの分類を受け取る

Evaluation が評価する各オブジェクトは、3 つの分類のいずれか 1 つを受け取ります。
画面にはラベルが表示され、エクスポートしたレポートには括弧内の値が表示されます。

| 分類 | 意味 | すること |
|---|---|---|
| **Automatic** (`AUTO`) | 主キーを持つ通常のテーブルのように、何もしなくても変換されます。 | 何もありません。 |
| **Review needed** (`MANUAL`) | 変換はされますが、何かが失われるか、動作が変わります: 破棄される大文字小文字を区別しない照合順序、`ENUM`、大きな LOB や JSON の列、`ON UPDATE` タイムスタンプなどです。 | 判断します: そのまま受け入れるか、Schema Conversion でターゲット DDL を変更するか、カットオーバー前にアプリケーションを変更します。 |
| **Unsupported** (`UNSUPPORTED`) | そのままでは移行できません: 主キーのないテーブル、トリガー・ルーチン・イベント、バイナリ列の主キー、1 つの Aurora DSQL データベースが保持できる数を超えるオブジェクトなどです。 | 移行する前に、ソースを変更するか、オブジェクトをデータベースの外で作り直します。 |

検出結果が **Recommended** の注記であることもあります。その場合、変換は完全かつ正確で、注記は
Aurora DSQL でうまく動かすための助言です: 無視した場合に失われるのは性能であって正確性ではなく、
オブジェクトの工数にも準備度スコアにも加算されません。最もよく見かけるのは、値を生成し続ける生成キー
([主キー戦略を選ぶ](#主キー戦略を選ぶ)) と、保持される外部キー ([外部キー](#外部キー)) の 2 つです。

オブジェクトは、その検出結果の中で最も厳しい分類を受け取り (**Recommended**
の注記は数えません)、すべての検出結果がその下に一覧されます。どのルールにも一致しないオブジェクトは
**Automatic** です。

### レポートを読む

レポートは上から順に読みます:

- **Migration readiness** は、オブジェクトごとの作業を 0〜100 で評価し、バンドを付けます。
- **Compatibility by object kind** は、種類ごとに **Automatic**、**Review needed**、**Unsupported**
  のオブジェクトを数えます。
- **Objects by importance (most critical first)** は、
  すべてのオブジェクトを種類ごとにまとめて一覧します。オブジェクトを展開すると、検出結果ごとの
  **Risk** (フラグが立った理由。助言の場合は **Note**) と **Recommendation**
  (すべきこと)、そしてオブジェクトの工数 (**Simple (< 2h)**、**Medium (2–6h)**、
  **Significant (> 6h)**) が表示されます。**Classification** と **Estimated manual effort**
  のフィルターで一覧を絞り込め、検索ボックスはオブジェクト名またはルール id に一致します。
- **Target analysis (Aurora DSQL)** は、ターゲットにすでに存在するオブジェクト
  (**Objects already exist on target**) と、そこで別のオブジェクトが保持している名前
  (**Names held on the target by another object**) を一覧するため、
  適用が予期しない衝突で途中で失敗することはありません。それぞれをスキップするか置き換えるかは、
  適用するときに選びます ([確認・編集・適用](#確認編集適用))。
- **Export:** **HTML**、**JSON**、**Text** のレポートは、
  すべてのオブジェクトとターゲット分析を一覧します。内容は決定論的で、AI の出力は含みません。

**準備度スコア。** **Automatic** のオブジェクトのコストは 0 で、それ以外のオブジェクトは工数の重み
(Simple 1、Medium 3、Significant 7。見積もりのない **Unsupported** のオブジェクトは Significant
として数えます) がコストになります。**Recommended** の注記はコストになりません。スコアは、
最悪の場合に対するそのコストの割合を 100 から引いた値で、作業が必要なオブジェクトが 1 つでもある間は
100 未満のままです。時間の見積もりとしてではなく、作業の優先順位付けと計画に使ってください。
バンドはまずブロッカーで決まるため、大きなソースでもブロッカーが平均に埋もれることはありません:

| バンド | 条件 |
|---|---|
| **Blocked** (赤) | マイグレーションが Aurora DSQL のデータベース制限を超える: 1 つのデータベースにテーブル 1,000 個、スキーマ 10 個、またはビュー 5,000 個を超える (ターゲットクラスターに既にあるものを含む)。解決するまでは他のことに意味がないため、スコアに関係なくこのバンドになります。 |
| **Unsupported objects** (アンバー) | **Unsupported** のオブジェクトが 1 つ以上あり、スコアだけなら Ready または Low effort になる場合。(スコアがそれより低い場合は、下の工数バンドがそのラベルのまま使われます。) |
| **Ready** / **Low effort** (緑) | スコア ≥ 90 / ≥ 70 で、**Unsupported** のオブジェクトがない。 |
| **Moderate effort** / **Significant effort** (アンバー) | スコア ≥ 45 / ≥ 1。 |
| **High effort** (赤) | スコア 0。 |

### レポートの活用方法

重大度の高い順に、上から順にリストを処理します:

1. **すべての Unsupported 項目を解決します** (`UNSUPPORTED`)。
   これらはマイグレーションをブロックします。主キーがない場合は追加し、
   トリガー・ルーチン・イベントはアプリケーションへ移し (スケジュールは Amazon EventBridge Scheduler
   へ)、Aurora DSQL がインデックスを作れない主キーを持つテーブルはキーを付け直します。
   それぞれの対処は [§2.4](#24-検出結果リファレンス) に示します。
2. **すべての Review needed 項目を判断します** (`MANUAL`)。各項目は、何が失われるか変わるのか、
   そして何をすべきかを示します: そのまま受け入れるか、Schema Conversion でターゲット DDL
   を変更するか、カットオーバー前にアプリケーションを変更します。過大な LOB や JSON の列
   (`OVERSIZED_LOB`) は、まず Data Migration の前提条件チェック (または
   **Check sizes**、[§13.5](13-excluding-tables-and-columns.md#135-特大-lob-の候補と-check-sizes))
   を実行し、値がすでに上限を超えている場合にのみ対処します: その内容を外部ストレージ (例:
   Amazon S3) へ移すか、列を除外します (その場合、ツールはそこに NULL を書き込みます)。
3. **Automatic 項目** (`AUTO`) については何もする必要がなく、**Recommended**
   の注記は任意の助言です。

> **ヒント:** 1 つの修正が当てはまるすべてのオブジェクトを見るには、そのルール id (`BYTEA_KEY` など)
> を一覧の検索ボックスに入力します。

### 最もよく出会う検出結果

- **主キーのない** テーブル (`NO_PRIMARY_KEY`) や、バイナリ列の主キー (`BYTEA_KEY`): ソースで
  キーを追加するか付け直します。
- **トリガー、ルーチン、イベント**: 移行されないので、ロジックをアプリケーションで作り直します。
- **テキストの比較が変わる** (`CI_COLLATION`): Aurora DSQL はテキストをバイトで比較するため、
  大文字小文字を区別しない MySQL の照合順序では `=`、`LIKE`、`UNIQUE` が大文字小文字を区別するように
  なります。
- **破棄されたデフォルト**、または MySQL の `ON UPDATE CURRENT_TIMESTAMP` (`DEFAULT_DROPPED`、
  `ON_UPDATE_TIMESTAMP`): アプリケーションで値を設定します。
- MySQL で `boolean` になる `TINYINT(1)` (`TINYINT_BOOLEAN`) と、ゼロの日付や不正な日付
  (`ZERO_DATES`)。

すべての検出結果を、ソースエンジン別に対処とともに [§2.4](#24-検出結果リファレンス) にまとめています。AI アシストは
検出結果の説明や修正案の下書きを手伝います ([§2.5](#25-ai-アシスト-オプション))。

---

## 2.2 Schema Conversion — DSQL DDL の生成・適用

評価結果を手にしたら、Schema Conversion がソーススキーマを Aurora DSQL の DDL
に変換し、ターゲットに適用します:

1. **Object browser** でオブジェクトを **選択** します: スキーマまたはオブジェクトにチェックを入れ
   (または **Select all**)、**Generate DDL for selected** をクリックします。
2. 各オブジェクトの DDL を **確認** し、主キー戦略を選び、検出結果が求める箇所でターゲット DDL
   を編集します。
3. DDL をターゲットに **適用** します。1 つずつでも、すべてを一括でも適用できます。

**Reset all** は生成した DDL と編集内容を破棄するため、新しい選択で生成し直せます。選択は、
ターゲットにどのオブジェクトを作成するかを決めます。代わりにデータ移行からテーブルや列を外すには、
Data Migration の **Exclude tables & columns...** を使います
([第 13 章](13-excluding-tables-and-columns.md)): 除外した列は Aurora DSQL
に残り、ツールが書き込むすべての行でその列は NULL になります。
ターゲットのテーブルがすでに存在する場合 (以前に適用した、または手作業で作成した場合)、
**Skip conversion & continue to Data Migration** で次のステップに進めます。Data Migration
は、ターゲットのテーブルがあるテーブルだけをロードします。

### 変換が代わりに行ってくれること

変換のほとんどは、あなたの作業を必要としません。MySQL のスキーマは Aurora DSQL の PostgreSQL
方言に変換され、PostgreSQL のスキーマはほぼそのまま保たれます。列型の要点は次のとおりです
(完全なマッピングは [§2.3](#23-型マッピング-リファレンス) にあります):

| | MySQL ソース | PostgreSQL ソース |
|---|---|---|
| 変わるもの | 各型は Aurora DSQL の型にマッピングされます: `TINYINT(1)` → `boolean`、`DATETIME` → `timestamp`、`BLOB` → `bytea`、`ENUM` → CHECK 付きの `text`。 | ほとんどの型はそのままです。Aurora DSQL にない型は、同じデータを保持する型に変更されます: 配列 → `jsonb`、`money` → `numeric(23,4)`、`inet` → `text`。 |
| Review needed | `TINYINT(1)`、`BIT`、`YEAR`、`TIME`、`ENUM`/`SET`、空間型、`MEDIUMTEXT`/`LONGTEXT`/`MEDIUMBLOB`/`LONGBLOB`/`JSON`、大文字小文字を区別しない照合順序 | 精度のない `numeric`、変更されるすべての型、`bytea`/`json`/`jsonb`、長すぎる `varchar(n)`/`char(n)`、`C` 以外の照合順序 |
| Unsupported | — | composite 型、domain、ユーザー定義の範囲型、拡張機能の型 (`citext`、`hstore`、PostGIS、pgvector)、システム型 (`oid`、`pg_lsn`、…) |
| 完全なマッピング | [MySQL → Aurora DSQL](#mysql--aurora-dsql) | [PostgreSQL → Aurora DSQL](#postgresql--aurora-dsql) |

変換が行うその他のこと:

- **デフォルト** は引き継がれます。これが最も重要なのは `NOT NULL` 列です (その列を省略した `INSERT`
  が引き続き動作します)。Aurora DSQL に対応のないデフォルトは破棄して報告され
  (`DEFAULT_DROPPED`)、MySQL の `ON UPDATE CURRENT_TIMESTAMP` は再現されません。
- **時計のデフォルト** (と、時計を読む MySQL のビュー) は、
  自動的にソースのタイムゾーンで書き込まれます
  ([ソースのタイムゾーンとサーバー設定](#ソースのタイムゾーンとサーバー設定))。
- **生成列。** PostgreSQL の `STORED` 生成列は保持され、Aurora DSQL が値を計算します
  (書き込みではその列を省く必要があり、Full Load と CDC はそうします)。MySQL の生成列と PostgreSQL
  の `VIRTUAL` 生成列は通常の列になります: 値はコピーされますが、
  カットオーバー後は何も値を計算しません。MySQL の式は、変換したうえでターゲット DDL に
  `GENERATED ALWAYS AS (…) STORED` として再追加できます。適用する前に決めてください: Aurora DSQL
  は後から生成列を通常の列に変えることはできますが、既存の列を生成列にすることはできません。
- **ビュー。** MySQL のビューは、MySQL の日付・文字列・数値・JSON
  の振る舞いも含めて変換され、PostgreSQL のビューは PostgreSQL
  が保存しているとおりにコピーされます。フラグが付いたビューは、
  適用する前にソースと並べて確認してください (`VIEW_NOT_CONVERTED`、または結果が異なるという注記。
  PostgreSQL では、型が変わる列を読むビューに `VIEW_REMODELLED_COLUMN`)。
- **インデックス** はデータのロードの後に構築されます (`CREATE INDEX ASYNC`)。MySQL の FULLTEXT と
  SPATIAL のインデックスは作成されず、PostgreSQL の GIN や GiST のインデックスは btree
  になり、その検索にはもう使えません。PostgreSQL の `UNIQUE … NULLS NOT DISTINCT` インデックスは
  `NULLS NOT DISTINCT` を保持します。
- **パーティショニング** は削除されます: Aurora DSQL は主キーによって自ら行を分散し、
  パーティションは 1 つのテーブルになります。
- **複数の MySQL データベース** は、1 つの Aurora DSQL データベースの中で、それぞれ
  1 つのスキーマになります。
- **名前** は引用符で囲むことで綴りどおりに保たれます
  (大文字と小文字が混在する名前に注意してください: `MIXED_CASE_IDENTIFIERS`)。
- **外部キー** は保持され、ロードの後に適用されます ([外部キー](#外部キー))。

### 主キー戦略を選ぶ

各テーブルのカードには **Primary key** の選択があります。適用する前に決めてください: Aurora DSQL
にテーブルができるとそのキーは変更できず、後から変えるには、CDC を止めた状態で **Replace**
と再ロードが必要です ([最終状態](#最終状態))。

| 戦略 | 提示される条件 | アプリケーションにとっての意味 |
|---|---|---|
| **Server-generated (IDENTITY)** | 1 列の生成キー (MySQL の `AUTO_INCREMENT`、PostgreSQL の `serial` または identity) の場合。キーが `bigint` に収まるときの既定 | Aurora DSQL がキャッシュ付きの `bigint` identity としてキーを生成し続けます: 欠番が生じ、値の順序は緩くなります。Full Load はソースの id を保ちます。MySQL では 3 つの挿入の慣用句が動作しなくなります: キーへの明示的な `NULL` や `0`、`LAST_INSERT_ID()` (`RETURNING` を使います)、そしてアプリケーションが自分で指定する id (identity を進めません) です。 |
| **Keep source PK** | 常に。ソースが生成しないキーと、`bigint` に収まらない MySQL の `BIGINT UNSIGNED` キーでは既定 | キーは型を保ち、Aurora DSQL では何も値を生成しません: ソースが生成していたキーでは、アプリケーションが挿入のたびに値を指定する必要があります。 |
| **Composite key** | キーの先頭に置ける高カーディナリティの列がテーブルにある場合 | **Leading column (high-cardinality)** で選んだ列がキーの先頭に置かれ (例: `(customer_id, id)`)、書き込みが Aurora DSQL のパーティションに分散されます ([§7.1](07-performance-and-tuning.md#プライマリキー戦略--ホットパーティションの回避))。アプリケーションのクエリ、結合、upsert は完全なキーを使う必要があり、元のキーの一意性は UNIQUE インデックスが保ちます。CDC を使う場合は、すべてのキー列がキャプチャされ続ける必要があります。値が決して変わらない列を選んでください: ツールはそれを確認できません。 |

Full Load が完了した後と、カットオーバー時にもう一度、ツールは各 identity をロード済みの id
の先へ進めるため、カットオーバー後のアプリケーションの最初の挿入が衝突することはありません
([§3.4](03-full-load.md#34-バッチ化冪等並列数を制限したロード)、
[§10.3](10-conclusion.md#103-カットオーバー-アプリケーションを-dsql-へ切り替える))。

### 外部キー

Aurora DSQL は外部キーを強制し、ツールは既定で外部キーを保持します (**Generate DDL for selected**
の横の **Preserve foreign keys**)。Schema Apply が外部キーを作成することはありません:
ロードは親テーブルと子テーブルを、親を子より先に書くという順序なしに書き込み、CDC
はテーブルをまたいで任意の順序で変更を適用するため、強制されたキーは、
早く届いただけの行を拒否してしまうからです。各テーブルのキーは専用の読み取り専用ペイン
**Foreign keys — applied by a separate action, not at Schema Apply**
に表示されます。
**Schema Apply 直後にターゲットへ外部キーが無いのは想定どおりで、失敗ではありません。**

**いつ適用されるか:**

- **Full load only** — ロードが終わった後、Data Migration ステップの明示的な **Apply foreign keys**
  操作で適用します (ロードの最後に自動では適用されません)。
- **CDC** — Cut over ステップで、最終ドレインの後、アプリケーションを再ポイントする前に適用します
  ([§10.3](10-conclusion.md#103-カットオーバー-アプリケーションを-dsql-へ切り替える))。
  再実行しても安全です。

**先に追加し、後で検証する。** 各キーは、Aurora DSQL が受け付ける唯一の形である `NOT VALID` で追加されます: 行を 1 行も読まずに、子テーブルのサイズにかかわらず数秒で追加され、その時点からすべての新しい書き込みを強制します。その後 Aurora DSQL が、ロード済みの行をバックグラウンドで検証します。

**検証が失敗したとき。** キーは `NOT VALID` のまま残ります: それを破る新しい書き込みは引き続きすべて拒否しますが、ロード済みの行の一部が、Aurora DSQL にない親行を参照しています。画面には、キーを破る行の数とその行を一覧するクエリが表示されます。Aurora DSQL 上で、トランザクションあたり最大 3,000 行ずつ行を修正し (不足している親を追加する、行が既存の親か NULL を指すようにする、または行を削除する)、**Validate again** をクリックします。

**追加する前に数えるキー。** 親側が設計上 Aurora DSQL にそろっていないキー (参照される列が除外されている、または親か子のテーブルが移行されない) は、先に数えます。子行が Aurora DSQL にない親行を参照していれば、そのキーは理由と行数を示す注記とともにスキップされます: 追加すると、そうした親を指すアプリケーションの書き込みを拒否してしまうためです。

**自分で追加・名前変更したキー。** ターゲット DDL で追加または名前を変更した外部キーも、ほかのキーと同じように適用・検証されます。名前は Aurora DSQL が保存するものを使います: 引用符のない名前は小文字で (`FK_Orders_Cust` は `fk_orders_cust`)。

**コスト。** 強制されたキーは、関係するテーブルへのすべての書き込みに読み取りを追加し
(書き込みの多いテーブルは先にベンチマークしてください)、競合は再試行可能な `40001`
として返ります。`CASCADE`、`SET NULL`、`SET DEFAULT` のアクションは Aurora DSQL
のトランザクションあたり 3,000 行の上限に算入されるため、それより大きいカスケードは失敗します:
子行の数に上限がない場合は `NO ACTION` か `RESTRICT` を優先してください。

**MySQL ソースで CDC を使う場合: カスケードはギャップです。** MySQL は `ON DELETE` / `ON UPDATE` の
`CASCADE`、`SET NULL`、`SET DEFAULT` をエンジン内部で実行し、
その子の変更はバイナリログに決して現れないため、CDC
は孤児化した子行や古くなった子行を残すことがあります (`FK_CASCADE_CDC_GAP`、**Review needed**)。
アクションを子行に対する明示的な文に置き換えるか、最終比較の前にソースへの書き込みを止めてください。
カットオーバーでは **I've cut over** が、そうしたキーと、MySQL が Aurora DSQL
より緩く一致させるテキスト列のキー (大文字小文字を区別しない照合順序、または `VARCHAR` での
`utf8mb4_bin` などの PAD SPACE の照合順序) が検証されるまで、
またはその検査なしでカットオーバーすると選ぶまで待ちます
([§10.3](10-conclusion.md#103-カットオーバー-アプリケーションを-dsql-へ切り替える))。

**PostgreSQL ソース。** キーは、`MATCH FULL`、`DEFERRABLE`、`ON DELETE SET NULL (…)`
の列リストなど、宣言している句を保持します。`NOT ENFORCED` または `PERIOD` で宣言された
PostgreSQL 18 のキーは作成されません (`PG_FK_NOT_ENFORCED`、`PG_FK_PERIOD`)。

**整合性をアプリケーション側で強制したい場合は?** **Preserve foreign keys**
をオフにします。するとツールはすべての経路から外部キーを除外しますが、関係は Validation
の孤児チェックのために引き続き保持します。この選択は再接続後も記憶されます。

### 確認・編集・適用

**Generate DDL for selected** の後、各オブジェクトはソース DDL と Aurora DSQL の DDL
を並べて表示し、変換が変更した箇所を強調表示して、**Conversion warnings** と **Recommendations**
を一覧します。

- 検出結果に対処するには、ターゲット DDL を **Edit** で編集し (例: ほかの数値を持つ `TINYINT(1)`
  列を `smallint` に変更する)、**Done** をクリックします。**Reset to generated**
  は編集を破棄します。
- AI アシストを有効にしていれば、AI DBA が変更の下書きを作成できます
  ([オプションの AI アシスト](#オプションの-ai-アシスト-amazon-bedrock))。
- **Apply to target** は 1 つのオブジェクトを適用します。
  オブジェクトがターゲットにすでに存在する場合、ツールはその場で **Replace**、**Skip**、**Cancel**
  のどれにするかを尋ねます: これが、DDL を変更した後にテーブルを再適用する方法です (Skip
  では古いテーブルが残ります)。
- **Apply generated DDL to target** カードは、生成したすべてのオブジェクトを適用します。
  **When an object already exists on the target** で **Skip if the object already exists** (既定) か
  **Replace existing objects (destructive)** を選び、**Apply all N generated objects to target**
  をクリックします。Replace は既存の各オブジェクトを削除して作り直すため、
  テーブルはその行を失います: ツールは削除するものを一覧し、**Confirm REPLACE** を求めます。
- **Apply results** は、各オブジェクトを作成・スキップ・失敗のいずれかとして示します。
  失敗したオブジェクトの DDL を修正し、**Retry failed (N)** をクリックします。

外部キーは適用に含まれず ([外部キー](#外部キー))、CDC
がターゲットにストリーミングしている間は適用がブロックされます ([最終状態](#最終状態))。

### 最終状態

Schema Conversion の後は、**Aurora DSQL のターゲットスキーマが確定** します: これが、Full Load
が書き込み、CDC がストリーミングする先のスキーマです。CDC は後続のソース DDL を
**伝播しません**。したがって CDC の実行中にソーススキーマを変更した場合は、対応する変更を
Aurora DSQL に自分で行う必要があります — CDC のストリーミング中にここで再適用するのではありません
(その間は **Replace** を含めて Apply がブロックされ、Replace はテーブルを削除します)。CDC
を停止した状態では、型・キーの変更は作り直しです: ここで **Replace**、次に **Drop & reload**
([第 4 章 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-は-スキーマ-ではなく-データ-を複製する--重要)
を参照)。

### アプリケーションのクエリを確認する

Schema Conversion が変換するのはスキーマであり、アプリケーションが送る SQL ではありません。MySQL
のクエリの変換、ターゲットでの読み取り専用のクエリのテスト、そして
AI アシストを有効にしている場合のチューニングには、サイドバーにあるオプションのツール
**Query Converter** を使います ([第 9 章](09-query-validation.md))。どちらのエンジンでも、
Aurora DSQL の楽観的並行性の下では動作が変わる `SELECT … FOR UPDATE` を確認してください。

---

## 2.3 型マッピング (リファレンス)

ソースの各列型に対して Schema Conversion が書き込むものと、それについて知っておくべきことです。
Aurora DSQL 自体の制限 (キー、インデックス、列、値のサイズ、データベースあたりのオブジェクト数) は
[第 6 章 §6.1](06-limitations.md#61-aurora-dsql-の機能制限スキーマがこれらに適合している必要があります)
にあります。

### MySQL → Aurora DSQL

Schema Conversion は各列を下の Aurora DSQL の型で書き、Full Load と CDC
は各値を同じように格納します: CDC はすべての MySQL の型を運びます。値をそのまま格納できない場合 (`2`
を持つ `TINYINT(1)`、負の `TIME`)、別の値を書き込むのではなく、Full Load はテーブルを失敗させ、CDC
はその行をデッドレターにします。MySQL の表示幅 (`INT(11)`) と `ZEROFILL` は MySQL
が数値をどう表示するかを変えるだけなので、削除されます。

#### 整数型

<!-- type-map: mysql -->
| MySQL 型 | Aurora DSQL 型 | 分類 | 知っておくべきこと |
|---|---|---|---|
| `TINYINT`, `TINYINT UNSIGNED` | `smallint` | AUTO | PostgreSQL には 1 バイトの整数がありません。 |
| `TINYINT(1)` (`BOOL`, `BOOLEAN`) | `boolean` | MANUAL `TINYINT_BOOLEAN` | MySQL の boolean の慣例です: `0` と `1` は `false` と `true` になります。それ以外の値は拒否されます。列にほかの数値がある場合は、適用する前に Schema Conversion で型を `smallint` に変更してください。`TINYINT(1) UNSIGNED` は通常の `smallint` です。 |
| `SMALLINT` | `smallint` | AUTO | |
| `SMALLINT UNSIGNED`, `MEDIUMINT`, `MEDIUMINT UNSIGNED`, `INT` (`INTEGER`) | `integer` | AUTO | すべての値が収まるよう、必要に応じて拡幅されます。 |
| `INT UNSIGNED`, `BIGINT` | `bigint` | AUTO | |
| `BIGINT UNSIGNED` | `numeric(20,0)` | AUTO | これより広い整数型はありません: `numeric(20,0)` は 18,446,744,073,709,551,615 までのすべての値を保持します。 |
| `BIT(n)` | `smallint`, `integer`, `bigint` or `numeric(20,0)` | MANUAL `BIT_TYPE` | Aurora DSQL にはビット列型がありません。列はすべての `BIT(n)` の値を保持できる最小の型になり (`BIT(15)` までは `smallint`、`BIT(31)` までは `integer`、`BIT(63)` までは `bigint`)、ビットが表す数値を格納します (`b'101'` は `5`)。ビット演算子や `b'…'` リテラルを使うクエリは書き換えてください。 |

**自動増分キー。** 主キー全体である `AUTO_INCREMENT` 列は `bigint GENERATED BY DEFAULT AS IDENTITY`
になり、Aurora DSQL が値を生成し続けます (欠番がないことも、厳密に増加することも、
もう保証されません)。複合キーの一部である列や `BIGINT UNSIGNED`
のキーは、値を生成せずに型を保つため、アプリケーションが値を指定する必要があります。
`BIGINT UNSIGNED` のキーでは、代わりに **Server-generated (IDENTITY)** を選ぶこともできます:
その場合キーは `bigint` になり、キーが 9,223,372,036,854,775,807 を超える行はロードされません。
[主キー戦略を選ぶ](#主キー戦略を選ぶ) を参照してください。

#### 固定小数点と浮動小数点

<!-- type-map: mysql -->
| MySQL 型 | Aurora DSQL 型 | 分類 | 知っておくべきこと |
|---|---|---|---|
| `DECIMAL(p,s)`, `DECIMAL(p,s) UNSIGNED` (`NUMERIC`, `DEC`, `FIXED`) | `numeric(p,s)` | AUTO | 精度とスケールは保たれます: MySQL の最大の `DECIMAL(65,30)` は、Aurora DSQL の上限 1000 に十分収まります。`UNSIGNED` は削除されるため、ターゲットは負の値も受け付けます。 |
| `FLOAT`, `FLOAT(M,D)`, `FLOAT UNSIGNED` | `real` | AUTO | `(M,D)` と `UNSIGNED` は削除されます: 値はそのままコピーされますが、新しい値は `D` 桁の小数に丸められることも、負の値として拒否されることもなくなります。 |
| `DOUBLE`, `DOUBLE(M,D)`, `DOUBLE UNSIGNED` (`REAL`, `DOUBLE PRECISION`) | `double precision` | AUTO | `FLOAT` と同じです。 |

#### 日付と時刻

<!-- type-map: mysql -->
| MySQL 型 | Aurora DSQL 型 | 分類 | 知っておくべきこと |
|---|---|---|---|
| `DATE` | `date` | AUTO | ゼロの日付や不正な日付 (`0000-00-00`、`2024-02-30`) には Aurora DSQL の値がありません: ソースがそれを保持しうる場合は、Full Load と CDC がそれをどう扱うかを Evaluation の `ZERO_DATES` 項目が示します。 |
| `DATETIME`, `DATETIME(fsp)` | `timestamp` | AUTO | 同じ壁時計の値です: タイムゾーンを適用したりずらしたりしません。小数秒は保たれます。ゼロの日付は `DATE` と同じです。 |
| `TIMESTAMP`, `TIMESTAMP(fsp)` | `timestamptz` | AUTO | 同じ時点を UTC で格納します。小数秒は保たれます。ゼロの日付は `DATE` と同じです。 |
| `TIME`, `TIME(fsp)` | `time` | MANUAL `TIME_TYPE` | MySQL の `TIME` は期間 (`-838:59:59` から `838:59:59`) ですが、Aurora DSQL の `time` は時刻です。`00:00:00` 未満または `24:00:00` を超える値は拒否されます。列が期間を保持する場合は、ロードする前に Schema Conversion で型を `interval` に変更してください (キーやインデックスがその列を使う場合は `text` に)。 |
| `YEAR` | `smallint` | MANUAL `YEAR_TYPE` | 年の数値 (`2024`) として格納されます。 |

#### 文字列、バイナリ、構造化

<!-- type-map: mysql -->
| MySQL 型 | Aurora DSQL 型 | 分類 | 知っておくべきこと |
|---|---|---|---|
| `CHAR(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`, `TEXT` | `text` | AUTO | |
| `MEDIUMTEXT`, `LONGTEXT` | `text` | MANUAL `OVERSIZED_LOB` | Aurora DSQL の 1 MiB の上限 (圧縮後のサイズで数えます) を超える値は格納できません: Full Load はその行を隔離し、CDC はその行をデッドレターにします。Data Migration の前提条件チェックが、ロードする前にそのような値を見つけます。 |
| `BINARY(n)`, `VARBINARY(n)`, `TINYBLOB`, `BLOB` | `bytea` | AUTO | バイトはそのままコピーされます。 |
| `MEDIUMBLOB`, `LONGBLOB` | `bytea` | MANUAL `OVERSIZED_LOB` | 1 MiB を超える値は格納できません (Aurora DSQL は `bytea` を圧縮しません): Full Load はその行を隔離し、CDC はその行をデッドレターにします。 |
| `ENUM('a','b',…)` | `text`, with a CHECK on the values | MANUAL `ENUM_SET_TYPE` | CHECK が許可される値を保ちます。並べ替えと比較は宣言順ではなくアルファベット順になり、MySQL の空文字列のエラー値 (`''`) は拒否されます。 |
| `SET('a','b',…)` | `text` | MANUAL `ENUM_SET_TYPE` | カンマ区切りのメンバー (`'a,b'`) として格納され、CHECK はありません: 許可される値はもう強制されません。 |
| `JSON` | `json` | MANUAL `OVERSIZED_LOB` | 同じ JSON テキストです。1 MiB (圧縮後) を超えるドキュメントは格納できません: Full Load はその行を隔離し、CDC はその行をデッドレターにします。 |

**大文字小文字を区別しない照合順序。** MySQL のテキスト列のほとんどがこれを使います
(`utf8mb4_0900_ai_ci` は MySQL 8 の既定です)。照合順序は引き継がれません: Aurora DSQL
はテキストを大文字小文字を区別して比較するため、すべての値が正確に移行されても、`=`、`LIKE`、
`ORDER BY`、`UNIQUE` の動作が変わります (`bob` があるときに `Bob` を拒否していた `UNIQUE`
列は、両方を受け付けます)。Evaluation はそのようなテーブルを MANUAL としてフラグ付けします
(`CI_COLLATION`)。`_bin` や `_cs` の照合順序はすでに大文字小文字を区別するため、フラグは付きません。

#### その他

<!-- type-map: mysql -->
| MySQL 型 | Aurora DSQL 型 | 分類 | 知っておくべきこと |
|---|---|---|---|
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | `bytea` | MANUAL `SPATIAL_TYPE` | Aurora DSQL には空間型がありません。ジオメトリは SRID なしの WKB バイトとして保たれ、空間関数と `SPATIAL` インデックスは使えません。 |

**Aurora DSQL がインデックスを作れない列。** `bytea` 列 (`BINARY`、`VARBINARY`、`BLOB`
系の型、または空間型から変換されたもの) は、主キーやインデックスの一部にできません:
そうした列を含む主キーはテーブルを UNSUPPORTED にし、そうした列のインデックスは作成されません
(`BYTEA_KEY`)。`interval` 列も同じなので、キーに使われていて期間を保持する `TIME` 列は `text`
にします。

### PostgreSQL → Aurora DSQL

Aurora DSQL は PostgreSQL 互換なので、ほとんどの型はそのまま保たれます。Aurora DSQL
にない型は、同じデータを保持する型に変更されるか (MANUAL:
アプリケーションは新しい型に合わせる必要があります)、そのような型がない場合はそのまま残され、
Schema Conversion で列を変更するまでテーブルは UNSUPPORTED と評価されます。Full Load
は、変換された列のすべての値を運びます。

**CDC** 列は、CDC が Full Load と異なる箇所を示します。Evaluation はこれらの列を
`CDC_UNCARRYABLE_COLUMN` として報告します。これが問題になるのは CDC で複製する場合だけです:

- *NULL で届く*: その列が CDC の変更イベントに含まれないため、CDC が挿入する行は NULL
  (または列のデフォルト) を受け取り、更新では古い値が残ります。デフォルトのない `NOT NULL`
  列では、それらの変更は代わりに失敗します。その場合のことは `CDC_UNCARRYABLE_COLUMN`
  項目が説明します。
- *デッドレターになる*: その列を設定するすべての変更がデッドレターになり、ターゲットに届きません。
- *別の値で届く*: CDC は Full Load とは異なる値を書き込みます。

*NULL で届く* と *別の値で届く* を検出できるのは CHECKSUM の検証だけです。それぞれの回避方法は
[第 4 章](04-cdc-and-dsql-constraints.md) と [§6.2](06-limitations.md#62-移行プロセスの制限)
にあります。

#### 整数型

<!-- type-map: postgres -->
| PostgreSQL 型 | Aurora DSQL 型 | 分類 | CDC | 知っておくべきこと |
|---|---|---|---|---|
| `smallint`, `integer`, `bigint` (`int2`, `int4`, `int8`) | `smallint`, `integer`, `bigint` | AUTO | — | 同じ型です。 |

**serial 列と identity 列。** `smallserial`、`serial`、`bigserial` と identity
列は、シーケンスが値を供給するこれらの整数型です。主キー全体であるものは
`bigint GENERATED BY DEFAULT AS IDENTITY` になり、Aurora DSQL が値を生成し続けます
([主キー戦略を選ぶ](#主キー戦略を選ぶ) を参照)。
複合キーの一部や別の列ではシーケンスが引き継がれないため、
アプリケーションが値を指定する必要があります (Evaluation はそのようなキーでない列を示します:
`NON_KEY_SEQUENCE`)。

#### 固定小数点と浮動小数点

<!-- type-map: postgres -->
| PostgreSQL 型 | Aurora DSQL 型 | 分類 | CDC | 知っておくべきこと |
|---|---|---|---|---|
| `numeric(p,s)` (`decimal(p,s)`) | `numeric(p,s)` | AUTO | — | 精度とスケールは保たれます (Aurora DSQL は PostgreSQL と同じく 1000 まで受け付けます)。 |
| `numeric` (`decimal`), with no precision | `numeric` | MANUAL `PG_UNCONSTRAINED_NUMERIC` | — | Aurora DSQL はこれを `numeric(18,6)` として格納します: 小数 6 桁を超える値は丸められ、整数部が 12 桁を超える値は収まりません。列がそのような値を保持する場合は、適用する前に Schema Conversion で明示的な `numeric(p,s)` を指定してください。 |
| `real`, `double precision` (`float4`, `float8`) | `real`, `double precision` | AUTO | — | 同じ型です。 |
| `money` | `numeric(23,4)` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 通貨書式のテキストではなく、正確な金額が保たれます。通貨の書式設定と `money` の演算はアプリケーションへ移します。 |

#### 日付と時刻

<!-- type-map: postgres -->
| PostgreSQL 型 | Aurora DSQL 型 | 分類 | CDC | 知っておくべきこと |
|---|---|---|---|---|
| `date`, `time without time zone`, `timestamp without time zone`, `timestamp with time zone` (`time`, `timestamp`, `timestamptz`) | `date`, `time`, `timestamp`, `timestamptz` | AUTO | — | 同じ型と精度です。 |
| `time with time zone` (`timetz`) | `timetz` | AUTO | — | 主キーやインデックスの一部にできません (`PG_UNINDEXABLE_COLUMN`)。 |
| `interval`, with or without fields or a precision (`interval day to second`, `interval(3)`) | `interval` | AUTO | — | フィールドは保たれ、精度は宣言から削除されます (値からは削除されません)。主キーやインデックスの一部にできません (`PG_UNINDEXABLE_COLUMN`)。 |

#### 文字列、バイナリ、構造化

<!-- type-map: postgres -->
| PostgreSQL 型 | Aurora DSQL 型 | 分類 | CDC | 知っておくべきこと |
|---|---|---|---|---|
| `character varying(n)`, `character varying`, `character(n)`, `text` (`varchar(n)`, `varchar`, `char(n)`) | `varchar(n)`, `varchar`, `char(n)`, `text` | AUTO | — | 同じ型です。Aurora DSQL では、値は `varchar` で最大 65,535 バイト、`char` で 4,096 バイト、`text` で 1 MiB を保持します。長さの制限がない列については、Evaluation がサイズの確認を推奨します (`OVERSIZED_LOB`)。 |
| `character varying(n)` with n over 65535, `character(n)` with n over 4096 | `text` | MANUAL `PG_CHARACTER_LENGTH` | — | Aurora DSQL はそのような宣言の長さを拒否します。`text` は 1 MiB までのすべての値を保持しますが、長さの制限はもう強制されません: アプリケーションがそれに依存している場合は `CHECK (length(col) <= n)` を追加してください。 |
| `bytea` | `bytea` | MANUAL `OVERSIZED_LOB` | — | バイトはそのままコピーされます。1 MiB を超える値は格納できず (Aurora DSQL は `bytea` を圧縮しません)、この列は主キーやインデックスの一部にできません (`BYTEA_KEY`)。 |
| `boolean`, `uuid` (`bool`) | `boolean`, `uuid` | AUTO | — | 同じ型です。 |
| `json`, `jsonb` | `json`, `jsonb` | MANUAL `OVERSIZED_LOB` | — | 1 MiB (圧縮後) を超えるドキュメントは格納できません: Full Load はその行を隔離し、CDC はその行をデッドレターにします。主キーやインデックスの一部にできません (`PG_UNINDEXABLE_COLUMN`)。 |
| `xml` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | ドキュメントのテキストは保たれますが、`xpath()` などの XML 関数はもう使えません。 |

#### その他

<!-- type-map: postgres -->
| PostgreSQL 型 | Aurora DSQL 型 | 分類 | CDC | 知っておくべきこと |
|---|---|---|---|---|
| A user-defined enum (`CREATE TYPE … AS ENUM`) | `text`, with a CHECK on the values | MANUAL `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL には `CREATE TYPE` がありません。CHECK が許可される値を保ち、デフォルトはテキストとして保たれます。並べ替えは enum の順序ではなくアルファベット順になります。 |
| A composite type (`CREATE TYPE … AS (…)`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL で届く | Aurora DSQL には `CREATE TYPE` がありません。Schema Conversion で列を `text` に変更してください (その場合 Full Load は、`(1,abc)` のような値のテキスト形式をコピーします)。 |
| A domain (`CREATE DOMAIN`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL は domain をサポートしますが、ツールは domain を作成しません: 先に Aurora DSQL で domain を作成するか、Schema Conversion で列を domain の基本型 (とその CHECK) に変更してください。CDC はこの列を基本型として扱います。 |
| `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | テキスト形式 (`[1,5)`) で保たれ、範囲演算子 (`@>`、`&&`) と関数 (`lower()`、`upper()`) はもう使えません。 |
| `int4multirange`, `int8multirange`, `nummultirange`, `tsmultirange`, `tstzmultirange`, `datemultirange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL で届く | 範囲型と同じです (テキスト形式は `{[1,3),[5,7)}`)。 |
| A user-defined range (`CREATE TYPE … AS RANGE`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL で届く | Schema Conversion で列を `text` に変更してください。 |
| `point` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | テキスト形式 (`(1,2)`) で保たれ、幾何演算子はもう使えません。 |
| `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL で届く | `point` と同じです。 |
| `inet`, `cidr`, `macaddr`, `macaddr8` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 正規化されたアドレスのテキストが保たれ、ネットワーク演算子と関数はもう使えません。 |
| `bit(n)`, with n of 2 or more | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | ビット列 (`'10101010'`) として保たれ、ビット演算子はもう使えません。 |
| `bit(1)` (`bit`), `bit varying(n)` (`varbit(n)`) | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | 別の値で届く | 上と同じです。CDC は `bit(1)` の値を `true`/`false` として書き込み、n より短い `bit varying(n)` の値は先頭をゼロで埋めて書き込みます。 |
| `bit varying` (`varbit`), with no length | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | デッドレターになる | 上と同じです。 |
| `tsvector`, `tsquery` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL で届く | テキストとして保たれ、全文検索の演算子とインデックスはもう使えません。 |
| `integer[]`, `text[]` and other arrays | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | — | JSON 配列として格納されます (`{1,2}` は `[1, 2]` になります): 配列の演算子と添字は JSON のものに置き換えてください。配列の列をキーとするテーブルは適用に失敗します (`PG_UNINDEXABLE_COLUMN`)。 |
| `numeric[]` (no precision), `time with time zone[]`, `bytea[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | デッドレターになる | 配列と同じです。`numeric[]` にはソースで精度を指定してください (`numeric(p,s)[]` は CDC が運びます)。 |
| `interval[]`, `money[]`, `xml[]`, `point[]`, `bit(n)[]`, `bit varying(n)[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL で届く | 配列と同じです。この表で *NULL で届く* とされているほかの型の配列も、NULL で届きます。 |
| `"char"`, `name`, `oid` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | PostgreSQL のシステム型です。Schema Conversion で列を `char(1)`、`varchar(63)`、または `bigint` に変更してください。 |
| `jsonpath`, `pg_lsn`, `xid`, `regclass` (and `regtype`, `regproc`, …) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL で届く | PostgreSQL のシステム型です。Schema Conversion で列を `text` に変更してください。 |
| Extension types: `citext`, `hstore`, `ltree`, PostGIS `geometry` and `geography` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL には拡張機能がありません。Schema Conversion で列を `text` に変更してください (その場合 Full Load は値のテキスト形式をコピーします)。変更後の `citext` 列は大文字小文字を区別して比較します。 |
| Extension types: pgvector `vector`, `halfvec`, `sparsevec`, and `cube` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL で届く | Schema Conversion で列を `jsonb` または `text` (`sparsevec` は `text`) に変更してください。Aurora DSQL での型が何であっても、CDC はこの列を運べません。 |

**照合順序。** Aurora DSQL はテキストを `C` 照合順序でのみ比較します。別の照合順序を持つ列
(`PG_COLLATION_DROPPED`) や、既定の照合順序が `C` でないデータベース (`PG_DATABASE_COLLATION`)
では、すべての値が正確に移行されても、移行後はテキストの並べ替えと比較が変わります。

**Aurora DSQL がインデックスを作れない列。** `bytea`、`json`、`jsonb`、`interval`、
`time with time zone` の列と、配列 (変換後は `jsonb`) は、主キーやインデックスの一部にできません:
そうした列を含む主キーがあるとテーブルは適用に失敗し、そうした列のインデックスは作成されません
(`BYTEA_KEY`、`PG_UNINDEXABLE_COLUMN`)。

---

## 2.4 検出結果リファレンス

Evaluation が報告する検出結果をソースエンジン別にまとめたリファレンスで、ソースのタイムゾーンとサーバー設定も含みます。レポートを処理しながら調べる用途で使ってください ([レポートの活用方法](#レポートの活用方法))。

### よくある検出結果と対処

以下は最もよく出会う検出結果で、あなたにとっての意味ごとにまとめています。画面ではそれぞれ、
関係するオブジェクトと値も示し、助言の全文を示します。
ソースエンジンごとに分けてあるので、**両方のエンジン** と自分のエンジンのブロックだけを読んでください。

タイムゾーンと、判定される MySQL の設定 (`SOURCE_TIME_ZONE`、`SQL_MODE`、
`EXPLICIT_DEFAULTS_FOR_TIMESTAMP`、`AUTO_INCREMENT_STEP`) は次のセクションで説明します。

#### 両方のエンジン

**マイグレーションをブロックするもの** (Unsupported)

| 検出結果 | 対処 |
|---|---|
| 主キーがない (`NO_PRIMARY_KEY`) | ソースで主キーを追加します (検出結果は昇格できる一意キーを示します): ツールはすべてのテーブルをそのキーでロード・複製・検証します。 |
| トリガー、ストアドプロシージャと関数、イベント。PostgreSQL のマテリアライズドビューと外部テーブル (`TRIGGER_UNSUPPORTED`、`PROC_PLPGSQL`、`EVENT_UNSUPPORTED`、`PG_UNSUPPORTED_RELATION`) | 移行されません。ロジックはアプリケーションで、スケジュールはデータベースの外 (例: Amazon EventBridge Scheduler と AWS Lambda) で、マテリアライズドビューはジョブが更新するテーブルとして作り直します。 |
| Aurora DSQL がインデックスを作れない列の主キー (`BYTEA_KEY`、`PG_UNINDEXABLE_COLUMN`) | ソースでテーブルのキーを付け直します (例: 値のテキスト形式やハッシュに)。そのような列は、MySQL の `BINARY`、`VARBINARY`、`BLOB`、空間型 (Aurora DSQL ではすべて `bytea`) と、PostgreSQL の `bytea`、`json`、`jsonb`、`interval`、`time with time zone`、配列です。そうした列のインデックスは作成されないため (**Review needed**)、`UNIQUE` インデックスは一意性を強制しなくなります。 |
| Aurora DSQL のデータベース制限の超過 (`TABLE_COUNT_LIMIT`、`SCHEMA_COUNT_LIMIT`、`VIEW_COUNT_LIMIT`) | ターゲットデータベースが、すでに保持しているものを含めて、1,000 個を超えるテーブル、10 個を超えるスキーマ、または 5,000 個を超えるビューを保持することになり、スコアは **Blocked** になります。マイグレーションを複数のクラスターに分けてください。 |
| テーブルまたは名前の制限の超過 (`TOO_MANY_COLUMNS`、`TOO_MANY_KEY_COLUMNS`、`IDENTIFIER_LENGTH`、`NAME_COLLISION`) | 255 を超える列、8 列を超える主キー、または (MySQL で) 先頭 63 バイトが一致する名前です: そのような列が 2 つあるとテーブルの `CREATE TABLE` が失敗し、そのようなテーブルやビューが 2 つあるとどちらも作成されません。ソースでテーブルや名前を変更してください。 |

**データを変更または拒否するもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| Aurora DSQL の 1 MiB の上限を超える値 (`OVERSIZED_LOB`) | ロードの前にサイズを確認してください ([レポートの活用方法](#レポートの活用方法) の手順 2): Full Load はそのような値を持つ行を隔離し、CDC はその行をデッドレターにします。 |
| Aurora DSQL にない型が変更される (`PG_UNSUPPORTED_TYPE`、`ENUM_SET_TYPE`、`BIT_TYPE`、`YEAR_TYPE`、`SPATIAL_TYPE`、`PG_CHARACTER_LENGTH`) | データは代替型で移行されます (PostgreSQL の配列は `jsonb`、enum は CHECK 付きの `text`、`money` は `numeric(23,4)`、MySQL の空間列は `bytea`): アプリケーションをその型に合わせてください。 |
| テキストの比較が変わる (`CI_COLLATION`、`PG_COLLATION_DROPPED`、`PG_DATABASE_COLLATION`) | Aurora DSQL はテキストをそのバイトで比較し (`C` 照合順序)、ソースの照合順序は引き継がれません。MySQL の大文字小文字を区別しない照合順序だった場合、`=`、`LIKE`、`ORDER BY`、`UNIQUE` は大文字小文字を区別するようになります (`Bob` と `bob` は 2 つの値になります)。PostgreSQL の別の照合順序だった場合は、テキストの並び順が変わります。それらのクエリとキーを確認してください。 |

**カットオーバー前にアプリケーションの変更が必要なもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| 破棄されたデフォルト、または MySQL の `ON UPDATE CURRENT_TIMESTAMP` (`DEFAULT_DROPPED`、`ON_UPDATE_TIMESTAMP`) | 値はアプリケーションが設定するようにしてください: その列を省略した `INSERT` は NULL を格納し (`NOT NULL` 列では拒否され)、更新してもタイムスタンプはもう更新されません。破棄されたデフォルトは、適用後に追加することもできます。 |
| 通常の列になる生成列 (`GENERATED_COLUMN`) | カットオーバー後は何も値を計算しません: アプリケーションで計算するか、適用する前に式を再追加してください ([変換が代わりに行ってくれること](#変換が代わりに行ってくれること))。PostgreSQL の `STORED` 列は保持されます。 |
| ソースの `CHECK` 制約 (`CHECK_CONSTRAINT_DROPPED`) | 引き継がれません。Aurora DSQL が受け付けるものはロードの後に 1 つずつ再追加するか (変換の注記がその文を示します)、規則をアプリケーションで強制してください。 |
| ツールが再現できないビュー (`VIEW_NOT_CONVERTED`、`VIEW_UNSUPPORTED_SQL`、`VIEW_REMODELLED_COLUMN`) | 変換したままでは適用に失敗するか、異なる結果を返します: 適用する前にターゲット DDL を編集するか、ロードの後に手作業で作り直してください。 |
| そのままの形では引き継がれないインデックス (`UNSUPPORTED_INDEX_TYPE`、`PG_INDEX_NOT_PRESERVED`、`EXPRESSION_INDEX`、`TOO_MANY_INDEXES`) | 全文検索、空間検索、GIN や GiST による検索はデータベースの外 (例: Amazon OpenSearch Service) へ移し、式インデックスはロードの後に作り直し (`CREATE INDEX ASYNC`)、セカンダリインデックスはテーブルあたり 23 個までにしてください。 |

#### MySQL のみ

**データを変更または拒否するもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| `TINYINT(1)` が `boolean` になる (`TINYINT_BOOLEAN`) | 0 と 1 以外の値は拒否されます: Full Load はテーブルを停止し、CDC はその行をデッドレターにします。列にほかの数値がある場合は、適用する前にターゲット型を `smallint` に変更してください。 |
| 期間として使われる `TIME` (`TIME_TYPE`) | 00:00:00 未満または 24:00:00 を超える値は拒否されます。期間を保持する列は、ロードする前にターゲット型を `interval` に変更してください。 |
| ゼロの日付や不正な日付 (`ZERO_DATES`) | Aurora DSQL にはそのような値がないため、Full Load と CDC は代わりに NULL、列のデフォルト、または `1970-01-01` を書き込みます ([§6.2](06-limitations.md#62-移行プロセスの制限))。検出結果のクエリでそれらを数え、ロードの前にソースで修正してください。 |

**カットオーバー前にアプリケーションの変更が必要なもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| 大文字と小文字が混在する名前 (`MIXED_CASE_IDENTIFIERS`) | Aurora DSQL は引用符のない名前を小文字に変換するため、`CustomerId` を引用符なしで書く SQL は何も見つけられません。そのような名前は綴りどおりに引用符で囲むか、Schema Conversion の前にソースで小文字に名前を変えてください (カットオーバー前にターゲットで変えてはいけません)。 |
| CDC での外部キーのカスケードアクション (`FK_CASCADE_CDC_GAP`) | CDC は MySQL のカスケードを複製しません: [外部キー](#外部キー) を参照してください。 |

#### PostgreSQL のみ

**マイグレーションをブロックするもの** (Unsupported)

| 検出結果 | 対処 |
|---|---|
| 行レベルセキュリティ (`PG_ROW_LEVEL_SECURITY`) | カットオーバー後は、テーブルにアクセスできるすべてのロールがすべての行にアクセスできます: ポリシーはアプリケーションで強制してください (検出結果がそれらを一覧します)。すべての行を読めるユーザー (スーパーユーザー、`BYPASSRLS` を持つロール、または `FORCE ROW LEVEL SECURITY` でないテーブルの所有者) で移行してください。そうでないと、ロードはそのテーブルで停止します。 |
| 代替型のない列型 (`PG_UNSUPPORTED_TYPE`) | composite 型、domain、拡張機能の型 (`citext`、`hstore`、PostGIS、pgvector)、またはシステム型です: 適用する前に、Schema Conversion で列のターゲット型を変更してください (例: `text` に)。 |

**データを変更または拒否するもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| 精度のない `numeric` (`PG_UNCONSTRAINED_NUMERIC`) | `numeric(18,6)` として格納されるため、小数 6 桁を超える部分は丸められ、整数部は最大 12 桁です。値にそれ以上が必要な場合は、適用する前に列に明示的な `numeric(p,s)` を指定してください。 |
| CDC が運べない列 (`CDC_UNCARRYABLE_COLUMN`) | CDC の場合のみ: 値が NULL や別の値で届くか、行がデッドレターになります。CDC をデプロイする前に、ソースで列の型を変更するか、列を除外するか、そのテーブルを Full load only で移行してください。 |

**カットオーバー前にアプリケーションの変更が必要なもの** (Review needed)

| 検出結果 | 対処 |
|---|---|
| キーではない `serial` 列や identity 列 (`NON_KEY_SEQUENCE`) | ターゲットには値を生成するものがないため、その列を省略した `INSERT` は拒否されるか (`23502`)、NULL を許す列では NULL を格納します。値はアプリケーションが指定するか、適用する前にターゲット DDL で列を `bigint GENERATED BY DEFAULT AS IDENTITY` と宣言し (列の型は後から変更できません)、カットオーバー前にロード済みの値の次から再開させてください。 |
| どの列にも所有されていないシーケンス (`PG_STANDALONE_SEQUENCE`) | 作成されません。カットオーバー前に作成し (検出結果がその文を示します)、カットオーバー時にソースの最終値の次へ進めてください: ツールはこれを同期しません ([§10.3](10-conclusion.md#103-カットオーバー-アプリケーションを-dsql-へ切り替える))。 |
| Aurora DSQL が宣言どおりに強制できない規則 (`PG_EXCLUSION_CONSTRAINT`、`PG_TEMPORAL_KEY`、`PG_FK_PERIOD`) | `EXCLUDE` 制約、`WITHOUT OVERLAPS` のキー、`FOREIGN KEY … PERIOD` は、カットオーバー後は強制されません: 規則をアプリケーションで強制するか、設計を変更してください。`WITHOUT OVERLAPS` の主キーを持つ大きなテーブルは、ロードする前にソースでそのキー列に btree インデックスを追加してください。 |
| `DEFERRABLE` の主キーまたは一意キー (`PG_DEFERRABLE_KEY`) | 即時に検査されるため、2 つの行に一時的に同じキー値を持たせる書き込み (入れ替えや番号の振り直し) は失敗します。CDC を使う場合、そのような主キーはテーブルの `REPLICA IDENTITY` になれません: `REPLICA IDENTITY DEFAULT` のままのテーブルは、ソースでキーを `NOT DEFERRABLE` で作り直すか、テーブルを `REPLICA IDENTITY FULL` に設定するまで、CDC の前提条件チェックで失敗します。 |
| インストール済みの拡張機能 (`PG_EXTENSION_UNSUPPORTED`) | Aurora DSQL には拡張機能がありません: アプリケーションからの呼び出しを置き換えてください (`gen_random_uuid()` は組み込みです)。 |

#### 助言、および不完全なレポート

**助言のみ** (Recommended)。マイグレーション自体のためにすることはありません:

- `AUTO_INCREMENT`: キーは値を生成し続けます ([主キー戦略を選ぶ](#主キー戦略を選ぶ))。Aurora DSQL
  で何も値を生成しない場合、この検出結果は代わりに **Review needed** になります。
- `FK_PRESERVED`: 外部キーは保持され、ロードの後に適用されます ([外部キー](#外部キー))。
- `PARTITIONED_TABLE`: パーティションは 1 つのテーブルになり、データは失われません。CDC
  を使う場合は、カットオーバーまでパーティションの保守 (drop、truncate、detach、attach)
  を止めてください: CDC はそれらの変更を運べません。
- `MULTIPLE_DATABASES` (MySQL): 各データベースは独自のスキーマになります。
  アプリケーションでスキーマ修飾した名前を使うか、アプリケーションの `search_path`
  を設定してください。
- `NAME_COLLISION` (インデックス):
  スキーマ内の別のテーブル・ビュー・インデックスが使っている名前のインデックスは、`<table>_<index>`
  として作成されます。
- `PG_FK_NOT_ENFORCED` (PostgreSQL 18): キーは作成されないため、
  ターゲットはソースと同じように動作します。そのようなキーを Aurora DSQL
  に強制させるには(Aurora DSQL にその形がない `PERIOD` で宣言されたキーは除きます)、そのキーの
  `ALTER TABLE … ADD CONSTRAINT … NOT VALID` をテーブルのターゲット DDL に追加してください。

**レポートが不完全なときは**、レポート自身がそう示します。MySQL
では、移行ユーザーが一覧できないオブジェクト (`SOURCE_VISIBILITY`:
`TRIGGER`、`EVENT`、`SHOW_ROUTINE` のいずれかがない場合で、最初の
2 つの権限はそれらのオブジェクトの変更も許可します) と、定義を読めないビュー
(`VIEW_DEFINITION_UNREADABLE`: `SHOW VIEW` がない場合) です。PostgreSQL
では、失敗したカタログの読み取り (`PG_CATALOG_UNREAD`) です。どちらでも、Evaluation
がカタログを読んでいる間に消えたテーブル (`CATALOG_CHANGED_DURING_READ`)
です。見えなかったオブジェクトは評価も移行もされません: 不足している権限を付与する
(または管理者としてそれらのオブジェクトを確認する) か、Evaluation を再実行してください。

### ソースのタイムゾーンとサーバー設定

Aurora DSQL のセッションは、別のタイムゾーンを設定しない限り UTC
で動作します。一方、タイムゾーンのない列は、書き込まれたときのソースの壁時計をそのまま保持します。
そのため Evaluation が読み取ったタイムゾーンが UTC でない場合、Schema Conversion
は時計を読む箇所にそのタイムゾーンを自動的に書き込みます: `DATETIME` (`timestamp`) のデフォルトは
`DEFAULT (now() AT TIME ZONE 'Asia/Seoul')` になり、MySQL ビューの `NOW()`
はそのタイムゾーンの時計を読みます。PostgreSQL のビューはそのままコピーされるため、
項目は時計を読むビューを確認できるよう一覧します。

`SOURCE_TIME_ZONE` 項目 (**Review needed**) は、それらの列とビューを示します。
あなたがすべきことはアプリケーション側にあります:
アプリケーションがセッションのタイムゾーンに依存している場合 (そのような列に `NOW()` や
`CURRENT_DATE` を書き込む、またはそのような列と比較する場合)、Aurora DSQL への新しい接続ごとに
`SET TIME ZONE 'Asia/Seoul'` を実行するか、SQL にタイムゾーンを明示します。
タイムゾーンを判別できない場合は、アドバイザリー項目がその旨を伝え、変換は UTC を前提とします:
ソースが UTC でない場合は、Schema Conversion でそれらのデフォルトを編集し、
アプリケーションでタイムゾーンを設定してください。読み取るタイムゾーンは、
アプリケーションのセッションが既定で受け取るものです: MySQL では GLOBAL の `time_zone`
(またはパラメータグループの `init_connect` が設定するもの)、PostgreSQL では既定の `TimeZone`
で、`ALTER DATABASE` や `ALTER ROLE` の既定値を含みますが、移行ユーザー自身の既定値は含みません。

MySQL ソースでは、さらに 3 つの設定が判定され、それぞれデータベースレベルの **Review needed**
項目になります (`ZERO_DATES` は上の表にあります):

| ソースの設定 | 検出結果 | Aurora DSQL での意味 |
|---|---|---|
| `ONLY_FULL_GROUP_BY` のない `sql_mode` | `SQL_MODE` | `GROUP BY` で決まらない列を選択するビューやクエリは拒否されます (`42803`)。ビューの場合は Schema Conversion が適用するときです。同じ項目は `ANSI_QUOTES` と `PIPES_AS_CONCAT` にもフラグを立てます: これらのモードでは、Query Converter に貼り付ける SQL が別の意味になります。 |
| `explicit_defaults_for_timestamp=OFF` | `EXPLICIT_DEFAULTS_FOR_TIMESTAMP` | MySQL では、`NOT NULL` と宣言された `TIMESTAMP` 列に NULL を書き込むと現在時刻が格納されましたが、Aurora DSQL はその書き込みを拒否します。アプリケーションが時刻を書き込むか、`INSERT` からその列を省いてください。 |
| 1 より大きい `auto_increment_increment` | `AUTO_INCREMENT_STEP` | Aurora DSQL の identity は 1 ずつ増えます: 増分に依存するロジック (書き込み元ごとのキー範囲) はアプリケーションへ移してください。 |

---

## 2.5 AI アシスト (オプション)

AI アシストは両方のステップで使えます。この節では何をするかと、有効にする方法を説明します。

### オプションの AI アシスト (Amazon Bedrock)

AI アシストはオプションで既定では無効であり、Amazon Bedrock 上でのみ動作します。変換のうち、人による判断が必要な部分を支援します:

- **Evaluation** では、検出結果の横にある **Ask AI about this finding** アイコンが、その検出結果について **AI DBA** パネルを開きます。
- **Schema Conversion** では、変換の警告の横にある AI アイコン、または **Ask AI DBA about converting this object** が、そのオブジェクトについて AI DBA を開きます。回答に DDL が含まれる場合、**Use as target DDL** はそれらの文をターゲット DDL にコピーし、ほかの編集と同じように確認して適用します。トリガー、ルーチン、イベントについては、**Ask AI DBA how to reimplement these** がそれぞれを作り直す方法を示します。

AI アシストが決定論的な変換を置き換えることはなく (AI を無効にしてもワークフローは同じです)、自分で適用するまでターゲットに届かず、見るのはスキーマと DDL のメタデータだけで、Full Load や CDC の行データを見ることはありません。

**有効化する方法:**

1. **Connect** 画面で **AI Assist** を展開し、**Enable AI Assist** をオンにします (**Bedrock settings** でモデルとリージョンを設定できます)。API キーの入力欄はありません: AI アシストは、ツールと同じ AWS 認証情報で Bedrock を呼び出し、その認証情報には選択したモデル (既定は `global.anthropic.claude-sonnet-5`) に対する `bedrock:InvokeModel` と `bedrock:InvokeModelWithResponseStream` が必要です。AWS 上にホストしたデプロイでは、スタックでも有効にする必要があります: [AI 支援変換の有効化 (オプション)](../../../deploy/DEPLOYMENT.ja.md#ai-支援変換の有効化-オプション) を参照してください。呼び出しのたびに Bedrock のコストと若干のレイテンシーが加わります。
2. **Verify AI access** のプリフライトを使用してください。これはこの認証情報で設定済みのモデルを呼び出す非ブロッキングのチェックです (有効な AWS プロファイルだけでは `bedrock:InvokeModel` は **保証されません**)。Bedrock がそのモデルを利用不可として拒否した場合、またはモデルの契約以外の理由で拒否した場合、チェックはフォールバックモデルも試します。フォールバックモデルが応答すると Model ID がそのモデルに切り替わり、結果は両方のモデルを示す警告になります。

**モデルがアカウントで有効になっていないとき。** Bedrock の Anthropic モデルは、AWS Marketplace の契約を通じてアカウントごとに 1 回有効化されます。その場合、AI DBA パネルと Connect 画面の AI Assist のステータスは *the configured model is not enabled for this AWS account yet* と表示します (初回利用 (first-time use) フォームが未提出の場合は、代わりに *not enabled or not available* と報告されることがあります)。AWS Marketplace の権限を持つ人が Amazon Bedrock コンソールでモデルの契約を承諾し (フォームを求められた場合は提出)、約 2 分待ってから **Verify AI access** を再実行してください (コマンドラインでの手順は `deploy/DEPLOYMENT.md` にあります)。新しいアカウントでは合格が一時的な場合があります: 契約がないことで呼び出しが止まるまでの最大約 15 分間は、呼び出しが成功することがあります。

---

**次へ:** [3. Full Load →](03-full-load.md)
