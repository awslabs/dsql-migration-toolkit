# 13. テーブルと列の除外

_言語: [English](../en/13-excluding-tables-and-columns.md) | [한국어](../ko/13-excluding-tables-and-columns.md) | **日本語**_

> **前へ:** [付録: 性能テスト結果](12-performance-test-results.md)

ソースにあるものすべてを Aurora DSQL へ移す必要はありません。個人データ(国民 ID、パスワードハッシュ、生年月日)をターゲットに入れたくない場合があります。ラージオブジェクト列が Aurora DSQL の値ごとの上限を超える値を保持していることもあります。そのような行は、そのままではロード時に隔離され、CDC も適用できません (第 4 章 §4.5)。また、監査用やステージング用のテーブルのように、まったく不要なテーブルもあるでしょう。これらはすべて 1 か所、**Data Migration** ステップの **Exclude tables & columns** ポップアップで選びます。ポップアップはすべての移行タイプ(Full load only、Full load + CDC、CDC only)で同じように動作し、除外はチェックボックスにチェックを入れるのではなく、短いテキストのルールとして書きます。

> **メンタルモデル:** ルールを書くと、Save がそれを 1 つの除外集合にまとめます。すべての経路がその同じ集合を読みます: Full Load、Start CDC、CDC キャプチャリスト、前提条件チェック、Validation、Cut over。除外された**テーブル**は移行されません。除外された**列**は Aurora DSQL 上に引き続き存在しますが、ツールが書き込むすべての行でその列は **NULL** になります。

---

## 13.1 除外の効果

| | テーブルの除外 | 列の除外 |
|---|---|---|
| ルール | `exclude table shop.audit_log` | `exclude shop.users.ssn` |
| ツール内 | テーブルはテーブルツリーで選択解除されます。チェックを外したのと同じです。ロードされず、CDC もキャプチャしません。 | テーブルは引き続き移行されます。ツールがこの列のソースの値を Aurora DSQL に書き込むことは決してありません。 |
| Aurora DSQL 上 | そのまま残されます。ロードされず、**削除されることもない**ため、既に存在するテーブルは行を保持します。 | 列はテーブルに残ります(Schema Conversion が定義したとおり)。最初の行が書き込まれる前に DEFAULT なしの nullable にされるため、ツールが書き込むすべての行でその列には NULL が格納されます(§13.6)。 |
| Validation | 比較されません。 | 行は通常どおり数えられ、比較されます。除外列は値の比較の対象になりませんが、Validation はその列が NULL であることを証明します(§13.9)。 |

早めに理解しておくべき点がいくつかあります。

- **DEFAULT ではなく NULL。** 除外列は NULL としてロードされ、ターゲット列の DEFAULT で埋められることはありません。DEFAULT のない NOT NULL 列を除外できるのもこのためです。ツールが先に NOT NULL を削除します。 NOT NULL の削除は**一方向**です(§13.4)。
- **Save しても Aurora DSQL では何も変わりません。** ターゲットの変更は、ロード、Start CDC、または Change & reload が実行されたときにだけ行われ、そのいずれも事前に変更内容を示します。
- **決して除外できない列があります。** キー列、保持される外部キーが参照する列、Aurora DSQL が計算する列は拒否され、その理由は常に表示されます(§13.4)。
- **データが移動すると集合は固定されます。** Full Load の後、または CDC スタックがデプロイされた後は、除外設定はロックされます。それでも変更はできますが、§13.7 の経路を通じてのみです。Full load only では、Full Load の後も移行するテーブルを変更できます。Full load + CDC では、CDC を開始するまでテーブルを外すことも追加することもでき、追加したテーブルはまず Full Load ステップでロードされます。通常は Retry unfinished tables が Full Load の開始点の下でロードします(§13.7)。

---

## 13.2 Exclude tables & columns ポップアップ

### 場所

**Data Migration** ステップのテーブル一覧で、**Tables to migrate** のすぐ下に **Exclude tables & columns...** ボタンがあります。その横のステータス行が、現在何が除外されているかを示します: `Nothing excluded`、保存済みの集合(`Rules exclude 2 tables, 5 columns`。自分でチェックを外したテーブルは数えません)、または集合をもう直接編集できないときの `... excluded - locked`(§13.7)。次の 2 つの状態は操作を求めます。

| ステータス行 | すること |
|---|---|
| `Your rules now ... - review`(警告) | 保存済みのルールが現在のスキーマに対しては異なる結果に解決されます(たとえば新しい列がワイルドカードに一致した場合)。このようなドリフトが**自動で適用されることは決してありません**。ポップアップを開いて Save すると受け入れられます。Start Full Load ダイアログもこの警告を繰り返し表示します。 |
| `N saved exclusions no longer apply. Starting is blocked until you review them.`(赤、**Review** 付き) | 保存済みのエントリが拒否されるか、消えた列のようにソースにもう存在しないものを指しています(§13.4)。**Review** をクリックしてください。 |

選択されたテーブルに Aurora DSQL の値の上限を超えうる列がある場合は、2 行目が表示されます(§13.5)。サイズチェック(**Check sizes** または前提条件チェック)がそのような値を測定すると、この行は警告に変わります。

### ルールの記述と確認

ポップアップには、ルールエディタ(1 行に 1 ルール。**?** アイコンで例を表示)と、その下の、入力を止めてから約 300 ms 後に更新されるライブプレビューがあります。プレビューは、各問題をその行とともに(修正方法がある場合はワンクリックの修正も付けて)一覧し、各ルールが何に一致するかを示し(`L3  matches 4 columns - 1 kept by include on L5 - 2 blocked (primary key, computed)`)、保存する前に結果をプレビューします: ルールが選択解除するテーブル、除外される列(「migrated as NULL」)、拒否されるすべての列とその理由(§13.4)、正確な `ALTER` 文(**Changes to Aurora DSQL**、§13.6)、そして CDC を含む移行タイプでは **CDC capture list**(`ColumnExcludeList`、最大 4,096 バイト、§13.8)。**Review all N items** は関係するすべての項目を 1 つの表にまとめ、**All / Excluded / Blocked / Warning** で絞り込めます。**Oversized-LOB candidates** は §13.5 を参照してください。

次のいずれかの問題が残っている間、**Save は無効のまま**です。

- 構文エラー。
- 何にも一致しない名前(プレビューは、大文字小文字だけが異なる名前を、それがなければ最大 3 つの近い候補を提案します)。
- ロックされている間にテーブル集合を変更することになるテーブルルール、または削除したテーブルルール(§13.7)。
- 名前に `:` を含むテーブルの列に一致する列ルール(§13.10)。
- Aurora DSQL のテーブルに存在せず、除外もされていないソース列。この場合、ロードは確実に 42703 で失敗します (§13.4)。修正はその列の `exclude` 行を追記しますが、クリックするのはあなたです。列が移行から自動的に外されることは決してありません。
- コンパクトな形でも 4,096 バイトを超える CDC キャプチャリスト(§13.8)。
- 選択されたテーブルが 1 つも残らなくなるルール。

修正は自分の行だけを書き換えるか、集合全体に対するもの(42703 のケース)ならルール行を追記します。Save は、プレビューが入力内容に追いつくまで待つため、まだ目にしていない内容が保存されることは決してありません。

### 保存、キャンセル、複数のタブ

- **Save** は、ルールのテキストを解決済みの結果とともに保存します: 除外列、テーブルの選択、そしてルールがどのテーブルを選択解除したか。Aurora DSQL では何も変わりません。
- **Esc や背景のクリックではポップアップは閉じない**ため、誤って編集内容を失うことはありません。 **Cancel**(または閉じるアイコン)は編集内容を破棄します。
- **ポップアップはブラウザのタブごとに最大 1 つです。** 開いた後に除外設定が変わった場合(別のタブ、アタッチされたスタック、あるいは実行やデプロイの開始によるもの)、またはロックがかかった場合、Save は拒否され、入力したテキストは保持されます。
- ルール、ルールが選択解除したテーブル、ツリーで付け直したチェック、保留中の行(§13.7)はセッションとともに保存され、セッションとともに復元されます。

### ポップアップが読み取り専用になる場合

除外設定がロックされていると、ポップアップは読み取り専用で開きます。次の内容が表示されます。

- ロックの原因と、このセッションで有効な対処。
- 保存済みのルール、保留中の行、そして保存済みの各エントリが現在何をするかについての注記。
- デプロイ済みのスタックについては、デプロイされたとおりのキャプチャリスト。スタックが既にストリーミングしており、セッションのリストがそれと異なる場合(つまり Start CDC がセッションのリストを拒否する場合)は、両方のリストが並べて表示されます。

提示されうるボタンは **Change & reload** と **Change migration type** です。後者は移行タイプの選択欄までスクロールするだけで、ツールがタイプを代わりに切り替えることは決してありません。§13.7 を参照してください。

---

## 13.3 ルール言語リファレンス

| 行 | 意味 |
|---|---|
| `exclude table schema.table` | テーブルを移行から外します(選択解除)。 |
| `exclude schema.table.column` | 列を NULL として移行します。 |
| `exclude type:name[,name...]` | これらの**ソース**データ型のすべての列を除外します。 |
| `include ...` | 上の 3 つの形のいずれかを例外として指定します。一致する `exclude` を取り消します。 |
| `# ...` | コメント。単独の行にも、ルールの後にも書けます。空行は無視されます。 |

MySQL ソースでは、`schema` はデータベース名です。PostgreSQL では、スキーマです。

**名前とキーワード。**

- `exclude`、`include`、`table`、`type:` は大文字小文字を区別しません。**名前は大文字小文字を区別します。** `shop.Users` は `shop.users` に一致せず、エラーは正しい綴りを提案します。
- 列ルールは常に 3 つの部分からなります。`table` のない 2 部分の名前はエラーで、その修正は `table` を挿入します。
- 1 行に 1 ルールを書きます。カンマ区切りのリストは `type:` の後にだけ書け、スペースを含めることはできません: `type:longtext, longblob` ではなく `type:longtext,longblob` と書きます。後者の形のエラーは **Remove the spaces** を提示します。
- ルールの後にある、コメントでないテキストはエラーで、その修正はそのテキストをコメントに変えます。

**ワイルドカード。** `*` が唯一のワイルドカードです。**1 つの名前部分の中で**、空を含む任意の文字の並びに一致し、`.` をまたぐことは決してありません。

- `exclude table shop.audit_*` は、`shop` 内の、名前が `audit_` で始まるすべてのテーブルに一致します。
- `exclude shop.*.password_hash` は、`shop` のすべてのテーブルにあるその列に一致します。
- `exclude *.*.ssn` は、すべての `ssn` 列に一致します。

`_`、`%`、`?` は普通の文字です。引用符で囲んだ `"*"` は文字どおりのアスタリスクで、型名にはワイルドカードを使えません。

**引用符。** 名前部分にスペース、`.`、`,`、`*`、`#`、`"` が含まれる場合は、二重引用符で囲みます。引用符の中では `"` を `""` と書きます。

```
exclude "my schema"."order.items".notes
exclude shop.reviews."say ""hi"""
```

改行を含む名前はルールとして書けません。代わりにそのテーブルを除外するか、Schema Conversion でターゲット DDL を変更してください。

**行の順序に関係なく include が優先します。** `include` と `exclude` が同じ項目に一致する場合、その項目は残されます。行の順序は決して影響しません(AWS DMS の選択ルールとは異なります)。テーブルと列は別々に扱われます: `include table` はテーブルルールだけを取り消し、列または `type:` の include は列ルールだけを取り消します。

**スコープ。** 列ルールと `type:` ルールは、選択されたテーブルにのみ効果があります。ある行の一致がすべて選択されていないテーブル上にある場合、その行には `only unselected tables` のフラグが付きます。同じ Save でテーブルルールが選択解除するテーブル上の列ルールは、効果がありません。

**テーブルルールは、何を選択解除したかを記憶します。** `exclude table s.t` は一致するテーブルの選択をツリーで外し、ポップアップはどのテーブルがルールによって選択解除されたかを記録します。後でルールを削除する (または `include table` を追加する)と、まさにそれらのテーブルだけが再び選択されます。自分でチェックを外したテーブルは、チェックが外れたままです。ルールで選択解除されたテーブルをツリーで再びチェックすると、それは意図的な上書きとして扱われます: ツリーのその行とポップアップがそのテーブルを明示しますが、ルールも変更しない限り、次の Save で再び選択解除されます。

**`type:` のキー。** `type:` ルールは**ソース**の列の型に一致し、その型は 1 つのキーに縮約されます。

| ソース | キー | 例 |
|---|---|---|
| MySQL | 小文字にした基本型を、`(` またはスペースの位置で切ったもの | `int unsigned` → `int`; `decimal(10,2) unsigned` → `decimal`; `varchar(255)` → `varchar`; `longtext` |
| PostgreSQL | typmod を除いた `format_type` 名。複数語の名前はそのまま残し(引用符で囲みます)、`[]` も残し、ユーザー定義型の大文字小文字も保持します | `numeric(10,2)` → `numeric`; `character varying(40)` → `"character varying"`; `integer[]`; `interval day to second(3)` → `interval` |

キーは、そのとおりに入力しても、typmod 付きで入力しても(`type:varchar(255)` は `type:varchar` を意味します)、大文字小文字を変えて入力しても(`type:TEXT` は `text` を見つけます)かまいません。別名はありません: PostgreSQL では `type:varchar` は `character varying` に一致しませんが、エラーが `type:"character varying"` を提案します。MySQL の `type:integer` も同様で、`int` が提案されます。

**例。** MySQL ソースで個人データとラージオブジェクトを扱う典型的な集合:

```
# 個人データ: これらの値は決してコピーしない
exclude *.*.ssn
exclude shop.*.password_hash
exclude shop.customers.date_of_birth

# Aurora DSQL の値ごとの上限を超えうるラージオブジェクト
exclude type:longtext,longblob
include shop.pages.body        # ページ本文は小さいので残す

# この移行に不要なテーブル
exclude table shop.audit_*
exclude table shop."tmp import"
```

PostgreSQL ソースでの同じ考え方:

```
exclude public.users.national_id
exclude type:bytea,jsonb
include public.settings.preferences
```

何にも一致しない行はエラーになるため、このスキーマに当てはまらない行は削除してください。たとえば、`ssn` 列を持つテーブルが 1 つもない場合、`exclude *.*.ssn` はエラーです。

---

## 13.4 除外できないもの、および警告

### 拒否される列

1 つの適格性ルールがすべての経路について判定し、その理由は常に表示されます。拒否された列はプレビューに `(not excluded)` として一覧され、保存されることは決してありません。それでも Save はできます。その列は単に移行されます。

| 列 | 表示 | 理由 |
|---|---|---|
| ソースの主キー列 | primary key | ツールは行の読み取り、重複排除、複製にこの列を必要とします。 |
| Schema Conversion で選んだターゲットの主キー列: 複合キーの先頭列、編集した DDL 内の名前付き `CONSTRAINT ... PRIMARY KEY` の列、または CDC の再キー化の列(`message.key.columns`) | target key | ローダーはすべての行のすべてのターゲットキー列を読み、CDC は各行をそのキーで書き込み・削除するため、この列を必要とします。 |
| 選択されたテーブルの保持される外部キーが参照する列(外部キーを保持している間のみ) | referenced by a foreign key | すべての子行が孤立し、外部キーを追加できなくなります。 |
| Aurora DSQL が計算する列: 保持される `GENERATED ... STORED` 列、または主キー外の identity 列 | computed | Aurora DSQL がその値を計算し、ツールが書き込むことは決してありません。DEFAULT を削除しても identity 列を NULL にはできません。 |

### 適用できなくなった保存済みの除外設定

保存済みの除外設定は古いセッションや復元から来ることがあるため、毎回改めて判定されます。

- **その集合のもとでまだデータが移動していない間は、** 適格性ルールが拒否するエントリ、またはソースにもう存在しないテーブルや列を指すエントリがあると、**Continue / Start Full Load**(ページヘッダーの Run を含む)、**Deploy CDC infrastructure**、既存スタックへの **Attach** が止められます: 「N saved column exclusions no longer apply: ... Remove them in Exclude tables & columns (under the table list) first.」
- **データが移動した後は、** 集合が既にロードまたはキャプチャされた内容の一部であるため、何も止めません。代わりに各エントリが報告されます。キーのエントリは「is still migrated」です(キーのエントリはすべての経路で集合から外されるため、Validation はその列を比較します)。計算列のエントリは「is already omitted」です。外部キーから参照される列は「stays excluded, but: ...」です。

### 警告(許可されますが、内容を確認してください)

これらの警告は Save をブロックしません。Start ダイアログが繰り返すのは最初の 2 つだけで、DROP NOT NULL と DROP DEFAULT の件数と文として表示されます。Change & reload は、reload するテーブルについてすべての警告を表示します。

| 除外列が... | 何が起きるか |
|---|---|
| Aurora DSQL 上で NOT NULL の場合 | NOT NULL が削除されます。**これは一方向です:** Aurora DSQL は NOT NULL をその場で戻すことができず、後でそのテーブルを Drop & reload したときにだけ元に戻ります。この列はカットオーバー後も nullable のままです。 |
| DEFAULT を持つ場合 | 移行のために DEFAULT が削除され、Cut over で復元されます。復元された DEFAULT は新しい行にのみ適用されます(§13.9)。 |
| 保持される外部キーの子側の列である場合 | 親との関係は移行されません(NULL は外部キーの検査を通過します)。そのキーが 2 列以上にわたる `MATCH FULL` の場合は、代わりに一部だけが NULL のキーが拒否されます: キーのすべての列を除外しない限り、移行された行でそのキーの検証が失敗し(Cut over はそのキーで Finish を保留します)、後でこの列だけを NULL にする書き込みは拒否されます(23503)。キーのすべての列を除外するか、この列を残すか、テーブルのターゲットスクリプトから `MATCH FULL`(またはそのキー)を削除してください。 |
| UNIQUE 制約またはインデックスに含まれる場合 | 移行された行はすべて NULL なので、それらに対して一意性は強制されません — ただし、そのインデックスが NULL を等しいものとして扱う `NULLS NOT DISTINCT`(PostgreSQL 15+)の場合は、インデックスの他の列を共有する移行行が最初の行の後で拒否されます(23505)。Drop & reload でも同じです(インデックスは行をロードする前に作成されます)。この列を残すか、テーブルのターゲットスクリプトでそのインデックスの `NULLS NOT DISTINCT` を削除してください。 |
| 残される生成列から使われている場合 | 生成列が NOT NULL で、この列が NULL のときに式が NULL を返す場合、すべての行が拒否されます(23502)。生成列が nullable の場合は NULL から計算され、ソースとは異なる値になります。 |
| ビューから参照されている場合(テキスト一致) | ビューは NULL を読みます。 |

NULL を拒否する手書きの `CHECK` 制約は、事前に検出され**ません**。そのようなテーブルは、ロード時にはっきりとエラーで失敗します(23514)。

**テーブル**の除外にも影響があり、プレビューはそれらを一覧します。

- ロードされず、Aurora DSQL 上ではそのまま残されます(削除されません)。
- 選択された子テーブルからそのテーブルへの外部キーは、外部キーの適用時(Full load only では Data Migration ステップ、CDC では Cut over)に、このテーブルが Aurora DSQL 上に持つ行に対してチェックされます。参照先の行がすべてあれば適用され、1 行でも欠けていれば孤立チェックでスキップされ、テーブルが Aurora DSQL 上になければ適用されません。
- そのテーブルを読むビューは行を返しません。
- そのテーブルに対する列ルールは効果がありません。

### Aurora DSQL のテーブルに無いソース列 (42703)

Schema Conversion でターゲット DDL から列を削除したものの、その列を除外しなかったとします。その場合、そのテーブルの Full Load のバッチはすべて 42703 で失敗し、CDC はそのテーブルへのすべての変更をデッドレターにします。そのため、**Start Full Load**、**Deploy CDC infrastructure**、**Start CDC** は「shop.t.c is not in your Aurora DSQL table; ... Add it back to the target DDL in Schema Conversion. Columns are never dropped from a migration automatically.」として止められます。列をターゲット DDL に戻すか、除外してください (ポップアップがその行を提示します)。報告されるのは確実に失敗する場合だけ、つまり適用済みの DDL とライブのターゲットテーブルの両方にその列が無い場合だけです。

### 前提条件チェック

前提条件チェックは除外設定を考慮します。

- `TARGET_COLUMNS_LOADABLE` は、ロード(または Start CDC)が先に nullable にするため、DEFAULT のない除外済みの NOT NULL 列をロード可能として扱います。
- PostgreSQL では、CDC が除外列を決してキャプチャしないため、`COLUMN_REPLICABLE` はそれらをスキップします。
- チェックの実行後に除外設定を変更した場合は、ブロックする代わりに中立的な注記がその旨を示します (「Exclusions changed since these checks ran at HH:MM UTC; size results describe the earlier set.」)。チェックの後に追加したテーブルは引き続きブロックします。

---

## 13.5 特大 LOB の候補と Check sizes

Aurora DSQL は 1 つの値のサイズを制限しています(ほとんどの型で 1 MiB。 [第 4 章 §4.5](04-cdc-and-dsql-constraints.md#45-値ごとの-1-mib-上限および-dlq) を参照)。ロード時、それより大きな値を持つ行は隔離されます。その行は Aurora DSQL に入れられず、隔離された行が 1,000 行を超えるとそのテーブルは失敗します。CDC もそのような行を適用できません(その行がどうなるかは §4.5 で説明しています)。代わりにその列を除外すれば、行は残り、その列は NULL になります。

ポップアップの折りたたまれた **Oversized-LOB candidates (N)** セクションは、対象範囲内の列のうち、その **型**が上限を超える値を保持しうるものを一覧します。

- **MySQL:** `mediumtext`、`longtext`、`mediumblob`、`longblob`、`json`。
- **PostgreSQL:** `text`、`bytea`、`json`、`jsonb`、および長さ指定のない `character varying`(または長さのない `bpchar`)。CHECK によって固定のリテラル集合に制限されている列は一覧されません。

これらは、Evaluation が `OVERSIZED_LOB` としてフラグ付けするのと同じ列です。各行には、ソースとターゲットの型、その型が許容する内容(たとえば「Type allows up to 4 GiB.」)、上限、そしてエディタに `exclude schema.table.column` を追加する **+ add rule** ボタンが表示されます。適格性ルールが拒否する列には、代わりにその理由が表示されます。`include` が残す列には、その include の行へのボタン(たとえば **Go to L5**) が表示されます。

**ポップアップは既定では何も測定しません**(前提条件チェックは独自のサイズチェックを実行します。前提条件チェックが測定した列は、その結果がここにも表示されます)。型はあくまで上限にすぎない点に注意してください。 `longtext` や `json` の列が数キロバイトを超える値を一度も保持しないこともあります。確実に知るには、 **Check sizes** をクリックします。

- 現在のページの候補(最大 25 個)を、ソース上で列ごとに負荷を限定したクエリを 1 回ずつ実行して測定します。各クエリは上限を超える最初の値で停止し、実行時間は最大 15 秒です。最悪の場合を含むコストは、クリックの前に示されます。
- 一度に実行されるパスは 1 つだけで、**Cancel** は現在の列の後で停止します。
- 各列には 3 つの結果のいずれかが付きます: **Over the limit found**、**No value over the limit**、 **Not determined**(読み取りが失敗した、15 秒より長くかかった、またはキャンセルされた)。「Not determined」が合格として数えられることは決してありません。
- **+ add rules for the N over the limit** は、上限を超えると判明したすべての列に対するルールを追加します。

各列は、その**適用済みのターゲット型**の上限に対して判定されます。たとえば、Schema Conversion で列を `varchar` に変更した後は、その型の上限が使われます。MySQL では、`utf8mb4` でないテキスト列はその列自身の文字セットで測定され、結果にもその旨が示されます。Aurora DSQL は UTF-8 で格納するため、サイズが大きくなりうるからです。PostgreSQL では、継承 (`INHERITS`) の親テーブルは、前提条件チェックでも Check sizes でも、Full Load が読むのと同じく自身の行だけで測定されます (`FROM ONLY`。 [§6.1](06-limitations.md#61-aurora-dsql-の機能制限スキーマがこれらに適合している必要があります))。子テーブルの値はその子テーブルのものとしてだけ数えられます。

このセクションは、強調表示しないものも示します: MySQL の空間列、PostgreSQL の配列、`xml`、 `tsvector`、幾何型、そしてマルチバイトのテキストを保持する大きな `varchar(n)` や `char(n)` の列です。 2 MiB を超える行はチェックされません。これらも、ルールを書けば除外できます。

---

## 13.6 Aurora DSQL 上で何が起きるか

### オーバーレイ: 最初の行の前に、DEFAULT なしの nullable にする

テーブルの最初の行が書き込まれる前に、ロードは各除外列を DEFAULT なしの nullable にします。列ごとに、まず NOT NULL を削除し、次に DEFAULT を削除します。

```sql
-- shop.users.ssn: NOT NULL、DEFAULT なし
ALTER TABLE "shop"."users" ALTER COLUMN "ssn" DROP NOT NULL
-- shop.users.status: NOT NULL DEFAULT 'active'
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP NOT NULL
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP DEFAULT
```

- **DEFAULT は先に記録されます。** 削除する前に、DEFAULT はオーバーレイ台帳(後述)に書き込まれるため、 Cut over で復元できます。ライブの DEFAULT が記録と異なる場合、ツールはどの ALTER よりも前に拒否するため、記録していない DEFAULT を失うことは決してありません。

オーバーレイは**すべての Full Load 経路**で実行されますで実行され、すべての Drop & reload による再作成と、ツールが再作成して完了しなかったテーブルの再試行も含まれます。アクティビティログには、`overlay planned`(削除する DEFAULT を正確に明示)、`overlay applied`、`column excluded`、そしてテーブルのロードの行に `loaded as NULL: ...` が表示されます。

### Start Full Load ダイアログは変更内容を先に示します

除外が関わる場合、Start Full Load ダイアログには次の内容が表示されます。

- 「N columns in M tables will load as NULL」(**show columns** 付き)。
- 「Aurora DSQL changes before loading: x x DROP NOT NULL (one-way), y x DROP DEFAULT (restored at Cut over)」と、**show statements** の下に実行順の正確な文。これらは Append と Drop を切り替えると再描画されます。
- drop して reload する必要のあるテーブル(次のセクション)。
- ルールで選択解除されたためロードされないテーブルの数。

除外が関わる場合は、ページヘッダーの **Run** も必ずこのダイアログを経由します。実行がダイアログで示した内容と異なる変更を行うことになる場合、実行は拒否され、ダイアログをもう一度開くよう求めます。

### 既に行を持つテーブル: Drop & reload

append は省略した列を決して変更しないため、append では格納済みの値を NULL に変えることはできません。そのためツールは、NULL と格納済みの値を決して混在させません。Start Full Load ダイアログは、次のテーブルに **Drop & reload を強制**します。

- 行のあるテーブルのうち、すべての行が現在の除外集合でロードされたわけではないもの。
- 行のあるテーブルのうち、以前除外した列が再び含められ、その列が NULL の行があるもの。
- ツールが再作成して完了しなかったテーブル。

強制されたテーブルは理由とともに赤いカードに一覧され、Append の選択肢はなく、確認ボタンは破壊的操作の色で「Drop N table(s), recreate and load」と表示されます。Cancel、Esc、背景のクリック、タブの消失は強制された集合を破棄するため、後の実行が drop を引き継ぐことは決してありません。エンジンも同じ判定を適用します。別の経路でそのようなテーブルに append しようとすると、そのテーブルは代わりに「needs Drop & reload: ...」で失敗します。

例外:

- ターゲットを読めない場合は、テーブルを未判定のままにして警告を出します。強制されることは決してありません。
- 同じ集合のもとでの再開や再試行が強制されることは決してありません。

CDC のストリーミング中は Full Load はまったく実行されません ([第 3 章 §3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し)): ダイアログを確定できないため、何も強制しません。

### Start CDC も同じオーバーレイを適用します

CDC only での Start CDC、Retry CDC、Stop 後の再 Start は、**コネクタを作成または変更する前に**同じオーバーレイを適用します。 Full Load と同じ文、順序、チェックを使うため、CDC が insert するすべての行で、除外列は NULL になります。

- オーバーレイを既に行った Full Load の後では、これは検証済みの no-op です。
- カットオーバーでの復元の後では、復元された DEFAULT を再び削除します。そのため、**DEFAULT の復元は最後の Start CDC の後に行ってください**。
- 何らかの失敗があると、Start は「Start CDC blocked: ...」として失敗し、コネクタには一切触れません。クリック後にセッションのターゲットが変わった場合も、ジョブは拒否します。
- CDC only の Start ダイアログは変更内容を一覧します: 「Aurora DSQL changes when CDC starts: N x DROP NOT NULL (one-way), M x DROP DEFAULT (restored at Cut over)」と、**show statements** の下の文です。

CDC は除外列に決して書き込まないため、**CDC の開始前から Aurora DSQL にある行は、除外列の値を保持します**。 CDC only の Start ダイアログはそれらのテーブル名を示し、各テーブルについて `overlay append` の WARNING が記録され、Validation がそれらを報告します(§13.9)。

### オーバーレイ台帳

削除した DEFAULT はターゲットから読み戻せないため、ツールは ALTER の前に、NOT NULL を削除したかどうかと復元の状態とともに、それを**オーバーレイ台帳** (overlay ledger)に記録します。台帳のキーはセッションではなく**ターゲット**(クラスタエンドポイント + データベース)です。そのため、復元されたセッションや Start over 後の新しいセッションでも、以前のセッションが変更した内容が見えます。**読めない台帳が「記録なし」として扱われることは決してありません。** Start ダイアログは Start を無効にして **Retry** を提示し、実行は何かを削除・書き込みする前にすべてのテーブルを失敗させ、Start CDC はブロックされます。

---

## 13.7 後から除外設定を変更する

### 集合がロックされる理由

ある除外集合のもとでデータが移動した後に、その集合をその場で変更すると、一部の行は NULL、他の行は値を持つという状態になってしまいます。CDC スタックも集合の独自のコピーを持っています。スタックはそのキャプチャリストとともにデプロイされており、停止したスタックも除外設定とコミット済みのオフセットを保持するため、 **Stop CDC ではそれは解除されません**。そのためツールは集合をロックし、あなたの状況で実際に有効な対処だけを提示します。

| あなたの状況 | 除外設定の変更方法 |
|---|---|
| まだ何も実行していない(Full Load も CDC スタックもない) | ロックされていません: ルールを編集して Save します。 |
| CDC インフラを削除中 | 削除が完了するまで待ちます。ポップアップはエンジンを考慮した見積もり(MySQL で約 15〜25 分、PostgreSQL で 2〜5 分)を表示します。 |
| CDC インフラを作成中 | 作成が完了するまで待ち、その後、該当する「CDC スタックが存在する」行に従います。 |
| CDC スタックが存在し(デプロイ済み、ストリーミング中、または停止中)、Full Load を実行した | 1. ストリーミング中なら Stop CDC を実行し、その後 CDC ステップで **Delete CDC infrastructure** を実行します。2. 影響を受けるテーブルに **Change & reload** を使います(CDC only: 先に Full load + CDC へ切り替えます)。3. 再度デプロイし、Start CDC を実行します(MySQL では下記の「削除後に再び CDC を開始する」を参照)。Full load + CDC でこの移行で CDC を一度も開始していなければ(Start CDC を送信したことがない)、テーブル一覧でテーブルのチェックを外すことも入れることもできます(下記の「Full Load 後にテーブル集合を変更する (Full load + CDC)」を参照)。 |
| CDC スタックが存在し、Full Load は実行しておらず、CDC only | ストリーミング中なら Stop CDC を実行し、Delete CDC infrastructure を実行してルールを変更し(CDC が一度でもストリーミングしていれば列ルールのみ: テーブル集合の変更には Start over が必要)、再度デプロイして Start CDC を実行します。スタックを削除すると、ストリームの位置も削除されます(下記参照)。 |
| CDC スタックが存在し、まだ Full Load を実行していない(Full load + CDC) | ストリーミング中なら Stop CDC を実行し、Delete CDC infrastructure を実行してルールを変更し、再度デプロイしてから Start Full Load を実行します。reload が必要なものはありません。CDC が既にストリーミングしていた場合は、Start Full Load ダイアログで drop と再作成を選びます。 |
| CDC スタックがなく、Full Load を実行した(Full load only または Full load + CDC) | **Change & reload**(次のセクション)。Full load only: テーブル集合も変更できます。Full load + CDC: CDC を開始するまでテーブルを外すことも追加することもできます(下記の 2 つのセクションを参照)。 |
| CDC スタックがなく、タイプを CDC only に切り替える前に Full Load を実行した | タイプを Full load + CDC に切り替え、そこで Change & reload を使い、その後 CDC を続けます。 |
| Full Load を実行したが、そのジョブレコードがもう利用できない | CDC スタックがあれば先に削除し、その後 **Start over** します。 |

補足事項:

- **スタックがある状態での Full load only。** 移行タイプが Full load only のときにスタックが存在する場合は、それを削除するために Full load + CDC に切り替えてください。Full load only には CDC ステップがないためです。
- **テーブル集合。** CDC only では、Full Load の後は固定されます。CDC を開始する前に集合を変更するには Full load + CDC に切り替えてください。Full load + CDC では、CDC を開始するまでテーブルを外すことも追加することもできます(下記の「Full Load 後にテーブル集合を変更する (Full load + CDC)」を参照)。それ以降は Start over を使います。Full load only については、下記の「Full Load 後にテーブル集合を変更する (Full load only)」を参照してください。
- **削除後に再び CDC を開始する。** MySQL では、Automatic の Start CDC は Full Load のウォーターマークから再シードします(そのロード以降の binlog がまだ存在している必要があります。パージされていれば Start CDC が警告します)。Change & reload はそのウォーターマークを維持するため、そのロード以降にキャプチャ対象テーブルの定義が変わっていれば(たとえばソースで追加された列)、Start CDC は拒否もします: その場合は、より新しいウォーターマークのために「Drop & reload」ですべてのテーブルを Re-run Full Load するか、Manual を選んで Fetch current position を使ってください([第 6 章](06-limitations.md)を参照)。PostgreSQL では、スタックを削除するとレプリケーションスロットと publication が削除されます。Delete CDC infrastructure のダイアログがその名前を示します: 削除できなかった場合も teardown はそのまま完了し、そのログがソースで実行する文を示します。その場合 teardown は、ツールが CDC 用に作成したソース認証情報シークレット (パスワード認証の場合) を削除予定にせず残します: そのシークレットにスロットの削除に必要な認証情報があるためで、ダイアログも示すとおり、スロットがなくなった後に自分で削除してください。削除されるスロットが Full Load のスロットであれば、Start CDC はもうそのスロットから再開できないため、Start CDC ダイアログで **「Re-snapshot every table instead」** を選んでください。この方法は、それが解除する Start が自身のチェックを通過する場合にだけ提示され、そうでなければダイアログがその Start の拒否を表示します (たとえば継承の子テーブルを持つキャプチャ対象テーブル。[§4.3](04-cdc-and-dsql-constraints.md) を参照)。その後に、そこで使える方法を示します: 継承のための拒否なら、代わりに「Drop & reload」で Re-run Full Load すると、その後の Start CDC はスナップショットしません (CDC only では、先にマイグレーションの種類を Full load + CDC に切り替えます)。その名前のスロットを使用できないための拒否なら、そのスロットを削除するか別の CDC スタック名を使ってから、「Drop & reload」で Re-run Full Load して Start CDC してください (CDC only では Start CDC をもう一度実行します)。
- **CDC only のスタックを削除する代償。** MySQL では、次の Automatic の Start CDC はソースの現在位置から開始します。Aurora DSQL が最後に適用した変更より後にキャプチャ対象のテーブルに加えられた変更は、複製 **されません**。それらを再コピーするには、Full load + CDC に切り替えて、新しいスタックの最初の Start CDC の前にその Full Load を実行し (そのスタックがストリーミングした後では、新しい Full Load の開始位置にはもう一度 Delete と再デプロイが必要です)、行を持つテーブルについては drop と再作成を選びます。PostgreSQL では、次の Start CDC がキャプチャ対象のテーブルを再びスナップショットしますが、その間にソースが削除した行は Aurora DSQL に残ります。どちらの場合も、 Aurora DSQL に既にある行は、新たに除外した列の値を保持します。

### Change & reload

Full Load を含む移行タイプで、実行が記録されており、CDC スタックがない場合、読み取り専用のポップアップが **Change & reload** を提示します。これは **Change exclusions & reload** を開きます。

- **列ルールは編集できます。** テーブル行は、テーブル集合をまだ変更できる場合にだけ選択を変更します(次の 2 つのセクションを参照)。選択から外したテーブルは Aurora DSQL 上にそのまま残されます(削除されず、CDC もキャプチャしません)。選択したテーブルのうち、まだロードされていないものは Retry unfinished tables がロードします(Full load + CDC ではロードされると CDC がキャプチャします)。前回の Full Load がすでにロードしたものはそのまま保持されますが、列ルールがそのテーブルの列を新たに除外する場合は、除外列が変わる他のテーブルと同様に **Drop & reload** の対象として表示されます。CDC がまだキャプチャしていないテーブルの列ルールは、ここでは何も drop しません。Full Load ステップでのそのテーブルのロードがルールを適用します。CDC を開始した後は、選択を変えることになる行はすべてエラーになります。CDC スタックがある場合、ポップアップは読み取り専用なので、代わりにテーブル一覧を使ってください。
- **何かを削除する前に、何が起きるかを正確に示します:** drop して reload する各テーブルを、列の差分 (`+ c (NULL)`、`- c`)と行数の見積もり(「~N rows re-read (estimate)」)とともに示し、さらにそれらのテーブルに対する警告と ALTER も示します。「Their rows on Aurora DSQL are permanently deleted; each table is recreated from your Schema Conversion DDL, made nullable where needed, and reloaded. Other tables are not touched.」
- **確認ボタンは操作内容を示します:** 破壊的操作の色で **Drop & reload** *table*、または **Drop & reload N tables** と表示されます。プレビューに示したテーブルにのみ作用します。Start ダイアログは続かず、reload はすぐに開始します。
- **テキストだけの変更では何も削除されません。** どのテーブルの除外列も変わらない場合(たとえば、コメントを追加しただけの場合)、ボタンは **Save** と表示され、ルールだけを保存します(Full load only と Full load + CDC では、テーブル行が変更するテーブルの選択も保存します)。

Change & reload は、CDC only の場合(ポップアップは代わりに **Change migration type** を提示します)や、 Full Load のジョブレコードが無くなっている場合(Start over を使います)には提示されません。Full Load が記録されていなければ何もロックされておらず、ルールを直接編集します。提示される場合でも、Full Load の実行中、外部キーの適用中、Cut over による DEFAULT の復元中、そしてアプリの再起動後に Connect ステップで接続を再検証するまでは、理由を示して無効になります。

### Full Load 後にテーブル集合を変更する (Full load only)

Full load only では、Full Load によってテーブル集合が固定されることはありません。Full Load、外部キーの適用、カットオーバーの DEFAULT 復元のいずれも実行中でなく、CDC スタックが存在しない間は、テーブル一覧でテーブルにチェックを入れるか外すか、Change & reload でテーブル行(`exclude table ...`)を使います。 Start over は不要です。

| 操作 | 結果 |
|---|---|
| **テーブルのチェックを外す** | Aurora DSQL 上にそのまま残されます(削除されません)。この移行はそのテーブルをもうロードも検証もしません。テーブルルールで選択を外した場合、そのテーブルの除外列のエントリは保持されません。 |
| **まだロードされていないテーブルにチェックを入れる** | **Retry unfinished tables** がロードします。ロード済みのテーブルをすべて保持し、元のウォーターマークを再利用します。Full Load ステップはそのテーブルを「Not loaded yet — selected after the last Full Load」と表示し、ロードされるまでステップは Failed と表示されます。新しいテーブルは一度もチェックされていないため、ツールに求められたら先に前提条件チェックを再実行してください。それまでの間、未完了のテーブルが新しいテーブルだけなら Retry unfinished tables は無効になり、他のテーブルも再試行する場合は新しいテーブルを拒否します。そのテーブルの列ルールは、この最初のロードで適用されます。Change & reload は、前回の Full Load に含まれていなかったテーブルを drop して reload することはありません。 |
| **前回の Full Load がすでにロードしたテーブルにチェックを入れる** | そのまま保持され、再度ロードはされません(Full Load ステップの Reload specific tables で reload できます)。ルールで選択から外していたテーブルに再びチェックを入れても、除外列は戻りません(そのエントリは保持されていません)。 Exclude tables & columns の行が警告し、Change & reload でそのテーブルにルールを再び適用できます。 |
| **外部キー** | **Apply foreign keys** の処理が完了した後にテーブルのチェックを外しても、その結果は保持されます。キーはそのまま残ります。テーブルにチェックを入れた後(または処理を途中で止めた後にチェックを外した場合)は、カードが再適用を求めます。すでにあるキーはそのまま保持され(何もしません)、欠けているキーはそれぞれすぐに追加されます。 |
| **Validation** | 新しく選択したテーブルがロードされたら、Validation を再実行してください。結果が Full Load 以降に選択したすべてのテーブルを対象にするまで(または Objects to validate でそのチップをオフにするまで)、Cut over は保留されます。そのようなテーブルがまだロードされていない間、Cut over は Data Migration ステップへ戻るよう案内します。 |
| **CDC タイプへの切り替え** | この移行で CDC を既に開始している場合を除き、テーブルのチェックを外すだけでは移行タイプはロックされず(既に開始している場合は、集合を元に戻すか Re-run Full Load を実行するまで、集合を変えるどの変更もタイプをロックします。その再実行の後でタイプを切り替え、ストリーミングした CDC スタックがまだあれば、Start CDC の前に CDC ステップで Delete CDC infrastructure して再デプロイしてください: Full load only には CDC ステップがなく、ストリーミングしたことのあるスタックは停止した位置から続けるだけで、より新しい開始位置から開始することは決してありません)、CDC タイプはロード済みのテーブルのうち、まだ選択されているものをキャプチャします。Full Load の後にチェックを入れたテーブルがある場合は、**Re-run Full Load** が現在の集合に対して新しいウォーターマークを取得するか、そのテーブルのチェックを再び外すまで、タイプセレクターはロックされます。Full load only でロードしただけのテーブルはウォーターマークに含まれませんが、Full load + CDC でガード付きのロードが追加したテーブルは含まれます(次のセクションを参照)。その再実行は **Drop & reload** で行ってください: CDC はその新しいウォーターマークから開始しますが、*Append* は以前のロードが書いた行を、その後のソースの UPDATE や DELETE を反映しないまま残します。 |

### Full Load 後にテーブル集合を変更する (Full load + CDC)

Full load + CDC では、この移行で CDC を一度も開始していない間、つまり Start CDC を送信したことがなく、コネクタが存在せず、CDC インフラが一度もストリーミングしていない間は、Full Load の後でもテーブル集合を変更できます。テーブル一覧でテーブルのチェックを外すか入れるか、Change & reload でテーブル行を使います。復元されたセッションでも同じように動作します。Start over は不要です。

| 操作 | 結果 |
|---|---|
| **テーブルのチェックを外す** | Aurora DSQL 上にそのまま残されます(削除されません)。CDC はそのテーブルをキャプチャせず、この移行はそのテーブルをもうロードも検証もしません。カットオーバーの DEFAULT 復元は、そのテーブルを復元せずに **Needs your decision** の下に表示します。テーブルルールで選択を外した場合、そのテーブルの除外列のエントリは保持されません。 |
| **Full Load が分割して読んだ MySQL テーブルのチェックを外す**(*Reader shards per large table* に 2 以上を明示的に指定、[§3.8](03-full-load.md#38-並列ロード)) | できません: そのコピーは複数のスナップショットの行を含み、CDC が Full Load の開始位置から再生して初めて一貫します。チェックボックスはロックされ(ロックアイコンに理由が表示されます)、Unselect all もそのテーブルは選択したままにし、そのテーブルの選択を外すテーブルルールは拒否され、Full load only とより後の開始位置からの Start CDC も拒否されます。チェックを外すには、CDC を開始する前に、Settings で Reader shards per large table を 0(自動)または 1(無効)にし、そのテーブルを Drop & reload でロードし直してから(終わっていなければ Retry unfinished tables、それ以外は Reload specific tables: リーダー 1 つで読むときは Append / Drop & reload の選択にかかわらず削除してロードし直します)、チェックを外してください。CDC を開始するとテーブルの集合は固定されるので(変更するには Start over)、CDC の再生の後に CHECKSUM モードの Validation がそのテーブルの一致を確かめると Full load only とより後の開始位置への拒否は終わりますが、チェックを外すことはできません。 |
| **Full Load がロードしなかったテーブルにチェックを入れる** | まず Full Load の開始点の下でロードされ、その後で初めて CDC がキャプチャします(下記)。 |
| **CDC インフラがそのテーブルを含めずにデプロイされた後で再びチェックを入れる** | CDC を開始するときにキャプチャされます。そのテーブルの Kafka トピックにはインフラの既定のパーティション数が使われ、この数はインフラのデプロイ時のテーブルに合わせて決められています。 |
| **exclude table ルールが選択を外したテーブル** | Change & reload でそのルールを削除した場合にだけ戻ります。そのときテーブルの列ルールが適用されます(テーブル一覧でチェックを入れることは拒否されます。その列ルールが適用されないためです)。ロックのアイコンとその理由が表示されます。 |
| **Unselect all** | 使えます。ただし、Deploy CDC infrastructure と Start CDC には、選択されたテーブルが 1 つ以上必要です。 |
| **1 つの CDC パイプラインがキャプチャできる数より多いテーブル**(一般的な名前なら約 80〜160 個、[第 6 章](06-limitations.md)を参照) | ロードできます: Full Load ステップがロードの前に警告し、ここで十分な数のテーブルの選択を外すまで、Deploy CDC infrastructure と Start CDC はそのキャプチャを拒否します(Full Load が分割して読んだテーブルは除く、上記参照: そのようなテーブルがキャプチャされている間、その拒否はテーブルを示し、Full load only を示しません)。 |

- **Full Load がロードしなかったテーブルにチェックを入れると**、そのテーブルはまず Full Load の開始点の下でロードされ、その後で初めて CDC がキャプチャします:
  - ツールに求められたら前提条件チェックを再実行してください(そのテーブルは一度もチェックされていません)。ロードされるまで、Full Load ステップはそのテーブルを「Not loaded under the Full Load's start point yet」と表示して Failed となり、Start CDC、Validation、Cut over は待機します。
  - **Retry unfinished tables** が元のウォーターマークの下でそのテーブルをロードし、ロード済みのテーブルはすべて保持します。そのロードは最初に、Full Load 以降のそのテーブルを CDC がギャップなくキャプチャできるかを確認します。確認に失敗するとテーブルはロードされず、その行に理由と対処が表示されます。
  - ロード先は**空の** Aurora DSQL テーブルだけです。テーブルに既に行がある場合、ダイアログはそのテーブルを drop して reload します。それらの行が、CDC がそのテーブルについてキャプチャする変更(MySQL では Full Load の開始点以降の変更、PostgreSQL ではそのテーブルが CDC の publication に加わって以降の変更)より古いかどうかをツールは判断できず、append は既にある行をすべてそのまま残すためです。
  - **MySQL:** Full Load は、ロードしない移行可能なテーブルの開始点での定義を記録し、ロードはそれを現在のテーブル定義と比較します。以前のバージョンの Full Load や、ロードの後に変換したテーブルにはその記録がありません。**Drop & reload** で **Re-run all tables** を使うか(すべてのテーブルに新しい開始点を取るため、append はロード済みの行をそのまま残します)、チェックを外してください。開始点以降に定義が変わった場合も、Drop & reload での Re-run all tables が必要です。
  - **PostgreSQL:** ツールはソース上でそのテーブル(継承の子テーブルは含めません)を CDC の publication に追加し、既に開いているトランザクションが終わってからテーブルをコピーします。その時点で、テーブルとそのすべてのパーティションには、ソース上で使える `REPLICA IDENTITY` が必要です: `DEFERRABLE` でない有効な主キーを持つ `DEFAULT`、有効な一意インデックスを持つ `USING INDEX`、または `FULL`(再キー化したテーブルは既に `REPLICA IDENTITY FULL` である必要があります。PostgreSQL 18 では identity に生成列を含めてはならないため、生成列を持つ再キー化したテーブルは拒否され、その行が **Keep source PK** に戻すよう示します。[第 6 章](06-limitations.md) を参照)。そうでないと、publication に加わった後そのテーブルのすべての UPDATE と DELETE がソースで失敗するため、そのロードは追加する前にテーブルを拒否し(identity を読み取れない場合も拒否します)、その行は見つかった原因に応じた修正方法を示します。ソースのユーザーはそのテーブルと publication の所有者である必要があります。**Delete CDC infrastructure**(スロットと publication を削除します)の後にテーブルを追加するには、Drop & reload での Re-run all tables が必要です。
  - CDC がそのテーブルをキャプチャするのは、**隔離された行がない**ロードの後だけです。**Change & reload は、CDC がまだキャプチャしていないテーブルを drop して reload しません**(Full Load の後に選択したテーブル、または隔離された行とともにロードされたテーブル): Save がそのテーブルの列ルールを保存し、Full Load ステップでのそのテーブルのロードがルールを適用します。
  - **代償:** MySQL では、CDC は Full Load の開始点以降にそのテーブルに加えられたすべての変更を新しいコピーの上に再生し、PostgreSQL では、そのテーブルが CDC の publication に加わって以降(コピーの直前から)のすべての変更をそのコピーの上にストリーミングします。どちらの場合も結果は収束します。既にコピーされた行と一意インデックスで一時的に衝突する変更はデッドレターになることがあり、その場合 Cut over は CHECKSUM による検証を待ちます。MySQL では、Full Load 以降のすべての変更がバイナリログにまだ残っている必要があります。そのテーブルをロードする前に CDC インフラをデプロイしていた場合、そのテーブルの Kafka トピックにはインフラの既定のパーティション数が使われます。

- **CDC インフラがデプロイされている場合、ポップアップは読み取り専用です:** テーブル一覧を使ってください。
- **チェックを外したテーブルだけで起きた失敗は、もうステップを止めません。** ステップは Done と表示され、Continue to CDC が使えるようになり、PostgreSQL では CDC はロード時のスロットから再開します。Retry unfinished tables がチェックを外したテーブルを再ロードすることはありません。
- **待つべきときは待機として案内し、Start over は求めません:** Full Load、外部キーの適用、カットオーバーの DEFAULT 復元が実行されている間、CDC インフラの作成中または削除中、失敗した作成がアカウントに残っている間(CDC ステップで削除してください)、そしてページを開いた後に CDC の状態を読み取る数秒の間です。CDC の状態を読み取れない場合は、CDC ステップがその理由を示します。AWS へのアクセスを修正し、**Re-check CDC state** を使ってください。
- **現在の集合で Re-run Full Load を実行すると**、選択されたすべてのテーブルに対する新しいウォーターマークを取得します。PostgreSQL では既存の publication を再利用し、そこにない選択されたテーブルを追加します(上記のとおり、使える `REPLICA IDENTITY` がある場合だけです。そうでない場合、実行はソースで何も変更する前に止まり、どのテーブルかと、その原因に応じた修正方法を示します)。その再実行は **Drop & reload** で行ってください: CDC はそのウォーターマークから開始しますが、append はロード済みの行をそのまま残します。
- **外部キー。** 選択されたテーブルは、チェックを外したテーブルを参照する外部キーを保持します。Cut over では、親の行が複製されていないキーを orphan チェックが報告します。
- **CDC を一度開始すると、その後停止していても、集合は再び固定されます。** CDC only: CDC を開始する前に集合を変更するには、Full load + CDC に切り替えてください。選択されたテーブルが Full Load の開始点の下でロードされていない間、Start CDC は拒否されます。

### 隔離またはデッドレターから

除外すべき列は、データが移動した後で初めて見つかることがよくあります。2 つの復旧ボタンが、同じポップアップへつながっています。

- **Full Load の隔離: 「Exclude column & reload」。** これは、フラグが付いた列ごとに 1 行を挿入済みの状態で Change & reload を開きますが、その行は**保存されていません** (`exclude s.t.c  # from the Full Load quarantine`)。そのため reload には引き続き意図的な 2 回のクリック (ボタン、次に確認)が必要です。(Full load + CDC で CDC がまだキャプチャしていないテーブルの場合、確認は通常の Save です。Retry unfinished tables の対象として表示される、Full Load ステップでのそのテーブルのロードがルールを適用します。§13.7 を参照。) `include` がその列を残している場合、ポップアップはその行を指し示します(たとえば **Go to L5**)。理由がそのテーブルのフラグ付き列のいずれも指していない場合(`maximum column size exceeded` のように型を示さない場合や、どの列にもない型を示す場合)は、LOB のセクションがそのテーブルに絞り込まれた状態で開くため、 **+ add rule** で列を選べます。CDC スタックがまだ除外設定を保持している場合(またはこのセッションで Change & reload が提示されない場合)は、読み取り専用のポップアップが開き、その行は保留中として記録されます。 Change & reload が提示されるものの、まだ実行できない場合(たとえば再起動後に再接続する前)は、ボタンは理由を示して無効になります。
- **CDC のデッドレター: 「Exclude column...」。** このボタンはすべての状態で有効です。 `exclude s.t.c  # from the CDC dead-letter queue` を保留中の行として記録し、その後、ロックが許すモードでポップアップを開きます。ロックの手順に従ってください(上の表を参照): Stop CDC を実行してから CDC インフラを削除し、Full Load を実行していた場合は Change & reload を使い(そうでなければルールを直接変更し)、その後デプロイして Start CDC を実行します。

**保留中の行**は、エディタに入るのを待っているルールです。読み取り専用のポップアップは、それぞれを `Pending: "exclude ..." (from ...) will be added when the rules can be edited again.` と表示します。保留中の行はセッションとともに保持されます。次にポップアップが編集可能な状態で開くと、それらはエディタに現れ、Save したときにだけ効果を持ちます。保留中の行がそれだけで除外対象を変えることは決してありません。

---

## 13.8 CDC キャプチャリスト

CDC はキャプチャの時点で列を除外します。除外列は変更ストリームから完全に除かれます。ツールはこのリストを cdc-stack の `ColumnExcludeList` パラメータとして送ります。リストは CDC がキャプチャするテーブルだけを対象とし、CDC がストリーミングしてはならない、Aurora DSQL が計算する生成列も含みます。

ツールは、各エントリがちょうど 1 つの列に一致するように書きます。連結すると同じ `db.table.column` 文字列になる 2 つの名前(名前部分に `.` が含まれる場合に起こりえます)は、キャプチャの時点では区別できません。両方が除外されていない限り、Deploy、Start CDC、CDC ステップがこれについて警告します。

**4,096 バイトの上限。** `ColumnExcludeList` は 1 つの CloudFormation パラメータであり、パラメータの値は 4,096 バイトに制限されています。

- **長すぎる場合。** ポップアップでは Save が無効になります(「Narrow the rules: exclude fewer columns in the tables CDC captures」)。Deploy CDC infrastructure と Start CDC は止められ、 Full load + CDC では Start Full Load も、ロードの前に止められます。メッセージはバイト数と、現在の状態で有効な対処を示します。
- **生成列だけで上限を超える場合。** その場合、除外を減らしても役に立ちません。Deploy CDC infrastructure と Start CDC は「capture fewer tables in this CDC pipeline, or use Full load only」として拒否します。ポップアップでは、これは Save をブロックしない警告で、Start Full Load は止められません。
- **スタックが既に持っている値が拒否されることは決してありません。**

**ルールの保存後にソースが追加または名前変更した列。** リストは、ルールを保存した時点で決まった具体的な列名を保持します。そのため、ワイルドカードや `type:` ルールに一致する新しい列や、ソースで名前変更された除外列はキャプチャされ、その列を含むそのテーブルへのすべての insert と update がデッドレターされます(SQLSTATE 42703)。 **Fix target schema…** はその列を追加しません: ルールが外しているデータをコピーすることになるためです。外したままにするには: Stop CDC、**Delete CDC infrastructure** を実行し、ルールがその列を認識するよう Step 1(Evaluation)を再実行します。名前変更された列の場合は、その列を除外していたルールが新しい名前に一致するよう変更します: 古い列名を書いたルールは `No column "…"` を報告し、名前が近ければ修正方法を提案しますが、ほかのテーブルにまだ一致するワイルドカードは何も報告せず、その列を通してしまいます。Full load + CDC では、先に Aurora DSQL で古い列も手作業で削除します(`ALTER TABLE … DROP COLUMN …`。ダイアログが文を示します): その列には NULL だけ、またはルールが外している値だけが入っており、Change & reload は更新後の定義にもう無い列の削除を拒否するためです。その後、[集合がロックされる理由](#集合がロックされる理由) のとおりにルールを保存し(Full load + CDC: **Change & reload**。Aurora DSQL にその列を NULL として作成します)、再度デプロイして Start CDC を実行します (MySQL では「削除後に再び CDC を開始する」を参照)。代わりに新しい列を複製するには、同じ手順の中でルールがその列に一致しないよう変更します。

**表示される内容。**

- ポップアップ内: Start CDC と同じコードで計算された **CDC capture list (B of 4,096 bytes)**。
- **Deploy CDC infrastructure** の確認には「Excluded: N tables, M columns. They are fixed once deployed: changing them later needs Delete CDC infrastructure.」と表示され、**show** の下に正確なリストが表示されます。
- CDC ステップには、読み取り専用のサマリー「Excluded: N tables, M columns」と、正確な値を収めた展開可能な **What CDC receives (B of 4,096 bytes; as deployed | Start CDC sends)** が表示されます。デプロイ済みの値とセッションの値が異なる場合、ステップは両方を表示します。

**列を除外しているスタックへのアタッチ。** 新しい未実行のセッションが、`ColumnExcludeList` で列を除外している開始可能な cdc-stack にアタッチすると、ツールがリストを採用するのは、そのとおりに読み戻せる場合だけです。採用された列は、`# adopted from <stack>` の印が付いたルール行として表示されます。Start CDC、Retry CDC、および Full load + CDC では Start Full Load が、一度でもストリーミングしたスタックと異なるセッションの集合を拒否し、両方の集合を示します。

---

## 13.9 Validation と Cut over

### Validation は除外列が NULL であることを証明します

テーブルの行数を数えるターゲット側のパスが、除外列ごとに値を保持している行の数も数えます。追加のスキャンは不要です。読むのはターゲットだけなので、MySQL ソースでも PostgreSQL ソースでも同じように動作し、何も除外していなければ SQL は変わりません。除外列は値の比較の対象になりませんが(ツールが書き込んでいないため)、 NULL であることの証明は次の場所に表示されます。

- テーブル別の **Excluded** チップ(`NULL`、または `N non-NULL`)。
- readiness の通知(「Excluded columns are NULL on Aurora DSQL」)。
- 失敗理由、AI チャットに渡すファクト、テキストレポート。

除外列に値があることの意味は、移行タイプによって異なります。

| 移行 | 除外列にある値 |
|---|---|
| Full load only、Full load + CDC、または Full Load を実行した CDC only | そのテーブルは**一致しない**とされ、カットオーバーはブロックされます: 「Excluded columns hold values: reload those tables with Drop & reload」。この値は通常、テーブルに既にあるデータの上に行が append されたことを意味します。append は格納済みの値を保持するため、Full Load を再実行してもこれらは消えません。対処は状態に従います: ストリーミング中はまず Stop CDC、CDC only ではタイプを切り替え、ジョブレコードが無い場合は Start over です。 |
| 既に行を持っていたターゲットへの CDC only | 警告のみ: 「Excluded columns hold values from before CDC started」。 |

NULL チェック自体が失敗した場合は、not determined として報告されます。テーブルの行数と判定には影響しません。

### Cut over は削除された DEFAULT を復元します

カットオーバーのランブックに **Excluded-column DEFAULTs restored: n of m** のブロックが加わります。正確な文、 **Restore DEFAULTs** ボタン、各列のライブの状態(pending、done、diverged、gone、unknown)が表示されます。

```sql
ALTER TABLE "shop"."users" ALTER COLUMN "status" SET DEFAULT ('active'::text)
```

- **自動復元。** DEFAULT が自動で復元されるのは、列がまだ除外されており、記録されたテキストが単一の式であり、 Schema Conversion の DDL が記録時から変わっていない(またはまだ同じ DEFAULT を宣言している)場合だけです。それ以外の保留中の列はすべて、理由と文とともに **Needs your decision** の下に一覧されます。記録されたテキストが単一の式であれば、その列には個別の **Restore** ボタンが付きます。そうでなければ、文を自分で実行してください。
- **上書きはされません。** その後に誰かが別の値に設定した DEFAULT は、そのまま残されます。
- **新しい行のみ。** 復元された DEFAULT は新しい行に適用され、移行済みの行は NULL のままです。
- **NOT NULL は削除されたままです。** Aurora DSQL は NOT NULL をその場で戻せないため、ブロックには nullable のまま残る除外列が一覧されます。
- **CDC を使う場合は、先に drain 完了の確認にチェックを入れます。** これは「I have frozen source writes, both CDC connectors are RUNNING, and CDC has drained to zero lag」のチェックボックスです。外部キーのブロックも同じチェックボックスを使い、どちらかのブロックが表示されているときは常に表示されます。ツールはラグを見られないため、それが事実であるときにだけチェックしてください。コネクタがまだスナップショット中またはリプレイ中に DEFAULT を復元すると、それらの行がその DEFAULT を受け取ります。スタックまたはコネクタがまだ変化している間は、復元は拒否されます。実行中の Full Load や Start CDC は復元をブロックし、その逆も同様です。 Start CDC を実行するたびに、このチェックボックスはクリアされます。
- **復元は最後の Start CDC の後に。** Start CDC は復元された DEFAULT を再び削除します(§13.6)。
- **DEFAULT の復元が残ったままでもカットオーバーはブロックされません。** 「I've cut over to DSQL」の上の警告に専用の **Restore DEFAULTs** ボタンがあり、確認操作は `cut over with N DEFAULTs not restored` をログに記録します。
- **選択されなくなったテーブル。** テーブルがもう移行に選択されていない列(Full load only と、CDC を開始する前の Full load + CDC はロード済みのテーブルの選択を外せます)は自動では復元されず、**Needs your decision** の下に表示されます。

台帳を読めない場合、ブロックはその旨を示し(「The record of dropped DEFAULTs could not be read ...」)、 **Check again** を提示します。

---

## 13.10 制限事項と、ライブで検証していない項目

**計画に織り込むべき制限。**

- **NOT NULL の削除は一方向です。** 元に戻るのは、後でそのテーブルを Drop & reload したとき(Schema Conversion の DDL からテーブルを再作成します)だけです。
- 名前に `:` を含むテーブルは、保存された列の除外を持てません。テーブル全体を除外するか、その列を残してください。改行を含む名前はルールとして書けません。
- ライブの CDC がストリーミングしているテーブルに対するオーバーレイの ALTER やカットオーバーの `SET DEFAULT` は、CDC シンクにそのテーブルの次のバッチを 1 行ずつ(より遅く)書き込ませます。これはシンク接続ごとに 1 回です。行が失われることはなく、デッドレターも発生しません。
- ツールは CDC のラグを見られません。復元の前に、ソースの書き込みが凍結され CDC が追いついたことを、 (カットオーバーのチェックボックスで)自分で確認します。
- Check sizes は型に基づき、オプトインです。上限を超えうるすべての型を強調表示するわけではなく、2 MiB を超える行はチェックしません(§13.5)。

**ライブで検証済み。** MySQL ソース(Aurora MySQL 8.0.42)から Aurora DSQL へ。120k 行・2 ワーカーの実行を含みます。

**ライブでは未検証**(ユニットテストのみ): PostgreSQL の Full Load オーバーレイと PostgreSQL の CDC 側、ほとんどのキャプチャリストのエスケープ(名前内の `.`、カンマ、前後の空白、大文字小文字違いの双子の形)、そして Fargate のデプロイが使う S3 ベースの台帳(ライブ実行ではローカルの SQLite ストアを使いました)。

---

**前へ:** [付録: 性能テスト結果](12-performance-test-results.md)
