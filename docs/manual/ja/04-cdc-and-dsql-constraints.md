# 4. CDC の動作と DSQL 制約の処理方法

_言語: [English](../en/04-cdc-and-dsql-constraints.md) | [한국어](../ko/04-cdc-and-dsql-constraints.md) | **日本語**_

> **前へ:** [3. Full Load](03-full-load.md)
>
> **CDC から列を外すには** [13.8 CDC キャプチャリスト](13-excluding-tables-and-columns.md#138-cdc-キャプチャリスト)を参照してください。

**CDC (Change Data Capture)** は、ほぼ無停止のカットオーバーを実現するための **オプション** のストリーミングパイプラインです。Full Load が既存の行をコピーした後、CDC はソース側で発生するすべての新規 insert/update/delete を反映し、DSQL を継続的に最新の状態に保ちます。これにより、長時間の停止を伴わず、最小限のダウンタイムでカットオーバーできます。

CDC が必要になるのは、**大規模または継続的な** マイグレーションの場合だけです。短時間の凍結が許容される一回限りのカットオーバーであれば、Full Load だけで十分です。

---

## 4.1 パイプライン

<p align="center"> <img src="../../images/architecture-cdc-pipeline.png" alt="CDC パイプライン: ソース変更ログ（MySQL binlog / PostgreSQL の論理レプリケーションスロット経由の WAL）→ Debezium ソースコネクタ → Amazon MSK（PK でキー付けしたテーブルごとのトピック + DLQ）→ カスタム DSQL シンクコネクタ → Aurora DSQL" width="900"> </p>

ソースの変更ログは 3 つのステージを通ります。両方の connector は **マネージドの MSK Connect** 上で実行され、ツール自身は **sink 用のコンピュートを一切持たず**、コントロールプレーンとして機能します (構成の作成、再開ポイントの確立、監視を行います)。

- **Debezium source connector（MySQL または PostgreSQL）** は、ソースの変更ログ — MySQL はバイナリログ、PostgreSQL は論理レプリケーションスロット経由の write-ahead log (WAL) — を読み込みます。ソースは読み取り専用のままで、認められた例外は 2 つです: PostgreSQL ソースではツールがマイグレーション対象テーブルだけにスコープした論理スロットと publication を作成（し、解体時に削除）します。また、`gtid_mode=ON` でない MySQL ソースでは、Full load + CDC の実行が開始位置を記録した直後に `FLUSH BINARY LOGS` を 1 回実行します ([§3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し))。
- **Amazon MSK (Kafka)** は耐久性のあるバックボーンです。**テーブルごとに 1 トピック** を持ち、主キーでキーイングし (ある行のすべての変更が 1 つのパーティションで順序を保つ)、加えてデッドレター (DLQ) トピック、アイドル区間でもコミット済みソース位置を前進させる **ハートビート** トピック、MySQL に限り Debezium の **スキーマ履歴** トピックを備えます。再起動にギャップがないのは、sink が適用していなかった変更を CDC トピックがまだ保持している間だけです (§4.3)。
- **カスタム DSQL sink connector** — このプロジェクトが所有する Java Kafka Connect プラグイン — が変更を DSQL に適用します。標準的な JDBC sink は DSQL の短命な IAM トークンで認証できず、楽観的並行性の競合 (`SQLSTATE 40001`) や DSQL が拒否する行も扱えません (§4.4)。CDC スタックはデプロイ時のプラグインをそのまま使い続けるため、このバージョンのツールに同梱されているものより古い sink プラグインをスタックが実行している間は CDC ステップがその旨を表示し (*This CDC infrastructure runs an older sink plugin*)、**Delete CDC infrastructure** の後に再デプロイすると同梱のプラグインが登録されます。

---

## 4.2 CDC は *スキーマ* ではなく *データ* を複製する — 重要

CDC は **行レベルのデータ変更** (insert / update / delete) を複製し、SQL 文や **DDL** は複製しません: ソースの `ALTER TABLE`、`CREATE`、`DROP` は **伝播されず**、Aurora DSQL のスキーマは **Schema Conversion** が作ったままです。

**CDC の実行中にソースのスキーマを変更する場合** は、対応する変更を Aurora DSQL に自分で行ってください (追加または名前変更された列は、ドリフトバナーの **Fix target schema…** が行います)。ただし Schema Conversion では行いません: CDC のストリーミング中は **Replace** を含めて Apply がブロックされます (Replace はテーブルとロード済みの行をまとめて削除し、その行は **Drop & reload** でしか戻せません)。ソースより先に Aurora DSQL へ NULL を許可する列を追加するのは安全です。

ターゲットが一致するまでの間、ターゲットが受け取れない行は **DLQ** に **デッドレター** されて再試行されず、そのようなテーブルが最後のデッドレターレコード以降に CHECKSUM を通過するまで Cut over はロックされたままです ([§5.5](05-validation.md#55-cdc-のデッドレターとカットオーバー))。デッドレターをまったく出さない変更もあります: 下の [デッドレターにならない変更](#デッドレターにならない変更-ソーススキーマの再確認) を参照してください。

### CDC 中のソーススキーマドリフトの検出と対処

ソースの `ALTER` を自分で気づく必要はありません。デッドレターされたレコードがソースの形状変更を示すと、CDC ステップに **「ソーススキーマ変更を検出」** バナーが表示されます。Aurora DSQL には `ALTER COLUMN … TYPE` も `SET NOT NULL` もなく、主キーをその場で変更することもできないため、下の各行は Aurora DSQL がサポートする修復方法を示します。ツールはターゲットのデータを自動で削除したり書き換えたりしません。

| バナーの行 | SQLSTATE | よくある原因 | Aurora DSQL での修復 |
|---|---|---|---|
| column added or renamed at the source | `42703` | `ADD COLUMN` / `RENAME COLUMN` | **Fix target schema…** (下記) |
| a NOT NULL column got no value（再確認が裏付けると "column dropped at the source"） | `23502`、列が送られていない | `DROP COLUMN`、除外された列、または PostgreSQL で大きな（TOAST された）値を変更しなかった更新 | 削除された列なら **Drop & reload** の前に `DROP COLUMN`（その列がある間 Drop & reload は拒否されます。主キー列は削除できません）、それ以外は `ALTER COLUMN … DROP NOT NULL` |
| a NULL sent for a NOT NULL column（裏付けられると "NULL rule changed at the source"） | `23502`、NULL が送られた | ソース列が NULL を許容するようになった | `ALTER COLUMN … DROP NOT NULL`（値は保たれます。NOT NULL に戻すことはできません） |
| column type changed at the source | `42804`（削除では `42883`） | 互換性のない型変更 | 作り直し: **Replace** + **Drop & reload** |
| a value its Aurora DSQL column cannot hold exactly | `22001`、`22003`、および sink の拒否 | 広げられたソースの型、または Schema Conversion で選んだより狭い型 | 同じ作り直し |
| a CHECK constraint rejected the row | `23514` | MySQL の `ENUM` や PostgreSQL の enum に追加された値（どちらも `text` + `CHECK` に移植されます） | 理由に記された名前で `DROP CONSTRAINT` の後 `ADD CONSTRAINT … CHECK (…) NOT VALID`、トランザクションごとに 1 文 |
| the change records' key no longer matches the Aurora DSQL primary key（赤: すべての insert と update が失われる） | `42P10` | ソースの主キーが変更された、またはテーブルが別のキーで再適用された | キーを戻すか作り直し（Step 1 を再実行、**Replace**、**Drop & reload**。キーを付け替えるテーブルは **Delete CDC infrastructure** と新しいデプロイも必要） |

**CDC only** には再ロード元の Full Load ステップがありません: 表で **Drop & reload** とある箇所は、Aurora DSQL 上でテーブルを手作業で直し、その間に脇へ置かれた行を修正するか (型の変更なら新しい型の列を追加して値をコピーし、古い列を削除して新しい列の名前を変更します。トランザクションあたり最大 3,000 行)、**Start over** を使ってください。

**Fix target schema…** (追加または名前変更された列用) は、正確な文を示し、承認したときだけ適用します (トランザクションごとに 1 DDL)。その後、そのテーブルの CDC は再開します。

- **新しい列** は追加されます (NULL 許容、デフォルトなし)。**名前変更された列** は、名前変更に見えるときに値を保ったまま名前が変更されます (たとえば Step 1 のインベントリの同じ位置に同じ型で古い名前がある、または大文字小文字だけ異なる場合)。それ以外は追加列として提示され、ソースにもうない Aurora DSQL の列を示す警告が付きます。ダイアログの推測が違っていたらキャンセルして手作業で直してください。
- **追加しないもの:** **生成** 列、Aurora DSQL が保持できない型 (手作業で追加する方法をダイアログが示します)、除外設定で外している列 ([§13.8](13-excluding-tables-and-columns.md#138-cdc-キャプチャリスト)): キャプチャリストが変わるまでその行はデッドレターされ続けます。**ENUM** 列は `CHECK` なしの `text` として追加され、警告にその `CHECK` を追加する文が示されます。
- **名前変更・削除・変更された主キーまたは CDC キーの列** には手を付けません: CDC はそのキーで行を書き込み、Aurora DSQL は主キーをその場で変更できないため、ダイアログは代わりに作り直す手順 (上の表の最後の行) を示します。

**すでに脇へ置かれた行をバックフィルするには:**

- **Full Load + CDC:** Step 1 (Evaluation) を再実行 → Stop CDC → **Drop & reload** でテーブルをリロード (Schema Conversion でその DDL を編集していなければ、新しいキー・型・NULL 規則・ENUM の値で作り直されます) → Start CDC → Validation で再確認します (そのテーブルは CHECKSUM が自動です)。
- **CDC only:** Aurora DSQL 上で行を手作業で修正する (トランザクションあたり最大 3,000 行) か、**Start over** を使います。デッドレターキューのカードは各レコードの主キーを示しますが、キーの値は整数か UUID のときだけ表示する（それ以外は伏せる）ため、キーの値が伏せられるテーブルには Start over を使ってください。

存在しなくなったターゲットテーブル (`42P01`)、一意キーの競合 (`23505`)、sink がバインドできない値 (`07006`) もバナーの行になり (*Stop CDC and reload these tables*、**CDC only** では *Stop CDC and repair these tables*)、修復は同じです。まずターゲットにテーブルがあることを確認してください (なければ Schema Conversion で再適用)。ターゲットで削除されたテーブルは、削除前に適用されたすべての行も失っており、どのデッドレターもそれらの行を挙げないため、そのテーブルには Start over を使ってください。`23505` の行は一意インデックスの名前と、起こり得る原因を示します (§4.4 と §4.6 を参照)。ソースがその `UNIQUE` インデックスを削除または緩和した場合 (CDC はこれを複製しません) は、Aurora DSQL でも削除してください。

そのほかのデッドレターは専用のバナーの行にならず、デッドレターキューのカードにだけ表示され、SQLSTATE があればそれも表示されます:

- Aurora DSQL には大きすぎる値 ([§4.5](#45-値ごとの-1-mib-上限および-dlq));
- sink が変換できない値。たとえば PostgreSQL の `timetz[]`、`bytea[]`、または制約のない `numeric[]` 列。Evaluation はこのような列を事前に示します (`CDC_UNCARRYABLE_COLUMN`。代わりに NULL で届く列も併せて: `interval[]`、`money[]`、`tsvector`、そして pgvector の `vector` のようなほとんどの拡張機能の型; [§6.2](06-limitations.md#62-移行プロセスの制限));
- ソースの主キーが削除された後のキーのない変更レコード (MySQL または PostgreSQL): ソーススキーマの再確認が "primary key changed at the source" として報告します;
- そのほかのすべての拒否 (たとえば `22P02`、`22007`/`22008`、`22021`、`23503`、`54000`)。

テーブルにソース DDL を実行した後は、そのテーブルを CHECKSUM で検証してください。

**列が正確に保持できない値** (MySQL・PostgreSQL ソース) は書き込まずに拒否します。sink は各数値を Aurora DSQL の列と照合するため、`boolean` 列に 0 または 1 以外の数値を `true`/`false` として黙って保存したり (MySQL の `TINYINT(1)` はソースの `ALTER` がまったくなくても 127 まで保持できます)、より少ない小数桁や整数に丸めたりすることはありません。その行はデッドレターになり、CDC ステップにはテーブルと列を示す **「Rows set aside: a value does not fit its Aurora DSQL column」** バナーが表示されます (値は決して記されません)。Aurora DSQL 自体も、`varchar`/`char` 列より長い文字列 (`22001`) や、整数・`numeric` 列の範囲を超える数値 (`22003`) を拒否し、そうした行にも同じバナーの行が示されます。確認されず、引き続き丸められるもの: `numeric` 列に入る `FLOAT`/`DOUBLE` の値と、精度を指定せずに宣言された PostgreSQL の `numeric` (Schema Conversion が警告するとおり `numeric(18,6)` で保存されます)。Aurora DSQL は列の型をその場で変更できないため、値を保持できる型でテーブルを作り直してください (たとえば `TINYINT(1)` は `smallint` で **Replace** してから **Drop & reload**。**CDC only**: バナーの表の下の説明を参照)。

**ソースの `TRUNCATE`** (MySQL・PostgreSQL ソース) は複製されません。Aurora DSQL には `TRUNCATE` 文がなく、そのイベントは削除する行を示さないためです。代わりに sink はそのイベントをデッドレターにし (TRUNCATE されたテーブルごとに 1 件)、CDC ステップにはテーブル名を示す **「Source TRUNCATE was not replicated」** バナーが表示され、そのテーブルへのその後の変更はストリーミングされ続け、ターゲットにはソースが削除した行が残ります (Validation が extra 行として報告します)。復旧方法:

- **Full Load + CDC:** Stop CDC → Full Load ステップで **Drop & reload** によりテーブルをリロード（CDC の停止中にのみ表示されます。**Append** は欠けている行を追加するだけなので、余分な行は削除できません）→ Start CDC → Validation でそのテーブルを再確認します。
- **CDC only:** リロード元の Full Load ステップはありません。Stop CDC → Aurora DSQL のテーブルから、ソースにもう存在しない行をトランザクションあたり最大 3,000 行ずつ自分で削除 → Start CDC → Validation でそのテーブルを再確認します。単一列の整数キーのテーブルなら、Validation の **Reconcile every record** が余分な行の数を数え、そのキーの一部を表示します。

ワークロードが定期的に TRUNCATE するテーブル (ステージングやキューのテーブルなど) は TRUNCATE のたびにこの状態になるため、Aurora DSQL でそのデータが不要なら移行対象から外してください。パーティションの保守 (MySQL の `TRUNCATE PARTITION` や `DROP PARTITION`、PostgreSQL でのパーティションの drop・detach・truncate、行を持つパーティションの attach) は変更イベントなしに行を変えるため、CDC は何も報告せず、余分な (または欠けた) 行は Validation でしか確認できません。そのようなテーブルも同じ方法で修復するか、カットオーバーまでパーティションの保守を止めてください。

**ドリフト中の遅延。** ターゲットに列・テーブル・スキーマがない、キーが変わった、列の型が変わった、または `NOT NULL` 列に値が来ないという理由で Aurora DSQL がテーブルの行を拒否すると、シンクはそのテーブルの以後の変更だけを 1 トランザクション 1 行で適用し、他のテーブルはバッチ適用を続けます。他のテーブルも同じシンクを共有しているため、修復の間は stream lag が増えることがあります。修復すると、そのテーブルの INSERT または UPDATE が 1 件適用された時点、または最後の拒否から 60 秒後にバッチ適用に戻ります。値単位の拒否(一意制約・`CHECK` 違反、長すぎる値や範囲外の値、バインドできない値)ではテーブルは 1 行ずつの適用に切り替わりません。そのような行を含む変更バッチは、他のテーブルの行も含めて引き続き 1 トランザクション 1 行で再適用されます。

### デッドレターにならない変更: ソーススキーマの再確認

ソースの DDL の中には、Aurora DSQL が 1 行も拒否しないまま CDC が書き込む値を変えるものがあり、デッドレターもバナーも現れません:

- ターゲットの列が nullable か DEFAULT を持つ **列の削除**: 新しい行には NULL か DEFAULT が入り、更新された行は古い値のままになります;
- ターゲットがすべての値をそのまま受け入れる **型の変更**(より広い `VARCHAR` や整数型)、または `numeric(p,s)` ターゲットで `FLOAT`/`DOUBLE` に変更された列(例: `DECIMAL(10,2)` → `DOUBLE`): このケースは確認しないため、値はデッドレターなしに丸められます(それ以外の丸められる値や `true`/`false` として保存される値はデッドレターになります。[上記参照](#cdc-中のソーススキーマドリフトの検出と対処));
- **テーブルの削除・名前変更**(または別スキーマへの移動): CDC はデプロイ時の名前でテーブルをキャプチャするため、ターゲットのテーブルは変化しなくなります(MySQL では同じ名前で作り直したテーブルがそのまま流れ込み、PostgreSQL では名前変更後、コネクタが再起動するまでの間だけ古いターゲットに変更が届きます);
- PostgreSQL で、CDC が変換できない型(たとえば `interval[]` や `money[]`)の **列の追加**、または **生成列**: その値はまったく届きません;
- **NULL ルール** の変更、または **主キー** の変更。

そのため CDC の実行中、ツールはソース上のキャプチャ対象テーブルの定義を読み直し(読み取り専用でテーブルのデータは読みません。CDC モニター・Validation・Cut over が開いている間、最大 5 分に 1 回)、移行の基準である Step 1(Evaluation)のインベントリと比較します: 各テーブルがまだ存在するか、列名(大文字小文字まで正確に)、型、NULL ルール、主キー。除外した列、CDC がキャプチャしない生成列、デフォルト・コメント・照合順序・インデックスは比較せず、**Fix target schema…** で追加した列は定義の一部とみなします(名前を変更した列はみなしません: Step 1 を再実行するまで、再確認は名前変更を報告し続け、Cut over を保留します)。差異は CDC ステップに表示され([§4.6](#ソーススキーマの再確認))、ソースが再び Step 1 のインベントリと一致し、その後テーブルが CHECKSUM で一致するまで、そのテーブルの Cut over を保留します([§5.7](05-validation.md#57-ソーススキーマの変更とカットオーバー))。

---

## 4.3 ギャップのない Full Load → CDC ハンドオフ

これは、バルクロードとストリームの間で **変更の取りこぼしも重複もない** ように設計された部分です。

### 仕組み

1. **Full Load がスナップショット時点でウォーターマークを記録します。** MySQL は binlog の file:position、GTID セットとソースの `gtid_mode`、PostgreSQL は論理レプリケーションスロットを作成したときに返される WAL LSN で、スロット名・publication 名とともに記録されます ([第 3 章 §3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し))。
2. **Start CDC はまさにそのウォーターマークから再開します。** スナップショット後の最初の変更から始まり — 「今」からでもなく、行を再読み込みすることもありません。MySQL ではツールが connector の開始オフセットに file:position と GTID セットをシードし (`gtid_mode=ON` では Debezium はまさにその GTID セットの次から開始します)、使えない開始位置は拒否します ([第 6 章](06-limitations.md))。PostgreSQL では、Full Load の整合点で作成されたスロットがそのままウォーターマークなので、何もシードされません。
3. **sink はすべての変更を PK で冪等に適用します。** そのため、オーバーラップやリトライがあっても重複は生じません。

Manual の開始位置は、binlog file:position とともに保存された GTID セットをシードします。バージョン 0.1.565 以前が保存した Manual の開始位置は GTID セットなしで復元されるため、`gtid_mode=ON` のソースでは Start CDC の前に **Fetch current position** をもう一度行うか、ちょうどその位置で実行済みだった GTID セットを入力し直してください。

> **これは `Full load + CDC` 実行についての説明です。** **Full load only** の後で CDC を開始しても無損失ですが、動作が異なります。**PostgreSQL** ではスロットを過去の位置に作成できないため、source connector が自前のスロットを作成し、**選択したすべてのテーブルを先に再スナップショット** します (ソースをもう一度読み込みます)。**MySQL** ではバイナリログが履歴を保持するため、再読み込みなしでウォーターマークから開始します — ただしストリーミングしたことのあるスタックでは Start CDC がより新しい開始位置を拒否するため、マイグレーションの種類を CDC only に切り替えてから、その CDC ステップで Delete CDC infrastructure して再デプロイし、Start CDC してください (Full load only には CDC ステップがありません。[第 3 章 §3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し))。どちらの場合も CDC はウォーターマークの後の変更しか運ばないため、その Full load only の実行が以前のロードがターゲットに残した行に append した場合は、先に **Drop & reload** で Re-run Full Load してください。`gtid_mode=ON` でないソースでは、MySQL のウォーターマークは処理中のコミットを待たずに記録された binlog の位置です: CDC が追いついたら CHECKSUM モードで検証するか、正確な開始位置が必要なら Full Load を Full load + CDC で再実行してください。選び方は [第 10 章 §10.1](10-conclusion.md) を参照してください。

### CDC を開始する前に

- **MySQL — binlog を維持してください。** ウォーターマーク位置の binlog が、CDC 開始時点でまだ存在している必要があります。Aurora MySQL はデフォルトで binlog を 24 時間しか保持せず、CDC スタックのデプロイだけで約 15〜20 分かかるため、**開始前に binlog の保持期間を延長してください** ([§1.6](01-setup.md#16-cdc-のためのソース準備))。すでに失われている場合は、**Drop & reload** で Full Load を再実行して新しいウォーターマークを取得します (ストリーミングしたことのあるスタックでは、Delete CDC infrastructure して再デプロイした後に)。
- **PostgreSQL — スロットを健全に保ってください。** スロットは作成された時点から WAL を保持するため、前提条件は binlog の保持期間ではなく `wal_level=logical` です。スロットが遅れすぎる (`wal_status='lost'`) と Start CDC がブロックされます (下の「Start CDC がブロックされるとき」を参照)。
- **Full Load を完了させてください。** Full Load が実行されている間、CDC ステップは *Start CDC waits for the Full Load to finish* を表示し、Start CDC は無効になります。逆に、CDC がストリーミングしている間は Full Load ステップは何も実行しません。先に Stop CDC してください。
- **Full Load は append しましたか?** ターゲットに既にあった行を見つけた場合 (*already there*。たとえば既定の *Append* での再実行)、開始位置のカードが *Gapless only if this Full Load did not append* を示します。append はそれらの行をそのまま残し、CDC はウォーターマークの後の変更しか運ばないため、Start CDC の前に **Drop & reload** で Re-run Full Load してください (CDC only では、その実行の間はマイグレーションの種類を Full load + CDC に切り替えます)。この通知は Start をブロックせず、append しなかったコピーの後にも表示されることがあり、Drop & reload の後に消えます。Full Load のダイアログが事前に警告します (*CDC from this run's new start point needs 'Drop & reload' …*)。
- **Full Load の後でテーブル集合を変更しますか?** [§13.7](13-excluding-tables-and-columns.md) を参照してください。

### 再開始

**Stop CDC 後の再起動** は、最後に記録された位置から続き、記録されたスキーマ履歴を再生します。行は再読み込みしません。Stop CDC は sink をドレインせずにコネクタを削除するため、sink が適用していなかった変更は CDC トピックからしか Aurora DSQL に届きません。**再起動にギャップがないのは、トピックがその変更をまだ保持している間だけです** (MSK での保持期間で、Start CDC はこれを確認しません)。**Stop CDC** のダイアログと *Ready to resume CDC* のカードがこれを伝えます。

| 状況 | Start CDC が行うこと | あなたが行うこと |
|---|---|---|
| Stop CDC 後の再起動、または CloudFormation がロールバックした Start の後 | 最後に記録された位置から続けます (PostgreSQL: スロットの位置)。スナップショットを完了しなかった source connector は、選択したすべてのテーブルをもう一度スナップショットします。 | トピックが未適用の変更をまだ保持している間は何もしません。保持期間が過ぎたら、Delete CDC infrastructure して再デプロイし、**Drop & reload** で Re-run Full Load してから Start CDC してください (CDC only: Delete CDC infrastructure して再デプロイし、Start CDC すると選択したすべてのテーブルをスナップショットします)。 |
| ストリーミングしたことのあるスタックでの、より新しい開始位置 (Re-run Full Load またはより新しい Manual の位置) | 拒否します (*This CDC stack cannot start from a newer start point*): そのスタックのトピックにはどの sink も適用していない変更が残っている可能性があります。 | Delete CDC infrastructure して再デプロイ (新しい空のトピック) し、Re-run Full Load するか Manual の位置を設定してから Start CDC してください。 |
| 新しい開始位置での最初の Start (MySQL) | まず、キャプチャ対象テーブルがその開始位置以降に変更されていないかをチェックし ([第 6 章](06-limitations.md))、行は再読み込みせずに、スキーマ履歴を現在のソーステーブルから再構築します。 | なし。 |

### Start CDC がブロックされるとき (PostgreSQL ソース)

Start CDC は、コネクタを作成する前に、publication、レプリケーションスロットとその位置、継承の子テーブルを確認します。ブロックの通知はまずその原因自体の対処を、続いて下の手順を示します。

| 表示されるもの | 対処 |
|---|---|
| *Replication slot invalidated*、*CDC's replication slot is not at this Full Load's start point*、存在しない publication またはスロット、選択したテーブルが欠けた publication | 下の手順に従います。 |
| *CDC's publication does not publish every change type* | publication の所有者として、ソースで `ALTER PUBLICATION … SET (publish = 'insert, update, delete')` を実行してから、下の手順に従います。publication が絞られていた間にコミットされた変更は戻りません。 |
| 同じ名前のスロットがあるが CDC が使えない (別のデータベースのスロット、または論理 `pgoutput` スロットではない) | そのスロットを削除するか、このマイグレーションに独自の CDC スタック名を与え、その後に下の手順に従います (CDC only では Start CDC をもう一度実行します)。 |
| *CDC cannot snapshot a table that has inheritance child tables* | **Drop & reload** で Re-run Full Load します (コネクタを以前に開始したスタックでは先に Delete CDC infrastructure して再デプロイし、Full Load ステップのない CDC only では、先にマイグレーションの種類を Full load + CDC に切り替えます)。または、テーブルの選択をまだ変更できる間に親の選択を外します。 |
| *CDC's publication and replication slot could not be checked* | ツールがソースを読めません (たとえばアプリの再起動後の Secrets Manager ソース)。Start CDC はブロックされませんが、これらのチェックは実行されません。Connect ステップでソース接続をテストしてから、Start CDC をもう一度開いてチェックが実行されるようにしてください。 |

**手順。** スタックのコネクタを開始したことがあるかどうかで異なります:

- コネクタを **一度も開始していない** スタック: **Drop & reload** で Re-run Full Load すると、スロットを自身のスナップショット位置に作り直し、その後に Start CDC します (Full Load ステップのない CDC only では、先にマイグレーションの種類を Full load + CDC に切り替えます)。または **Re-snapshot every table instead** — Start CDC のダイアログは、再スナップショットする Start 自体がチェックを通る場合にのみこれを提示します (そうでなければ *Re-snapshot every table is not offered here: a Start that re-snapshots is refused too.* と表示し、通用する手順を示します)。
- コネクタを **以前に開始した** スタック (その後停止した、または最後の Start がロールバックされた場合): CDC トピックにどの sink も適用していない変更が残っている可能性があり、Debezium は以降の Start でスナップショットをスキップするため、再スナップショットは提示されず、手順は新しいトピックからです: Delete CDC infrastructure して再デプロイし、**Drop & reload** で Re-run Full Load してから Start CDC します。Full Load ステップのない CDC only では、Delete CDC infrastructure して再デプロイし、新しいスタックで Start CDC すると、選択したすべてのテーブルをスナップショットします (その間にソースで削除された行は Aurora DSQL に残り、Validation が EXTRA の行として報告します)。

開始位置のカードの **Resume from the Full Load's slot instead** は、記録された再スナップショット (*Manual re-snapshots instead of resuming from this load's slot*) を元に戻します。スロット名は CDC スタック名から決まるため、同じソースのマイグレーションごとに独自の CDC スタック名を使ってください。

---

## 4.4 sink がデータパスで DSQL の制約を処理する方法

DSQL は分散型かつ PostgreSQL 互換であるため、sink は従来の MySQL/JDBC ライターのようには動作できません。各制約に対する処理は次のとおりです。

| DSQL の制約 | カスタム sink の処理方法 |
|---|---|
| **IAM トークン認証 (パスワードなし)** | 短命な IAM トークンを TLS 経由のパスワードとして使用し、**有効期限前に更新** して、長時間実行される CDC が期限切れトークンで停止しないようにします。 |
| **楽観的並行性制御 (ロックなし)** | `SQLSTATE 40001` の場合、競合した **トランザクション** を指数バックオフ + ジッターで再実行します (最大 10 回): ≤3000 行のチャンク 1 つ (適用は冪等なので安全)、または拒否された行を隔離するために 1 行ずつ適用している間は 1 行。再実行は毎回クリーンなトランザクションから始まります: 失敗した試行を先にロールバックします。Aurora DSQL はコミット時だけでなく、ステートメントの実行中にも `40001` を返すことがあるためです。 |
| **一時的なサービス状態 (`XX000` "server unavailable")** | Aurora DSQL は `XX000` を一時的な状態として文書化しているため、sink はこれを `40001` とまったく同じように扱います: 失敗した試行をロールバックしてから **トランザクション** を同じバックオフで再実行し、両者は最大 10 回の予算を共有します。その予算を超えて続く場合、sink は再接続し、Aurora DSQL が再び応答するまで Kafka Connect がバッチを再配信するため、そのような行は **デッドレター化されません**。その間、CDC ステップはシンクが停止していると表示し ("The sink is not applying changes")、シンクコネクタのログには `DSQL apply failed (transient sqlstate=XX000); Connect will retry` が繰り返し出力されます。Aurora DSQL が再び応答すると、ストリームは自動的に再開します。`XX000` が繰り返し発生する場合は AWS サポートに問い合わせてください。 |
| **主キー以外の一意インデックス (`23505`)** | Aurora DSQL は一意インデックスを即座に検査するため、別の行がまだその値を持っている間は変更が拒否されることがあります: 主キーの `UPDATE` (Debezium はこれを古いキーの delete と新しいキーの insert として複製し、両者は別々のパーティションを通ることがあるため、insert が先に届くことがあります) や、ある行から別の行へ渡される一意の値です。sink (プラグイン v53 以降) はそのような変更と、そのパーティション上の以降の変更をすべて、値を空ける変更が適用されるまで保留します: ほかのパーティションがすべて適用され続ける間、間隔を伸ばしながら (500 ms から 10 s まで) 再試行し、最大 5 分間です (Aurora DSQL の障害で待機が中断された場合はそれより長くなります。詳細は [§6.2](06-limitations.md#62-移行プロセスの制限))。5 分は保留された変更ごとに適用されます。待機はそのテーブルの Stream lag として表れます (§4.6)。リキーされていないテーブルで同じ行の後の変更がそれを完全に置き換える場合は (通常の delete、または同じ列のイメージ)、競合した変更は代わりにスキップされます (デッドレターにはなりません)。リキーされた (Composite key) テーブルでは何もスキップしません。スキップした変更の行は、その置き換えが適用されるまでターゲット上の値がそのまま残ります。そのため、スキップした変更が空けるはずだった値を必要とする別の行の変更は、5 分後にデッドレターになることがあります ([§6.2](06-limitations.md#62-移行プロセスの制限) に該当するケースがあります)。5 分を超えると SQLSTATE `23505` とインデックス名でデッドレターになり (*a unique constraint rejected the row*)、デッドレターが出たすべてのテーブルと同じく、そのテーブルが CHECKSUM で一致するまで Cut over は保留されます ([§5.5](05-validation.md#55-cdc-のデッドレターとカットオーバー))。 |
| **トランザクションあたり ≤ 3000 行** | ≤ 3000 行かつ最大 8 MiB のバイト予算のチャンク単位で適用し、チャンクごとに 1 回 `commit()` します。テキスト・JSON・バイナリの値はすべてそのサイズで数えるため、大きな JSON ドキュメントのまとまりも、Aurora DSQL のトランザクションあたり・メッセージあたり 10 MiB の上限を十分下回るように分割されます。 |
| **ステートメント単位の UPDATE / リプレイなし** | すべての変更は **PK でキーイングされた冪等な upsert または delete** です (`INSERT ... ON CONFLICT (pk) DO UPDATE`、`DELETE ... WHERE pk = ?`、Kafka のトゥームストーン)。同じイベントの再適用は安全です。 |
| **接続の切断 (アイドルクローズ / トークン期限切れ / ワーカー再生成)** | 切断された接続やハーフオープンの接続を検出し、新しいトークンで再接続して **同じオフセットをリプレイ** します (適用が冪等なため安全)。レコードを破棄することはありません。接続エラーは一時的なものとして扱ってリトライし、poison row と誤認しません。Aurora DSQL が接続を開くこと自体を拒否した場合 — IAM トークン (`28xxx`)、開いている接続が多すぎる (`53300`)、接続レートの超過 (`53400`) — も同様です: 新しく署名したトークンでバッチを再試行し、何もデッドレターにしません。 |
| **外部キー (ドレイン後のカットオーバーで適用)** | sink がストリーミングしている間、ターゲットに外部キーはありません: sink はテーブルをまたぐ変更を親→子の順序なしに適用するため、強制されたキーがあると先に届いた子行を拒否します(`23503`、デッドレター)。キーは最終ドレインの後、カットオーバーで追加されます: `NOT VALID` ですぐに(数秒、その時点からすべての新規書き込みを強制)追加し、ロード済みの行はその後 Aurora DSQL がバックグラウンドで検証します([第 2 章](02-evaluation-and-schema-conversion.md))。ソースの `ON DELETE` / `ON UPDATE` アクションはストリームに届かない(InnoDB がエンジン内部で適用する)ため、それが変更した子行でその検証が失敗することがあります: MySQL ソースでは **I've cut over** は、そうしたキーが検証されるまで、またはその検査なしでカットオーバーすると選ぶまで待ちます。 |

CDC スタックはデプロイ時のプラグインをそのまま使い続けます: 古い sink プラグインがこの表と異なる動作をする場合は CDC ステップがその旨を表示し (*This CDC infrastructure runs an older sink plugin*)、**Delete CDC infrastructure** して再デプロイするまでそのままです。

---

## 4.5 値ごとの 1 MiB 上限、および DLQ

DSQL は **約 1 MiB を超える単一の値** (`TEXT`/`bytea` 値) を拒否します。パイプラインは特大サイズの値を **3 つの帯域** に分けて処理します。

| 値のサイズ | 処理内容 |
|---|---|
| **≤ 1 MiB** | 通常どおり適用されます。 |
| **1 MiB – 8 MiB** | sink は **書き込み前に** 各値を測定し、特大サイズの値を **DLQ** に **隔離 (quarantine)** します (適用されることは決してありません)。一方でそのレコードのテーブルの残りの値はそのまま流れ続けます。 |
| **> 8 MiB** | Kafka にまったく入ることができません。これらは **キャプチャ時に除外** する必要があり、そうすれば特大サイズの LOB 列はパイプラインに到達しません。ドロップする列は Data Migration ステップのテーブル一覧の下にある **Exclude tables & columns** でルールにより除外します — その *Oversized-LOB candidates* セクションが Evaluation が `OVERSIZED_LOB` としてフラグ付けするのと同じ集合を示します。ツールが書き込む行では、除外した列は NULL になります。CDC 開始前から Aurora DSQL にあった行は値を保持し(CDC は除外した列を書き込みません)、Validation がそれらの行を数えます。 |

### デッドレター化される対象

特大サイズの値に加えて、DLQ は DSQL が **恒久的に** 拒否するあらゆるレコードを隔離します — 型の不一致、制約違反、ターゲット列の欠落 (たとえば伝播されていないソースの `ALTER` の後) など — また、Aurora DSQL の列が値を変えてしまうために sink が拒否する値も隔離します ([§4.2](#cdc-中のソーススキーマドリフトの検出と対処))。一時的な失敗 (OCC `40001`、Aurora DSQL の一時的な `XX000` "server unavailable"、接続の切断) は **リトライ** され、デッドレター化はされません。ソースのスキーマ変更で説明できる拒否は修復方法とともにバナーの行として表示され (§4.2)、スキーマと関係のない値単位のデータエラー (たとえば `22P02`) は SQLSTATE だけが表示されます。値単位の拒否 (一意制約・`CHECK` 違反、長すぎる値や範囲外の値) では、その周りの行が 1 トランザクション 1 行で適用され、insert・update の一意制約違反 (`23505`) は、まず値を空ける変更のために最大 5 分間保留され (§4.4)、それより長く続く場合にだけデッドレターになります。

### DLQ が表面化する場所 — 読み取る Kafka トピックではなく CloudWatch

sink は隔離した各レコードをその **CloudWatch** connector ロググループに記録し、ツールの監視機能がそれらの行をパースして UI に反映します (テーブルごとの「Quarantined」件数と、ダウンロード可能な単一のエラーログ)。記録される理由には **SQL テンプレート** (列名 + `?` プレースホルダ) が含まれますが、**行の値や認証情報は一切含まれません**。適用もデッドレター化もできないレコードは、サイレントにスキップされるのではなくタスクを **明示的に失敗** させます。

### Full Load が隔離した行に対する以後の変更を CDC はどう扱うか

Full Load がある行を隔離した場合 (特大サイズの値を書き込めなかったため)、その行は **ターゲットに存在せず**、ソースには残っています。以後その行が変更されたときの挙動は、操作の種類によって分かれます:

| 存在しない行に対するソースの操作 | CDC の動作 | 結果 |
|---|---|---|
| `DELETE` | `DELETE … WHERE pk = ?` が **0 行** に一致します。sink は 0 行の delete をエラーとして扱わないため、そのまま適用・コミットされます。 | **正常。** 意図した最終状態 (「その行がターゲットに無い」) が既に成立しています。 |
| 値を 1 MiB 未満に縮める `UPDATE` | Debezium が **after-image 全体** を送り、sink が upsert として書き込みます。既存行が無いため、行は **INSERT** されます。 | **ギャップが自己修復します。** 対応不要。 |
| 値が依然として特大サイズの `UPDATE` | sink は書き込みの **前** に値を測定するため、同じ理由で **DLQ** に隔離されます。 | ギャップは残りますが **可視** です (DLQ の深さ / モニターの「Quarantined」)。 |
| *別の* 行の `INSERT` | 影響なし。 | 正常。 |

Full Load 後の隔離ギャップは自ら知らせてくれません: ソースがその行に二度と触れなければ CDC に複製するものはなく、0 行の delete は通常の再処理と区別できません。これを報告するのは **Validation (ステップ 4)** であり、だからこそ Full Load の完全性判定はそちらを指しています。実務上の指針は変わりません: カットオーバー前に特大サイズのソース値を修正して、CDC を停止した状態でそのテーブルを **Reload** するか (Stop CDC、Reload、それからもう一度 Start CDC: CDC のストリーミング中は Full Load ステップは再ロードを実行しません、[第 3 章 §3.5](03-full-load.md#35-ウォーターマーク--cdc-への橋渡し))、特大サイズの LOB 列をキャプチャ段階で除外し (§4.5) パイプラインに入れないようにしてください。

---

## 4.6 CDC 進捗のモニタリング (マイグレーションモニター)

CDC 実行中、Data Migration 画面はテーブルごとのライブモニターを表示します。手動リロードなしで自動更新され、ソースに対しては**スキャン不要**です — 本番 DB に `COUNT(*)` を実行しません。列は次のとおりです。

| 列 | 意味 |
|---|---|
| **Full Load rows** | ワンショットスナップショットがテーブルにロードした行数。 |
| **Inserts** / **Updates** / **Deletes** | Full Load 以降にこのテーブルへ適用された CDC の insert・update・delete の累計数。 |
| **Quarantined** | **DLQ** に隔離された変更イベントの件数(恒久的に拒否された行: 型の不一致、1 MiB を超える特大サイズの値、制約 / スキーマドリフト)。 |
| **Source rows (est.)** | スキャン不要のカタログ **見積り**。**Target rows** — DSQL の正確なカウント。 |
| **Stream lag** | ターゲットがソースより**時間的に**どれだけ遅れているか(下記参照)。 |
| **Consistency** | 色付きバッジ: 緑 *consistent* = カウント一致 · *replicating…* = 追いつき中 · 赤 *rows missing* = 最新の変更は届いたが途中で行が消失 · 赤 *data quarantined* = DLQ に未適用イベントあり。 |

### Stream lag — 行数ではなく実時間の指標

**Stream lag** はエンドツーエンドのレプリケーション遅延です: 各変更について、シンクが **適用時刻 − ソースのコミット時刻** を記録し、モニターは、最近のウィンドウで観測された **最悪 (最大) の遅延** (直近の単一値ではありません) を時間で表示します: `caught up`(1 秒未満 / ストリーム消化済み)、`8.5s behind`、`2m 10s behind`、`1h 4m behind`。真の**時間**遅延なので、**あらゆる**プライマリキー型で正確であり、最新の insert だけでなく update/delete の遅延も反映します。シンクが**一意インデックスのために保留した変更**(§4.4)は、待っている間そのテーブルの遅延として数えられます。適用されるかデッドレターになると遅延は下がります。パーティション単位の遅延はありません。時間メトリクスがまだ利用できない場合(古いシンクプラグイン、またはデータポイントがまだ無い場合)、モニターは `N behind (PK)` または `caught up` を表示します。単一列の整数 PK でのみ機能します。

遅延の送出は厳密に **best-effort** であり、レプリケーションに影響しません。ソースの最後の書き込みから少なくとも 5 分経った後の「Stream lag → caught up」を(保留された変更の遅延は表れるまでに 1〜2 分かかることがあります) [カットオーバー](10-conclusion.md)に進んで安全なシグナルとして使ってください。

### パイプラインの健全性と change flow

テーブル別テーブルの上に、**パイプライン健全性カード**が各コネクタの状態と **「change flow」** ステータス行を表示します — *Streaming*、*No changes flowing — idle*、または **「Sink stalled — changes are NOT reaching DSQL」** — ソース poll とシンク send の rec/s ゲージ 2 つとともに、アイドルなストリームと詰まったストリームを一目で見分けられます。**ライブ stream-lag チャート**は最悪のエンドツーエンド遅延を時系列で描きます。

確定した stall の通知は、シンクコネクタのログを指します。シンクの send レートが回復するまで cut over しないでください。繰り返される行の意味:

| ログの行 | 意味 | 対処 |
|---|---|---|
| `Commit of offsets timed out` | シンクのコンシューマーがグループから外れた | Stop CDC してから Start CDC し、再参加させる |
| `DSQL apply failed (transient sqlstate=XX000)` | 一時的な Aurora DSQL の状態 | シンクは自動的に再開します。長引く場合は AWS サポートに問い合わせる |
| `DSQL apply failed (transient sqlstate=…)` が `08xxx`、`57xxx` または `null` | シンクがクラスターに到達できない | クラスターがまだ存在するか、コネクタのサブネットがポート 5432 でそのエンドポイントに到達できるか — セキュリティグループ、NAT ゲートウェイまたは Aurora DSQL VPC エンドポイントとその DNS — そしてコネクタの IAM ロールがまだ接続できるかを確認する |
| 同じ行が `28xxx`、`53300` または `53400` | Aurora DSQL が接続を開くことを拒否した | `28xxx` は IAM トークン(コネクタの IAM ロールがまだ接続できるかを確認)、`53300` はクラスターの開いている接続の上限(他の場所のアイドル接続を閉じるかクォータを引き上げる)、`53400` は新しい接続のレート。接続が開けばシンクは再開します |
| 同じ行が `40001` | 別の何かが Aurora DSQL で同じ行に書き込んでいる | cut over まで止める |

コネクタのタスクが停止すると、その行は赤に変わります。コネクタの CloudWatch ワーカーログにツールが認識する失敗 — Debezium が解析できない MySQL の DDL 文、スキーマ履歴の問題、作成できない PostgreSQL の publication、到達できないソースなど — があれば、行には *Stopped —* に続いて原因が表示され、コネクタの下のエラー通知が何をすべきかを示します。それ以外の場合、行には *Stopped — a task is not running. Restart required.* と表示され、詳細はロググループ `/msk-connect/<stack>-cdc` にあります。そのときライフサイクルカードは CDC がストリーミングしていないことを示し、**Stop CDC** を提示します: Retry CDC は既存のコネクタをそのままにするため、停止したコネクタを再起動できません。Stop CDC の後も、原因は次の Start まで Start CDC カードに残ります。

### DLQ を調べる

**DLQ インスペクター**が、デッドレターされたレコードを **Time / Table / SQLSTATE / Reason** で一覧します — ページング・フィルタ対応、テーブル別の内訳と depth バッジ付き — UI を離れずに poison 行をトリアージできます。**「Download CDC error log」** で NDJSON としてエクスポートします。(Reason には SQL テンプレートが含まれ、行の値は含まれません — §4.5 参照。) レコード一覧はメモリ上にしかなく、アプリの再起動後には残りません。テーブルごとの件数、最新時刻、ドリフトの種類は残り、Cut over が確認するのはそれです ([§5.5](05-validation.md#55-cdc-のデッドレターとカットオーバー))。

### リキーされた PostgreSQL テーブル: REPLICA IDENTITY の再確認

CDC がリキーするテーブル(ソースの主キー外の列を加えた複合ターゲットキー)では、ソースの `REPLICA IDENTITY` がテーブルとすべてのパーティションで **FULL** である必要があります。FULL 未満では `DELETE` が追加された列を載せないため、エラーもデッドレターもなく Aurora DSQL に行が残ることがあり、追加された列だけを変更する `UPDATE` はデッドレターになり(*a unique constraint rejected the row*)、古い行が残ります。そのためモニターが `REPLICA IDENTITY` を読み直し(読み取り専用、最大 2 分に 1 回、MySQL ソースでは行いません)、次の通知を表示します:

| 通知 | 意味 | 対処 |
|---|---|---|
| **赤 — 「Deletes on re-keyed tables can be lost」** | テーブル(またはそのパーティションのいずれか)が FULL 未満です。Cut over は保留され、修正前に行われた delete は再生されません。 | ソースで `ALTER TABLE <table> REPLICA IDENTITY FULL` を実行してください — 親は伝播せず、新しいパーティションは `DEFAULT` で始まるため、各パーティションにも — そのうえで **Check again** を押します。PostgreSQL 18 で生成列を持つテーブルには、通知が FULL の代わりに **Keep source PK** を示します([第 6 章](06-limitations.md) を参照)。 |
| **黄 — 「REPLICA IDENTITY is FULL again」** | 修正は反映されましたが、FULL 未満だった間の delete が欠けている可能性があります。Validation がそのテーブルを自動的に CHECKSUM で比較し、Cut over は修正後に開始した一致を待ちます([§5.6](05-validation.md#56-リキーされた-postgresql-テーブルとカットオーバー))。ただし、そこで **Continue without this check** を選んだ場合は待ちません。 | テーブルが異なる場合: CDC を追いつかせてから Stop CDC、Full Load ステップで **Drop & reload** でリロード、Start CDC、再確認。CDC only には Full Load ステップがないため、代わりに Aurora DSQL 上の余分な行を手作業で削除してください。FULL 未満だったテーブルを CDC がリキーしなくなった場合(Schema Conversion でテーブルを **Keep source PK** に戻して CDC を再デプロイした、またはソースの主キーが複合キーを含むようになった)も、同じ通知が「REPLICA IDENTITY was below FULL on re-keyed tables」という見出しで表示されます: それ以前に失われた delete には引き続き CHECKSUM が必要です。 |
| **情報 — 「Can't re-check REPLICA IDENTITY on the source」** | ソースのパスワードはメモリ上にしかないため、アプリの再起動後はツールが接続できません。 | Connect ステップで再接続してください。その間、これらのテーブルを FULL に保ってください。 |
| **黄 — 「Can't re-check REPLICA IDENTITY on the source」** | 最後の読み取りが失敗しました(2 分後に再試行します)。 | その間、これらのテーブルを FULL に保ってください。 |

### ソーススキーマの再確認

モニターはソース上のキャプチャ対象テーブルの定義を読み直し([§4.2](#デッドレターにならない変更-ソーススキーマの再確認) 参照)、DLQ カードの中ではなくその隣に次の通知を表示します:

| 通知 | 意味 | 対処 |
|---|---|---|
| **赤 — 「Source schema changed on N captured table(s)」** | テーブルごとに 1 行で、何が変わったか(列の削除・追加・名前変更、型・NULL ルール・主キーの変更、テーブルが見つからない)と、それが CDC の書き込む行に与える影響を示します。Cut over は保留されます。DLQ バナーが同じ種類の変更としてすでに報告しているもの(それで拒否された行とともに)は、そのバナーにだけ表示されます。どちらの場合も Cut over は保留されます。 | ソースが最終的な定義になった後でテーブルを揃え直し(下記)、**Check again** を押します。 |
| **黄 — 「The source schema changed while CDC ran」** | ソースが再び Step 1 のインベントリと一致しました(または Step 1 の再実行がテーブルの定義を変えた、またはテーブルがインベントリから外れた)が、その間に CDC が書き込んだ値が異なる可能性があります。 | Validation がこれらのテーブルを自動的に CHECKSUM で比較し、Cut over はその時刻以降に開始した一致を待ちます([§5.7](05-validation.md#57-ソーススキーマの変更とカットオーバー))。Step 1 のインベントリから外れたテーブルは比較できないため、Cut over ステップで **Continue without this check** により確認(acknowledge)してください。 |
| **情報 — 「Can't re-check the source schema」** | ソースのパスワードはメモリ上にしかないため、アプリの再起動後はツールが接続できません。 | Connect ステップで再接続してください。 |
| **黄 — 「Can't re-check the source schema」** | 最後の読み取りが失敗したか、キャプチャ対象テーブルを 1 つも見つけられませんでした(5 分後に再試行します)。 | その間、移行対象テーブルに DDL を実行しないでください。 |

ソースが最終的な定義になった後でテーブルを揃え直すには:

| 変わったもの | 揃え直す方法 |
|---|---|
| 列の追加・名前変更 | **Fix target schema…** (そのテーブルに DLQ バナーがすでに表示している場合を除く): 新しい列は追加し、名前変更された列は値を保つよう名前を変更します。Validation がその列を比較できるよう、続けて Step 1 を再実行してください。キーの変更や、主キー・CDC キー列の名前変更では何も変更せず、生成列や除外設定で外している列は追加しません (ダイアログが理由と対処を示します)。 |
| 列の削除、または型・NULL ルールの変更 | ソースの変更を元に戻すか: **Full Load + CDC** — Step 1 を再実行します (以後の再確認はそれと比較します)。Aurora DSQL のテーブルも変える必要がある場合は、CDC を追いつかせてから (Stream lag: caught up) Stop CDC、削除された列は先に Aurora DSQL で削除し (Drop & reload は新しい定義にないターゲット列を削除しません)、Full Load ステップで **Drop & reload** によりテーブルをリロードし、Start CDC します。列の型が変わった場合は、先に Schema Conversion で新しい型を指定します (Aurora DSQL は列の型をその場で変更できません)。CDC の停止中は移行対象テーブルにほかの DDL を実行しないでください。**CDC only** — ソースに合わせる必要がある部分は Aurora DSQL のテーブルを手作業で変更し、変更以降に CDC が書き込んだ行を修正してから (再生はされません)、Step 1 を再実行します。 |
| 主キー | ソースでキーを戻すか、(Full Load + CDC) Step 1 を再実行して **Drop & reload** でリロードし、新しいキーで作り直します。キー列が名前変更または削除された場合は、先に Schema Conversion でテーブルを **Replace** により再適用します (Drop & reload は古いキー列の削除を拒否し、Aurora DSQL はその列を削除できません)。CDC only では **Start over** を使います。CDC キー列 (Composite key) が名前変更された場合、通知は Fix を表示せず、作り直す手順を自ら示します。 |
| テーブルの名前変更・削除 | CDC は固定のテーブル一覧をキャプチャするため、その場で揃え直せません: ソースで名前を戻すか、**Start over** で新しい名前で移行してください。意図して削除した場合は Step 1 を再実行します: そのテーブルは再確認の対象から外れ、Cut over が確認(acknowledge)を求めます。 |

---

**次へ:** [5. Validation →](05-validation.md)
