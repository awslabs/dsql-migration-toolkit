# Appendix: パフォーマンステスト結果

_言語: [English](../en/12-performance-test-results.md) | [한국어](../ko/12-performance-test-results.md) | **日本語**_

> **前へ:** [11. お客様 FAQ](11-customer-faq.md)

この付録は、2 つのデータ経路 — **Full Load** (本ツールの一括ローダー) と **CDC** (Debezium → MSK → カスタム DSQL シンク) — のスループット測定を記録したものです。数値は、スループットを左右する要因と期待値を示す例として読んでください。**性能の保証ではありません**: ハードウェア、スキーマ、ソース、Aurora DSQL のウォームアップ状態によって変わります。測定は、ソースおよび Aurora DSQL と同じ VPC 内の **ECS Fargate** (CDC ではマネージド **MSK Connect**) で行いました (ネットワーク RTT は 1 ms 未満)。

---

## ひと目でわかる結果

| 経路 | 構成 | 結果 |
|---|---|---|
| Full Load、16 テーブル × 20M 行 | 16 vCPU、PostgreSQL ソース | 全体で約 336,000 rows/s (定常で約 600,000) |
| Full Load、16 テーブル × 20M 行 | 16 vCPU、MySQL ソース | 全体で約 193,000 rows/s (定常で約 226,000) |
| Full Load、150M 行の 1 テーブル | 16 vCPU、PostgreSQL ソース | 全体で約 205,000 rows/s (定常で約 280,000) |
| Full Load、150M 行の 1 テーブル | 16 vCPU、MySQL ソース | 既定 (1 セッション) で定常約 55,500 rows/s。Full load + CDC で *Reader shards per large table* 16 なら全体で約 164,000 rows/s |
| CDC | 既定のシンクサイズ (4 MCU) / 8 MCU | 約 21,000 rows/s / 約 26,000 rows/s |

---

## Full Load

### 最大サイズでの実行 (16 vCPU)

| コンポーネント | 設定 |
|---|---|
| **ECS Fargate** | 16 vCPU、120 GB メモリ (最大のタスク) |
| **ソース** | Aurora MySQL と Aurora PostgreSQL、`db.r8g.16xlarge` |
| **ターゲット** | Aurora DSQL、`ap-northeast-2` |
| **ローダー設定** | *Tables in parallel* 16、それ以外はすべて既定 (*Reader shards per large table* は自動、*Batches per table* 8、*Rows per batch* 2000) |
| **測定** | `scripts/measure_performance.py full-load` を単発の ECS タスクとして実行 |

| 実行 | 行数 | 所要時間 | 全体 | 定常 | 律速要因 |
|---|---|---|---|---|---|
| PostgreSQL、16 テーブル | 320M | 15 分 53 秒 | 約 336K rows/s | 約 600K rows/s | Aurora DSQL (タスクは約 3.5 コアを使用) |
| PostgreSQL、1 テーブル: 16 プロセスで 32 区間 | 150M | 12 分 12 秒 | 約 205K rows/s | 約 280K rows/s | Aurora DSQL |
| MySQL、16 テーブル | 320M | 27 分 | 約 193K rows/s | 約 226K rows/s | タスクの CPU (16 vCPU すべてが稼働) |
| MySQL、1 テーブル、Full load only: 1 セッション | 150M のうち 75M (25 分で停止) | — | — | 約 55.5K rows/s | 1 コア |
| MySQL、1 テーブル、Full load + CDC: 別々のスナップショットで読む 16 部分 | 150M | 15 分 12 秒 | 約 164K rows/s | 約 226K rows/s | タスクの CPU (以前のコピーの DROP と並行) |

### スループットを左右する要因

- **効くのは進行中のバッチ数ではなくキー範囲です。** Aurora DSQL はテーブルの 1 つのキー範囲を、進行中の数にかかわらず約 20,000 rows/s で書き込むため、大きなテーブルは書き込みをキー範囲に分散して初めて速くなります。ローダーは既定でこれを行います ([§7.2](07-performance-and-tuning.md#72-並列度のチューニング))。テーブルごとに 1 区間の場合と比べ、150M 行のテーブルは PostgreSQL で約 10 倍、部分に分けた MySQL で 8 倍、1 セッションの MySQL で 2.8 倍速くロードされました。
- **MySQL のロードはタスクの CPU に、PostgreSQL のロードは Aurora DSQL に律速されます。** MySQL の行の読み取りと変換は、同時にロードするテーブルまたは部分ごとにほぼ 1 コアを使います。PostgreSQL の値はそのまま通過するため、16 テーブルのロードは約 3.5 コアで 336K rows/s に達しました。*Tables in parallel* はタスクの vCPU 数に設定してください。
- **巨大な MySQL テーブル 1 つは、既定では 1 コアの速さでロードされます。** 1 つのセッションがその区間を 1 つのスナップショットから読むためです。binlog の開始位置からの Full load + CDC では、*Reader shards per large table* を明示的に指定すると、別々のスナップショットで読む部分に分けて複数のコアで読み、CDC が再生した時点で一貫します ([§3.8](03-full-load.md#38-並列ロード))。
- **新しいテーブルにはウォームアップがあります。** Aurora DSQL は新しいテーブルをパーティション 1 つで始め、大きくなるにつれて分割するため、ロードの最初の 1 分ほどは定常の速さより遅くなります。
- **ホストと以前のジョブが数値を動かします。** 同じ MySQL のロードが Fargate のホストによって最大約 1.8 倍速く動き、以前の 75M 行のコピーの DROP は、Aurora DSQL のバックグラウンドジョブとして次のロードと並行して 1 時間以上続きました。

---

## CDC のスループット

CDC はボトルネックの位置が Full Load と異なるパイプラインです: `Debezium (ソース) → MSK トピック → カスタム DSQL シンク` の構成で、シンクが **DSQL の書き込みレイテンシバウンド** です (ステートメントごとに分散往復が 1 回)。スケーリングのパラメータは、キャプチャするテーブル数からツールが推論します ([§7.2](07-performance-and-tuning.md#72-並列度のチューニング))。

| コンポーネント | 構成 |
|---|---|
| **ソースコネクタ** | MSK Connect 上の Debezium MySQL、`ConnectorMcuCount`=4 |
| **シンクコネクタ** | カスタム `dsql-sink`、`SinkTasksMax` を 4→8 にスケール |
| **ワークロード** | `customers_sample.orders` へ一括 INSERT する ECS タスク 4 つ (ソースへ約 20,000 rows/s) |
| **測定** | CloudWatch `AWS/KafkaConnect` の `SourceRecordWriteRate` / `SinkRecordSendRate`。DSQL ターゲットの行数の増分で突合 |

- **シンクはレイテンシバウンドだったため、減らした往復の 1 回 1 回が利得でした。** 初期のバージョンは 1 行ごとに 1 往復で適用し、CPU が低いまま 300–2,000 rows/s に留まっていました。適用をバッチにまとめ、隠れていた行ごとのメタデータ往復をなくすと、8 パーティションで約 18,700 rows/s になりました。
- **シンクのサイズはソースとは別に決めてください。** その後シンクは CPU バウンドになり (4 MCU で約 80%、約 21,000 rows/s)、シングルタスクの Debezium ソースには CPU の余裕があります。`SinkMcuCount` (既定 4) はソースの `ConnectorMcuCount` とは別のつまみで、**8 MCU** で CPU 約 34% のまま約 26,000 rows/s に達しました。
- **それ以上は、ソース側がパイプラインを制限します。** binlog リーダー 1 つと、ソースデータベース自体の容量です。2 vCPU のソースは CPU 93% でパイプラインの上限を決め、スケールアップして初めて解消しました。出荷されている producer のチューニングで、ソース自体は約 31,000 records/s を処理しました。
- **シンクの CPU が低く、パーティションのスケールがサブリニアだからといって、サーバー側の競合が証明されるわけではありません。** ここでは隠れたクライアント側の往復が同じ症状を生み、DSQL の `OccConflicts` は終始 0 でした。サーバーのせいにする前に、`OccConflicts` と `ReadOnlyTransactions` を確認してください。
- **ソースの再起動は自動的に吸収されます** — パイプラインはコミット済みの binlog オフセットからギャップなく再開します ([§7.2](07-performance-and-tuning.md#72-並列度のチューニング))。

---

## 測定の再現

```bash
AWS_REGION=<region> APP_STACK=<app-stack> \
DB_HOST=<source-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
MEASURE_SCHEMA=<schema> MEASURE_TABLES="<table> <table>" \
TABLE_PARALLELISM=16 VARIANTS=auto \
deploy/run_measure_on_fargate.sh
```

[`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh) は、測定をソースの VPC 内で単発の Fargate タスクとして実行します。`VARIANTS=auto` はローダーの既定を測定し、ほかのバリアントはスクリプトに一覧があります。測定そのものは [`scripts/measure_performance.py`](../../../scripts/measure_performance.py) です: テーブルごと・時間ウィンドウごとのスループット、書き込み経路のリトライとレイテンシ、Aurora DSQL がインデックスの構築を終えた時点を報告します。使い方 (`--reader-shards`、`--cdc-mode`、…) はスクリプトの冒頭にあり、`PERF_SOURCE_ENGINE=postgres` で PostgreSQL ソースを対象にします。ターゲットテーブルを DROP して再作成するため **非本番のターゲットでのみ** 使用し、実行を比べる前に以前のコピーを先に DROP して、Aurora DSQL のバックグラウンドジョブが終わるのを待ってください (`--wait-sys-jobs`)。

CDC は、cdc-stack をデプロイしてソースに一定の insert ワークロードを流し、CloudWatch `AWS/KafkaConnect` からパイプラインの速度 (`-debezium-source` コネクタの `SourceRecordWriteRate`、`-dsql-sink` コネクタの `SinkRecordSendRate`) を読み取り、一定の間隔での DSQL ターゲットの `COUNT(*)` の増分と突合します。

---

**前へ:** [11. お客様 FAQ](11-customer-faq.md)

**次へ:** [13. テーブルと列の除外 →](13-excluding-tables-and-columns.md)
