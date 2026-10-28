# Appendix: パフォーマンステスト結果

_言語: [English](../en/12-performance-test-results.md) | [한국어](../ko/12-performance-test-results.md) | **日本語**_

> **前へ:** [11. お客様 FAQ](11-customer-faq.md)

この付録は、2 つのデータ経路 — **Full Load** (本ツールの一括ローダー) と **CDC** (Debezium → MSK → カスタム DSQL シンク) — のスループット測定を記録したものです。数値は、スループットを左右する要因と期待値を示す例として読んでください。**性能の保証ではありません**: ハードウェア、スキーマ、ソース、Aurora DSQL のウォームアップ状態によって変わります。測定は、ソースおよび Aurora DSQL と同じ VPC 内の **ECS Fargate** (CDC ではマネージド **MSK Connect**) で行いました (ネットワーク RTT は 1 ms 未満)。

---

## ひと目でわかる結果

| 経路 | 構成 | 結果 |
|---|---|---|
| Full Load、4 テーブル | 8 vCPU、MySQL ソース | 約 32,000 rows/s |
| Full Load、1 テーブルを 8 プロセスに分割 | 8 vCPU、MySQL ソース、空のターゲット | 持続 41,000–51,000 rows/s (ピーク約 67,000) |
| Full Load、20 テーブルで約 1 TB | 16 vCPU、MySQL ソース、複合キー | 9.157 億行を 2 時間 27 分で (平均約 103K rows/s) |
| Full Load、20 テーブルで約 1 TB | 16 vCPU、PostgreSQL ソース、複合キー | 8.6 億行を 1 時間 42 分で (平均約 140K rows/s) |
| Full Load、巨大な単一テーブル (8.65 億行) | 16 vCPU、PostgreSQL ソース、既定の設定 | 約 20–23K rows/s (単一リーダー) |
| CDC | 既定のシンクサイズ (4 MCU) / 8 MCU | 約 21,000 rows/s / 約 26,000 rows/s |

---

## Full Load

### スループットを左右する要因 (8 vCPU)

| コンポーネント | 設定 |
|---|---|
| **ECS Fargate** | 8 vCPU (8192 CPU ユニット)、16 GB メモリ |
| **ソース** | Aurora MySQL (RDS)、`customers_sample` スキーマ |
| **ターゲット** | Aurora DSQL、us-east-1 |
| **テーブル** | `order_items` (33.6M 行)、`orders` (8.5M)、`payments` (8.4M)、`customers` (小) |
| **測定** | `scripts/measure_performance.py full-load` と CloudWatch の進捗モニタリング |

| 構成 | rows/s | CPU |
|---|---|---|
| 1 プロセス内のスレッド (コア 1 つ)、2 テーブル | 12,277 | 110% (コア 1 つ) |
| テーブルごとに 1 プロセス、2 テーブル | 22,365 | 207% |
| テーブルごとに 1 プロセス、4 テーブル | 32,270 | 311% |
| 大きな 1 テーブルを 8 プロセスに分割、空のターゲット (単純な `INSERT`) | 持続 41,000–51,000、ピーク約 67,000 | 777% |
| 同じテーブル、既存の行に append (`ON CONFLICT`) | 約 35,000 | 439% |

- **Full Load はネットワークではなく CPU バウンドです。** Python のプロセス 1 つは 1 コアで行を変換するため、スループットは並列プロセスの数に比例します。テーブル並列度を vCPU 数に近づけてください (§7.2)。
- **空のターゲットへのロードが最速の経路です。** **Drop & reload** の後は、ローダーが `ON CONFLICT` のない単純な `INSERT` を使うため、楽観的並行性制御の競合が完全になくなります。既存の行に append すると、おおよそ 15–30% 遅くなりました。
- **同時に書き込むプロセスが約 8 つを超えると、限界は CPU から DSQL の書き込み容量へ移ります** (ピークで約 67K rows/s を観測)。

### 大規模: 約 1 TB (16 vCPU)

| | MySQL ソース | PostgreSQL ソース |
|---|---|---|
| **ソース** | Aurora MySQL `db.r7g.8xlarge` (一時的にサイズ拡大) | Aurora PostgreSQL 16.14 `db.r7g.8xlarge` (一時的にサイズ拡大) |
| **データセット** | 20 テーブル × 45.78M 行 = 915.7M 行 (≈ 1.07 TB) | 20 テーブル × 43M 行 = 860M 行 (≈ 1.02 TB) |
| **ローダー設定** | 20 テーブルすべてで複合 PK (`dist_key`)、`TABLE_PARALLELISM=16`、`BATCH_PARALLELISM=32`、バッチ行数 3000 | 同じ |
| **完了** | 20/20 テーブル、失敗 0 | 20/20 テーブル、失敗 0 |
| **総所要時間 (wall)** | 2 時間 27 分 32 秒 | 1 時間 42 分 31 秒 |
| **平均スループット** | 約 103K rows/s | 約 140K rows/s |
| **16 テーブルが並列で動いた区間** | 約 131K rows/s (16 vCPU 飽和) | 約 194K rows/s (16 vCPU のうち約 12) |
| **OCC `40001` のリトライ** | 0 | 0 |

どちらの測定も同じ Fargate サイズと同じ Aurora DSQL クラスターを使い、UI ではなく自動スクリプト (ECS RunTask) で実行しました。

- **複合主キーはホットパーティションのボトルネックを取り除きます。** 単調増加する `AUTO_INCREMENT` キーはすべての挿入を末尾のキー範囲に集中させますが、カーディナリティの高い列を先頭に付けると書き込みが分散し、リトライはまさに 0 でした (§7.1)。
- **テーブル数をテーブル並列度の範囲内に収めてください。** 20 テーブルに対してスロットが 16 だったため、最後の 4 テーブルが単独で動き (約 21K rows/s)、約 31 分が余分にかかりました。テーブル並列度をテーブル数以上にしていれば、MySQL の測定は約 1 時間 56 分だったはずです。
- **PostgreSQL ソースは、より少ない CPU でより速くロードします。** 値がほぼそのまま通過するため、行あたりの CPU コストが MySQL の方言変換より低くなります。MySQL は CPU が飽和し、PostgreSQL は飽和しませんでした。
- **大きなデータは、可能なら複数のテーブルに分けてください。** 作成直後の DSQL テーブルはパーティション 1 つで始まり、負荷の下で分割されるため、単一テーブルのロードは遅く (約 16K rows/s) 始まって 120–150K rows/s まで上がります。20 テーブルなら最初から 20 倍のパーティションがあります。

### 巨大な単一テーブル

現行のツールは、単一の大きなテーブルを既定で **1 つのリーダー** でロードし、書き込みのあるソースで同時リーダーが異なるスナップショットを読むことがないようにしています。8.65 億行の PostgreSQL テーブルでは、リーダー 1 つで約 20–23K rows/s (テーブル全体で約 12 時間) で、静的なソースでシャード 4 つを強制すると約 42K rows/s に達しましたが、CPU ではなく書き込みで頭打ちでした。巨大な単一テーブルについては、*Reader shards per large table* ([§3.8](03-full-load.md#38-並列ロード)、[§7.2](07-performance-and-tuning.md#72-並列度のチューニング)) を参照してください。

---

## CDC のスループット

CDC はボトルネックの位置が Full Load と異なるパイプラインです。Full Load は **CPU バウンド** で、CDC は `Debezium (ソース) → MSK トピック → カスタム DSQL シンク` の構成でシンクが **DSQL の書き込みレイテンシバウンド** です (ステートメントごとに分散往復が 1 回)。スケーリングのパラメータは、キャプチャするテーブル数からツールが推論します ([§7.2](07-performance-and-tuning.md#72-並列度のチューニング))。

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
AWS_REGION=us-east-1 \
DB_HOST=<rds-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
TARGET_ENDPOINT=<dsql-cluster-endpoint> \
MEASURE_SCHEMA=customers_sample \
MEASURE_TABLES="order_items orders payments customers" \
TABLE_PARALLELISM=8 \
BATCH_PARALLELISM=8 \
deploy/run_measure_on_fargate.sh
```

完全な A/B 測定ハーネスは [`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh)、プロセス内のスループットと OCC を報告するツールは [`scripts/measure_performance.py`](../../../scripts/measure_performance.py) を参照してください。

CDC は、cdc-stack をデプロイしてソースに一定の insert ワークロードを流し、CloudWatch `AWS/KafkaConnect` からパイプラインの速度 (`-debezium-source` コネクタの `SourceRecordWriteRate`、`-dsql-sink` コネクタの `SinkRecordSendRate`) を読み取り、一定の間隔での DSQL ターゲットの `COUNT(*)` の増分と突合します。

---

**前へ:** [11. お客様 FAQ](11-customer-faq.md)

**次へ:** [13. テーブルと列の除外 →](13-excluding-tables-and-columns.md)
