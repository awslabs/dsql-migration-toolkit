# Appendix: パフォーマンステスト結果

_言語: [English](../en/12-performance-test-results.md) | [한국어](../ko/12-performance-test-results.md) | **日本語**_

> **前へ:** [11. お客様 FAQ](11-customer-faq.md)

この付録は、開発中に 2 つのデータパス — **Full Load**（ツールの Python バルクローダー）と
**CDC**（Debezium → MSK → カスタム DSQL シンクのパイプライン）— について実施されたスループット
測定を記録し、各最適化段階が最終性能にどのように貢献したかを示します。すべての測定は、ソース
RDS MySQL およびターゲット Aurora DSQL と同一 VPC 内の **ECS Fargate**（CDC の場合はマネージド
**MSK Connect**）で実施されました（1ms 未満のネットワーク RTT）。

---

## テスト環境

| コンポーネント | 構成 |
|---|---|
| **ECS Fargate** | 8 vCPU（8192 CPU ユニット）、16 GB メモリ |
| **ソース** | Aurora MySQL（RDS）、`customers_sample` スキーマ |
| **ターゲット** | Aurora DSQL、us-east-1 |
| **テーブル** | `order_items`（3360万行）、`orders`（850万）、`payments`（840万）、`customers`（小規模） |
| **測定ツール** | `scripts/measure_performance.py full-load` + CloudWatch progress モニタリング |

---

## Full Load スループットの進化

### Stage 1: ベースライン（ThreadPool、GIL 制約）

初期実装はテーブル単位の並列に `ThreadPoolExecutor` を使用。Python の GIL により
vCPU 数に関係なく CPU 1 コアのみ活用。

| 構成 | rows/s | CPU | 備考 |
|---|---|---|---|
| 0.5 vCPU, tp=4, bp=8, page=1000 | 4,243 | 50% | 元のデフォルト |
| 4 vCPU, tp=4, bp=8, page=1000 | 9,732 | 113% | vCPU 追加 → スケジューリング改善のみ |
| 8 vCPU, tp=2, bp=8, page=5000 | 12,277 | 110% | コード最適化（v0.1.67） |

**診断：** どの vCPU でも CPU ~110%（1 コア）固定 = GIL シグネチャ。

### Stage 2: コード最適化（v0.1.67、依然 GIL 制約）

行あたりの GIL 保持時間を削減する最適化：

| 最適化 | 効果 |
|---|---|
| MySQL キーセットページサイズ 1000 → 5000 | ソースラウンドトリップ 5 倍削減 |
| `build_insert_statement` SQL テンプレートキャッシュ | バッチあたり約 40K オブジェクト割り当て排除 |
| `_iter_batches` レイジーバイト推定 | `_estimate_row_bytes` 呼び出し 90%以上削減 |
| `_flatten_params` リスト内包表記 | パラメータシリアライゼーション約 40% 高速化 |
| `convert_row` パススルー高速パス | 大部分のカラムが `convert_value` をスキップ |

**結果：** +41% 改善（4,243 → 6,000 rows/s、0.5 vCPU）。依然 GIL 制約。

### Stage 3: マルチプロセス並列化（v0.1.68）

`ThreadPoolExecutor` → `ProcessPoolExecutor` 置換 — 各ワーカープロセスが独自の GIL、
独自の MySQL 接続、独自の DSQL 接続プールを保持。

| テスト | テーブル | tp | rows/s | CPU | vs ベースライン |
|---|---|---|---|---|---|
| A: ThreadPool | 2（orders, payments） | 2 | 12,277 | 110% | 1× |
| B: ProcessPool Phase 1 | 2（orders, payments） | 2 | 22,365 | 207% | **1.82×** |
| C: ProcessPool Phase 1 | 4（全テーブル） | 4 | 32,270 | 311% | **2.63×** |
| D: ProcessPool + PK shard | 1（order_items） | 4 | 41,000 | 415% | **3.34×** |
| E: ProcessPool + PK shard | 1（order_items） | 8 | 51,000 | 777% | **4.15×** |
| F: 統合プール（旧、混合シャードなし） | 4（全テーブル） | 4 | 19,500 | 179% | 1.59× |
| **G: 統合プール + 自動シャード** | **4（全テーブル）** | **8** | **34,800** | **561%** | **2.83×** |

### Stage 4: Replace パス最適化

DROP+recreate 直後の空テーブルにロードする場合、プレーン `INSERT`（ON CONFLICT なし）
を使用し OCC 競合を完全排除：

| テスト | rows/s（持続） | rows/s（ピーク） | CPU |
|---|---|---|---|
| ProcessPool + shard, SKIP_EXISTING（追記） | 35,000 | 35,333 | 439% |
| ProcessPool + shard, NONE（replace/空テーブル） | **41,000–51,000** | **67,000** | 777% |

---

## まとめ：200GB 単一テーブルロード時間推定

| バージョン | アプローチ | rows/s | 200GB 推定 | 改善 |
|---|---|---|---|---|
| v0.1.67 以前 | ThreadPool, page=1000 | ~4,000 | 約 46 時間 | — |
| v0.1.67 | ThreadPool, コード最適化 | ~6,000 | 約 31 時間 | 1.5× |
| v0.1.67 | ThreadPool, 8 vCPU | ~15,000 | 約 12 時間 | 3.8× |
| **v0.1.68** | **ProcessPool, tp=4, 8 vCPU** | **~41,000** | **約 4 時間** | **10×** |
| **v0.1.68** | **ProcessPool, tp=8, 8 vCPU** | **~51,000** | **約 2.5 時間** | **18×** |

> 推定値は行あたり平均約 300 バイトを仮定。実際の時間は行幅、ネットワークレイテンシ、
> DSQL クラスター負荷、OCC 衝突率により異なります。

---

## CDC スループット

CDC はボトルネックが異なるパイプラインです。Full Load は **CPU/GIL バウンド**の Python プロセス
ですが、CDC は `Debezium（ソース）→ MSK トピック → カスタム DSQL シンク`であり、シンクが
**DSQL 書き込みレイテンシバウンド**です。この測定（2026-07-08）が出荷されたコネクタコード
（`dsql-sink` プラグイン）と [§7.2](07-performance-and-tuning.md#72-並列度のチューニング) の
スマートデフォルトを導きました。

### CDC スループットに影響するパラメータ

| パラメータ | 場所 | 効果 |
|---|---|---|
| `topic.creation.default.partitions` | cdc-stack（推論） | シンクの並列単位 — シンクタスク 1 つがパーティション 1 つを消費。**不可逆**（増やすのみ）。 |
| `SinkTasksMax` | cdc-stack（推論） | シンクコネクタの書き込み並列度；実効値はパーティション数で上限。 |
| `ConnectorMcuCount` | cdc-stack（推論） | ワーカーあたりの MSK Connect コンピュートユニット（1/2/4/8）。 |
| `SinkBatchMaxRows` | cdc-stack（3000、固定） | DSQL 書き込みトランザクションあたりの行数（DSQL のハード上限）。 |
| `consumer.max.poll.records` | シンクワーカー設定 | 1 回の `put()` に渡すレコード数 — シンクが 1 つの JDBC `executeBatch` にまとめられる上限。 |
| `max.batch.size` / `max.queue.size` | ソースコネクタ | ストリーミング反復あたりに排出する binlog イベント数 / reader→producer キュー深さ。 |
| `producer.batch.size` / `linger.ms` / `compression.type` | ソースワーカー設定 | Kafka produce バッチのサイズ・充填遅延・圧縮。 |

コネクタのスケーリングノブ（パーティション / `SinkTasksMax` / `ConnectorMcuCount`）は
**キャプチャテーブル数から推論**され、UI には公開されません —
[§7.2 → CDC](07-performance-and-tuning.md#72-並列度のチューニング) を参照。

### テスト環境（CDC）

| コンポーネント | 構成 |
|---|---|
| **ソースコネクタ** | MSK Connect 上の Debezium MySQL、`ConnectorMcuCount`=4 |
| **シンクコネクタ** | カスタム `dsql-sink`、`SinkTasksMax` を 4→8 にスケール |
| **ワークロード** | `customers_sample.orders` にバルク INSERT する ECS タスク 4 つ（ソースに約 20,000 rows/s 流入） |
| **測定** | CloudWatch `AWS/KafkaConnect` の `SourceRecordWriteRate` / `SinkRecordSendRate`、DSQL ターゲットの行数増分でクロスチェック |

### CDC スループットの進化

| 段階 | 設定 | シンク rows/s | シンク CPU | ボトルネック | vs ベースライン |
|---|---|---|---|---|---|
| 1: 単一パーティション | 1 パーティション / 1 タスク | 292 | — | パーティション数 = 1（並列度なし） | 1× |
| 2: パーティション化 | 4 パーティション / 4 タスク | ~550 | 5% | シンクが**行あたり 1 往復**で適用 | 1.9× |
| 3: バッチ適用（**プラグイン v13**） | 4 パーティション / 4 タスク | ~1,165 | 7% | ソース（producer 未チューニング） | **4.0×** |
| 4: ソースチューニング（**プラグイン v14**） | 8 パーティション / 8 タスク | ~1,500 | 6.5% | DSQL 書き込み競合 | **5.1×** |
| 5: マルチ行リライト（**プラグイン v15**） | 8 パーティション / 8 タスク | ~1,925 | ~10% | DSQL 書き込み競合 | **6.6×** |

3 つのコード/設定変更が大部分を担いました:

- **プラグイン v13 — バッチシンク適用。** シンクは連続する同一 SQL 変更イベントの最大ランを、
  行ごとの `executeUpdate()` ではなく 1 つの JDBC `executeBatch()` にまとめます。DSQL はレイテンシ
  バウンドなので（各文が分散往復；タスクは CPU ~5%）、行ごとの往復をバッチ送信に畳むことでシンク
  スループットが **2 倍**になりました（~550 → ~1,165 rows/s）。`consumer.max.poll.records` も
  500 → 3000 に引き上げ、1 回のポーリングで ≤3000 行のトランザクションを満たすようにしました。
- **プラグイン v14 — ソース producer チューニング。** より大きなバッチ/キュー + `lz4` 圧縮 producer
  バッチにより、ソースは ~1,940 → **~31,000 rec/s（16×）** となり、ソースが真の天井ではなく
  バッチが不足していただけだったことを証明しました。これにより、シンク→DSQL 書き込みが真の最終
  ボトルネックとして明らかになりました。
- **プラグイン v15 — マルチ行 INSERT リライト。** pgjdbc `reWriteBatchedInserts=true` を有効化すると、
  各 `executeBatch` が 1 つのマルチ行 `INSERT ... VALUES (..),(..) ON CONFLICT` にまとまり — N 回の
  execute 往復が 1 回に — シンクが ~1,500 → **~1,925 rows/s（+30%）** に向上しました。各同一 SQL ランを
  PK ごとの 1 行に先に dedup することで安全にしています（リライトされたマルチ行 `ON CONFLICT` は重複
  競合キーを拒否）。

v14 では 8 パーティションのシンクは ~1,500 rows/s に到達（DSQL 適用のクロスチェックは 1,484 rows/s）
しましたが、4→8 のスケールは **~1.4 倍（サブリニア）** にとどまりました: 同一テーブルへの同時 upsert が
DSQL 内部で競合するためです。スマートデフォルトが実効並列度を 8 で上限とするのは、まさにこの理由です。
v15 のリライトは同じ 8 パーティションで往復をさらに削減し、~30% を追加して ~1,925 rows/s に到達しました。

---

## 主な知見

### Full Load

1. **GIL が Python データパイプラインの天井。** I/O を解放する C 拡張（psycopg3）が
   あっても、行あたりの Python 変換が支配し 1 コアにシリアライズ。
2. **`spawn` コンテキストの ProcessPoolExecutor が正しい GIL バイパス。** 各ワーカーが
   独自の MySQL エンジン + DSQL コネクタを構築 — プロセス間行転送不要。
3. **OCC 競合は既存データへの同時ライター数に比例。** 32 ライターが同じ行に
   ON CONFLICT を発行するとライブロック。空テーブルへのプレーン INSERT で完全排除。
4. **tp=8 でスループット上限が CPU → DSQL ライト容量に移行。** 約 8 ライタープロセス
   以上は DSQL サーバー側のライトスループットがボトルネック（ピーク約 67K rows/s 観測）。
5. **最適設定：** `TABLE_PARALLELISM` = vCPU 数。ローダーが自動的に大規模テーブルをシャード。

### CDC

6. **シンクは CPU バウンドではなくレイテンシバウンド。** CPU ~5–7% でシンクは計算ではなく DSQL
   往復を待っていた — したがってレバーはより多くのコンピュートではなく、*より少なく大きい*書き込み
   （バッチ `executeBatch`）。
7. **行ごとの往復をバッチ化することが CDC 最大の単一改善**（プラグイン v13 のみで ~550 → ~1,165 rows/s）。
8. **ソースは最初から天井ではなかった。** producer チューニング（バッチ/キュー/`lz4`）で 16×
   ~31,000 rec/s に到達；MySQL サーバーあたり単一の Debezium タスクで十分。
9. **シンク並列度はサブリニアにスケール。** 4 → 8 パーティションで 2 倍ではなく ~1.4 倍 — 同一テーブル
   への同時 upsert が DSQL 内部で競合するため。そのためスマートデフォルトは実効並列度を 8 で上限。
10. **パーティション数は不可逆**なので、ツールは永続的に誤設定され得る UI ノブではなく、キャプチャ
    テーブル数から作成時に推論。

---

## 再現方法

```bash
AWS_REGION=us-east-1 \
MEASURE_SCHEMA=customers_sample \
MEASURE_TABLES="order_items orders payments customers" \
TABLE_PARALLELISM=8 \
BATCH_PARALLELISM=8 \
deploy/run_measure_on_fargate.sh
```

詳細は [`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh)
（A/B 測定ハーネス）と
[`scripts/measure_performance.py`](../../../scripts/measure_performance.py)
（スループット + OCC レポーティングツール）を参照。

CDC は cdc-stack をデプロイし、ソースに定常的な INSERT ワークロードをかけたうえで、CloudWatch
`AWS/KafkaConnect` からパイプラインのレートを読み取ります（`-debezium-source` コネクタの
`SourceRecordWriteRate`、`-dsql-sink` コネクタの `SinkRecordSendRate`）。固定区間で DSQL
ターゲットの `COUNT(*)` 増分とクロスチェックします。

---

**前へ:** [11. お客様 FAQ](11-customer-faq.md)
