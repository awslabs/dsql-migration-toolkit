# Appendix: パフォーマンステスト結果

_言語: [English](../en/12-performance-test-results.md) | [한국어](../ko/12-performance-test-results.md) | **日本語**_

> **前へ:** [11. お客様 FAQ](11-customer-faq.md)

この付録は、開発中に実施された Full Load スループット測定を記録し、各最適化段階が
最終性能にどのように貢献したかを示します。すべての測定は、ソース RDS MySQL および
ターゲット Aurora DSQL と同一 VPC 内の **ECS Fargate** で実施されました
（1ms 未満のネットワーク RTT）。

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

## 主な知見

1. **GIL が Python データパイプラインの天井。** I/O を解放する C 拡張（psycopg3）が
   あっても、行あたりの Python 変換が支配し 1 コアにシリアライズ。
2. **`spawn` コンテキストの ProcessPoolExecutor が正しい GIL バイパス。** 各ワーカーが
   独自の MySQL エンジン + DSQL コネクタを構築 — プロセス間行転送不要。
3. **OCC 競合は既存データへの同時ライター数に比例。** 32 ライターが同じ行に
   ON CONFLICT を発行するとライブロック。空テーブルへのプレーン INSERT で完全排除。
4. **tp=8 でスループット上限が CPU → DSQL ライト容量に移行。** 約 8 ライタープロセス
   以上は DSQL サーバー側のライトスループットがボトルネック（ピーク約 67K rows/s 観測）。
5. **最適設定：** `TABLE_PARALLELISM` = vCPU 数。ローダーが自動的に大規模テーブルをシャード。

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

---

**前へ:** [11. お客様 FAQ](11-customer-faq.md)
