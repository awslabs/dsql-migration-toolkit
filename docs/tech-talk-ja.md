---
marp: true
theme: default
paginate: true
title: MySQL to DSQL Migrator — Migration Architecture & Data Path Deep Dive
class: dense
style: |
  /* 全スライドに dense を適用（frontmatter class: dense）。特定スライドだけ例外にするにはそのスライド先頭に <!-- _class: 別クラス --> */
  section.dense { font-size: 21px; }
  section.dense h1 { font-size: 30px; }
  section.dense h2 { font-size: 22px; }
  section.dense table { font-size: 19px; }
  section.dense pre { font-size: 16px; }
  section.dense li { line-height: 1.3; }
---

<!--
発表資料（DB 専門家向け・日本語）。Marp / reveal-md でレンダリング可。そのまま Markdown として読んでもよい。
スライド区切りは水平線（ハイフン3つ）、スピーカーノートは HTML コメントブロック。
時間配分（発表20分 + デモ5分）: 導入2 · アーキテクチャ4 · Evaluation/Schema 3 · Full Load 4 · CDC 5 · Validation/AI 2 · まとめ（ホットパーティション）1（+予備）。
-->

<style scoped>
section h1 { font-size: 60px; }
section h2 { font-size: 34px; }
</style>

# MySQL to DSQL Migrator
## Migration Architecture & Data Path Deep Dive

<!--
- （口頭）社内技術共有 · 発表20分 + デモ5分 · 対象: DB 専門家。
- 上の GitLab リンクは公開リポジトリ — 参加者が clone して追従できる。
- このツールは RDS/Aurora MySQL を Aurora DSQL（PostgreSQL-16 互換・分散型）へ移行する Web ツール。
- 今日は「何をするか」より「どう動くか」— 特にアーキテクチャ、Full Load、CDC の内部を深く。
- DSQL は分散アーキテクチャゆえ、水平スケールに合わない機能（FK、トリガー、同期インデックス等）を意図的に除外 → だからこれは「アップグレード」ではなく「異種移行」である。
-->

---

# 今日の3つの視点

1. **異種（heterogeneous）移行である** — アップグレードではない
   - MySQL → PostgreSQL 方言 → DSQL 制約、2-hop 変換

2. **2つのデータ経路が DSQL へ収束する**
   - Full Load（一度きりのバルク）+ オプションの CDC（継続ストリーミング）
   - **Full Load は Debezium スナップショットではない** — ツール自前の Python バルクローダー

3. **ツールはコントロールプレーン**である
   - 設定・バルクロード・ウォーターマーク・モニタリングのみ担当
   - データ整合性の原則: **静かな損失より、うるさい失敗**

<!--
- 3つの視点が発表全体を貫く。特に (2)「Full Load ≠ Debezium スナップショット」と (3)「loud fail over silent loss」を記憶。
- 目標は完全自動の無停止ではなく: 移行可能性の評価 → 決定論的にできることは自動化 → 人手が必要な箇所を明確に浮かび上がらせる。
-->

---

# なぜこのツールが必要か — MySQL ≠ Aurora DSQL

| | **RDS/Aurora MySQL（ソース）** | **Aurora DSQL（ターゲット）** |
|---|---|---|
| エンジン系統 | MySQL | **PostgreSQL(-16) 方言・互換** → 異種（heterogeneous） |
| アーキテクチャ | 単一ノードストレージ(heap) | **分散 · PK でストレージをパーティショニング** |
| 外部キー | あり | **なし**（アプリ層で担保） |
| トリガー・ストアドプロシージャ | あり | **なし** |
| インデックス作成 | 同期 | **`CREATE INDEX ASYNC`**（ロード後にバックフィル） |
| トランザクション | 大きな TX 可 | **≤3000行 · DDL 1個 · 値あたり 1 MiB · ≤5分** |
| 並行性制御 | ロックベース | **楽観的(OCC) — 衝突時 40001 リトライ** |
| 認証 | パスワード | **短命 IAM トークン（~15分）** |
| PK | 任意 | **必須**（AUTO_INCREMENT はホットパーティションを誘発） |

> 単純なダンプ/リストアや標準 JDBC ローダーではこれらの制約を越えられない → **DSQL を理解する専用ツール**が必要

<!--
- このスライドが「なぜこのツールか」の答え。一つひとつが後続スライドの設計根拠になる:
  分散+PKパーティショニング → ホットパーティション/PK戦略、≤3000行 → バッチローダー、OCC 40001 → statement リトライ、IAM トークン → カスタムシンク、FK なし → preserve-in-report。
- 核心の一行: mysqldump や汎用ツールの 'full load' は DSQL のバッチ上限・OCC・IAM トークン・型差を扱わない。
-->

---

# アーキテクチャ（俯瞰）— 2つのデータ経路が DSQL へ収束

![w:1000](../deploy/architecture-aws-simple.png)

- **Migration Tool**（ECS Fargate・Web UI）がソースを読み（**convert + bulk load**）DSQL へ書く = Full Load
- **CDC パイプライン**（点線ボックス）はマネージド MSK Connect 上: Debezium → MSK → **カスタム DSQL シンク**
- **Offset-seeder Lambda** が Full Load → CDC を gapless に繋ぐ

<!--
- まず大きな絵。上段の経路 = Full Load（変換+バルクロード）、下段のボックス = オプションの CDC。
- "CDC pipeline runs on managed MSK Connect — no servers owned" のラベルを強調: 我々はコンピュートを運用しない。
- 次のスライドで本番の全体アーキテクチャ（ネットワーク/IAM/セキュリティ）へ拡大。
-->

---

# アーキテクチャ（全体）— app-stack + cdc-stack

![w:1080](../deploy/architecture-aws.png)

- **app-stack**（常時）: ALB（オプション Cognito）· ECS Fargate（ECR Public）· Secrets Manager ·（オプション）Bedrock
- **cdc-stack**（オプション・VPC プライベート）: MSK + MSK Connect（Debezium ソース + カスタムシンク）· Offset-seeder Lambda · S3 Gateway VPC エンドポイント
- **コントロールプレーン vs データプレーン**: ツールは設定・バルクロード・ウォーターマーク・モニタリングのみ、シンク実行はマネージド MSK Connect

<!--
- なぜ単一タスク（desiredCount=1）? コントロールプレーンゆえ状態分割は不要（no-bloat）。イメージのローリング交換時に短いダウンタイム → 次の状態3層スライドで管理。
- ソース MySQL は顧客所有、両スタックの外。ブラウザは UI のみ（データ経路には無い）。
- セキュリティ: Cognito は既定 off、インターネット露出(0.0.0.0/0)の組み合わせはテンプレートが強制的に禁止。最小権限 IAM（task/execution role 分離）。
- 時間が短ければこのスライドは「左=app-stack 常時、右の VPC ボックス=cdc-stack オプション」の2行で流してよい。
-->

---

# 状態(state)管理 — 層ごとの保存場所と寿命

| 層 | 保存場所 | タスク交換時 |
|---|---|---|
| **① 認証情報** | セッション別 **プロセスメモリのみ** | 消滅（元々残さない） |
| **② ワークベンチ/ジョブ状態** | ローカル SQLite（`/tmp`、揮発性） | 消滅 → 再接続後に再実行 |
| **③ 移行済みデータ・スキーマ** | **DSQL 本体** | 保持（無関係） |

- **Property 7**: 認証情報はディスク・ログ・レポート・ジョブ状態に **絶対** 残さない。セッション終了で破棄。
- タスクのローリング交換 → ①② は消え ③ は残る → **再接続時に読み取り専用 Evaluation を再実行して自動復旧**。
- セッション Cookie 署名シークレットはスタックが自動生成（運用者の入力なし、DB 認証情報ではない）。

<!--
- DB 専門家へのアピール点: 認証情報が絶対にディスクに触れないのは強制ルール（コードレビューのゲート）。
- ③ が DSQL なのでタスクが死んでもデータ/スキーマは安全。再接続すればターゲットを introspect して状態を復元。
-->

---

# デプロイモデル — 最小設定で即デプロイ

- イメージは **ECR Public** 公開 → ビルド不要
- コネクタプラグインのアーティファクトが **コミット済み** → Java/Maven ツールチェーン不要
- ツールが **自前の S3 バケットを作成** → アーティファクトを自ら アップロード
- CDC インフラを **自動検出** → 推論不能なもの（VpcId 等）のみ入力
- リージョンは **DSQL エンドポイントから導出**（`…dsql.ap-northeast-2.on.aws` → `ap-northeast-2`）

**最小権限 IAM 分離**
- task role: `dsql:DbConnect(+Admin)`、読み取り専用 `GetCluster`、ソースシークレット範囲の `secretsmanager:GetSecretValue`
- `bedrock:InvokeModel` は **AI を有効にした時のみ**（許可モデル ARN 範囲）

<!--
- デプロイ利便性が設計の最上位原則。「新規 clone + 最小設定」が目標。
- クロスリージョン移行は非対応 — CDC データプレーンが DSQL リージョンの VPC 内でソースへプライベートに到達する必要があるため。単一リージョン前提。
-->

---

# 6ステップ移行ワークフロー

```
Connect → 1.Migration plan → 2.Evaluation → 3.Schema Conversion
        → 4.Data Migration → 5.Validation → 6.Cut over
```

- 各ステップは独立した状態（未開始 / 進行中 / 完了 / 失敗）、個別に実行・再実行可能
- **Migration plan** の唯一の永続的効果: CDC インフラを事前プロビジョニングするか否か
  - 詳細方式（Full+CDC vs CDC only）は後の Data Migration で、**やり直せる**
- **Cut over** は人が行う作業 → ツールはランブックのみ提供（Run アクション無し）

<!--
- 強制ウィザードではなくガイド型フロー。前段ステップが未完なら UI は案内するのみ。
- 今日のディープダイブは 2·3（Evaluation/Schema）と 4（Full Load/CDC）、5（Validation）。
-->

---

# ステップ2·3: Evaluation & Schema Conversion
## データを移す前に、DSQL が何を拒否するかを先に把握する

**オブジェクト別3段階分類** — 全ソースオブジェクトを AUTO / MANUAL / UNSUPPORTED のいずれかに判定
- ルールに引っ掛からなければ既定 AUTO。複数ルールに該当したら **最も厳しい等級を採用**: UNSUPPORTED > MANUAL > AUTO
- そのうえで該当した **全ルールの理由と推奨アクションはレポートに併記**され、どの指摘も埋もれない

**ルールベース2段階変換**（`sqlglot`）
```
MySQL DDL → [sqlglot: MySQL→PostgreSQL 方言] → [DSQL 制約レイヤ] → DSQL DDL
                                                FK 除去 · インデックス→ASYNC
                                                DDL トランザクション分離 · 型マッピング
```
- 決定論的変換が **常に先に** 実行され、AI(Bedrock) は MANUAL/UNSUPPORTED のみ補強（レビュー・承認後に反映）

<!--
- 核心メッセージ: 接続とデータ移動の間の「決定論的ゲート」— 試行錯誤ではなく予測可能な変換。
- Aurora MySQL ユーザーにとってこのステップは「DSQL が受け入れないもの」を1行も移す前に、読み取り専用で安価に知れる地点。
- AI は決定論経路を代替せず補強のみ（既定 off）、review-only + 明示的承認ゲートを通過して初めてターゲットへ反映。
-->

---

# Schema 変換の主要な決定

| 項目 | 処理 |
|---|---|
| **型マッピング** | TINYINT(1)→boolean, BIT(n)→int, ENUM/SET→text+CHECK, BLOB→bytea, DATETIME→timestamp |
| **外部キー** | DSQL に FK 無し → DDL から除去しつつ **レポートに保存** +「アプリ層で担保」を推奨 |
| **副次インデックス** | `CREATE INDEX ASYNC`（ロード後に非同期バックフィル）— FULLTEXT/SPATIAL は UNSUPPORTED |
| **DDL 適用** | トランザクションあたり **DDL 1個**、40001/OC001 を安全にリトライ（idempotent） |
| **PK 戦略** | 整数維持 / UUID / キャッシュ identity / **複合 PK（新規）** |

- **損失の透明性**: パース不能なビューも静かに捨てず **placeholder + MANUAL フラグ**
- **fail-loud**: `{0,1}` 以外の TINYINT(1) 値を静かに true に潰さず、テーブルの Full Load を中断

<!--
- FK "preserve-in-report" が損失透明性の象徴。消えるのではなく、浮かび上がる。
- PK 戦略の理由（ホットパーティション）は Full Load ディープダイブで詳述。ここでは「選択肢がある」だけ。
- DSQL ハード制約: カラム ≤255/テーブル、テーブル ≤1000/DB、DB 1個/クラスター、DECIMAL ≤38、値あたり 1 MiB。
-->

---

# ステップ4 A: Full Load エンジン内部 (1/2)
## Debezium スナップショットではない — 専用 Python バルクローダー

**読み取り: PK keyset ページネーション（OFFSET ではない）**
```sql
SELECT <cols> FROM <table>
WHERE pk > :last ORDER BY pk LIMIT 1000        -- 複合 PK は行値タプル比較
-- START TRANSACTION WITH CONSISTENT SNAPSHOT (InnoDB REPEATABLE READ), server-side cursor
```
- OFFSET は毎ページ先頭を再スキャン → ソースに O(n²)。keyset はインデックス seek、ページあたり ~1000行のみ in-flight
- **メモリはテーブルサイズに依らず1ページで bounded** — テーブル全体を RAM に載せない
- 単一の一貫スナップショット → ライブソースが変化しても安全。**PK 必須**（無ければ UNSUPPORTED）

**書き込み: バッチ `INSERT ... ON CONFLICT`**
- バッチサイズ = min(≤2000行/**≤3000 ハードキャップ**、パラメータ ≤65535、バイト ≤8 MiB)
- idempotent(ON CONFLICT) → 同じバッチを複数回ロードしても重複なし。CDC 同時実行時は「既存はスキップ」

<!--
- なぜ専用ローダーか: 汎用ツールの full load は内部的に JDBC INSERT で、DSQL 特化の OCC 処理が無い。
- 一貫スナップショット内でウォーターマークも同一トランザクションでキャプチャ → スナップショット時点と binlog 座標が正確に一致（後の CDC ハンドオフの根拠）。
- 注意: REPEATABLE READ スナップショットはテーブル読み取りの間ずっと開いており、書き込みの多いソースでは InnoDB undo purge を妨げうる（History List Length）。
-->

---

# ステップ4 A: Full Load エンジン内部 (2/2)
## 再開・並列・失敗隔離

**決定論的再開**: 行が keyset(PK) 順に流れる → **バッチ i = 常に同じ PK 範囲**
→ バッチが安定した再開単位。中断/リトライは **未完了範囲のみ** 再実行（重複なし）

**並列モデル（v0.1.68〜）: マルチプロセス — スレッドではない**
- `table_parallelism`（worker プロセス数、既定4·vCPU に合わせる）× `batch_parallelism`（プロセス内 DSQL 接続、既定8）
- **`ProcessPoolExecutor`**: テーブル（または shard）ごとに **自前の OS プロセス = 自前の GIL・CPU コア**
- **大型の単一整数 PK テーブル → PK 範囲 shard へ自動分割**、whole-table worker と統合プールで一緒にスケジュール
- 同時 DSQL 接続 ≈ table_par × batch_par（クラスター上限 10,000接続・100新規/秒 の範囲内で余裕）

**OCC リトライは statement 単位**（バッチ全体ではない）
- 40001(OC000 データ/OC001 スキーマ) → 衝突した `INSERT` **文1つだけ** をバックオフ+ジッターで最大10回
- バッチ全体の再送信は衝突していない99%の行を再度支払う + 広いキー範囲 = livelock リスク

**失敗隔離の2系統**（意図的に異なる）
- **行 quarantine**: DSQL が SQLSTATE で行を拒否（1MiB超/制約違反）→ バッチを二分探索でその行だけ隔離（**PK+理由のみ、値は絶対に記録しない**）、残りをロード、実行は失敗と判定
- **table-fatal**: 無損失変換が不能（例 TINYINT(1)=2）→ `ValueConversionError`（SQLSTATE 無し）→ テーブルのロードをうるさく中断

<!--
- statement 単位リトライがこのツールの署名。後の CDC でカスタムシンクがこれをそのままミラーする。
- 行 quarantine vs table-fatal の違い: 前者は DSQL が拒否（SQLSTATE あり）、後者は DSQL に問う前の read/convert 中に発生（SQLSTATE 無し）。どちらも「静かに素通りしない」。
- インデックスはロード後に最後、CREATE INDEX ASYNC で作る（ロード中だと全 INSERT がインデックス維持コストを支払う）。
-->

---

# Full Load パフォーマンス — GIL の壁とその突破（実測）

**発見①: ネットワークではなく CPU-bound**
- ソースリーダーが行ごとに MySQL→DSQL 型変換を **純 Python（GIL 保持）** で実行
- ThreadPool 時代: どの vCPU でも CPU **~110%（1コア）に固定** = GIL のサイン。reader シャーディング（スレッド）も **~0%**

**突破②: マルチプロセス (v0.1.68) — `ThreadPool → ProcessPoolExecutor`**
- テーブル/shard ごとに **自前プロセス = 自前 GIL・自前コア**。大型整数-PK テーブルは **PK 範囲 shard に自動分割**

| アプローチ (8 vCPU Fargate) | rows/s | CPU | 200GB 見積 |
|---|---|---|---|
| ThreadPool (v0.1.67、従来) | 12,277 | 110% | ~12時間 |
| ProcessPool、4テーブル混合、tp=8 | **34,800** | 561% | ~5時間 |
| ProcessPool、単一大型テーブル shard、tp=8 | **51,000** | 777% | **~2.5時間 (18×)** |

→ **最適設定: `table_parallelism = vCPU 数`**（ローダーが大型テーブルを自動 shard）
→ tp=8 付近でボトルネックが **CPU → DSQL サーバー write 容量** へ移動（~67K rows/s peak）

<!--
- このスライドが前回 tech-talk 以降で最大の更新。以前は「GIL が壁、CPU を上げよ」で終わっていたが、今はマルチプロセスでその壁を越えた。
- 核心の物語: スレッドでは GIL のため vCPU を使えない → プロセスごとに GIL が別なのでコアを実際に使い切る → 18×（200GB 46h→2.5h）。
- spawn context を使用、各 worker が自前の MySQL engine + DSQL 接続プールを構築（プロセス間の行転送なし）。テストダブルはスレッドフォールバックを自動使用（後方互換）。
- Replace 経路: 空テーブルには plain INSERT（ON CONFLICT 無し）で OCC 競合を排除 → 41K〜51K sustained、67K peak。
-->

---

# 並列度はスループットのダイヤルではなくスロットル — OCC ガードレール

**マルチプロセスで CPU の壁を越えた後、次の壁は OCC（サーバー write 競合）**

**並列度ガードレール（実測）**: 32→128 接続へ倍増 → スループットは **+5%のみ**、
リトライしたバッチ比率は **9.6%→12.8%**（単調 PK が同じキー範囲へ集中）

**OCC storm**: 同時 writer 多 + **既にデータのある** ターゲット + `ON CONFLICT` → livelock リスク
（観測: CPU 暴走なのに進捗0）。**処方**: 空ターゲットには plain `INSERT`、replace 経路は DROP+recreate を先に

**ソース負荷**は別レバー: `table_parallelism` が同時ソース読み取り圧 → 低めで開始(2〜4)、余裕を見てランプ

→ まとめ: **① CPU（マルチプロセスで解消）→ ② OCC（PK戦略・空ターゲット）→ ③ IAM トークン/TLS コールドスタート** の順にボトルネックが現れる

<!--
- 旧「並列度 +5%、リトライ 9.6→12.8%」の実測はそのまま有効 — ただし今は CPU の壁を越えた *後* の話へ位置づけが変わった。
- OCC storm はマルチプロセス導入時に実際に踏んだ地雷: 32 writer が populated ターゲットへ ON CONFLICT → 8分+ 0行。空ターゲット plain INSERT で解決。
- メモリは table_par × batch_par × ~8 MiB（テーブルサイズに依らず）。Fargate の CPU/メモリの組（8vCPU→16GiB）が既に充足。
-->

---

# パフォーマンスケーススタディ — Composite PK A/B（in-VPC 実測）

**実験**: `orders`+`payments`、PK 戦略のみ変更 → keep（整数）vs composite `(customer_id, id)`
（orders=処理群 / payments は `customer_id` が無いため整数 PK 維持=対照群）

| 条件 | keep 全体 rows/s | composite 全体 rows/s | CPU |
|---|---|---|---|
| **0.5 vCPU · bp8** | 4,270 | 4,243 (**0.99x**) | ~50%（1コアの壁） |
| **4 vCPU · bp16** | **10,055** | **10,088 (1.00x)** | 109〜111% |

- **composite は両条件とも0%差。** DSQL `CommitLatency`: 両方 **p50 ~47ms · p99 60〜120ms** → **ホットパーティションのロングテール無し**
- この A/B は **スレッド（単一プロセス）** 時代の実測 → ボトルネックがクライアント CPU なので、サーバー分散レバー（composite）は利得0

> **教訓: 最適化の前にボトルネックを測れ。** このワークロードの壁はサーバー書き込みではなく **クライアント CPU** だった。だから答えは composite ではなく **マルチプロセス（CPU の壁を突破）** だった。composite ははるかに高い write 並行性・真の単調-PK ホットスポットでのみ価値を持つ。

<!--
- この A/B はマルチプロセス以前（スレッド）の測定であることを明確に — だから CPU が壁で、その壁の真の解法が後のマルチプロセスだった、という物語につなぐ。
- 対照群(payments)が処理群(orders)と同じく 0.99x → 環境ノイズではなく本当に無効果。
- composite が輝く条件: (a) クライアント CPU ボトルネックを（マルチプロセスで）先に無くした後、(b) はるかに高い write 並行性で単調 PK がパーティションを熱くする時。
-->

---

# ステップ4 B: CDC パイプライントポロジ

![w:960](../deploy/architecture-cdc-pipeline.png)

- CDC は **オプション**。Full Load が既存行をコピー、CDC がその後の insert/update/delete を反映 → 最小ダウンタイム
- テーブルごとにトピック + **PK キーイング** → ある行の全変更が1パーティションに **順序保存**
- スキーマはランタイム内蔵の JSON コンバータで伝達（別途スキーマレジストリ不要）

<!--
- 短い凍結が許されれば Full Load のみで十分。CDC は大規模/継続移行向け。
- ツールはこのパイプラインを in-process で回さない → コントロールプレーンに過ぎない（次の次のスライド）。
-->

---

# なぜカスタムシンクか — 設計判断の核心

**標準マネージド JDBC シンク vs カスタムシンク**

| | 標準 JDBC シンク | **カスタム DSQL シンク** |
|---|---|---|
| OCC(40001) リトライ | **batch 単位** | **statement 単位** |
| 高競合の大規模 CDC | スループット **崩壊(collapse)** | 衝突文のみリトライ、バッチは進行 |
| IAM 短命トークン | ✗ | 15分トークンを2分の余裕で更新 |
| ≤3000行バッチ | ✗ | チャンクごとに commit 1回 |

> "Java is a consequence of the runtime, not a preference."

- マネージド MSK Connect = マネージド Kafka Connect → プラグインは **JVM jar** でなければならない
- Python `core/` のトークン生成・OCC リトライ・DSQL dialect ロジックを **Java でミラー**
- **限定的(bounded) なクロス言語の重複はマネージドランタイムの代償** — write-contract パリティテストで強制

<!--
- 「決定変更8」がこの根拠。標準 JDBC シンクが 40001 を batch でリトライ → 3000行まるごと再生 → 広いキー範囲 → livelock。
- Full Load(Python) と CDC シンク(Java) が同じ型マッピングに従うよう共有パリティテストで強制 → どの経路で移しても同じ行が同一にロードされる。
- CDC 固有: BIGINT UNSIGNED は precise モード、JSON は PGobject ラップ、GEOMETRY は .wkb 抽出。
-->

---

# gapless ハンドオフ — Full Load から CDC へ繋ぐ

**gapless はパイプラインの両端で守られる — 入口と適用の両方**

**① 入口（開始点）: Full Load が終わったその地点からストリーミングを始める**
- Full Load がスナップショットを取った瞬間の **ウォーターマーク**（binlog 位置 + GTID）を記録しておく
- CDC 開始時、ソースコネクタが立ち上がる **前に** VPC 内の Lambda がそのウォーターマークを Debezium の開始オフセットとして埋める
  - Lambda である理由: MSK Serverless の接続先が VPC 専用でアプリから直接埋められない
- その結果 Debezium は **「今」ではなく「スナップショット直後の最初の変更」から** 読む → 先頭の取りこぼしなし
  - `snapshot.mode=recovery`: 行を再読み込みせずスキーマ履歴のみ再構成し、シードされたオフセットから再開

**② 出口（適用）: 同じ変更が重複適用されても安全**
- シンクが PK 基準の `ON CONFLICT` upsert / PK delete で適用 → **リトライ・再生しても重複なし**（idempotent）
- 接続が切れたらその地点のオフセットを再生(replay) → 結果的に **実質1回(effectively-once)**

⚠️ **必ず押さえる前提**: ウォーターマークが指す **binlog が CDC 開始時点まで残っている** こと
- Aurora MySQL の既定保持は24時間だが CDC スタックのデプロイだけで15〜20分 → **開始前に保持期間を延ばす**（例: 7日）。消えていたら Full Load を再実行して新しいウォーターマークを得る必要がある

<!--
- 核心のフレーミング: gapless は1点ではなく「入口 + 出口」の2層で守られる。Lambda(入口)は先頭の取りこぼし防止、シンクの idempotent(出口)は途中の取りこぼし防止。
- 実際に起きた損失は「出口」のバグだった: 接続切れを poison と誤分類し未適用行のオフセットを飛ばした → isTransient 再分類で修正（リトライ）。Lambda(入口)は元々正常だった。
- 前提条件(binlog 保持)は現場で最も見落とされる。binlog_format=ROW、binlog_row_image=FULL も必須。
-->

---

# CDC データ整合性 — 静かな損失なし

**transient vs permanent エラーの区別がリトライ/DLQ の基準**
- **transient**（リトライ、DLQ に送らない）: OCC 40001、接続切れ（idle close/トークン失効/ワーカー交換）
  - 死んだ/half-open 接続を検知 → 新トークンで再接続 → 同じオフセットを再適用（PK idempotent、重複なし）
- **permanent**（DLQ 隔離）: 型不一致、制約違反、存在しないターゲットカラム（伝播していないソース ALTER）、超大型値
- 両方とも不可なら → **静かにスキップせずタスクをうるさく失敗させる**

**値あたり 1 MiB 上限 — 3区間**
- ≤1 MiB 正常 / 1–8 MiB シンクが write 前に測定し **DLQ 隔離**（Kafka 上限 4→8 MiB へ引き上げ）/ >8 MiB **キャプチャ段階で `column.exclude.list` によりドロップ**

**DLQ は Kafka ではなく CloudWatch で見る**
- 隔離理由には **SQL テンプレート（カラム名+`?`）のみ** — 行値・認証情報は絶対に無い → ツールがパースして UI に「テーブル別 Quarantined + エラーログのダウンロード」

<!--
- 「CDC はスキーマではなくデータを複製する」(include.schema.changes=false)。ソースの DDL 変更は伝播しない → 先に DSQL へ直接再適用。それまでは合わない行が DLQ に隔離（静かに消えない）。
- 接続切れを poison row と誤認しないのが核心 — 以前のデータ損失モードだった。
- 複合 PK: message.key.columns でソースから再キーイング → ある行の変更が同じパーティションで順序維持 + シンクが ON CONFLICT(pk...)/DELETE WHERE を正確に構成。
-->

---

# ステップ5: Validation — 最終確定の判定はここだけ

Full Load/ウォーターマークの行数は **スキャン無しの推定**（ソースを節約するため）。正確な判定は **Validation のみ**。

**テーブルごと3段階の漸増する厳密さ**（コスト↑）
1. **行数** = ソース vs ターゲットの正確な `COUNT(*)`（安価）
2. **チェックサム** = 順序非依存のテーブルチェックサムを両側で同一計算 →「行数は同じだが値が違う」を捕捉（全行読み取り）
   - ロジック: 行ごとの `MD5(カラム群)` 先頭60ビットを整数化 → テーブル全体で `SUM`（順序非依存）→ ソース=ターゲット比較。クロスエンジン正規化（NULL センチネル・型レンダリング一致）で同じデータ=同じハッシュ、FLOAT は除外
3. **突合** = 両側の全 PK をソートマージ → `missing_on_target`/`extra_on_target` を正確に指摘（**単一整数 PK のみ**）

**判定の AND チェーン**: テーブル matched = (COUNT 一致) AND (チェックサム一致) AND (PK セット整合)
→ レポート is_match = (∀テーブル matched) AND (orphan==0)。証拠が無ければ 'not deeply checked'（偽の一致ではない）

**ライブソース変化の補正**（検証中もソースは変わり続ける）
- スナップショット時点の GTID（ウォーターマーク）と **今** のソース GTID を比較 → ソースがその間に前進したか確認
- 前進していれば、ソースがターゲットより多いのは **移行バグではなくスナップショット以降に増えた新規データ** として区別しレポート
- →「説明できる差分」（新規アクティビティ · 意図した quarantine · 未収束の CDC）を除外し、**説明できない欠落のみ** を真の問題として表示

<!--
- ターゲット不足の診断: (a) ドリフト、(b) 意図した quarantine(1MiB超)、(c) まだ収束していない CDC 削除 → この3つで説明できれば健全。説明できない欠落/余剰 PK が真に捕まえる対象。
- 差分サンプルも PK+チェックサムトークンのみ、行値は絶対に露出しない。読み取り専用 CLI(compare_rows.py / cdc_consistency_check.py、exit 0 ゲーティング) もある。
-->

---

# AI DBA & Query Playground（オプション）
## 証拠ベース — 実測 DPU で証明

- AI 補助は **opt-in**（既定 off）、**コントロールプレーン専用**、**データ経路には絶対に無い**
- クエリ変換もスキーマと **同じ sqlglot エンジン** + AUTO/MANUAL/UNSUPPORTED + アンチパターンのタグ付け（`SELECT ... FOR UPDATE` 等）
- **ターゲット安全実行**: SELECT→EXPLAIN(ANALYZE 読み取り専用)、DDL→dry-run+**ROLLBACK**、DML→**ブロック**

**DSQL クエリチューニング規則（一般 PG の助言と異なる）**
- **PK がすなわちテーブル** — PK ソート B-tree、heap 無し。インデックスが無ければ Seq Scan ではなく **Full Scan**
- **compute↔storage 分離** → 流れてくる全行が **DPU** を発生。**フィルタを下へ押し込む** のが核心のレバー
- フィルタ3層: Query Processor Filter（最悪）→ Storage Filter(INCLUDE) → **Index Condition（最良）**
- 除外: VACUUM/REINDEX/fillfactor/プランナ GUC/`cost=` 引き下げ（DSQL に合わない）

<!--
- Tune with AI DBA ボタンは変換した SELECT が Test on target を通過した後にのみ表示 — AI が推測ではなく実際のプランに基づくように。
- 証明ループ: EXPLAIN ANALYZE で前後の DPU デルタをチャットへフィードバック → モデルの主張ではなく測定値が根拠。改善なしなら正直にそう言う。自動適用しない（human-review ゲート）。
-->

---

# まとめの考察: ホットパーティションとアプリケーションのクエリ変更

**DSQL は PK でストレージを分散** → 単調増加 PK（AUTO_INCREMENT/タイムスタンプ）は書き込みが1パーティションへ集中（ホットパーティション）。対策: UUID/キャッシュ identity/**複合 PK `(高カーディナリティの先頭カラム, 元の PK)`**。

**しかし — ホットパーティションが常にボトルネックとは限らない（実測）**
- keep vs composite A/B(orders+payments): **0.5 vCPU·bp8 でも 4 vCPU·bp16 でもスループット差なし(0.99〜1.00x)**
- DSQL CommitLatency: 両方 **p50 ~47ms / p99 60〜120ms** — 数秒級のホットパーティション・ロングテール **なし**
- ボトルネックは **サーバー書き込みではなくクライアント CPU**（§Full Load）→ 真の解法は composite ではなく **マルチプロセス**（GIL の壁を突破、18×）だった
- マルチプロセスで CPU の壁を越え tp=8 まで押して初めてボトルネックが **DSQL サーバー write** へ移動 — composite はそこから価値を持つ

**複合 PK の本当のコスト: アプリケーションのクエリが変わる**
- PK が `(customer_id, id)` になると、アプリの参照・結合・**upsert が新しい複合キーを使わねばならない**、先頭カラムは **不変** でなければならない
- 元キーの一意性を保つため `UNIQUE INDEX ASYNC` が別途必要、CDC は `message.key.columns` の再キーイングが必要

> **結論**: ボトルネックは層で来る — **CPU（マルチプロセスで解消）→ OCC/ホットパーティション → サーバー write**。ホットパーティション対策（PK 変更）は **CPU の壁を越えてサーバー書き込みの壁に実際にぶつかった時** に価値がある。まず測定し、composite は **クエリ変更コスト** と併せて判断せよ。

<!--
- 正直な診断: composite PK 機能は正常動作するがこのワークロードでは利得0だった — ボトルネックがサーバーではなくクライアント GIL だったため。その GIL の壁の真の解法がマルチプロセス(18×)だった。
- メッセージ: 「ホットパーティションは実在するが、対策を入れる前に測れ。ボトルネックは CPU→OCC→サーバー write の順に層をなす。そして複合 PK はタダではない — アプリのクエリが変わる。」
- composite が輝く条件: (a) マルチプロセスでクライアント CPU ボトルネックを先に無くした後、(b) はるかに高い write 並行性で単調 PK がパーティションを熱くする時。
-->

---

# Demo（5分）

## ここから実際にツールを実行します

- 6ステップワークフローを UI で: Connect → Evaluation/Schema Conversion → Full Load → Validation
- 今日話したことを画面で確認:
  - 3分類(AUTO/MANUAL/UNSUPPORTED) レポート · side-by-side DDL diff
  - Full Load 進捗（テーブル別 rows/s）· 失敗隔離
  - Validation 判定 ·（オプション）AI DBA 証明ループ

**Q&A はデモの最中・後に自由に**

<!--
- デモ開始。（社内テスト環境で実行 — 実行方法は発表資料に入れない）
- 時間が無ければ Full Load の進捗画面 + Validation 判定だけ見せても核心は伝わる。
-->

---

# ありがとうございました / 参考

- マニュアル: `docs/manual/ja/`（0〜11章、各ステップ詳細）
- デプロイガイド: `deploy/DEPLOYMENT.ja.md`
- カスタムシンク: `connectors/dsql-sink/`

**核心の3行まとめ**
1. 異種移行 — 決定論を優先し、人手が必要な箇所を浮かび上がらせる
2. Full Load（ストリーミングバルク、**マルチプロセスで GIL を回避 → 200GB 46h→2.5h、18×**）+ CDC（カスタムシンク、statement-OCC、gapless）→ DSQL
3. 静かな損失より、うるさい失敗 — 認証情報はメモリのみ、判定は Validation のみ

<!--
- 締め。質問を促す。
-->
