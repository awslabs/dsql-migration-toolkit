# mysql-dsql-migrator — ユーザーマニュアル（日本語）

_言語を選択してください:_

- 🇬🇧 [English](../en/README.md)
- 🇰🇷 [한국어](../ko/README.md)
- 🇯🇵 **日本語**

> 英語版が正本です。翻訳版と内容が食い違う場合は、英語版が優先されます。

このツールを使って **Amazon RDS / Aurora MySQL** データベースを **Amazon Aurora
DSQL** へ移行するためのガイド付きマニュアルです。本書は、**MySQL には精通しているが、
これから Aurora DSQL を使い始めようとしているデータベース運用（DB Operation）担当者**
を対象としています。DSQL は分散データベースとしての設計上、PostgreSQL と比べても
かなり多くの点が異なっており、本マニュアルはそれらの相違をまたいで変換する作業を
ツールが *どのように* 支援するのかを説明します。

> プロジェクトが初めてですか？ まずアーキテクチャと利用する AWS サービスの概要を
> [最上位の README](../../../README.ja.md) でお読みください。本マニュアルは、実際に
> 移行を実行する手順を順を追って案内する、タスク指向のコンパニオン文書です。
>
> 本マニュアルは、ツールが **すでに実行中**（ローカルまたは AWS 上）であることを前提と
> しています。まだ起動していない場合は、先に
> [`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) でデプロイしてから
> 戻ってきてください。[Set up](01-setup.md) の章ではローカル実行についても扱っています。

## このツールとは

**異種（heterogeneous）かつ決定論（deterministic）優先の移行**を実行する **Web ツール**
（およびインポート可能なエンジン）です。つまり MySQL → PostgreSQL 方言 → DSQL 制約という
流れで変換します。**ソースは常に読み取り専用**です。移行は **Connect** を事前ステップと
する 5 ステップのガイド付きフローです。

```
Connect → 1. Evaluation → 2. Schema Conversion → 3. Data Migration → 4. Validation → 5. Cut over
```

Data Migration は **Full Load**（ツール独自のバルクローダー）と、任意で行うストリーミング
**CDC**（ほぼ無停止のカットオーバーのための、独立した任意のパイプライン）で構成されます。
最終ステップの **Cut over** は、Validation を通過した後にアプリケーションを DSQL へ切り替える
ための運用ランブックです。

## マニュアルの目次

| # | 章 | 学べること |
|---|---|---|
| 0 | [Before you begin](00-before-you-begin.md) | 事前チェックリスト — 最初のステップから計画を左右する、必ず知っておくべき事実（同一リージョン限定、読み取り専用ソース、DSQL が省いている機能、CDC は任意・課金対象）。**ここから始めてください。** |
| 1 | [Set up](01-setup.md) | 前提条件、ツールの実行方法（ローカルまたは AWS）、ソースとターゲットへの接続方法。 |
| 2 | [Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md) | DSQL に移せるもの／移せないものをツールがどう評価するか（AUTO / MANUAL / UNSUPPORTED、工数見積り、名前の衝突）と、スキーマの変換・適用。完全な **MySQL → DSQL の型・制約リファレンス** を含みます。 |
| 3 | [Full Load](03-full-load.md) | バルクスナップショットロードの動作: ストリーミング export、冪等なバッチロード、ウォーターマーク（watermark）、そして失敗をどう隔離するか。 |
| 4 | [CDC and DSQL constraints](04-cdc-and-dsql-constraints.md) | ストリーミング CDC の動作、隙間のない Full Load → CDC ハンドオフ、そして DSQL の制約（値あたり 1 MiB、OCC、IAM 認証、外部キー適用の延期）をデータ経路でどう扱うか。 |
| 5 | [Validation](05-validation.md) | ターゲットがソースと一致することをツールがどう証明するか: 行数、チェックサム、PK 全体の突き合わせ、ライブソースのドリフト。 |
| 6 | [Limitations](06-limitations.md) | 計画に必ず織り込むべき、実際に強制される制限（DSQL の制約、単一リージョン CDC、単一タスクのコントロールプレーン）。 |
| 7 | [Performance and tuning](07-performance-and-tuning.md) | データ経路をこのように構築した理由（AWS に基づく根拠: OCC リトライ、ホットパーティションの PK、トランザクションの枠、非同期インデックス、IAM トークン）、Full Load / Validation / CDC の並列度をどうチューニングするか — ローカルおよび Fargate 上で — そして、その根拠を裏付ける再現可能な実測例。 |
| 8 | [Testing — DSQL が要求するシナリオ](08-testing-and-verification.md) | Aurora DSQL の各特性が *否応なく* テストさせる移行シナリオ（トランザクション上限、OCC、値あたり 1 MiB、IAM トークン、非同期インデックス、強制される FK、隙間のないハンドオフ、ドリフト）と、ツールがそれぞれをどう検証するか — オフラインおよび実際の AWS 上で。 |
| 9 | [Query Converter と AI DBA](09-query-validation.md) | 任意の Query Converter: 単一の MySQL クエリを Aurora DSQL へ変換し、ターゲット上で読み取り専用でテストし（`EXPLAIN` / `EXPLAIN ANALYZE` + DPU コスト）、**AI DBA** に DSQL の効率に合わせて書き直させ、再テストによって改善を証明します。 |
| 10 | [Conclusion](10-conclusion.md) | どの経路をいつ使うか、推奨されるエンドツーエンドのフロー、次に進む先。 |
| 11 | [Customer FAQ](11-customer-faq.md) | 顧客が最もよく尋ねる質問 — Full Load、CDC、制限、型マッピング、検証、カットオーバー／ロールバック、運用 — を、ツールの実際の動作に基づいて回答し、詳細へのリンクを添えています。 |
| 12 | [付録: 性能テスト結果](12-performance-test-results.md) | チューニングの根拠を裏付ける、実測の Full Load / Validation / CDC スループットの例と方法論。 |

## MySQL ユーザーへの Aurora DSQL についての注意

Aurora DSQL は **MySQL ではなく**、**Aurora MySQL をそのまま置き換えられるものでもありません**。
**PostgreSQL** ワイヤプロトコルを話し、**短命の IAM トークン**（パスワードなし）で認証し、
**分散型**（ロックではなく楽観的並行性）であり、水平方向にスケールしない機能は意図的に省いて
います — **トリガーなし、ストアドプロシージャなし、トランザクションあたりの行数制限、値あたり
1 MiB の制限**（一方、外部キーは **サポートされ、強制されます**）。本マニュアルは、これらが問題になる箇所ごとに一つひとつ指摘し、
ツールがそれに対して何を行うかを示します。これにより、DSQL のルールを苦労して学び直す必要が
ないようにしています。
