# mysql-dsql-migrator — ユーザーマニュアル（日本語）

_言語を選択してください:_

- 🇬🇧 [English](../en/README.md)
- 🇰🇷 [한국어](../ko/README.md)
- 🇯🇵 **日本語**

> 英語版が正本です。翻訳版と内容が食い違う場合は、英語版が優先されます。

このツールを使って **Amazon RDS / Aurora MySQL または PostgreSQL** データベースを
**Amazon Aurora DSQL** へ移行するためのガイド付きマニュアルです。本ツールは **2 つの
ソースエンジン — RDS / Aurora MySQL と RDS / Aurora PostgreSQL —** をサポートしており、
**Connect** 画面で選択します。本書は主に、**MySQL には精通しているが、これから Aurora
DSQL を使い始めようとしているデータベース運用（DB Operation）担当者** を対象としています。
DSQL は分散データベースとしての設計上、PostgreSQL と比べてもかなり多くの点が異なっており、
本マニュアルはそれらの相違をまたいで変換する作業をツールが *どのように* 支援するのかを
説明します。PostgreSQL ソースも **完全にサポート** されており、2 つの経路は異なります —
MySQL ソースは **異種（heterogeneous）** 変換であり、PostgreSQL ソース → DSQL は（どちらも
PostgreSQL-16 ワイヤを話すため）**ほぼ同一（near-identity）** の変換です。

> プロジェクトが初めてですか？ まずアーキテクチャと利用する AWS サービスの概要を
> [最上位の README](../../../README.ja.md) でお読みください。本マニュアルは、実際に
> 移行を実行する手順を順を追って案内する、タスク指向のコンパニオン文書です。
>
> 本マニュアルは、ツールが **すでに実行中**（ローカルまたは AWS 上）であることを前提と
> しています。まだ起動していない場合は、先に
> [`deploy/DEPLOYMENT.ja.md`](../../../deploy/DEPLOYMENT.ja.md) でデプロイしてから
> 戻ってきてください。[Set up](01-setup.md) の章ではローカル実行についても扱っています。

## このツールとは

**決定論（deterministic）優先の移行**を実行する **Web ツール**（およびインポート可能な
エンジン）であり、経路はソースエンジンによって異なります。**MySQL ソース** は **異種
（heterogeneous）** 経路である MySQL → PostgreSQL 方言 → DSQL 制約をたどり、**PostgreSQL
ソース** は方言ステップを省いて **ほぼ同一（near-identity）** の PostgreSQL-16 → DSQL 経路
（値をそのまま引き渡し、同じワイヤ、DSQL の制約と非対応の型のみを反映）をたどります。**MySQL
ソース（および PostgreSQL を Full Load 専用でのみ使う移行）には決して書き込みを行いません。**
`gtid_mode=ON` でないソースでの MySQL の Full load + CDC 実行は、CDC の開始位置を記録した直後に
`FLUSH BINARY LOGS` を 1 回実行するだけです（`RELOAD` が必要で、データは書き込まれません）。書き込みを
伴う唯一の例外は **PostgreSQL CDC** で、Full Load の整合性ポイントにおいて、ツールが移行対象テーブル
だけに限定した pgoutput 論理レプリケーションスロットと publication を作成し、後片付け（teardown）
時に削除します。また、再キー化したテーブルに `REPLICA IDENTITY FULL` を設定し、Full Load の後に
選択したテーブルをその publication に追加します（Full load + CDC、CDC を開始する前。autocommit、
許可リストに限定、監査記録あり）。これがツールがソースに対して行う
唯一の書き込みです。移行は **Connect** を事前ステップとする 5 ステップのガイド付きフローです。

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
| 0 | [Before you begin](00-before-you-begin.md) | 事前チェックリスト — 最初のステップから計画を左右する、必ず知っておくべき事実（同一リージョン限定、読み取り専用ソース — ただし PostgreSQL CDC の監査されるスロット／publication は例外、DSQL が省いている機能、CDC は任意・課金対象）。**ここから始めてください。** |
| 1 | [Set up](01-setup.md) | 前提条件、ツールの実行方法（ローカルまたは AWS）、ソースとターゲットへの接続方法、CDC のためのソース準備。 |
| 2 | [Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md) | DSQL に移せるもの／移せないものをツールがどう評価するか（AUTO / MANUAL / UNSUPPORTED、工数見積り、名前の衝突）と、スキーマの変換・適用。完全な **MySQL → DSQL および PostgreSQL → DSQL の型・制約リファレンス** を含みます（PostgreSQL リファレンスはほぼ同一で、DSQL が非対応の PG 型 — 配列、幾何（geometric）、ネットワーク、xml、money、bit/varbit、range/multirange、enum、composite、tsvector/tsquery、pgvector — は Evaluation と Schema Conversion で示します: 値を保持する代替型は MANUAL、代替型がなければ UNSUPPORTED）。 |
| 3 | [Full Load](03-full-load.md) | バルクスナップショットロードの動作: ストリーミング export、冪等なバッチロード、ウォーターマーク（watermark）、そして失敗をどう隔離するか。 |
| 4 | [CDC and DSQL constraints](04-cdc-and-dsql-constraints.md) | ストリーミング CDC の動作、隙間のない Full Load → CDC ハンドオフ、そして DSQL の制約（値あたり 1 MiB、OCC、IAM 認証、外部キー適用の延期）をデータ経路でどう扱うか。 |
| 5 | [Validation](05-validation.md) | ターゲットがソースと一致することをツールがどう証明するか: 行数、チェックサム、PK 全体の突き合わせ、ライブソースのドリフト。 |
| 6 | [Limitations](06-limitations.md) | 計画に必ず織り込むべき、実際に強制される制限（DSQL の制約、単一リージョン CDC、単一タスクのコントロールプレーン）。 |
| 7 | [Performance and tuning](07-performance-and-tuning.md) | Aurora DSQL に対してツールが代わりに行うこと（OCC リトライ、ホットパーティションの PK、トランザクションの枠、非同期インデックス、IAM トークン）、Full Load / Validation / CDC の並列度のチューニング — ローカルおよび Fargate 上で — そしてソースへの負荷を抑える方法。 |
| 8 | [テストと検証](08-testing-and-verification.md) | あなたのデータで Aurora DSQL へ移すときに起こり得る状況（大きなテーブル、1 MiB の値、あいまいな型、OCC の競合、長いストリーム、隙間のないハンドオフ、ドリフト）ごとに、ツールが代わりに行うことと、その結果をどこで確認するか — そして意図的に厄介なデータで実際の AWS 上で得た 100% 一致の結果。 |
| 9 | [Query Converter と AI DBA](09-query-validation.md) | 任意の Query Converter: 単一の MySQL クエリを Aurora DSQL へ変換し、ターゲット上で読み取り専用でテストし（`EXPLAIN` / `EXPLAIN ANALYZE` + DPU コスト）、**AI DBA** に DSQL の効率に合わせて書き直させ、再テストによって改善を証明します。 |
| 10 | [Conclusion](10-conclusion.md) | どの経路をいつ使うか、推奨されるエンドツーエンドのフロー、次に進む先。 |
| 11 | [Customer FAQ](11-customer-faq.md) | 顧客が最もよく尋ねる質問 — Full Load、CDC、制限、型マッピング、検証、カットオーバー／ロールバック、運用 — を、ツールの実際の動作に基づいて回答し、詳細へのリンクを添えています。 |
| 12 | [付録: 性能テスト結果](12-performance-test-results.md) | 実測した Full Load と CDC のスループット — それを左右する要因、MySQL・PostgreSQL ソースでの約 1 TB の実行 — と、測定の再現方法。 |
| 13 | [テーブルと列の除外](13-excluding-tables-and-columns.md) | **Exclude tables & columns** ルールポップアップ: ルール言語、除外できないもの、除外した列が Aurora DSQL で NULL になる仕組み、後から除外設定を変更する方法 (Change & reload)、CDC キャプチャリスト、Validation と Cut over。 |

## MySQL・PostgreSQL ユーザーへの Aurora DSQL についての注意

Aurora DSQL は **MySQL ではなく**、**Aurora MySQL をそのまま置き換えられるものでもありません**。
**PostgreSQL** ワイヤプロトコルを話し、**短命の IAM トークン**（パスワードなし）で認証し、
**分散型**（ロックではなく楽観的並行性）であり、水平方向にスケールしない機能は意図的に省いて
います — **トリガーなし、ストアドプロシージャなし、トランザクションあたりの行数制限、値あたり
1 MiB の制限**（一方、外部キーは **サポートされ、強制されます**）。そして PostgreSQL ワイヤを話すとはいえ、
**Aurora PostgreSQL をそのまま置き換えられるものでもありません**。IAM トークン認証と上記のトランザクション
あたり・値あたりの制限が加わり、トリガー・ストアドプロシージャの
サポートが省かれた **制約された分散型 PostgreSQL** であるため、PostgreSQL ユーザーも通常の Aurora
PostgreSQL のように扱うことはできません。（これらの DSQL の相違は、ソースエンジンに関係なく適用されます。）
本マニュアルは、これらが問題になる箇所ごとに一つひとつ指摘し、
ツールがそれに対して何を行うかを示します。これにより、DSQL のルールを苦労して学び直す必要が
ないようにしています。
