---
marp: true
theme: default
paginate: true
title: 이기종 변환 · Full Load · CDC · 성능 최적화 · 핫 파티션
class: dense
style: |
  section.dense { font-size: 21px; }
  section.dense h1 { font-size: 30px; }
  section.dense h2 { font-size: 22px; }
  section.dense table { font-size: 19px; }
  section.dense pre { font-size: 16px; }
  section.dense li { line-height: 1.35; }
---

# 이기종 변환 — MySQL을 어떻게 DSQL로 옮기나
## 같은 엔진 업그레이드가 아니다 — 2-hop 결정론적 변환

**변환 파이프라인 (`sqlglot`)**
```
MySQL DDL → [MySQL→PostgreSQL 방언] → [DSQL 제약 레이어] → DSQL DDL
                                       FK 제거 · 인덱스→ASYNC
                                       DDL 트랜잭션 분리 · 타입 매핑
```

**① 스키마(DDL) 변환 — 타입 매핑**

| MySQL | DSQL(PostgreSQL) |
|---|---|
| `TINYINT(1)` | `boolean` |
| `BIT(n)` | 정수 |
| `ENUM` / `SET` | `text` + `CHECK` |
| `BLOB` / `BINARY` | `bytea` |
| `DATETIME` | `timestamp` |

**② 구조 차이 메우기**: 외래 키 제거(**리포트에 보존** + 앱 계층 권장) · 보조 인덱스 `CREATE INDEX ASYNC` · DDL 트랜잭션당 1개 분리 · PK 필수화

**③ 값(데이터) 변환**: 로더(Python)와 CDC 싱크(Java)가 **동일한 타입 매핑 계약**을 따름 — 어느 경로로 옮겨도 같은 행이 동일하게 적재(패리티 테스트로 강제). BIGINT UNSIGNED precise, JSON 래핑, GEOMETRY→WKB 등

**결정론 우선 · AI는 보강만**: 규칙 변환이 항상 먼저, AI(Bedrock)는 MANUAL/UNSUPPORTED만 보강(검토·승인 후). 무손실 불가 값(예 `TINYINT(1)=2`)은 조용히 뭉개지 않고 **시끄럽게 중단**

> 모든 소스 객체를 **AUTO / MANUAL / UNSUPPORTED**로 먼저 분류(Evaluation) → 한 행도 옮기기 전에 "DSQL이 무엇을 거부할지" 파악

---

# Full Load — 초기 벌크 복사 (전용 Python 로더)
## Debezium 스냅샷이 아니다 — DSQL 제약을 이해하는 자체 벌크 로더

- **읽기**: PK keyset 스트리밍 (`WHERE pk > :last LIMIT 1000`, OFFSET 아님)
  - 일관 스냅샷 · 메모리는 한 페이지로 bounded(테이블 크기 무관) · **PK 필수**
- **쓰기**: 배치 `INSERT ... ON CONFLICT` (≤3000행/txn) · **문장 단위 OCC 재시도**(배치 전체 아님) · 멱등(재실행해도 중복 없음)
- **재개**: 배치 i = 항상 같은 PK 범위 → 중단/재시도는 **미완료 범위만**
- **성능**: 멀티프로세스(`ProcessPoolExecutor`)로 **GIL 우회** — 테이블/shard마다 자체 코어, 대형 테이블 PK-range 자동 샤딩
  - 실측 **200GB ~46h → ~2.5h (18×)**, `table_parallelism = vCPU 수`가 최적
- **실패는 시끄럽게**: 행 quarantine(PK+사유만 기록) 또는 table-fatal 중단

> 일회성 복사. 짧은 다운타임이 허용되면 Full Load만으로 충분.

---

# CDC — 연속 변경 복제 (선택, 최소 다운타임)
## Debezium → MSK(Kafka) → 커스텀 DSQL 싱크 · 관리형 MSK Connect 위에서 실행

- **역할**: Full Load 이후의 insert/update/delete를 계속 반영 → 컷오버 다운타임 최소화
- **행 데이터만 복제**(SQL·DDL 전파 안 함) · 테이블당 토픽 + PK 키잉으로 행별 순서 보존
- **왜 커스텀 싱크?**: 표준 JDBC 싱크는 40001을 batch 단위 재시도 → livelock
  - 커스텀 싱크 = **문장 단위 OCC 재시도** + IAM 단기 토큰 갱신 + ≤3000행 배치
- **gapless 핸드오프**: binlog/GTID **워터마크**로 Full Load 종료 지점부터 시작(입구) + 싱크 `ON CONFLICT` 멱등 적용(출구) → effectively-once
- **무결성**: transient(재시도) vs permanent(**DLQ 격리**) 구분, 둘 다 불가면 시끄럽게 실패
- **전제**: 워터마크가 가리키는 **binlog 보존** 필수(예: 7일), VPC 내 소스 프라이빗 도달

> 대규모·지속 마이그레이션용. 툴은 컨트롤 플레인 — 파이프라인은 관리형 MSK Connect가 실행.

---

# 성능 최적화 & 튜닝 옵션
## 어떻게 빠르게 했나 + 무엇을 돌릴 수 있나

**Full Load — 어떻게 최적화했나**
- **GIL 우회**: `ThreadPool → ProcessPoolExecutor` — 테이블/shard마다 자체 프로세스=자체 코어 (행별 타입 변환이 순수 Python·CPU-bound였음) → **200GB ~46h→~2.5h (18×)**
- **대형 테이블 자동 PK-range 샤딩** · **prefetch 큐**(읽기 선행) · **replace 경로는 빈 테이블 plain `INSERT`** 로 OCC 경합 제거

| Full Load 튜닝 (env) | 기본 | 효과 |
|---|---|---|
| `TABLE_PARALLELISM` | 4 | 동시 worker 프로세스 수 — **vCPU 수에 맞춤** |
| `BATCH_PARALLELISM` | 8 | 프로세스 내 동시 `INSERT` 배치(DSQL 연결) |
| `BATCH_ROWS` | 2000 | 배치당 행 수 (DSQL ≤3000 하드캡) |
| `PREFETCH` / `SHARD_MIN_ROWS` | 켜짐 / 100만 | 읽기 선행 / 이 행수↑ 테이블만 샤딩 |

**CDC — 튜닝 옵션 (cdc-stack CloudFormation 파라미터)**
- `SinkTasksMax`(기본2, **토픽 파티션 수로 상한**) · `SourceTasksMax`(1) · `ConnectorMcuCount`(1/2/4/8) · `ConnectorWorkerCount` · `SinkBatchMaxRows`(≤3000)
- **병렬수는 다이얼이 아니라 스로틀**: 무작정 올리면 OCC 충돌만 늘어남 → **PK 전략을 먼저** · UI 사이드바 Performance tuning으로 재배포 없이 실행 간 조정

**CDC 파티션 스마트 컨트롤**
- **테이블당 토픽 1개 + PK 키잉** → 한 행의 모든 변경이 한 파티션에 순서 보존
- MSK Serverless는 토픽 auto-create 안 함 → Debezium이 데이터 토픽 자체 생성(`topic.creation`)
- **내부 compacted 토픽 31→3 축소**: MSK Serverless는 compacted 파티션 **120 상한**이고 커넥터 재배포마다 소비·회수 안 됨 → 오프셋/상태 토픽 1파티션으로 낮춰 **쿼터 ~10배 절약**(재배포 여유)
- 토픽 `max.message.bytes` 1→**8 MiB** 상향 → 1–4 MiB 레코드가 Kafka를 통과해 싱크에서 DLQ 격리 가능(브로커 단계 무손실 드롭 방지)

---

# 핫 파티션과 Composite Key
## DSQL은 PK로 스토리지를 분산 — 단조 증가 PK는 쓰기가 한 파티션에 쏠린다

**문제 — 핫 파티션**
- `AUTO_INCREMENT`/타임스탬프 PK는 단조 증가 → 모든 신규 쓰기가 **마지막 split 하나**로 집중
- 논리적으로 충돌하지 않아도 그 파티션 리더가 직렬화 → **OCC 40001 폭증**, `CommitLatency` 롱테일
- Evaluation에서 드러냄(`AUTO_INCREMENT`→MANUAL), Schema Conversion에서 PK 전략 제공

**해결 — Composite Key (서버 측 한계를 실제로 움직이는 유일한 레버)**
- 원본 PK 앞에 **고카디널리티 컬럼**을 붙여 복합 PK로: `(id)` → `(customer_id, id)`
- PK 순서 저장이므로 선행 컬럼이 분산 → 쓰기가 수많은 키 범위로 흩어짐
- **소스 MySQL 불변** · 원본 키 유일성은 `UNIQUE INDEX ASYNC`로 유지
- **CDC도 커넥터/플러그인 변경 없음** — Debezium `message.key.columns`로 소스에서 **재키잉** → 싱크의 `ON CONFLICT`/`DELETE`가 타깃 복합 키와 일치, 순서 보존

**단, 공짜가 아니다 — 먼저 측정하라**
- 실측: keep vs composite A/B에서 처리량 차이 **0** (병목이 서버 write가 아니라 클라 CPU였음)
- 복합 PK 전환 시 앱의 **조회·조인·upsert가 새 복합 키를 써야 하고, 선행 컬럼은 불변**이어야 함
- → composite는 **진짜 핫 파티션(부하 시 낮은 OCC인데 CommitLatency 롱테일)** 이고 자연스러운 분산 컬럼이 있을 때만 값어치

> 병목은 층으로 온다: **① CPU(멀티프로세스로 해소) → ② OCC/핫 파티션(PK 전략) → ③ 서버 write**. 대책 넣기 전에 어느 층인지 측정하라.
