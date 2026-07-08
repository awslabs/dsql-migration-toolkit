# Appendix: 성능 테스트 결과

_언어: [English](../en/12-performance-test-results.md) | **한국어** | [日本語](../ja/12-performance-test-results.md)_

> **이전:** [11. 고객 FAQ](11-customer-faq.md)

이 부록은 개발 과정에서 두 데이터 경로 — **Full Load**(툴의 Python 벌크 로더)와
**CDC**(Debezium → MSK → 커스텀 DSQL 싱크 파이프라인) — 의 처리량 측정을 기록하며, 각
최적화 단계가 최종 성능에 어떻게 기여했는지 보여줍니다. 모든 측정은 소스 RDS MySQL 및 타깃
Aurora DSQL과 동일 VPC 내 **ECS Fargate**(CDC의 경우 관리형 **MSK Connect**)에서
수행되었습니다 (1ms 미만 네트워크 RTT).

---

## 테스트 환경

| 구성요소 | 설정 |
|---|---|
| **ECS Fargate** | 8 vCPU (8192 CPU units), 16 GB 메모리 |
| **소스** | Aurora MySQL (RDS), `customers_sample` 스키마 |
| **타깃** | Aurora DSQL, us-east-1 |
| **테이블** | `order_items` (33.6M행), `orders` (8.5M), `payments` (8.4M), `customers` (소형) |
| **측정 도구** | `scripts/measure_performance.py full-load` + CloudWatch progress 모니터링 |

---

## Full Load 처리량의 진화

### Stage 1: Baseline (ThreadPool, GIL 제약)

초기 구현은 테이블 단위 병렬에 `ThreadPoolExecutor`를 사용. Python GIL 때문에 vCPU
수와 무관하게 CPU 1코어만 활용.

| 설정 | rows/s | CPU | 비고 |
|---|---|---|---|
| 0.5 vCPU, tp=4, bp=8, page=1000 | 4,243 | 50% | 원래 기본값 |
| 4 vCPU, tp=4, bp=8, page=1000 | 9,732 | 113% | vCPU 추가 → 스케줄링 개선만 |
| 8 vCPU, tp=2, bp=8, page=5000 | 12,277 | 110% | 코드 최적화 (v0.1.67) |

**진단:** 어떤 vCPU에서든 CPU ~110% (1코어) 고정 = GIL 서명.

### Stage 2: 코드 최적화 (v0.1.67, 여전히 GIL 제약)

행당 GIL hold 시간을 줄이는 최적화:

| 최적화 | 효과 |
|---|---|
| MySQL keyset page size 1000 → 5000 | 소스 round-trip 5배 감소 |
| `build_insert_statement` SQL 템플릿 캐싱 | 배치당 ~40K 객체 할당 제거 |
| `_iter_batches` lazy 바이트 추정 | `_estimate_row_bytes` 호출 90%+ 제거 |
| `_flatten_params` 리스트 컴프리헨션 | 파라미터 직렬화 ~40% 빠름 |
| `convert_row` passthrough fast path | 대부분 컬럼이 `convert_value` 건너뜀 |

**결과:** +41% 개선 (4,243 → 6,000 rows/s, 0.5 vCPU). 여전히 GIL 제약.

### Stage 3: 멀티프로세스 병렬화 (v0.1.68)

`ThreadPoolExecutor` → `ProcessPoolExecutor` 전환 — 각 worker 프로세스가 자체 GIL,
자체 MySQL 연결, 자체 DSQL 연결 풀 보유.

| 테스트 | 테이블 | tp | rows/s | CPU | vs baseline |
|---|---|---|---|---|---|
| A: ThreadPool | 2 (orders, payments) | 2 | 12,277 | 110% | 1× |
| B: ProcessPool Phase 1 | 2 (orders, payments) | 2 | 22,365 | 207% | **1.82×** |
| C: ProcessPool Phase 1 | 4 (전체) | 4 | 32,270 | 311% | **2.63×** |
| D: ProcessPool + PK shard | 1 (order_items) | 4 | 41,000 | 415% | **3.34×** |
| E: ProcessPool + PK shard | 1 (order_items) | 8 | 51,000 | 777% | **4.15×** |
| F: 통합 풀 (이전, 혼합 shard 없음) | 4 (전체) | 4 | 19,500 | 179% | 1.59× |
| **G: 통합 풀 + 자동 shard** | **4 (전체)** | **8** | **34,800** | **561%** | **2.83×** |

### Stage 4: Replace 경로 최적화

방금 DROP+recreate한 빈 테이블에 로드할 때, plain `INSERT` (ON CONFLICT 없음) 사용으로
OCC 경합 완전 제거:

| 테스트 | rows/s (sustained) | rows/s (peak) | CPU |
|---|---|---|---|
| ProcessPool + shard, SKIP_EXISTING (append) | 35,000 | 35,333 | 439% |
| ProcessPool + shard, NONE (replace/빈 테이블) | **41,000–51,000** | **67,000** | 777% |

---

## 요약: 200GB 단일 테이블 로드 시간 예상

| 버전 | 접근법 | rows/s | 200GB 예상 | 개선 |
|---|---|---|---|---|
| v0.1.67 이전 | ThreadPool, page=1000 | ~4,000 | ~46시간 | — |
| v0.1.67 | ThreadPool, 코드 최적화 | ~6,000 | ~31시간 | 1.5× |
| v0.1.67 | ThreadPool, 8 vCPU | ~15,000 | ~12시간 | 3.8× |
| **v0.1.68** | **ProcessPool, tp=4, 8 vCPU** | **~41,000** | **~4시간** | **10×** |
| **v0.1.68** | **ProcessPool, tp=8, 8 vCPU** | **~51,000** | **~2.5시간** | **18×** |

> 예상치는 행당 평균 ~300 바이트 가정. 실제 시간은 행 폭, 네트워크 레이턴시, DSQL 클러스터
> 부하, OCC 충돌률에 따라 다릅니다.

---

## CDC 처리량

CDC는 병목이 다른 파이프라인입니다. Full Load는 **CPU/GIL 바운드**인 Python 프로세스지만,
CDC는 `Debezium(소스) → MSK 토픽 → 커스텀 DSQL 싱크`이며 싱크가 **DSQL 쓰기 지연 바운드**입니다.
이 측정(2026-07-08)이 출하된 커넥터 코드(`dsql-sink` 플러그인)와
[§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)의 스마트 기본값을 이끌었습니다.

### CDC 처리량에 영향을 주는 파라미터

| 파라미터 | 위치 | 효과 |
|---|---|---|
| `topic.creation.default.partitions` | cdc-stack (추론) | 싱크의 병렬 단위 — 싱크 태스크 1개가 파티션 1개를 소비. **비가역적**(늘리기만 가능). |
| `SinkTasksMax` | cdc-stack (추론) | 싱크 커넥터 쓰기 병렬수; 실효값은 파티션 수로 상한. |
| `ConnectorMcuCount` | cdc-stack (추론) | 워커당 MSK Connect 컴퓨트 단위(1/2/4/8). |
| `SinkBatchMaxRows` | cdc-stack (3000, 고정) | DSQL 쓰기 트랜잭션당 행 수(DSQL 하드 한도). |
| `consumer.max.poll.records` | 싱크 워커 설정 | 한 `put()`에 넘기는 레코드 수 — 싱크가 하나의 JDBC `executeBatch`로 묶을 수 있는 상한. |
| `max.batch.size` / `max.queue.size` | 소스 커넥터 | 스트리밍 반복당 배출 binlog 이벤트 수 / reader→producer 큐 깊이. |
| `producer.batch.size` / `linger.ms` / `compression.type` | 소스 워커 설정 | Kafka produce 배치의 크기·채움 지연·압축. |

커넥터 스케일링 노브(파티션 / `SinkTasksMax` / `ConnectorMcuCount`)는 **캡처 테이블 수로부터
추론**되며 UI에 노출되지 않습니다 — [§7.2 → CDC](07-performance-and-tuning.md#72-병렬수-튜닝) 참고.

### 테스트 환경 (CDC)

| 구성 요소 | 구성 |
|---|---|
| **소스 커넥터** | MSK Connect의 Debezium MySQL, `ConnectorMcuCount`=4 |
| **싱크 커넥터** | 커스텀 `dsql-sink`, `SinkTasksMax` 4→8 스케일 |
| **워크로드** | `customers_sample.orders`에 벌크 INSERT하는 ECS 태스크 4개(소스에 ~20,000 rows/s 유입) |
| **측정** | CloudWatch `AWS/KafkaConnect`의 `SourceRecordWriteRate` / `SinkRecordSendRate`, DSQL 타깃 행 수 증분으로 교차검증 |

### CDC 처리량의 진화

| 단계 | 설정 | 싱크 rows/s | 싱크 CPU | 병목 | vs 베이스라인 |
|---|---|---|---|---|---|
| 1: 단일 파티션 | 1 파티션 / 1 태스크 | 292 | — | 파티션 수 = 1 (병렬수 없음) | 1× |
| 2: 파티션 적용 | 4 파티션 / 4 태스크 | ~550 | 5% | 싱크가 **행당 왕복 1회**로 적용 | 1.9× |
| 3: 배치 적용 (**플러그인 v13**) | 4 파티션 / 4 태스크 | ~1,165 | 7% | 소스(producer 미튜닝) | **4.0×** |
| 4: 소스 튜닝 (**플러그인 v14**) | 8 파티션 / 8 태스크 | ~1,500 | 6.5% | DSQL 쓰기 경합 | **5.1×** |

두 개의 코드/설정 변경이 대부분을 해결했습니다:

- **플러그인 v13 — 배치 싱크 적용.** 싱크가 연속된 동일 SQL 변경 이벤트의 최대 구간을 행별
  `executeUpdate()` 대신 하나의 JDBC `executeBatch()`로 묶습니다. DSQL은 지연 바운드이므로
  (각 문이 분산 왕복; 태스크는 CPU ~5%), 행별 왕복을 배치 전송으로 접으면 싱크 처리량이 **2배**로
  증가했습니다(~550 → ~1,165 rows/s). `consumer.max.poll.records`도 500 → 3000으로 올려 한 번의
  poll이 ≤3000행 트랜잭션을 채우도록 했습니다.
- **플러그인 v14 — 소스 producer 튜닝.** 더 큰 배치/큐 + `lz4` 압축 producer 배치로 소스가
  ~1,940 → **~31,000 rec/s(16×)** 가 되어, 소스가 실제 천장이 아니라 배치가 덜 찼던 것임을
  증명했습니다. 이로써 싱크→DSQL 쓰기가 진짜 최종 병목으로 드러났습니다.

8 파티션에서 싱크는 ~1,500 rows/s에 도달(DSQL 적용 교차검증 1,484 rows/s)했지만, 4→8 스케일은
**~1.4배(sublinear)**에 그쳤습니다: 동일 테이블에 대한 동시 upsert가 DSQL 내부에서 경합하기
때문입니다. 스마트 기본값이 실효 병렬수를 8로 상한하는 이유가 바로 이것입니다.

---

## 핵심 발견

### Full Load

1. **GIL이 Python 데이터 파이프라인의 천장.** I/O를 해제하는 C 확장(psycopg3)이 있어도
   행당 Python 변환이 지배하여 1코어에 직렬화.
2. **`spawn` context의 ProcessPoolExecutor가 올바른 GIL 우회.** 각 worker가 자체 MySQL
   engine + DSQL connector를 구축 — 프로세스 간 행 전송 불필요.
3. **OCC 경합은 기존 데이터에 대한 동시 writer 수에 비례.** 32 writer가 같은 행에
   ON CONFLICT 치면 livelock. 빈 테이블에 plain INSERT로 완전 제거.
4. **tp=8에서 처리량 상한이 CPU → DSQL write 용량으로 이동.** ~8 writer process 이상은
   DSQL 서버측 write throughput이 병목 (~67K rows/s peak 관측).
5. **최적 설정:** `TABLE_PARALLELISM` = vCPU 수. 로더가 자동으로 대형 테이블을 shard.

### CDC

6. **싱크는 CPU 바운드가 아니라 지연 바운드.** CPU ~5–7%에서 싱크는 연산이 아니라 DSQL 왕복을
   기다리고 있었음 — 따라서 레버는 더 많은 컴퓨트가 아니라 *더 적고 큰* 쓰기(배치 `executeBatch`).
7. **행별 왕복을 배치화하는 것이 CDC 최대 단일 개선**(플러그인 v13만으로 ~550 → ~1,165 rows/s).
8. **소스는 애초에 천장이 아니었음.** producer 튜닝(배치/큐/`lz4`)으로 16× ~31,000 rec/s 도달;
   MySQL 서버당 단일 Debezium 태스크로 충분.
9. **싱크 병렬수는 sublinear하게 증가.** 4 → 8 파티션이 2배가 아니라 ~1.4배 — 동일 테이블 동시
   upsert가 DSQL 내부에서 경합하기 때문. 그래서 스마트 기본값이 실효 병렬수를 8로 상한.
10. **파티션 수는 비가역적**이므로, 툴은 영구적으로 잘못 설정될 수 있는 UI 노브 대신 캡처 테이블
    수로부터 생성 시점에 추론.

---

## 재현 방법

```bash
AWS_REGION=us-east-1 \
MEASURE_SCHEMA=customers_sample \
MEASURE_TABLES="order_items orders payments customers" \
TABLE_PARALLELISM=8 \
BATCH_PARALLELISM=8 \
deploy/run_measure_on_fargate.sh
```

자세한 내용은 [`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh)
(A/B 측정 하네스)와
[`scripts/measure_performance.py`](../../../scripts/measure_performance.py)
(처리량 + OCC 리포팅 도구) 참고.

CDC는 cdc-stack을 배포하고 소스에 꾸준한 INSERT 워크로드를 걸어둔 뒤, CloudWatch
`AWS/KafkaConnect`에서 파이프라인 속도를 읽습니다(`-debezium-source` 커넥터의
`SourceRecordWriteRate`, `-dsql-sink` 커넥터의 `SinkRecordSendRate`). 고정 구간에서 DSQL
타깃의 `COUNT(*)` 증분으로 교차검증합니다.

---

**이전:** [11. 고객 FAQ](11-customer-faq.md)
