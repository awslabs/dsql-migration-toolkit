# Appendix: 성능 테스트 결과

_언어: [English](../en/12-performance-test-results.md) | **한국어** | [日本語](../ja/12-performance-test-results.md)_

> **이전:** [11. 고객 FAQ](11-customer-faq.md)

이 부록은 두 데이터 경로 — **Full Load**(도구의 벌크 로더)와 **CDC**(Debezium → MSK → 커스텀 DSQL
싱크) — 의 처리량 측정을 기록합니다. 숫자는 처리량을 좌우하는 요인과 기대치를 보여 주는 예시로
읽으세요. **성능 보증이 아닙니다**: 하드웨어, 스키마, 소스, Aurora DSQL의 워밍업 상태에 따라
달라집니다. 측정은 소스 및 Aurora DSQL과 동일 VPC 내 **ECS Fargate**(CDC는 관리형 **MSK
Connect**)에서 수행했습니다(1ms 미만 네트워크 RTT).

---

## 한눈에 보기

| 경로 | 구성 | 결과 |
|---|---|---|
| Full Load, 4개 테이블 | 8 vCPU, MySQL 소스 | ~32,000 rows/s |
| Full Load, 한 테이블을 8개 프로세스로 분할 | 8 vCPU, MySQL 소스, 빈 타깃 | 지속 41,000–51,000 rows/s (피크 ~67,000) |
| Full Load, 20개 테이블 약 1 TB | 16 vCPU, MySQL 소스, composite key | 915.7M행을 2시간 27분에 (평균 ~103K rows/s) |
| Full Load, 20개 테이블 약 1 TB | 16 vCPU, PostgreSQL 소스, composite key | 860M행을 1시간 42분에 (평균 ~140K rows/s) |
| Full Load, 거대한 단일 테이블(8.65억 행) | 16 vCPU, PostgreSQL 소스, 기본 설정 | ~20–23K rows/s (단일 리더) |
| CDC | 기본 싱크 크기(4 MCU) / 8 MCU | ~21,000 rows/s / ~26,000 rows/s |

---

## Full Load

### 처리량을 좌우하는 요인 (8 vCPU)

| 구성요소 | 설정 |
|---|---|
| **ECS Fargate** | 8 vCPU (8192 CPU units), 16 GB 메모리 |
| **소스** | Aurora MySQL (RDS), `customers_sample` 스키마 |
| **타깃** | Aurora DSQL, us-east-1 |
| **테이블** | `order_items` (33.6M행), `orders` (8.5M), `payments` (8.4M), `customers` (소형) |
| **측정 도구** | `scripts/measure_performance.py full-load` + CloudWatch progress 모니터링 |

| 구성 | rows/s | CPU |
|---|---|---|
| 한 프로세스의 스레드(코어 1개), 2개 테이블 | 12,277 | 110% (코어 1개) |
| 테이블당 프로세스 1개, 2개 테이블 | 22,365 | 207% |
| 테이블당 프로세스 1개, 4개 테이블 | 32,270 | 311% |
| 큰 테이블 1개를 8개 프로세스로 분할, 빈 타깃(단순 `INSERT`) | 지속 41,000–51,000, 피크 ~67,000 | 777% |
| 같은 테이블, 기존 행 위에 append(`ON CONFLICT`) | ~35,000 | 439% |

- **Full Load는 네트워크가 아니라 CPU 바운드입니다.** Python 프로세스 하나는 한 코어에서 행을
  변환하므로 처리량은 병렬 프로세스 수에 비례합니다. 테이블 병렬수를 vCPU 수에 가깝게
  잡으세요(§7.2).
- **빈 타깃에 적재하는 경로가 가장 빠릅니다.** **Drop & reload** 뒤에는 로더가 `ON CONFLICT` 없는
  단순 `INSERT`를 쓰므로 낙관적 동시성 경합이 완전히 사라집니다. 기존 행 위에 append하면 대략 15–30%
  느렸습니다.
- **동시 쓰기 프로세스가 약 8개를 넘으면 한계가 CPU에서 DSQL 쓰기 용량으로 옮겨 갑니다**(피크 약 67K
  rows/s 관측).

### 대규모: 약 1 TB (16 vCPU)

| | MySQL 소스 | PostgreSQL 소스 |
|---|---|---|
| **소스** | Aurora MySQL `db.r7g.8xlarge` (임시 업사이즈) | Aurora PostgreSQL 16.14 `db.r7g.8xlarge` (임시 업사이즈) |
| **데이터셋** | 20개 테이블 × 45.78M행 = 915.7M행 (≈ 1.07 TB) | 20개 테이블 × 43M행 = 860M행 (≈ 1.02 TB) |
| **로더 설정** | 20개 전부 composite PK(`dist_key`), `TABLE_PARALLELISM=16`, `BATCH_PARALLELISM=32`, batch rows 3000 | 동일 |
| **완주** | 20/20 테이블, 실패 0 | 20/20 테이블, 실패 0 |
| **총 소요 (wall)** | 2시간 27분 32초 | 1시간 42분 31초 |
| **평균 처리량** | ~103K rows/s | ~140K rows/s |
| **16개 테이블 병렬 구간** | ~131K rows/s (16 vCPU 포화) | ~194K rows/s (16 vCPU 중 약 12) |
| **OCC `40001` 재시도** | 0 | 0 |

두 측정 모두 같은 Fargate 크기와 같은 Aurora DSQL 클러스터를 썼고, UI가 아니라 자동 스크립팅(ECS
RunTask)으로 구동했습니다.

- **composite 기본 키가 핫 파티션 병목을 없앱니다.** 단조 증가 `AUTO_INCREMENT` 키는 모든 insert를
  맨 끝 키 범위로 몰지만, 카디널리티가 높은 컬럼을 앞에 붙이자 쓰기가 분산되어 재시도가 정확히
  0이었습니다(§7.1).
- **테이블 수를 테이블 병렬수 이내로 맞추세요.** 20개 테이블에 슬롯 16개라 마지막 4개 테이블이 혼자
  돌며(약 21K rows/s) 약 31분이 더 걸렸습니다. 테이블 병렬수를 테이블 수 이상으로 잡았다면 MySQL
  측정은 약 1시간 56분이었을 것입니다.
- **PostgreSQL 소스는 더 적은 CPU로 더 빠르게 적재합니다.** 값이 거의 그대로 통과하므로 행당 CPU
  비용이 MySQL의 방언 변환보다 낮습니다. MySQL은 CPU가 포화였고 PostgreSQL은 그렇지 않았습니다.
- **큰 데이터는 가능하면 여러 테이블로 나누세요.** 갓 생성된 DSQL 테이블은 파티션 1개로 시작해 부하
  속에 분할되므로, 단일 테이블 적재는 저조하게(약 16K rows/s) 시작해 120–150K rows/s까지 오릅니다.
  20개 테이블은 시작부터 20배의 파티션을 갖습니다.

### 거대한 단일 테이블

현행 도구는 단일 대형 테이블을 기본으로 **리더 하나**로 적재해, 쓰기가 일어나는 소스에서 동시
리더들이 서로 다른 스냅샷을 읽는 일이 없게 합니다. 8.65억 행 PostgreSQL 테이블에서 리더 하나는 약
20–23K rows/s(테이블 전체에 약 12시간)였고, 정적 소스에서 샤드 4개를 강제하면 약 42K rows/s로 CPU가
아니라 쓰기에 묶였습니다. 거대한 단일 테이블은 *Reader shards per large
table*([§3.8](03-full-load.md#38-병렬-적재), [§7.2](07-performance-and-tuning.md#72-병렬수-튜닝))을
참고하세요.

---

## CDC 처리량

CDC는 병목 지점이 Full Load와 다른 파이프라인입니다. Full Load는 **CPU 바운드**이고, CDC는
`Debezium(소스) → MSK 토픽 → 커스텀 DSQL 싱크` 구조에서 싱크가 **DSQL 쓰기 지연
바운드**입니다(문장마다 분산 왕복 1회). 스케일링 파라미터는 도구가 캡처 테이블 수로부터
추론합니다([§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)).

| 구성 요소 | 구성 |
|---|---|
| **소스 커넥터** | MSK Connect의 Debezium MySQL, `ConnectorMcuCount`=4 |
| **싱크 커넥터** | 커스텀 `dsql-sink`, `SinkTasksMax` 4→8 스케일 |
| **워크로드** | `customers_sample.orders`에 벌크 INSERT하는 ECS 태스크 4개(소스에 ~20,000 rows/s 유입) |
| **측정** | CloudWatch `AWS/KafkaConnect`의 `SourceRecordWriteRate` / `SinkRecordSendRate`, DSQL 타깃 행 수 증분으로 교차검증 |

- **싱크는 지연 바운드였으므로, 줄인 왕복 하나하나가 이득이었습니다.** 초기 버전은 행당 왕복 1회로
  적용해 CPU가 낮은 채 300–2,000 rows/s에 머물렀고, 적용을 배치로 묶고 숨은 행당 메타데이터 왕복을
  없애자 파티션 8개에서 약 18,700 rows/s가 되었습니다.
- **싱크의 크기는 소스와 따로 정하세요.** 이후 싱크는 CPU 바운드가 되고(4 MCU에서 약 80%, ~21,000
  rows/s), 단일 태스크 Debezium 소스는 CPU에 여유가 있습니다. `SinkMcuCount`(기본 4)는 소스의
  `ConnectorMcuCount`와 별개의 노브이며, **8 MCU**에서 CPU ~34%로 약 26,000 rows/s에 도달했습니다.
- **그 이상은 소스 쪽이 파이프라인을 제한합니다.** binlog 리더 하나와 소스 데이터베이스 자체의
  용량입니다. 2 vCPU 소스는 CPU 93%로 파이프라인을 막다가 스케일업한 뒤에야 풀렸습니다. 출하된
  producer 튜닝으로 소스 자체는 약 31,000 records/s를 소화했습니다.
- **싱크 CPU가 낮고 파티션 확장이 선형 미만이라고 해서 서버 측 경합이 증명되지는 않습니다.**
  여기서는 숨은 클라이언트 왕복이 같은 증상을 냈고 DSQL의 `OccConflicts`는 내내 0이었습니다. 서버를
  탓하기 전에 `OccConflicts`와 `ReadOnlyTransactions`를 확인하세요.
- **소스 재부팅은 자동으로 흡수됩니다** — 파이프라인이 커밋된 binlog 오프셋부터 gap 없이
  재개합니다([§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)).

---

## 측정 재현

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

전체 A/B 측정 하니스는
[`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh), 프로세스 내 처리량 +
OCC 보고 도구는 [`scripts/measure_performance.py`](../../../scripts/measure_performance.py)를
참고하세요.

CDC는 cdc-stack을 배포하고 소스에 꾸준한 insert 워크로드를 건 뒤, CloudWatch `AWS/KafkaConnect`에서
파이프라인 속도(`-debezium-source` 커넥터의 `SourceRecordWriteRate`, `-dsql-sink` 커넥터의
`SinkRecordSendRate`)를 읽고, 일정 구간 동안 DSQL 타깃 `COUNT(*)` 증분과 교차검증합니다.

---

**이전:** [11. 고객 FAQ](11-customer-faq.md)

**다음:** [13. 테이블과 컬럼 제외 →](13-excluding-tables-and-columns.md)
