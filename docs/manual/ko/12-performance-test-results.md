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
| Full Load, 테이블 16개 × 20M 행 | 16 vCPU, PostgreSQL 소스 | 전체 ~336,000 rows/s (안정 구간 ~600,000) |
| Full Load, 테이블 16개 × 20M 행 | 16 vCPU, MySQL 소스 | 전체 ~193,000 rows/s (안정 구간 ~226,000) |
| Full Load, 150M 행 테이블 하나 | 16 vCPU, PostgreSQL 소스 | 전체 ~205,000 rows/s (안정 구간 ~280,000) |
| Full Load, 150M 행 테이블 하나 | 16 vCPU, MySQL 소스 | 기본값(세션 하나)으로 안정 구간 ~55,500 rows/s; Full load + CDC에서 *Reader shards per large table* 16으로 전체 ~164,000 rows/s |
| CDC | 기본 싱크 크기(4 MCU) / 8 MCU | ~21,000 rows/s / ~26,000 rows/s |

---

## Full Load

### 최대 규모 실행 (16 vCPU)

| 구성요소 | 설정 |
|---|---|
| **ECS Fargate** | 16 vCPU, 120 GB 메모리(가장 큰 태스크) |
| **소스** | Aurora MySQL과 Aurora PostgreSQL, `db.r8g.16xlarge` |
| **타깃** | Aurora DSQL, `ap-northeast-2` |
| **로더 설정** | *Tables in parallel* 16, 나머지는 모두 기본값(*Reader shards per large table*은 실행에 따로 적지 않으면 자동, *Batches per table* 8, *Rows per batch* 2000) |
| **측정 도구** | `scripts/measure_performance.py full-load`, 일회성 ECS 태스크로 실행 |

| 실행 | 행 | 시간 | 전체 | 안정 구간 | 제한 요인 |
|---|---|---|---|---|---|
| PostgreSQL, 테이블 16개 | 320M | 15분 53초 | ~336K rows/s | ~600K rows/s | Aurora DSQL(태스크는 코어 약 3.5개 사용) |
| PostgreSQL, 테이블 하나: 프로세스 16개에서 구간 32개 | 150M | 12분 12초 | ~205K rows/s | ~280K rows/s | Aurora DSQL |
| MySQL, 테이블 16개 | 320M | 27분 | ~193K rows/s | ~226K rows/s | 태스크의 CPU(16 vCPU 모두 사용) |
| MySQL, 테이블 하나, Full load only: 세션 하나 | 150M 중 75M(25분에 중단) | — | — | ~55.5K rows/s | 코어 하나 |
| MySQL, 테이블 하나, Full load + CDC: 별도 스냅샷 부분 16개 | 150M | 15분 12초 | ~164K rows/s | ~226K rows/s | 태스크의 CPU, 이전 사본의 DROP과 동시에 실행 |

### 처리량을 좌우하는 요인

- **동시 진행 배치가 아니라 키 범위입니다.** Aurora DSQL은 테이블의 키 범위 하나를 동시 진행 수와
  관계없이 약 20,000 rows/s로 쓰므로, 큰 테이블은 쓰기를 여러 키 범위로 분산해야만 빨라지며, 로더가
  기본으로 이를 합니다([§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)). 테이블당 범위 하나와
  비교하면 150M 행 테이블은 PostgreSQL에서 약 10배, 부분으로 나눈 MySQL에서 8배(둘 다 전체 기준),
  세션 하나의 MySQL에서 2.8배(안정 구간 기준) 빨리 적재됐습니다.
- **MySQL 적재는 태스크의 CPU에, PostgreSQL 적재는 Aurora DSQL에 묶입니다.** MySQL 행을 읽고
  변환하느라 동시에 적재되는 테이블 또는 부분마다 코어 약 하나가 바쁩니다. PostgreSQL 값은 그대로
  통과하므로 테이블 16개가 코어 약 3.5개로 336K rows/s에 적재됐습니다. *Tables in parallel*을
  태스크의 vCPU 수로 설정하세요.
- **거대한 MySQL 테이블 하나는 기본으로 코어 하나의 속도로 적재됩니다.** 세션 하나가 스냅샷
  하나에서 그 구간들을 읽습니다. binlog 시작 지점이 있는 Full load + CDC에서
  *Reader shards per large table*을 명시하면 별도 스냅샷 부분으로 나누어 여러 코어에서 읽으며,
  복사본은 CDC가 재생해야 일관됩니다([§3.8](03-full-load.md#38-병렬-적재)).
- **새 테이블은 워밍업이 필요합니다.** Aurora DSQL은 새 테이블을 파티션 하나로 시작해 커지면서
  분할하므로, 적재의 처음 1분 정도는 안정 구간보다 느립니다.
- **호스트와 앞선 작업이 수치를 움직입니다.** 같은 MySQL 적재가 한 Fargate 호스트에서 다른
  호스트보다 최대 약 1.8배 빨랐고, 이전 75M 행 사본을 삭제하자 Aurora DSQL의 백그라운드 작업이 1시간
  넘게 실행되며 다음 적재와 함께 돌았습니다.

---

## CDC 처리량

CDC는 병목 지점이 Full Load와 다른 파이프라인입니다: `Debezium(소스) → MSK 토픽 → 커스텀 DSQL 싱크`
구조에서 싱크가 **DSQL 쓰기 지연 바운드**입니다(문장마다 분산 왕복 1회). 스케일링 파라미터는 도구가
캡처 테이블 수로부터 추론합니다([§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)).

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
AWS_REGION=<region> APP_STACK=<app-stack> \
DB_HOST=<source-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
TARGET_ENDPOINT=<dsql-cluster-endpoint> \
MEASURE_SCHEMA=<schema> MEASURE_TABLES="<table> <table>" \
TABLE_PARALLELISM=16 VARIANTS=auto \
deploy/run_measure_on_fargate.sh
```

[`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh)는 소스의 VPC에서
일회성 Fargate 태스크로 측정을 실행합니다. `VARIANTS=auto`는 로더의 기본값을 측정하며, 다른 variant는
스크립트에 나열되어 있습니다. [`scripts/measure_performance.py`](../../../scripts/measure_performance.py)가
측정 자체입니다: 테이블별·시간 구간별 처리량, 쓰기 경로의 재시도와 지연, 그리고 Aurora DSQL이 인덱스
빌드를 끝낸 시점을 보고합니다. 사용법은 그 헤더에 있으며(`--reader-shards`, `--cdc-mode`, …),
`PERF_SOURCE_ENGINE=postgres`로 PostgreSQL 소스를 측정합니다. 대상 테이블을 삭제 후 다시 만들므로
**비프로덕션 타깃에서만** 쓰고, 실행을 비교하기 전에 이전 사본을 먼저 삭제하고 Aurora DSQL이
백그라운드 작업을 끝내게 하세요(`--wait-sys-jobs`).

CDC는 cdc-stack을 배포하고 소스에 꾸준한 insert 워크로드를 건 뒤, CloudWatch `AWS/KafkaConnect`에서
파이프라인 속도(`-debezium-source` 커넥터의 `SourceRecordWriteRate`, `-dsql-sink` 커넥터의
`SinkRecordSendRate`)를 읽고, 일정 구간 동안 DSQL 타깃 `COUNT(*)` 증분과 교차검증합니다.

---

**이전:** [11. 고객 FAQ](11-customer-faq.md)

**다음:** [13. 테이블과 컬럼 제외 →](13-excluding-tables-and-columns.md)
