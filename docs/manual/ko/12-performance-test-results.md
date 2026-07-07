# Appendix: 성능 테스트 결과

_언어: [English](../en/12-performance-test-results.md) | **한국어** | [日本語](../ja/12-performance-test-results.md)_

> **이전:** [11. 고객 FAQ](11-customer-faq.md)

이 부록은 개발 과정에서 수행된 Full Load 처리량 측정을 기록하며, 각 최적화 단계가 최종
성능에 어떻게 기여했는지 보여줍니다. 모든 측정은 소스 RDS MySQL 및 타깃 Aurora DSQL과
동일 VPC 내 **ECS Fargate**에서 수행되었습니다 (1ms 미만 네트워크 RTT).

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

## 핵심 발견

1. **GIL이 Python 데이터 파이프라인의 천장.** I/O를 해제하는 C 확장(psycopg3)이 있어도
   행당 Python 변환이 지배하여 1코어에 직렬화.
2. **`spawn` context의 ProcessPoolExecutor가 올바른 GIL 우회.** 각 worker가 자체 MySQL
   engine + DSQL connector를 구축 — 프로세스 간 행 전송 불필요.
3. **OCC 경합은 기존 데이터에 대한 동시 writer 수에 비례.** 32 writer가 같은 행에
   ON CONFLICT 치면 livelock. 빈 테이블에 plain INSERT로 완전 제거.
4. **tp=8에서 처리량 상한이 CPU → DSQL write 용량으로 이동.** ~8 writer process 이상은
   DSQL 서버측 write throughput이 병목 (~67K rows/s peak 관측).
5. **최적 설정:** `TABLE_PARALLELISM` = vCPU 수. 로더가 자동으로 대형 테이블을 shard.

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

---

**이전:** [11. 고객 FAQ](11-customer-faq.md)
