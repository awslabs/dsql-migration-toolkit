# 8. 테스트 — DSQL 특성이 강제하는 시나리오

_언어: [English](../en/08-testing-and-verification.md) | **한국어**_

이 장은 하나의 생각을 중심으로 구성됩니다: **각 Aurora DSQL 특성은 반드시 테스트해야 하는 특정
마이그레이션 시나리오를 강제한다.** MySQL→MySQL 마이그레이션은 이것들을 증명할 필요가 없습니다 — DSQL이
다른 엔진(PostgreSQL 와이어, 분산, 서버리스, IAM 인증, 낙관적 동시성)이기 때문에 존재하는 시나리오입니다.
아래 각 DSQL 특성마다, 그것이 강제하는 시나리오와 도구가 그것을 어떻게 행사하는지를 정리합니다.

> 모든 시나리오는 두 테스트 층위가 뒷받침합니다: 동작을 결정론적으로 증명하는 **오프라인 스위트**(Python
> ~1,700 + Java 42, AWS 불필요 — seam 주입), 그리고 의도적으로 실패하는 행과 함께 실제 인프라에서
> 증명하는 **라이브 end-to-end 실행**(실제 RDS MySQL + Aurora DSQL + MSK)(§8.2 요약).

---

## 8.1 DSQL 특성이 강제하는 시나리오

### 트랜잭션 형태 — DSQL은 모든 쓰기 트랜잭션을 제한한다

DSQL은 트랜잭션당 **≤ 3,000행**, **수정 데이터 ≤ 10 MiB**, **≤ 5분**, **DDL 1개**를 허용합니다. 이를
무시한 로더는 즉시 실패하거나 조용히 잘립니다.

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 트랜잭션당 ≤ 3,000행 | 한 트랜잭션보다 큰 테이블; 한도에 정확히 닿는 배치; 한도 초과 요청 | 배치 행 수 캡; 한도 수용·초과 거부(`test_batch_size_at_the_hard_cap_is_accepted`, `…_above_the_hard_cap_is_rejected`) |
| 트랜잭션당 ≤ 10 MiB (8 MiB 안전 예산) | 행 수보다 **바이트**가 먼저 한도 초과하는 넓은 행; 예산보다 큰 단일 행 | 행 수 분할보다 바이트 분할 먼저; 초대형 단일 행은 단독 배출(`test_iter_batches_splits_on_byte_budget_before_row_count`, `…_single_oversized_row_yields_alone`) |
| 트랜잭션당 DDL 1개 | 테이블의 보조 인덱스 생성 | 각 인덱스 DDL을 자체 트랜잭션에서(`test_indexes_created_after_all_data_each_its_own_statement`) |

### 낙관적 동시성 — DSQL엔 락이 없고, 충돌은 커밋 시점(40001)에 드러난다

DSQL은 쓰기 충돌을 커밋 시점에 감지해 `SQLSTATE 40001`을 반환합니다. 지는 트랜잭션은 **재실행**해야 하고,
경합 시 자주 발생합니다.

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 쓰기 시 40001 충돌 | 배치가 직렬화 충돌 후 재시도로 성공 | 백오프/지터 문장 단위 재시도로 복구(`test_occ_conflict_on_batch_is_retried_then_succeeds`); CDC 싱크도 동일 로직(`OccRetryTest.java`) |
| 재시도 예산 소진 | 충돌이 끝내 안 풀림 | **실패**로 기록, 절대 무음 드롭 안 함(`test_exhausted_occ_conflict_is_recorded_as_failure`) |
| 고동시성 vs 연결 쿼터(10,000/클러스터, 100/초) | 많은 테이블 × 많은 배치 동시 | 총 진행 연결 한정(`test_parallel_connection_use_is_bounded`); 기본 4 테이블 × 8 배치 = 32 ≪ 쿼터 |

### 값당 1 MiB 한도 — 큰 단일 값은 저장 불가

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 값 > 1 MiB | 한도 초과 LOB/TEXT 값이 Full Load 중 **그리고** CDC 중 | Full Load는 행 단위 **quarantine**(PK + 사유 기록, 테이블 계속 적재); 싱크는 쓰기 전 측정해 **DLQ**(`DsqlSinkTask` oversized 가드) |
| 값 > 8 MiB (Kafka 통과 불가) | 더 큰 컬럼 | Debezium `column.exclude.list`로 **캡처 단계에서 제외**, Evaluation `OVERSIZED_LOB` 플래그가 구동 |

### IAM 토큰 인증 — 비밀번호 없음, 15분 토큰, 60분 연결

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 단기 토큰 / 끊긴 연결 | 토큰보다 오래 가거나 60분 연결 한도에 닿는 장기 적재·CDC 스트림 | 풀이 Dead 또는 half-open(끊겼지만 살아있는 듯 보이는) 연결을 폐기하고, 재시도가 **새** 토큰으로 재연결(`test_transient_connection_error_is_retried_and_recovers`, `test_pool_discards_connection_after_in_use_error`; 싱크: `DsqlSinkTaskTest`, `DsqlIamTokenProviderTest`) |

### 비동기 인덱스 — `CREATE INDEX ASYNC`, 데이터 후 빌드

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 인덱스를 비동기로, 적재 후 빌드 | 보조 인덱스 있는 테이블; 데이터 배치가 실패하는 적재 | 인덱스는 **모든** 데이터 적재 후에만 생성; 데이터 배치가 하나라도 실패하면 인덱스 **생성 안 함**(`test_indexes_created_after_all_data…`, `test_indexes_are_skipped_when_a_data_batch_fails`) |

### 스키마 차이 — FK 없음, PK 필수, 미지원 타입/객체

DSQL은 외래 키·트리거·저장 프로시저·일부 타입을 제외하고 기본 키를 요구합니다.

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 외래 키 없음 | FK 많은 소스 | FK를 DDL에서 제거하고 `MANUAL` 노트로 보존; Validation **고아 검사**가 앱 측 무결성 확인(`test_orphan_records_are_detected_and_fail_the_match`) |
| 기본 키 필수 | PK **없는** 테이블; **복합** PK | 무 PK는 사전 차단(`UNSUPPORTED`)이고 keyset export가 거부; 복합 PK는 행-값 튜플 비교로 적재(`test_exporter.py`, 시나리오 문서) |
| 미지원 타입/객체 | 공간 타입, `DECIMAL` 정밀도 > 38, 컬럼 > 255, 트리거/루틴 | Evaluation에서 사유와 함께 `UNSUPPORTED` 플래그(`test_converter.py`, assessor 테스트) |
| TINYINT(1) → boolean, 범위 초과 | `2`를 가진 `TINYINT(1)` | **시끄러운, 테이블 전체 실패** — `2`를 `true`로 평탄화 거부(무음 손상 없음) |
| 타입 이질성(MySQL → PG 방언) | 최대 타입 다양성 스키마 | Full Load(Python)와 CDC 싱크(Java)가 각 타입을 **동일한** 저장 형태로 인코딩해야 함 — 공유 **write-contract** 패리티 테스트로 강제(`test_dsql_write_contract.py`) |

### 무손실 Full Load → CDC 핸드오프 — 가장 어려운 정합성 속성

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 벌크 적재 후 갭/중복 없이 스트리밍 재개 | 스냅샷 적재, 라이브 INSERT/UPDATE/DELETE 워크로드, 워터마크부터 CDC 시작, 수렴 | 오프셋을 워터마크로 시드 + `snapshot.mode=recovery`; PK 기준으로 적용하므로 겹쳐도 중복이 안 생김(`test_cdc_pipeline.py`, `test_cdc_offset_seed.py`, `test_offset_seeder_lambda.py`) |
| CDC는 데이터 복제, DDL 아님 | CDC 중 소스 스키마 변경 | 타깃 형태에 안 맞는 행은 손실이 아니라 **DLQ**로(`test_cdc_dlq.py`) |

### 라이브 소스가 계속 변함 — 드리프트를 올바르게 귀속해야 함

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 마이그레이션 중/후 소스 전진 | 소스 GTID가 워터마크를 지나친 상태에서 검증 | 워터마크 GTID로 드리프트 감지·보고 → 행 수 차이를 버그가 아니라 **신규 소스 활동**으로 귀속(`test_drift_since_snapshot_is_reported`, `test_no_drift_when_gtid_unchanged`) |
| 수 같지만 데이터 다름 | 행 수는 같으나 값이 다른 행 | **체크섬**이 포착; 행 수만의 "일치"는 절대 신뢰 안 함(`test_deliberate_data_mismatch_with_equal_counts_is_not_a_match`) |

### 재개 가능성 — 중단이 작업을 잃거나 중복시키면 안 됨

| DSQL 특성 | 테스트한 시나리오 | 행사 방법 |
|---|---|---|
| 다시 적용해도 안전 | 배치 재실행 / 중단된 적재 재개 | `INSERT … ON CONFLICT`는 중복 안 함; 재개는 미완료 PK 범위만 재실행하고 무중단 상태로 수렴(`test_reapplying_batches_does_not_duplicate_rows`, `test_resume_skips_done_batches_and_converges_to_uninterrupted_state`) |

**스위트 실행:** `\.venv/bin/python -m pytest -q`, `cd connectors/dsql-sink && mvn -q test`.

**브라우저 UI E2E (선택):** `tests/e2e`의 Playwright 스위트가 실제 웹 UI를 헤드리스 브라우저로 구동합니다
(앱을 서브프로세스로 띄움). opt-in이며 — 기본 `pytest` 실행에서는 제외됩니다 — `playwright` dev 의존성과
Chromium 빌드(1회)가 필요합니다:

```bash
uv sync                                         # playwright dev 의존성 설치
.venv/bin/python -m playwright install chromium # 브라우저 1회 다운로드
.venv/bin/python -m pytest -m e2e               # 브라우저 E2E 스위트 실행
```

기본 "smoke" E2E는 외부 인프라가 필요 없습니다(UI 렌더·네비게이션·Query Playground 변환 검증).
**connected** 티어는 라이브 인프라로 실제 흐름을 구동합니다 — Connect 화면에서 소스+타깃 검증 후 Query
validation → Test on target(`EXPLAIN ANALYZE` + DPU), 그리고 Amazon Bedrock이 도달 가능하면 Tune with
AI DBA → 재테스트 → 코드블록별 copy 버튼까지:

```bash
RUN_E2E_CONNECTED=1 .venv/bin/python -m pytest -m e2e   # connected 티어 포함
```

의존성이 없을 때 절대 깨지지 않도록 3중 게이트입니다: `RUN_E2E_CONNECTED=1`로 opt-in한 뒤, 소스 MySQL +
타깃 Aurora DSQL이 실제로 도달 가능하지 않으면(`.env` 기준) 깔끔히 skip하고, AI 튜닝 케이스는 Bedrock이
도달 가능하지 않으면 추가로 skip합니다. 연결값은 앱이 Connect 폼을 프리필할 때 쓰는 것과 동일한 `.env`에서
옵니다.

---

## 8.2 시나리오를 한데 모으기 — 라이브 end-to-end 실행

위 시나리오들은 **실제 AWS**(RDS MySQL + Aurora DSQL + MSK)에서 **함께** 행사되기도 했습니다. 한 번의
실행에서 최대한 많은 DSQL 특성을 건드리도록 설계된 전용 스키마로:

- parent → child / lob 외래 키 체인(**FK 없음** + **PK** + **고아 검사** 시나리오 강제).
- 최대 타입 다양성 — 모든 정수/unsigned 변형, `DECIMAL`(정밀도 > 38 포함), `FLOAT`/`DOUBLE`, `BIT`,
  collation, 전체 DATE/TIME family, `ENUM`/`SET`/`JSON`, 전체 LOB family(**타입 이질성**·**미지원 타입**
  시나리오 강제).
- **의도적으로 실패하는 행**: ~1.5 MiB LOB 값(**1 MiB quarantine/DLQ** 시나리오 강제), 격리된 테이블의
  `TINYINT(1)` = `2`(**시끄러운 테이블 전체 실패** 시나리오 강제).

이 실행은 **무손실 핸드오프** 시나리오를 실제로 수행했습니다 — Full Load → 라이브 워크로드 → 워터마크부터
CDC → 수렴 → 권위 있는 PK 단위 대조. 실제 인프라에서 테스트할 가치가 바로 여기 있습니다: 초기 실행이
오프라인 스위트로는 잡을 수 없던 진짜 CDC 데이터 손실 버그(워터마크와 CDC 시작 사이 연속 블록 손실, DLQ
없음)를 드러냈고, 무음 실패하는 오프셋 시드와 Debezium schema-history 갭으로 추적해 **둘 다 수정**했습니다.
최종 실행의 목적은 그 수정이 유효함을 증명하는 것이었습니다.

### 검증 결과

수정 후 전체 실행을 재수행한 결과, **모든 clean 테이블이 정확히 대조됐고 — 설명되지 않는 불일치는 0건**
이었습니다:

| 테이블 | 소스 행 | 타깃 행 | 타깃 누락 | 타깃 잉여 | 판정 |
|---|---|---|---|---|---|
| `typetest_parent` | 3,906 | 3,906 | 0 | 0 | **MATCH ✓** |
| `typetest_child` | 3,785 | 3,785 | 0 | 0 | **MATCH ✓** |
| `typetest_lob` | 2,309 | 2,309 | 0 | 0 | **MATCH ✓** |

이것이 증명하는 바를 항목별로:

- **데이터 손실 0, 중복 0.** Full Load **와** 라이브 CDC 전체에서, PK 단위 대조가 모든 테이블에서
  **누락 0 / 잉여 0**을 확인 — 모든 소스 행이 정확히 한 번 도착했고, 모든 소스 삭제가 적용됐습니다. 이전의
  연속 갭(라운드당 50~70행)은 **0**으로 떨어졌습니다.
- **의도된 실패는 손실이 아니라 포착됐습니다.** ~1.5 MiB 초대형 LOB 행은 올바르게 **quarantine**(설계대로
  타깃에 부재, PK로 기록)됐고, 범위 초과 `TINYINT(1)` 행은 **시끄러운 테이블 전체 실패**를 유발 — 정확히
  안전한 동작입니다. 무엇도 조용히 버려지거나 조용히 손상되지 않았습니다.
- **수가 아니라 값이 일치했습니다.** Validation의 체크섬/대조가 실제 데이터(전체 이질적 타입 표면)를
  비교했으므로, 여기서 "MATCH"는 행이 *수만 같은* 게 아니라 *값이 같다*는 뜻입니다.
- **배포 자체도 통과했습니다.** Fargate 재배포가 `UPDATE_COMPLETE` + HTTP 200에 도달 — 일회성 로컬 설정이
  아니라 실제 출시 구성으로 만든 결과입니다.

**도입자를 위한 결론:** 모든 DSQL 차이를 — 실패하도록 만든 행까지 포함해 — 압박하도록 일부러 설계한
스키마에서, 마이그레이션은 **모든 clean 테이블 100% 일치, 설명되지 않는 불일치 0건**을 냈고, *실패해야 할*
행은 손실이 아니라 포착·보고됐습니다. 그리고 마이그레이션 끝에 **Validation을 직접 실행**하므로,
전환 전 *당신의* 데이터에 대해 동일한 증거 — 정확한 행 수, 체크섬, PK 단위 대조 — 를 얻습니다. 결과를
믿어달라는 게 아니라, 직접 재현하는 것입니다.

---

**다음:** [9. 쿼리 검증과 AI DBA →](09-query-validation.md)
