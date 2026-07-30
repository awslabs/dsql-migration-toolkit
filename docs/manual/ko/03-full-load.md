# 3. Full Load 동작 방식

_언어: [English](../en/03-full-load.md) | **한국어** | [日本語](../ja/03-full-load.md)_

> **이전:** [2. Evaluation과 Schema Conversion](02-evaluation-and-schema-conversion.md)

**Full Load**는 기존 행을 소스 MySQL에서 Aurora DSQL로 옮기는 도구 자체의 벌크 복사입니다. Debezium
스냅샷이 **아니라** 데이터를 스트리밍하며 DSQL 제약을 지키도록 만든 전용 로더입니다. 테이블을 선택한 뒤
**Data Migration** 단계에서 실행합니다.

> **머릿속 모델:** 로더는 소스를 PK 순서로 한 페이지씩 읽어, 작고 재시도 가능하며 **여러 번 적용해도
> 중복이 생기지 않는** 배치로 DSQL에 씁니다 — 테이블이 아무리 커도 메모리는 한정된 채로 유지되고, 중단되면
> 이어서 재개됩니다.

---

## 3.1 큰 그림

```
Source MySQL                         Aurora DSQL
  │  keyset 페이지 (PK > last, LIMIT)    ▲  배치 INSERT ... ON CONFLICT
  │  스트리밍 server-side cursor          │  (≤3000행, ≤8 MiB, OCC 재시도)
  └──────────►  타입 변환  ──────────────┘
       (읽기 전용, 일관된 스냅샷)
```

1. **워터마크 캡처**(소스 히스토리의 일관된 지점).
2. PK 페이지 단위로 **행 스트리밍 추출**.
3. 읽어 들인 각 값을 DSQL 형태로 **변환**.
4. 한정된 배치(재적용해도 중복 없음)로 DSQL에 **동시 적재**.
5. 이후 보조 **인덱스** 빌드.

소스는 일관된 스냅샷 안에서 **읽기 전용**으로 읽히며, 로더는 절대 수정하지 않습니다.

---

## 3.2 스트리밍 export — 어떤 크기든 한정된 메모리

로더는 `OFFSET`이 아니라 **기본 키 keyset 페이지네이션**으로 읽습니다:

```sql
SELECT <cols> FROM <table>
WHERE pk > :last           -- (복합 PK는 행-값 튜플 비교 사용)
ORDER BY pk
LIMIT :batch_size          -- DEFAULT_BATCH_SIZE = 1000
```

각 페이지의 마지막 행 PK로 `:last`를 전진시키며, 짧은 페이지가 끝을 알릴 때까지 반복합니다. 쿼리는
`START TRANSACTION WITH CONSISTENT SNAPSHOT`(InnoDB repeatable read) 안에서 **server-side/스트리밍
커서**로 실행되므로:

- 테이블 전체가 **RAM에 올라오지 않음** — 메모리가 한 페이지로 한정;
- 실행 중인 소스가 계속 바뀌어도 읽기는 **단일 일관 스냅샷**으로 유지됨.

**기본 키가 필요합니다.** PK 없는 테이블은 keyset 페이지네이션이 불가능해 거부됩니다(Evaluation에서도
`UNSUPPORTED`로 플래그됨 — DSQL도 PK를 요구). 단일·복합 PK 모두 지원합니다 — 복합 키는 단순히 허용되는 수준을 넘어, 단조 증가 키에서 발생하는 쓰기 핫 파티션에 대한 이 도구의 권장 해결책입니다([7장 §7.1](07-performance-and-tuning.md#기본-키-전략--핫-파티션-회피) 참조).

> **바쁜 프로덕션 소스의 읽기 부하가 걱정되나요?** 여러 테이블을 한꺼번에 로드해도 하나의 레버(테이블
> 병렬수)로 제한되며, 소스를 건강하게 유지하는 구체적 단계가 있습니다 —
> [7장 §7.3 — 소스 부하 최소화](07-performance-and-tuning.md#73-소스-부하-최소화) 참고.

---

## 3.3 행을 읽어 들이면서 타입 변환

행을 한 건씩 읽어 들이는 동안 각 값이 DSQL 저장 형태로 변환됩니다. 이는 Schema Conversion 매핑을
그대로 따르므로 컬럼 타입과 실제 값이 서로 어긋나지 않습니다. MySQL 사용자가 알아둘 예:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → 정수(소스 바이트에서 디코딩).
- `DATETIME` → UTC로 정규화된 `timestamp`; `TIMESTAMP` → `timestamptz`.
- `BLOB`/`BINARY`/`VARBINARY` 계열 → `bytea`.

전체 매핑(및 "외래 키 없음" 같은 DSQL 제약 처리)은
[2장 §2.3](02-evaluation-and-schema-conversion.md#23-mysql--dsql-타입과-제약-처리-참조)과 Schema Conversion
단계에 있습니다.

---

## 3.4 배치·재적용 안전·한정 병렬 적재

읽어 들인 행은 **다중 행 `INSERT ... ON CONFLICT`** 문으로 모여, 한정된 DSQL 커넥션 풀에서 **동시에**
적재됩니다. 여기의 모든 한도는 실제 DSQL 제약이며 도구가 대신 처리합니다:

| 제약 | 로더 동작 | 기본값 / 상한 |
|---|---|---|
| 트랜잭션당 ≤ 3000행 | 배치 행 수 제한 | `DEFAULT_BATCH_ROWS = 2000`, 하드 상한 `3000` |
| 바인드 파라미터 한도 | 한도에 맞게 배치 크기 제한 | `MAX_STATEMENT_PARAMETERS = 65535` (`65535 // 컬럼수`) |
| write-txn 크기 | 넓은 행 분할 | `MAX_BATCH_BYTES = 8 MiB` |
| 낙관적 동시성(`40001`) | 백오프+지터로 배치 재시도 | 최대 10회 |
| 한정된 자원 사용 | 동시 배치 제한 | 테이블당 `DEFAULT_PARALLELISM = 8`, 동시 `4`개 테이블 |

**다시 돌려도 안전합니다(idempotent).** 적재는 `INSERT ... ON CONFLICT`를 쓰므로 같은 배치를 다시
적재해도 중복이 생기지 않습니다. CDC가 함께 돌 때는 로더가 "기존 건너뛰기" 모드를 써 **더 최신 CDC 적용
행을 덮어쓰지 않습니다.**

**인덱스는 마지막.** 보조 인덱스는 모든 데이터 배치 성공 **후** `CREATE INDEX ASYNC`로, 각자 자체
단일 DDL 트랜잭션에서 생성됩니다(DSQL은 트랜잭션당 한 개 DDL, 인덱스를 비동기 빌드).

---

## 3.5 워터마크 — CDC로의 다리

적재 전 도구는 **워터마크**를 캡처합니다: 같은 일관 스냅샷 트랜잭션 안에서 기록되는 소스 바이너리 로그의
일관된 지점. 내용:

- **binlog 파일 + 위치**,
- **GTID 셋**(`@@GLOBAL.gtid_executed`)과 `server_uuid`,
- **UTC 스냅샷 타임스탬프**,
- 테이블별 **근사** 행 수(스캔 없는 `information_schema` 추정 — 단순 카운트를 위해 소스를 풀스캔하지
  않음).

워터마크는 이후 **무손실 CDC 핸드오프**를 가능하게 합니다: CDC가 스냅샷이 끝난 바로 그 지점부터 변경을
스트리밍 — 갭도 중복도 없음([4장](04-cdc-and-dsql-constraints.md) 참조). RDS 설정이 `SHOW MASTER
STATUS`를 제한하면 binlog/GTID 필드가 비게 되며, 그 경우 CDC 핸드오프만 못 받을 뿐 Full Load는 정상
동작합니다.

> 워터마크의 행 수는 **의도적으로 근사**입니다(소스 부담 최소화). 정확한 `COUNT(*)`와 체크섬은
> **Validation**([5장](05-validation.md))의 역할이지 Full Load가 아닙니다.

---

## 3.6 실패를 격리하는 방식 — 그리고 격리되지 않는 하나

이 부분은 잘 이해해 두세요. 두 실패 동작이 의도적으로 다릅니다.

### 행 단위 quarantine (테이블은 계속 적재)

적재 시 **DSQL이 특정 행을 거부**하면(SQLSTATE가 있는 에러 — 예: 1 MiB 초과 값, 제약 위반), 로더는
테이블을 실패 처리하지 **않습니다.** 배치를 단일 문제 행까지 이진 분할해 그 행을 **quarantine**하고(그
**기본 키와 사유만 기록 — 값은 절대 기록 안 함**) 나머지를 적재합니다. quarantine된 행은 다운로드
가능한 에러 로그에 나타납니다.

### 테이블 전체 실패(table-fatal)

값이 **무손실로 변환될 수 없을 때** — 대표 사례는 DSQL `boolean`에 매핑된 `TINYINT(1)` 컬럼이 `{0,1}`
밖의 값(예: `2`)을 가질 때 — exporter가 `ValueConversionError`를 던집니다. 여기엔 **SQLSTATE가
없으므로**(DSQL에 묻기도 전, 읽기/변환 중 발생) 행 단위 quarantine이 **아니라** 그 테이블 적재를
**시끄럽게** 멈춥니다. 의도된 동작입니다 — `2`를 `true`로 뭉개 데이터를 조용히 손상시키지 않습니다.
소스 데이터를 고치거나(또는 컬럼 제외) 다시 실행하세요.

> **"시끄러운" 실패가 "조용한" 실패보다 나은 이유:** DSQL `boolean`은 `2`를 표현할 수 없습니다.
> 도구는 조용히 틀린 값 대신 눈에 보이고 고칠 수 있는 실패를 선택합니다.

### 실행 수준 판정

행을 **하나라도** quarantine한 테이블은 **실패**(미완료)로 보고되며, 실행은 성공으로 보고되지
않습니다 — 미완료 적재가 깨끗한 적재로 오인되지 않습니다. 다운로드 에러 로그가 어떤 PK가 왜
quarantine됐는지 정확히 나열합니다.

---

## 3.7 재개 가능성

행이 **keyset(PK) 순서**로 스트리밍되므로, 배치 *i*는 매 실행마다 동일한 PK 범위에 매핑됩니다. 그래서
배치는 **안정적이고 결정론적인 재개 단위**입니다:

- 완료된 배치는 `DONE`으로 기록;
- 중단/재시도는 미완료 범위만 다시 실행;
- `INSERT ... ON CONFLICT` 덕분에 몇 번을 다시 실행해도 중복이 생기지 않음.

"실패 테이블 재시도" 경로는 실패 테이블만 대기 상태로 리셋하고, **원래 워터마크를 재사용**하며, 완료된
테이블은 유지합니다 — 작업을 다시 하거나 CDC 핸드오프 지점을 잃지 않습니다.

> **명령줄에서 Full Load 실행 (선택).** 동일한 벌크 로더를 CLI 스크립트로도 쓸 수 있습니다 —
> `scripts/run_full_load.py`(먼저 계획 출력, 그다음 `--yes`; 선택적으로 `--clean`,
> CDC 워터마크 캡처용 `--watermark-out`) — 웹 UI 없이 자동화나 대용량 실행에 유용합니다. 자세한 내용은
> [`scripts/README.md`](../../../scripts/README.md) 참고. 소스는 읽기 전용, 로드는 UI와 똑같이
> 멱등입니다.

---

## 3.8 멀티프로세스 병렬화 (GIL 우회)

Python의 GIL(Global Interpreter Lock)은 단일 프로세스를 CPU 1코어로 제한합니다.
Full Load의 행별 타입 변환과 배치 조립은 CPU-bound Python이므로, 이전에는 8 vCPU
Fargate 태스크에서도 ~15,000 rows/s가 한계였습니다.

v0.1.68부터 로더는 **`ProcessPoolExecutor`**를 사용합니다 — 각 테이블(또는 shard)이
자체 OS 프로세스에서 자체 GIL + CPU 코어로 실행됩니다.

### 동작 방식

- **소형 테이블** (shard 불가 또는 행 수 미달): 테이블당 1 worker 프로세스.
- **대형 테이블** (단일 정수 PK): 자동으로 K개 PK range shard로 분할, 각각 별도
  worker 프로세스에서 로드.
- **모든 work unit이 하나의 bounded pool 공유** — `table_parallelism`이 동시 worker 수를 제어.

```
ProcessPoolExecutor(max_workers=table_parallelism)
  ├─ customers (소형)           → 1 worker
  ├─ orders (9M, int PK)       → 2 shard workers
  ├─ payments (9M, int PK)     → 2 shard workers
  └─ order_items (33.6M, int PK) → 3 shard workers
                                    ───────────────
                                    8 workers = 8 cores
```

### 튜닝

대규모 마이그레이션에서는 worker 수를 태스크의 vCPU 수에 맞추세요 — pool slot을
whole-table worker와 shard worker에 배분하는 일은 로더가 알아서 합니다. 관련 설정
(`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`)과 각각의 한도, 그리고 소스
부하와의 관계는 [7장 §7.2 — 병렬도 튜닝](07-performance-and-tuning.md#72-병렬수-튜닝)에
모여 있습니다. 이 설계가 실제로 낸 처리량(그리고 이것이 대체한 ThreadPool 기준선)은
[Appendix: 성능 테스트 결과](12-performance-test-results.md)에 있습니다.

---

**다음:** [4. CDC와 DSQL 제약 →](04-cdc-and-dsql-constraints.md)
