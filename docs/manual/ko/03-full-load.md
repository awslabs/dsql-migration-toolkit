# 3. Full Load 동작 방식

_언어: [English](../en/03-full-load.md) | **한국어** | [日本語](../ja/03-full-load.md)_

> **이전:** [2. Evaluation과 Schema Conversion](02-evaluation-and-schema-conversion.md)
>
> **테이블이나 컬럼을 빼려면** [13. 테이블과 컬럼 제외](13-excluding-tables-and-columns.md)를 보세요. 제외한 컬럼은 Aurora DSQL에 NULL로 로드됩니다.

**Full Load**는 기존 행을 소스 데이터베이스 — RDS / Aurora **MySQL** 또는 RDS / Aurora **PostgreSQL** —
에서 Aurora DSQL로 옮기는 도구 자체의 벌크 복사입니다. Debezium
스냅샷이 **아니라** 데이터를 스트리밍하며 DSQL 제약을 지키도록 만든 전용 로더입니다. 테이블을 선택한 뒤
**Data Migration** 단계에서 실행합니다.

> **머릿속 모델:** 로더는 소스를 PK 순서로 한 페이지씩 읽어, 작고 재시도 가능하며 **여러 번 적용해도
> 중복이 생기지 않는** 배치로 DSQL에 씁니다 — 테이블이 아무리 커도 메모리는 한정된 채로 유지되고, 중단되면
> 이어서 재개됩니다.

---

## 3.1 큰 그림

```
Source (MySQL / PostgreSQL)          Aurora DSQL
  │  keyset 페이지 (PK > last, LIMIT)    ▲  멱등 배치 INSERT
  │  스트리밍 server-side cursor          │  (≤3000행, ≤8 MiB, OCC 재시도)
  └──────────►  타입 변환  ──────────────┘
       (읽기 전용, 일관된 스냅샷)
```

1. **워터마크 캡처**(소스 히스토리의 일관된 지점).
2. PK 페이지 단위로 **행 스트리밍 추출**.
3. 읽어 들인 각 값을 DSQL 형태로 **변환**.
4. 한정된 배치(재적용해도 중복 없음)로 DSQL에 **동시 적재**.
5. 이후 보조 **인덱스** 빌드.

소스는 일관된 스냅샷 안에서 **읽기 전용**으로 읽히며, 로더는 절대 수정하지 않습니다. MySQL 소스는 —
그리고 PostgreSQL의 Full Load 전용 마이그레이션도 — 절대 쓰이지 않습니다. 유일한 예외는 **PostgreSQL
CDC**입니다: 워터마크 시점(§3.5)에 도구가 논리 복제 슬롯과, 정확히 마이그레이션 대상 테이블로만 한정된
publication(`FOR TABLE ONLY`: 테이블의 상속 자식 테이블은 함께 추가되지 않고 — Full Load도 그 테이블 자신의
행만 읽음(`FROM ONLY`, [§6.1](06-limitations.md#61-aurora-dsql-기능-한계-스키마가-이에-맞아야-함) 참고) —
파티션된 테이블의 파티션은 추가됨)을 생성하고 teardown 시 삭제합니다. 또한 재키잉된 테이블에
`REPLICA IDENTITY FULL`을 설정하고,
publication의 `publish_via_partition_root`를 바로잡습니다: Full Load에서는 재사용하는 publication을(슬롯을
만들기 전에), Start CDC 때마다(publication이 아직 없으면 — 처음 하는 CDC only Start나 Full Load 전용 실행
이후의 Start처럼 — 그 설정으로 빈 publication을 만들고, 커넥터가 테이블을 추가합니다), 그리고 Full Load
이후 선택한 테이블을 추가하기 전에 바로잡습니다. 그런 테이블(Full load + CDC, CDC를 시작하기 전)은 복사
직전에 publication에 추가되고, 도구는 그 시점에 열려 있던 트랜잭션을 기다리기 위해 트랜잭션 ID를 하나
받습니다. 모두 autocommit이고, 소규모 allowlist로 제한되며, 감사됩니다. 이것이 도구가 어떤 소스에든
수행하는 유일한 쓰기입니다. 워터마크 시점에 publication을 만들거나 거기에 테이블을
추가하기 전에, 추가할 모든 테이블에 사용 가능한 `REPLICA IDENTITY`가 있는지(그리고
PostgreSQL 18에서는 재키잉된 테이블에 생성 컬럼이 없는지 — 그 테이블에 필요한 `FULL`이 깨지므로) 확인하며,
없으면 실행은 소스에서 아무것도 바꾸기 전에 멈추고 그 테이블과 원인에 맞는 해결 방법을 알려 줍니다. 같은
이름의 복제 슬롯이 사용 중이거나, 다른 데이터베이스에 속하거나, 논리 `pgoutput` 슬롯이 아니면(예: 같은 CDC
스택 이름을 쓰는 다른 마이그레이션의 슬롯) 쓰기 전에 멈추며, 이 데이터베이스의 비활성 슬롯은 이 스택의
이전 슬롯으로 보고 교체합니다. 선택한 테이블 중 파티션된 테이블이 있는데 Full Load의
`publish_via_partition_root` 바로잡기가 실패하면, 실행은 슬롯을 만들기 전에 멈추고 publication 소유자로
실행할 `ALTER PUBLICATION` 구문을 알려 줍니다. Start CDC에서는 바로잡기가 실패하거나 세션에 소스
비밀번호가 없어 실행되지 못해도 Start가 멈추지 않으며, 그 로그가 같은 구문을 알려 줍니다. Full Load 이후
선택한 테이블도 추가하기 전에 같은 확인을 거치며, 없으면 그 테이블은 로드되지 않고 그 행에 이유가 표시됩니다
([13.7](13-excluding-tables-and-columns.md)).

---

## 3.2 스트리밍 export — 어떤 크기든 한정된 메모리

로더는 `OFFSET`이 아니라 **기본 키 keyset 페이지네이션**으로 읽습니다:

```sql
SELECT <cols> FROM <table>
WHERE pk > :last           -- 복합 PK: 소스의 PK 인덱스가 처리하는 커서(아래 참조)
ORDER BY pk
LIMIT :batch_size          -- DEFAULT_BATCH_SIZE = 5000
```

각 페이지의 마지막 행 PK로 `:last`를 전진시키며, 짧은 페이지가 끝을 알릴 때까지 반복합니다. 쿼리는
단일 repeatable-read 스냅샷 트랜잭션(MySQL: InnoDB의 `START TRANSACTION WITH CONSISTENT SNAPSHOT`;
PostgreSQL: `REPEATABLE READ` 트랜잭션) 안에서 **server-side/스트리밍 커서**로 실행되므로:

- 테이블 전체가 **RAM에 올라오지 않음** — 메모리가 한 페이지로 한정;
- 실행 중인 소스가 계속 바뀌어도 읽기는 **단일 일관 스냅샷**으로 유지됨.

**커서를 쓰는 방식.** 커서는 기본 키 인덱스의 순서를 따르며, 그 인덱스가 범위로 처리하는 형태로
작성됩니다. MySQL의 복합 키는 명시적 사전식 OR 확장 `(k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR …`을
씁니다(MySQL 5.7은 행 값 비교를 인덱스로 처리하지 못함). PostgreSQL은 행 값 비교
`(k0, k1, …) > (:last_0, :last_1, …)`을 쓰며, 이는 기본 키와 각 파티션의 기본 키에 대한 인덱스 조건이
됩니다(PostgreSQL에서 OR 확장은 필터일 뿐이라 페이지마다 키를 처음부터 다시 읽게 됨). MySQL `ENUM`·`SET`
키 컬럼은 레이블이 아니라 MySQL이 정렬에 쓰는 숫자 — 멤버 인덱스 또는 비트마스크 — 로 페이지를 넘깁니다:
`ENUM`은 커서의 멤버 뒤에 선언된 멤버들로(`col IN (…)`, 인덱스가 처리), `SET`은 비트마스크로
(`col + 0 > n`, 인덱스가 처리하지 못하므로 `SET` 컬럼으로 시작하는 키는 페이지마다 키를 처음부터 다시 읽음).
멤버는 Evaluation이 기록한 것이므로, 소스에서 컬럼 정의가 바뀌면 먼저 Evaluation과 Schema Conversion을
다시 실행하세요 — 읽기가 키 순서에 놓을 수 없는 키 값을 만나면 테이블을 멈추고 그렇게 알립니다.

**기본 키가 필요합니다.** PK 없는 테이블은 keyset 페이지네이션이 불가능해 거부됩니다(Evaluation에서도
`UNSUPPORTED`로 플래그됨 — DSQL도 PK를 요구). 단일·복합 PK 모두 지원합니다 — 복합 키는 단순히 허용되는 수준을 넘어, 단조 증가 키에서 발생하는 쓰기 핫 파티션에 대한 이 도구의 권장 해결책입니다([7장 §7.1](07-performance-and-tuning.md#기본-키-전략--핫-파티션-회피) 참조).

> **바쁜 프로덕션 소스의 읽기 부하가 걱정되나요?** 여러 테이블을 한꺼번에 로드해도 하나의 레버(테이블
> 병렬수)로 제한되며, 소스를 건강하게 유지하는 구체적 단계가 있습니다 —
> [7장 §7.3 — 소스 부하 최소화](07-performance-and-tuning.md#73-소스-부하-최소화) 참고.

---

## 3.3 행을 읽어 들이면서 타입 변환

행을 한 건씩 읽어 들이는 동안 각 값이 DSQL 저장 형태로 변환됩니다. 이는 Schema Conversion 매핑을
그대로 따르므로 컬럼 타입과 실제 값이 서로 어긋나지 않습니다. **MySQL 소스**가 만들어 내는 예:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → 정수(소스 바이트에서 디코딩).
- `DATETIME` → UTC로 정규화된 `timestamp`; `TIMESTAMP` → `timestamptz`.
- `BLOB`/`BINARY`/`VARBINARY` 계열 → `bytea`.

**PostgreSQL 소스**에서는 Full Load 중 MySQL 방식의 값 변환이 **없습니다**: 양쪽 끝이 모두
PostgreSQL-16 wire(양쪽 모두 psycopg)이므로 DSQL이 지원하는 값은 그대로 통과합니다. PostgreSQL 타입
처리 — 미지원 타입은 remodel 대상으로 플래그, `numeric` 정밀도 클램핑, `DEFAULT`/serial/identity 미방출 —
는 즉석 값 변환이 아니라 **Schema Conversion**에서 일어납니다.

전체 매핑(및 "값당 1 MiB 한도" 같은 DSQL 제약 처리)은
[2장 §2.3](02-evaluation-and-schema-conversion.md#23-mysql--dsql-타입과-제약-처리-참조)과 Schema Conversion
단계에 있습니다.

---

## 3.4 배치·재적용 안전·한정 병렬 적재

읽어 들인 행은 **멱등 배치 INSERT**로 모여, 한정된 DSQL 커넥션 풀에서 **동시에**
적재됩니다. 여기의 모든 한도는 실제 DSQL 제약이며 도구가 대신 처리합니다:

| 제약 | 로더 동작 | 기본값 / 상한 |
|---|---|---|
| 트랜잭션당 ≤ 3000행 | 배치 행 수 제한 | `DEFAULT_BATCH_ROWS = 2000`, 하드 상한 `3000` |
| 바인드 파라미터 한도 | 한도에 맞게 배치 크기 제한 | `MAX_STATEMENT_PARAMETERS = 65535` (`65535 // 컬럼수`) |
| write-txn 크기 | 넓은 행 분할 | `MAX_BATCH_BYTES = 8 MiB` |
| 낙관적 동시성(`40001`) | 백오프+지터로 배치 재시도 — 일시적 연결 끊김과 Aurora DSQL의 일시적 `XX000`(server unavailable)에도 적용 | 최대 20회 |
| 한정된 자원 사용 | 동시 배치 제한 | 테이블당 `DEFAULT_PARALLELISM = 8`, 동시 `4`개 테이블 |

**다시 돌려도 안전합니다(idempotent).** 클린 적재는 새로 만든 빈 타깃에 평범한 다중 행 `INSERT`로 넣고,
데이터가 이미 있을 수 있으면(재시도, 또는 이미 행이 있는 테이블에 append) **"기존 건너뛰기"** 모드로
전환합니다 — insert하되 기존 키는 덮어쓰지 않고 건너뜀 — 그래서 재실행해도 중복이 안 생기고 **더 최신 CDC
적용 행을 덮어쓰지 않습니다.** (DSQL의 *다중 행* `ON CONFLICT DO NOTHING`은 비충돌 행을 조용히 누락시키는
것이 관찰돼 의도적으로 피합니다.) 다만 CDC가 방금 삭제한 행과 처음부터 없던 행은 구별하지 못하므로, CDC가
타깃으로 스트리밍하는 동안에는 Full Load가 실행되지 않고 Start CDC는 실행 중인 Full Load를 기다립니다
([§3.5](#35-워터마크--cdc로의-다리)): 그렇지 않으면 테이블을 복사하는 동안 소스가 삭제한 행이 다시
쓰입니다.

**완전 적재 후 identity 시퀀스를 전진시킵니다.** 테이블 키를 캐시드 identity 컬럼
(`GENERATED BY DEFAULT AS IDENTITY`)으로 변환했다면, 적재 완료 시 `ALTER TABLE … RESTART WITH max(pk)+1`을
실행해 **컷오버 후 첫 insert가** 마이그레이션된 키와 **충돌하지 않게** 합니다. **완전(또는 gap 수용) 적재에서만**
실행되고 부분/실패 적재에서는 실행되지 않습니다; 평범한 정수 키는 시퀀스가 없어 건드리지 않습니다.

**인덱스는 마지막.** 보조 인덱스는 모든 데이터 배치 성공 **후** `CREATE INDEX ASYNC`로, 각자 자체
단일 DDL 트랜잭션에서 생성됩니다(DSQL은 트랜잭션당 한 개 DDL, 인덱스를 비동기 빌드). Aurora DSQL이
`XX000`("server unavailable")으로 응답했거나 실행 중에 연결이 끊긴 인덱스 구문도 같은 방식으로 재시도합니다. 재시도에서 같은
이름의 인덱스가 이미 있으면 생성된 것으로 세지 않습니다: Aurora DSQL에서 인덱스 이름은 스키마
단위라 다른 테이블의 인덱스일 수도 있기 때문입니다. Full Load 화면과 에러 로그에는 없는 인덱스가
아니라 **not confirmed**(확인되지 않음)로, 앞선 시도가 만들었을 수 있다는 안내와 함께 표시되므로,
인덱스를 추가하기 전에 타깃을 확인하세요. 화면의 알림은 `UNIQUE` 인덱스가 생성되지 않은 것이 확실할 때만
제약이 빠졌다고 말하며, 없는 인덱스와 확인되지 않은 인덱스가 섞여 있으면 둘을 모두 밝힙니다.

---

## 3.5 워터마크 — CDC로의 다리

적재 전 도구는 **워터마크**를 캡처합니다: 같은 일관 스냅샷 트랜잭션 안에서 기록되는 소스 변경
스트림의 일관된 지점입니다. 모든 워터마크는 **UTC 스냅샷 타임스탬프**와 테이블별 **근사** 행 수(스캔
없는 `information_schema` 추정 — 단순 카운트를 위해 소스를 풀스캔하지 않음)를 담습니다. 엔진별 좌표는
다릅니다:

- **MySQL** — **binlog 파일 + 위치**, 그리고 **GTID 셋**(`@@GLOBAL.gtid_executed`), 소스의
  **`gtid_mode`**, `server_uuid`. 이들은 `SHOW BINARY LOG STATUS`(MySQL 8.2+/8.4, `SHOW MASTER STATUS`가
  제거된 버전)로 읽고, 그 이하(≤ 8.0.x)에서는 `SHOW MASTER STATUS`로 폴백합니다. `gtid_mode=ON`인
  소스에서는 CDC가 워터마크의 GTID 셋 다음부터 시작하고, 그 밖에는 그 파일 + 위치부터 시작합니다.
  워터마크는 위치를 읽기 직전에 마이그레이션 대상 테이블마다 컬럼 정의의 짧은 지문도 기록하므로,
  Start CDC가 그 이후 테이블이 바뀌었는지 알 수 있습니다([6장](06-limitations.md) 참조).
- **PostgreSQL** — binlog 좌표가 아니라 **WAL LSN**(`Watermark.wal_lsn`). 일관성 지점에서 도구가
  **publication**(정확히 마이그레이션 대상 테이블에 대한 `FOR TABLE`, 절대 `FOR ALL TABLES` 아님)과
  `pgoutput` **논리 복제 슬롯**을 생성하고, 슬롯이 반환한 일관 LSN을 슬롯·publication 이름과 함께
  기록합니다. 이 LSN이 WAL을 고정하여 무손실 `snapshot.mode=never` CDC 핸드오프를 가능하게 합니다.
  PostgreSQL에는 GTID / `server_uuid` 개념이 없고 `SHOW BINARY LOG STATUS`도 없습니다 — LSN은 생성된
  슬롯에서 옵니다.

워터마크는 이후 **무손실 CDC 핸드오프**를 가능하게 합니다: CDC가 스냅샷이 끝난 바로 그 지점부터 변경을
스트리밍 — 갭도 중복도 없음([4장](04-cdc-and-dsql-constraints.md) 참조). CDC 사전 조건이 충족되지 않으면 —
MySQL에서는 바이너리 로깅이 꺼져 있거나 권한이 제한된 경우, PostgreSQL에서는 `wal_level`이 `logical`이
아니거나 슬롯/publication을 만들 수 없는 경우(복제 권한 누락 또는 standby 엔드포인트) — 좌표가 우아하게
비워지고 그 경우 CDC 핸드오프만 못 받을 뿐 Full Load는 정상 동작합니다.

> 워터마크의 행 수는 **의도적으로 근사**입니다(소스 부담 최소화). 정확한 `COUNT(*)`와 체크섬은
> **Validation**([5장](05-validation.md))의 역할이지 Full Load가 아닙니다.

### 새 시작 지점에는 새 CDC 토픽이 필요합니다

**새** 워터마크를 캡처하는 것은 CDC에 대한 결정이기도 하므로, CDC 스택이 **스트리밍한 적 있는**
**Full load + CDC** 마이그레이션 — Start 뒤에 멈췄거나, 커넥터가 그대로 있거나, CloudFormation이 Start를
롤백한 스택 — 에서는 새 워터마크를 가져가는 모든 작업이 **거부됩니다**: **Start Full Load**,
**Re-run Full Load**, **Re-run all tables**는 비활성화되고, 페이지 헤더의 **Run**은 아무것도 시작하지
않습니다. Stop CDC(그리고 롤백된 Start)는 싱크를 드레인하지 않고 제거하므로, 그 싱크의 컨슈머 그룹은 새
스냅샷보다 오래된 변경을 다시 적재한 테이블 위에 다시 적용하고, 그동안 소스는 이전 정지 지점과 새 스냅샷
사이의 변경을 건너뜁니다 — 조용히, CHECKSUM 검증만이 그것을 드러냅니다. 그래서 알림은 대신 경로를 알려 줍니다: **Delete CDC infrastructure** 후 다시
배포(새 스택은 빈 토픽으로 시작), 그다음 **Re-run Full Load**를 **Drop & reload**로(PostgreSQL에서는 복제
슬롯을 자신의 스냅샷 지점에 다시 만듦), 그다음 **Start CDC**.

- **페이지 헤더의 Run도 같은 방식으로 막힙니다.** Start / Re-run Full Load와 똑같이 새 시작 지점을
  가져가기 때문입니다. 스트리밍한 적 있는 스택에서는 아무것도 시작하지 않습니다: 경고 토스트가 같은 경로를
  알려 주고, 작업은 만들어지지 않고, 워터마크도 캡처되지 않으며(PostgreSQL에서는 복제 슬롯이 교체되지
  않음), 단계 상태와 이전 Full Load 기록도 그대로 남습니다. 아직 **스트리밍 중인** 스택도 여기에
  포함됩니다.
- **CDC가 살아 있는 동안**에는 Full Load 작업에 자체 게이트가 먼저 걸립니다: *Stop CDC first (CDC step →
  Stop CDC), then Delete CDC infrastructure and deploy it again before a new Full Load.* 원래 시작 지점을
  유지하는 **Retry unfinished tables**, **Reload specific tables**, 테이블별 **Reload**도 비활성화되며,
  그중 하나를 위해 이미 열려 있는 확인 대화 상자는 확인할 수 없습니다. 이들은 대신 먼저 Stop CDC를 하고 그
  작업을 실행한 뒤 Start CDC를 다시 하라고 알려 줍니다: 그러면 CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지
  않은 변경을 CDC 토픽이 아직 보존하는 동안(MSK에서의 보존 기간)만 빈틈이 없습니다. 스트리밍 중인 싱크
  옆에서의 적재는 타깃에 없는 행만 insert하므로, 테이블을 복사하는 동안 소스가 삭제한 행이 CDC가 그 삭제를
  적용한 뒤에 Aurora DSQL에 도착해 그대로 남을 수 있습니다. COUNT나 CHECKSUM 검증만 보여 주는 남는
  행입니다. 같은 이유로 Start CDC가 아직 진행 중인 동안에는 페이지 헤더의 **Run**도 아무것도 시작하지 않고,
  Full Load가 실행되는 동안에는 Start CDC가 기다립니다
  ([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)).
- **이 세션에서 CDC 상태를 아직 읽지 못했으면**(화면을 막 열었거나 앱을 재시작한 직후), 또는 읽을 수
  없었으면 새 시작 지점은 추측하지 않고 **기다립니다**: 알림이 그렇게 말하고 CDC 단계의
  **Re-check CDC state**를 알려 줍니다(읽기에 실패했다면 먼저 AWS 접근 권한을 고치세요).
- **막히지 않는 것:** **Retry unfinished tables**, **Reload specific tables**, 테이블별 **Reload**,
  **Change & reload**는 모두 **원래** 시작 지점을 유지하므로 CDC가 멈춘 곳에서 이어 가고 — Stop CDC 후의
  모든 재시작처럼, 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만 빈틈이 없음
  ([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)) — 다시 적재한 테이블은
  수렴합니다. Full Load 단계는 **Full load only**와 **CDC only** 마이그레이션을 막지 않지만, 그 유형으로
  바꿔도 이 게이트를 우회할 수는 없습니다(다음 항목).
- **Start CDC도 더 새 시작 지점을 거부합니다(MySQL).** MySQL 소스에서 Start CDC는 스트리밍한 적 있는 CDC
  스택을 마이그레이션 유형과 상관없이, 스택이 마지막으로 시작한 지점보다 새 시작 지점으로(또는 멈춘 스택의
  마지막 Start가 시작 지점을 주지 않았다면 어떤 시작 지점으로도) 앞당기는 것을 거부합니다: 예를 들어 Full load +
  CDC에서 **Full load only**로 바꾼 뒤 Re-run Full Load가 잡은 워터마크나, 더 뒤의 **Manual** 지점입니다.
  시작 지점 카드와 Start CDC 카드가 *This CDC stack cannot start from a newer start point*와 함께 경로 —
  위의 경로, CDC only에서는 **Delete CDC infrastructure** 후 다시 배포한 뒤 새 스택에서 **Start CDC** — 를
  보여 주고 Start CDC는 비활성화됩니다. Start CDC 대화 상자, Start나 Retry CDC 클릭, 그리고 무엇이든
  바꾸기 전에 그때 읽은 스택으로 판단하는 Start CDC 작업(*Start CDC blocked: This CDC stack has
  streamed, so Start CDC does not move it forward to a newer start point …*)도 거부합니다. 거부된 Manual
  지점은 시작 지점을 **Automatic**으로 되돌리면 스트림이 이어지는 경우 그렇다고도 알려 줍니다. 그 지점과
  같거나 이전인 시작 지점, 또는 시작 지점이 없는 Start는 스트림을 이어 가며 거부되지 않습니다.
  CloudFormation이 롤백한 첫 Start 뒤에는 롤백이 스택의 파라미터를 되돌리므로 파라미터에는 그 Start가 준
  시작 지점이 더 이상 없지만, 스택의 CloudFormation 이벤트에는 아직 남아 있습니다(오프셋 시더의 속성):
  앱은 다음 Start를 자신이 기록한 그 지점과 비교하고, 앱을 재시작한 뒤에는(또는 다른 세션에서 한
  Start라면) 그 이벤트에서 읽은 그 지점과 비교합니다. 그보다 뒤의 시작 지점은 거부되고, 같은 시작
  지점에서 다시 시작하는 것은 거부되지 않으며, 그보다 이전의 시작 지점은 Start CDC를 클릭할 때, 또는
  이 세션이 그 지점을 아직 읽지 않았을 때는 Start CDC 작업에서 거부됩니다(*This start point cannot be
  applied*: 그 오프셋은 시드된 채로 남고, 시더는 되감지 않습니다). 이벤트에서도 그 지점을 얻지 못하면
  — 예를 들어 이벤트를 읽을 수 없거나, 그 Start가 오프셋 시더 Lambda 대신 앱 안에서 Kafka를
  시드했다면(단일 EC2 호스트가 그렇게 합니다,
  [§1.4](01-setup.md#14-단일-ec2-호스트에서-실행-소스에서)) — 어떤 시작 지점도 그 지점과 비교할 수
  없으므로, 그 스택에서는 같은 시작 지점을 포함해 모든 시작 지점이 같은 경로와 함께 거부됩니다(*This CDC
  stack's last start point is not known*: *CloudFormation rolled back this stack's last Start, and
  the start offset that Start offered is no longer known …*). CDC only 경로는 새 스택을 Full Load의
  워터마크에서 시작하므로, 거부된 지점이 그 워터마크(Automatic)이면 거부가 그 경로의 조건을 덧붙입니다:
  그 Full Load가 타깃에 이미 있던 행에 append했다면 그 Start 전에 **Drop & reload**로 다시 실행하세요.
  CDC only에는 Full Load 단계가 없으므로 그 실행 동안 마이그레이션 유형을 Full load only로 바꾼 뒤 다시
  CDC only로 돌아옵니다(append는 그 행을 그대로 두고, 워터마크에서 시작한 CDC는 그 뒤의 변경만
  가져옵니다). 비활성화된 Start의 툴팁과 작업의 오류에서는 그 경로가 *Delete CDC infrastructure and deploy it
  again, then Start CDC (if the Full Load appended to rows already on the target, re-run it choosing
  'Drop & reload' first)*로 표시됩니다. 새 스택에서는 그 Start가 거부되지 않으며, 이 세션이 그
  Full Load가 앞선 복사본 위에 append했을 수 있음을 보여 주면 CDC 단계가 그 Start
  전에 그 조건을 알려 줍니다([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)).
  PostgreSQL에는 이런 거부가 없습니다: Start CDC는 Full Load의 워터마크와 상관없이 스택 자체의
  복제 슬롯에서 이어 가고, Full load only 실행은 그 슬롯을 건드리지 않으므로, 스택의 소스 커넥터가
  스트리밍했다면 CDC는 Stop CDC 후의 모든 재시작처럼(위) 멈춘 곳에서 이어 갑니다(그렇지 않으면 Debezium이
  먼저 선택한 모든 테이블을 스냅샷합니다).
- 클릭과 작업 시작 사이에 스택이 스트리밍을 시작하면, 엔진이 작업 스레드에서 아무것도 읽거나 쓰기 전에
  거부합니다: 그 실행은 *FullLoadRefusedError*와 같은 한 줄 이유로 실패로 기록됩니다.

---

## 3.6 실패를 격리하는 방식 — 그리고 격리되지 않는 하나

이 부분은 잘 이해해 두세요. 두 실패 동작이 의도적으로 다릅니다.

### 행 단위 quarantine (테이블은 계속 적재)

적재 시 **DSQL이 특정 행을 거부**하면(SQLSTATE가 있는 에러 — 예: 1 MiB 초과 값, 제약 위반), 로더는
테이블을 실패 처리하지 **않습니다.** 배치를 단일 문제 행까지 이진 분할해 그 행을 **quarantine**하고(그
**기본 키와 사유만 기록 — 값은 절대 기록 안 함**) 나머지를 적재합니다. quarantine된 행은 다운로드
가능한 에러 로그에 나타납니다.

Aurora DSQL의 일시적 상태는 거부가 아닙니다: `40001`, 끊긴 연결, `XX000`("server unavailable")은
배치 전체를 재시도하며(§3.4), 재시도를 다 써도 해결되지 않은 배치는 실패로 처리됩니다 — 테이블은
**Retry unfinished tables**로 다시 시도할 수 있고, 그 때문에 quarantine되는 행은 없습니다.

### 테이블 전체 실패(table-fatal)

값을 **조용히 바꾸지 않고는 쓸 수 없을 때** exporter가 `ValueConversionError`를 던집니다:

- `boolean`에 매핑된 `TINYINT(1)`이 `{0,1}` 밖의 값(예: `2`)을 가질 때(export 중 값이 변환되는 MySQL 소스만);
- 두 엔진 모두에서, `numeric(p,s)` 컬럼이 유지하는 것보다 소수 자릿수가 많은 값, 또는 정수 컬럼에 들어가는
  소수. 테이블을 읽을 때 한 번 소스 컬럼의 실제 타입으로 감지하므로, 타입이 맞는 테이블은 비용이 없습니다.
  Step 1 이후 소스 타입이 넓어졌거나 Schema Conversion에서 더 좁은 타입을 골랐을 때 생깁니다. 정밀도 없는
  PostgreSQL `numeric`을 정밀도 없는 `numeric` 타깃에 넣는 경우는 거부하지 않으며(Aurora DSQL의 문서화된
  `numeric(18,6)` 기본값), `numeric` 컬럼에 들어가는 `FLOAT`/`DOUBLE` 소스 값은 검사하지 않고 계속 반올림됩니다;
- 00:00:00–23:59:59.999999를 벗어난 MySQL `TIME`.

여기엔 **SQLSTATE가 없으므로**(DSQL에 묻기도 전, 읽기/변환 중 발생) 행 단위 quarantine이 **아니라** 그
테이블 적재를 **시끄럽게** 멈춥니다. 의도된 동작입니다 — `2`를 `true`로 뭉개거나 `1.2345`를 `1.23`으로
반올림해 데이터를 조용히 손상시키지 않습니다. 메시지에는 컬럼, 그 타입, 규칙만 적히며 값은 절대 적히지
않습니다. Schema Conversion에서 값을 담을 수 있는 타입을 컬럼에 지정하거나(소스 타입이 바뀌었다면 먼저 Step 1을
다시 실행한 뒤 **Replace**로 다시 적용), 소스 데이터를 고치거나 컬럼을 제외한 다음 그 테이블을 재시도하세요.

> **"시끄러운" 실패가 "조용한" 실패보다 나은 이유:** DSQL `boolean`은 `2`를 표현할 수 없습니다.
> 도구는 조용히 틀린 값 대신 눈에 보이고 고칠 수 있는 실패를 선택합니다.

### 실행 수준 판정

행을 **하나라도** quarantine한 테이블은 **실패**(미완료)로 보고되며, 실행은 성공으로 보고되지
않습니다 — 미완료 적재가 깨끗한 적재로 오인되지 않습니다 — **단, "격리된 행 수용하고 계속"을 명시적으로
선택하면** gap을 인지한 채 실행이 완료됩니다(Validation에는 여전히 표시). 재시도 가능한 테이블 실패는 항상
차단합니다. 다운로드 에러 로그가 어떤 PK가 왜 quarantine됐는지 정확히 나열합니다.

---

## 3.7 재개 가능성

행이 **keyset(PK) 순서**로 스트리밍되므로, 배치 *i*는 매 실행마다 동일한 PK 범위에 매핑됩니다. 그래서
배치는 **안정적이고 결정론적인 재개 단위**입니다:

- 완료된 배치는 `DONE`으로 기록;
- 중단/재시도는 미완료 범위만 다시 실행;
- `INSERT ... ON CONFLICT` 덕분에 배치를 다시 실행해도 중복이 생기지 않음.

**Retry unfinished tables** 경로는 미완료 테이블만 대기 상태로 리셋하고, **원래 워터마크를 재사용**하며,
완료된 테이블은 유지합니다 — 작업을 다시 하거나 CDC 핸드오프 지점을 잃지 않습니다.

로드 후에 선택한 테이블(Full load only는 테이블 집합을 바꿀 수 있습니다.
[13.7](13-excluding-tables-and-columns.md) 참고)도 같은 재시도가 로드하지만, 원래 워터마크는 그 테이블을
포함하지 않으므로 CDC 핸드오프는 원래 집합에만 유효합니다: Re-run Full Load가 현재 집합에 대한 새
워터마크를 잡을 때까지(또는 그 이후 선택한 테이블의 체크를 해제할 때까지) 마이그레이션 유형은 Full load only에 머뭅니다.
그 재실행은 **Drop & reload**로 하세요: 그러면 CDC가 그 워터마크에서 시작하는데, *Append*는 앞선 적재가
쓴 행을 그 뒤 소스의 UPDATE와 DELETE 없이 그대로 둡니다.

이 마이그레이션에서 CDC를 이미 시작한 경우가 아니라면, 테이블의 체크를 해제하는 것만으로는 유형이 잠기지 않습니다: CDC는 로드된 테이블 중 아직 선택된 테이블을 캡처합니다. Full load + CDC에서는 로드 후 CDC를 시작하기 전까지 테이블의 체크를 해제할 수 있습니다. 그 테이블은 Aurora DSQL에 그대로 남고, CDC가 캡처하지 않으며, 워터마크는 캡처되는 모든 테이블을 여전히 포함하고(핸드오프는 계속 gapless), 체크를 해제한 테이블에서만 생긴 실패는 더 이상 단계를 붙잡지 않습니다.

Full load + CDC에서는 CDC를 시작하기 전까지 로드 후에 테이블을 체크할 수도 있습니다. Retry unfinished tables가 [13.7](13-excluding-tables-and-columns.md)의 점검(CDC를 시작한 적이 없음, Aurora DSQL 테이블이 비어 있거나 drop 후 재적재됨, CDC가 그 테이블을 공백 없이 캡처할 수 있음: MySQL에서는 워터마크 이후 그 테이블의 정의가 바뀌지 않았고, PostgreSQL에서는 사용할 수 있는 replica identity가 있으며 복사 전에 CDC의 publication에 들어감)을 거친 뒤 원래 워터마크 아래에서 그 테이블을 로드합니다. 그러면 워터마크가 그 테이블을 포함하게 되고, CDC는 다른 테이블과 함께 그 워터마크에서 그 테이블을 캡처하므로, 그 테이블의 핸드오프도 gapless입니다. MySQL에서 Full Load가 시작 지점 정의를 기록하지 않은 테이블(이전 버전의 Full Load, 또는 로드 후에 변환된 테이블)은 대신 Re-run all tables가 로드하며, 이때 **Drop & reload**를 선택하세요: 모든 테이블에 대해 새 시작 지점을 잡는데, append는 이미 로드된 행을 그대로 두기 때문입니다. Full load only는 바뀌지 않습니다: 거기서의 로드는 워터마크를 절대 확장하지 않습니다.

> **명령줄에서 Full Load 실행 (선택).** 동일한 벌크 로더를 CLI 스크립트로도 쓸 수 있습니다 —
> `scripts/run_full_load.py`(먼저 계획 출력, 그다음 `--yes`; 선택적으로 `--clean`,
> CDC 워터마크 캡처용 `--watermark-out`) — 웹 UI 없이 자동화나 대용량 실행에 유용합니다. 자세한 내용은
> [`scripts/README.md`](../../../scripts/README.md) 참고. 소스는 읽기 전용(유일한 예외는 §3.1에서 설명한
> PostgreSQL-CDC 슬롯과 publication)이며, 로드는 UI와 똑같이 멱등입니다.

---

## 3.8 멀티프로세스 병렬화 (GIL 우회)

Python의 GIL(Global Interpreter Lock)은 단일 프로세스를 CPU 1코어로 제한합니다.
Full Load의 행별 타입 변환과 배치 조립은 CPU-bound Python이므로, 이전에는 8 vCPU
Fargate 태스크에서도 ~15,000 rows/s가 한계였습니다.

v0.1.68부터 로더는 **`ProcessPoolExecutor`**를 사용합니다 — 각 테이블(또는 shard)이
자체 OS 프로세스에서 자체 GIL + CPU 코어로 실행됩니다.

### 동작 방식

- **소형 테이블** (shard 불가 또는 행 수 미달): 테이블당 1 worker 프로세스.
- **대형 테이블** (선행 PK 컬럼이 정수인 경우 — 단일 정수 PK, 또는 `(tenant_id, id)`처럼
  선행 컬럼이 정수인 복합 PK): K개 PK range shard로 분할해 각각 별도 worker 프로세스에서
  소스의 겹치지 않는 구간을 로드. 이 reader-range 샤딩은 **기본적으로 꺼져 있으며**
  (`full_load_reader_shards=1`), `FULL_LOAD_READER_SHARDS` > 1로 켭니다. 또한 모든 shard가
  하나의 공유 스냅샷을 읽는 경우 — PostgreSQL 소스 — 에서만 동작합니다. MySQL 테이블은 항상
  리더 하나를 사용합니다: 그 shard들은 각자 자신의 스냅샷을 읽게 되는데, 이를 맞출 수 있는 것은
  스트리밍 중인 CDC 싱크 옆에서의 실행뿐이고, CDC가 스트리밍하는 동안에는 Full Load가 실행되지
  않습니다([§3.5](#35-워터마크--cdc로의-다리)).
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
(`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`, `FULL_LOAD_READER_SHARDS`)과 각각의 한도, 그리고 소스
부하와의 관계는 [7장 §7.2 — 병렬도 튜닝](07-performance-and-tuning.md#72-병렬수-튜닝)에
모여 있습니다. 이 설계가 실제로 낸 처리량(그리고 이것이 대체한 ThreadPool 기준선)은
[Appendix: 성능 테스트 결과](12-performance-test-results.md)에 있습니다.

---

**다음:** [4. CDC와 DSQL 제약 →](04-cdc-and-dsql-constraints.md)
