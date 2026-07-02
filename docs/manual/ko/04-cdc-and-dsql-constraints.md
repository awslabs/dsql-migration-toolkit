# 4. CDC 동작 방식과 DSQL 제약 처리

_언어: [English](../en/04-cdc-and-dsql-constraints.md) | **한국어**_

**CDC(Change Data Capture)**는 거의 무중단 전환을 위한 **선택적** 스트리밍 파이프라인입니다. Full
Load가 기존 행을 복사한 뒤, CDC는 소스의 모든 신규 insert/update/delete로 DSQL을 계속 최신 상태로
유지합니다 — 긴 중단 없이 최소 다운타임으로 전환할 수 있게 합니다.

CDC가 필요한 경우는 **대규모 또는 지속적** 마이그레이션뿐입니다. 짧은 동결이 허용되는 일회성
전환이라면 Full Load만으로 충분합니다.

---

## 4.1 파이프라인

```
Source MySQL ──binlog (ROW+GTID, 읽기 전용)──►  Debezium MySQL 소스 커넥터
                                                        │  변경 이벤트
                                                        ▼
                                                 Amazon MSK (Kafka)
                                          테이블별 토픽, PK로 키잉  + DLQ
                                                        │
                                                        ▼
                                       커스텀 DSQL 싱크 커넥터 (자체 Java 플러그인)
                                          IAM 토큰 · 재적용해도 안전한 upsert/delete
                                          문장 단위 OCC 재시도 · ≤3000행 배치
                                                        │
                                                        ▼
                                                  Aurora DSQL
```

- **Debezium MySQL 소스 커넥터**가 소스 바이너리 로그를 읽기 전용으로 읽어 변경 이벤트를 냅니다.
- **Amazon MSK(Kafka)**가 내구성 백본: **테이블당 토픽 1개**, 기본 키로 키잉(한 행의 모든 변경이 한
  파티션에 순서대로 유지), 그리고 DLQ 토픽.
- **커스텀 DSQL 싱크 커넥터**(이 프로젝트가 소유한 Java Kafka Connect 플러그인)가 변경을 DSQL에
  적용합니다. 두 커넥터 모두 **관리형 MSK Connect**에서 실행되며, 도구는 **자체 싱크 컴퓨트를 돌리지
  않고** 컨트롤 플레인 역할만 합니다(구성 작성, 시작 오프셋 시드, 모니터링).

왜 *커스텀* 싱크이고 표준 JDBC 싱크가 아닌가? 표준 JDBC 싱크는 낙관적 동시성 충돌(`SQLSTATE 40001`)을
**배치 단위로** 재시도해, 고경합 TB 규모 CDC에서 처리량이 붕괴합니다. 커스텀 싱크는 **문장 단위**로
재시도하고 DSQL의 단기 IAM 토큰, ≤3000행 배치, 재연결을 처리합니다(§4.4).

---

## 4.2 CDC는 스키마가 아니라 데이터를 복제 — 중요

CDC는 **행 수준 데이터 변경**(insert/update/delete)을 복제합니다. SQL 문이나 **DDL**은 복제하지
**않습니다.** 구체적으로:

- Debezium은 `include.schema.changes=false`로 실행되고, 싱크는 DML만 적용.
- 소스 **DDL**(`ALTER TABLE`, `CREATE`, `DROP` 등)은 DSQL로 **전파되지 않음.**

DSQL 타깃 스키마는 **Schema Conversion** 단계에서 고정됩니다. **CDC 중 소스 스키마를 바꾸면**,
동일 DDL을 DSQL에 직접(Schema Conversion으로) **먼저** 적용하세요. 그 전까지, 타깃 형태에 맞지 않는
행(예: 새 컬럼 참조)은 손실되지 않고 **DLQ**로 격리됩니다 — 조용히 사라지지 않습니다.

---

## 4.3 무손실 Full Load → CDC 핸드오프

벌크 적재와 스트림 사이에 **누락된 변경도 중복도 없음**을 보장하는 부분입니다.

1. Full Load가 스냅샷 지점에서 **워터마크**(binlog 위치 + GTID)를 캡처했습니다
   ([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)).
2. CDC를 시작하면 도구가 커넥터의 **시작 오프셋을 정확히 그 워터마크로 시드**합니다(소스 커넥터가
   시작하기 전에 in-VPC Lambda가 오프셋 레코드를 기록). 그래서 Debezium은 **스냅샷 이후 첫 변경**부터
   스트리밍 — "지금"부터가 아니며, 데이터를 다시 읽지도 않습니다.
3. 소스 커넥터는 **`snapshot.mode=recovery`**로 실행됩니다: 오프셋이 이미 시드돼 있으므로 Debezium은
   내부 **스키마 히스토리**를 **현재 소스 테이블**로부터 재구성한 뒤(binlog 이벤트를 디코딩하기 위해)
   **행 데이터는 다시 읽지 않고** 시드된 오프셋부터 재개합니다.

결과: 스냅샷과 "지금" 사이의 모든 변경이 정확히 한 번 적용됩니다. 싱크는 PK를 기준으로 적용하므로 같은
변경이 겹치거나 재시도돼도 **중복이 생기지 않습니다**.

> **워터마크 위치의 binlog가 CDC 시작 시점에 여전히 존재해야 합니다.** 이 핸드오프는 소스가 워터마크
> 위치의 바이너리 로그를 아직 삭제하지 않았을 때만 동작합니다. RDS/Aurora는 기본적으로 binlog를
> 공격적으로 삭제하고(Aurora MySQL은 24시간 보존), CDC 스택 배포에만 ~15~20분이 걸리므로 **시작 전에
> binlog 보존을 늘리세요** — [§1.1](01-setup.md#11-사전-요구사항) 참조. 해당 구간이 이미 사라졌다면 CDC가
> 무손실로 재개할 수 없고, 새 워터마크를 얻기 위해 Full Load를 다시 실행해야 합니다.

> **왜 단순 schema-only가 아니라 `recovery`인가?** 시드된 오프셋이 있으면 Debezium은 "재개" 경로를
> 타며 기존 스키마 히스토리 토픽을 기대합니다. `recovery`는 행을 다시 스냅샷하지 않고 그 히스토리를
> 라이브 DB에서 재구성하는 모드 — "데이터는 내가 이미 적재했으니 이 오프셋부터 재개만 해" 상황에 정확히
> 맞습니다.

---

## 4.4 싱크가 데이터 경로에서 DSQL 제약을 처리하는 방식

DSQL은 분산형·PostgreSQL 호환이라 싱크는 고전적 MySQL/JDBC 라이터처럼 동작할 수 없습니다. 제약별 처리:

| DSQL 제약 | 커스텀 싱크의 처리 |
|---|---|
| **IAM 토큰 인증(비밀번호 없음)** | 단기 IAM 토큰(admin 또는 standard)을 생성해 TLS 위 JDBC 비밀번호로 사용하고, 만료 전 **갱신**(15분 토큰, 2분 갱신 여유)해 장기 CDC가 만료 토큰에 멈추지 않게 함. |
| **낙관적 동시성(락 없음)** | `SQLSTATE 40001`에 대해 배치 전체가 아닌 **문장 단위**로 지수 백오프+지터로 재시도(최대 10회). 경합 시 처리량 차이의 핵심. |
| **트랜잭션당 ≤ 3000행** | ≤ 3000행 청크로 적용(기본 배치 1000), 청크당 한 번 `commit()`. |
| **문장 단위 UPDATE/재생 없음** | 모든 변경을 **PK 기준 upsert/delete**로 적용: insert/update는 `INSERT ... ON CONFLICT (pk) DO UPDATE`, delete는 `DELETE ... WHERE pk = ?`(Kafka tombstone 포함). 같은 이벤트를 다시 적용해도 안전(중복 없음). |
| **커넥션 끊김(idle close / 토큰 만료 / 워커 교체)** | Dead 커넥션이나 half-open(끊겼지만 살아있는 듯 보이는) 커넥션을 감지해 새 토큰으로 재연결하고 **같은 오프셋부터 다시 적용** — 같은 변경을 다시 적용해도 중복이 안 생기므로 안전하며, 레코드를 버리지 않음. 연결 오류는 일시적(transient) 오류로 보고 재시도하며, 손상된 행(poison row)으로 오인하지 않음. |

---

## 4.5 1 MiB 값당 한도, 그리고 DLQ

DSQL은 **약 1 MiB를 초과하는 단일 값**(`TEXT`/`bytea`)을 거부합니다. 파이프라인은 초대형 값을 **세
구간**으로 처리합니다:

| 값 크기 | 처리 |
|---|---|
| **≤ 1 MiB** | 정상 적용. |
| **1 MiB – 8 MiB** | 싱크가 쓰기 **전에** 각 값을 측정해 초대형 값을 **DLQ로 quarantine**(절대 적용 불가). 그런 레코드가 DLQ에 닿도록 Kafka를 통과하려면 토픽·클라이언트 한도를 상향(기본 4 MiB, 최대 8 MiB). |
| **> 8 MiB** | Kafka에 들어갈 수 없음. **캡처 단계에서 제외**해야 함: Debezium `column.exclude.list`가 초대형 LOB 컬럼을 드롭(Evaluation `OVERSIZED_LOB` 플래그로 구동)해 파이프라인에 닿지 않게 함. |

### DLQ로 가는 것

초대형 값 외에도, DLQ는 DSQL이 **영구적으로** 거부하는 레코드를 격리합니다 — 타입 불일치, 제약 위반,
없는 타깃 컬럼(예: 전파 안 된 소스 `ALTER` 이후). 일시적(transient) 실패(OCC `40001`, 커넥션 끊김)는
**재시도**되며 DLQ로 가지 않습니다.

### DLQ가 보이는 곳 — Kafka 토픽이 아니라 CloudWatch

싱크는 quarantine된 레코드를 **CloudWatch** 커넥터 로그 그룹에 로깅하고, 도구 모니터링이 그 줄을
파싱해 UI(테이블별 "Quarantined" 카운트와 단일 다운로드 에러 로그)로 보여 줍니다. 로깅된 사유에는 **SQL
템플릿**(컬럼명 + `?` placeholder)이 들어가며 — **행 값이나 자격증명은 절대 포함하지 않음** — DSQL이
거부한 정확한 문장 형태를 데이터 노출 없이 볼 수 있습니다. 적용도 DLQ 격리도 불가능한 레코드는 조용히
건너뛰지 않고 태스크를 **시끄럽게 실패**시킵니다 — 조용한 손실보다 가시성.

---

## 4.6 MySQL → DSQL 타입과 제약 처리 (참조)

Schema Conversion과 데이터 경로가 방언을 잇기 위해 하는 일입니다. Full Load 값 변환기와 CDC 싱크가
모두 따르는 동일 매핑입니다(공유 "write contract"가 둘을 일치시킴).

### 타입 매핑 (전체 참조)

아래의 모든 MySQL 데이터 타입에 대해, Schema Conversion이 생성하는 대상 DDL 타입 **및**
Aurora DSQL에 저장되는 값의 형태를 정리합니다. 두 마이그레이션 경로 — Full Load 벌크 로더(Python)와
CDC 싱크(Java) — 가 동일한 매핑을 따르며, 공유 **write-contract** 패리티 테스트로 강제되므로 어느
경로로 옮겨도 같은 소스 행이 동일하게 적재됩니다. 분류: **AUTO** = 자동·무손실, **MANUAL** = 변환되나
검토/결정 필요, **UNSUPPORTED** = 자동 변환 불가(재설계).

#### 정수 타입

| MySQL 타입 | Aurora DSQL 타입 | 저장 값 형태 | 분류 | 비고 |
|---|---|---|---|---|
| `TINYINT` | `smallint` | `smallint` | AUTO | 부호 있는 8비트. |
| `TINYINT(1)` | `boolean` | `boolean` (`true`/`false`) | MANUAL | MySQL boolean 관례; `0/1`→`false/true`. `{0,1}` **밖 값은 시끄럽게 실패**(조용한 평탄화 없음). |
| `SMALLINT` | `smallint` | `smallint` | AUTO | 부호 있는 16비트. |
| `MEDIUMINT` | `integer` | `integer` | AUTO | PostgreSQL에 3바이트 정수 없음; `integer`가 부호 있는 24비트 범위를 커버. |
| `INT` / `INTEGER` | `integer` | `integer` | AUTO | 부호 있는 32비트. |
| `BIGINT` | `bigint` | `bigint` | AUTO | 부호 있는 64비트. |
| `TINYINT UNSIGNED` | `smallint` | `smallint` | AUTO | `0..255` 보존을 위해 확장. |
| `SMALLINT UNSIGNED` | `integer` | `integer` | AUTO | `0..65535` 보존을 위해 확장. |
| `MEDIUMINT UNSIGNED` | `integer` | `integer` | AUTO | `0..16M` 보존을 위해 확장. |
| `INT UNSIGNED` | `bigint` | `bigint` | AUTO | `0..4.29B` 보존을 위해 확장. |
| `BIGINT UNSIGNED` | `numeric(20,0)` | `numeric(20,0)` | AUTO | 더 넓은 정수 타입이 없음; `2^64-1` 전 범위 보존. (CDC는 `bigint.unsigned.handling.mode=precise` 필요.) |
| `INT(11)`, `BIGINT(20)`, … (표시 폭) | bare `smallint`/`integer`/`bigint` | `smallint`/`integer`/`bigint` | AUTO | `(N)` 표시 폭은 **드롭**(MySQL에서 표시용; PostgreSQL 정수는 폭을 받지 않음). |
| `BIT(n)` | `smallint`(n≤15) / `integer`(≤31) / `bigint`(≤63) / `numeric(20,0)`(64) | `smallint`/`integer`/`bigint`/`numeric(20,0)` | MANUAL | DSQL에 **`BIT` 타입 없음**; 비트 패턴이 나타내는 부호 없는 정수로 저장. |

#### 고정소수점 & 부동소수점

| MySQL 타입 | Aurora DSQL 타입 | 저장 값 형태 | 분류 | 비고 |
|---|---|---|---|---|
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `numeric(p,s)` | `numeric(p,s)` | AUTO | 정밀도/스케일 보존. **정밀도 > 38은 UNSUPPORTED**(DSQL은 NUMERIC을 38로 제한). |
| `DECIMAL(p,s) UNSIGNED` | `numeric(p,s)` | `numeric(p,s)` | AUTO | 부호 없음은 표현 불가하며 저장상 의미 없음. |
| `FLOAT` | `real` | `real` | AUTO | 단정밀도 float. |
| `FLOAT(M,D)` | `real` | `real` | AUTO | `(M,D)` 표시 스펙 드롭(PostgreSQL `float`은 스케일이 아닌 정밀도 하나만 받음). |
| `DOUBLE` / `DOUBLE UNSIGNED` | `double precision` | `double precision` | AUTO | 배정밀도 float. |

#### 날짜 & 시간

| MySQL 타입 | Aurora DSQL 타입 | 저장 값 형태 | 분류 | 비고 |
|---|---|---|---|---|
| `DATE` | `date` | `date` | AUTO | |
| `DATETIME` | `timestamp`(시간대 없음) | `timestamp` (UTC wall-clock) | AUTO | **UTC**로 취급/정규화. |
| `DATETIME(6)` | `timestamp` | `timestamp` (UTC, microsecond precision) | AUTO | 소수 초를 마이크로초까지 보존. |
| `TIMESTAMP` | `timestamptz` | `timestamptz` (UTC instant) | AUTO | 절대 UTC 시각으로 저장. |
| `TIME` | `time`(시간대 없음) | `time` | AUTO | 범위 내 `00:00:00..23:59:59`. **범위 밖** MySQL `TIME`(음수 또는 `> 24h`, MySQL 범위 `-838:59:59..838:59:59`)은 `time` 표현이 없어 **시끄럽게 실패**(대신 `interval` 컬럼 필요). |
| `YEAR` | `smallint` | `smallint` (integer year) | MANUAL | DSQL에 `YEAR` 타입 없음; `1901–2155`가 `smallint`에 들어가며 정수 연도로 저장(`YEAR` 표시 의미는 보존 안 됨). |

#### 문자열, 바이너리, 구조화

| MySQL 타입 | Aurora DSQL 타입 | 저장 값 형태 | 분류 | 비고 |
|---|---|---|---|---|
| `CHAR(n)` | `char(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`/`TEXT`/`MEDIUMTEXT`/`LONGTEXT` | `text` | `text` | AUTO | 단일 값 **> ~1 MiB**는 DSQL이 거부 → 행 단위 격리(Full Load) / DLQ(CDC); 초대형 LOB 컬럼은 Evaluation에서 플래그. |
| `COLLATE`(예: `utf8mb4_*_ci`)가 있는 `CHAR`/`VARCHAR`/`TEXT` | 동일, **collation 드롭** | `text` (collation dropped) | MANUAL | DSQL은 기본 collation 사용; 대소문자 무시 collation은 보존 안 됨 → MANUAL 플래그. |
| `BINARY(n)` / `VARBINARY(n)` | `bytea` | `bytea` (raw bytes) | AUTO | 길이 수식어 드롭(PostgreSQL `bytea`는 받지 않음). |
| `TINYBLOB`/`BLOB`/`MEDIUMBLOB`/`LONGBLOB` | `bytea` | `bytea` (raw bytes) | AUTO | 바이너리 페이로드를 바이트 단위로 보존. |
| `ENUM('a','b',…)` | `text` + `CHECK (col IN ('a','b',…))` | `text` | MANUAL | DSQL에 `ENUM` 없음; 순서 의미 보존 안 됨. |
| `SET('x','y',…)` | `text` | `text` (comma-joined) | MANUAL | 무손실 매핑 없음; 다중 값 set 의미는 앱에서 처리. |
| `JSON` | `json` | `json` | AUTO | (CDC는 JSON 텍스트를 `PGobject(type=json)`로 감싸 `json` 컬럼을 타겟팅.) |
| 공간(`GEOMETRY`/`POINT`/`LINESTRING`/…) | `bytea` | `bytea` (raw WKB bytes) | MANUAL | DSQL에 공간 타입 없음; 데이터는 원시 WKB 바이트로 **보존**(Full Load는 `ST_AsBinary(col)`, CDC는 Debezium geometry의 `.wkb` 추출; **SRID는 드롭**). `geometry` *컬럼 타입* 자체는 UNSUPPORTED로 플래그되지만 값은 손실되지 않음. |

### 구조적 제약

| DSQL 규칙 | 도구 동작 |
|---|---|
| **외래 키 없음** | FK 정의는 DDL에서 제거하되 **리포트에 보존**, 무결성을 앱에서 강제하라는 MANUAL 노트. |
| **기본 키 필수** | PK 없는 테이블은 **UNSUPPORTED**(적재 불가). |
| **`TRUNCATE` 없음** | "교체" 적재는 **DROP + 재생성**, `TRUNCATE` 사용 안 함. |
| **트랜잭션당 한 개 DDL** | 스키마 변환은 실행 단위당 정확히 한 개 DDL 문을 내보냄. |
| **`CREATE INDEX ASYNC`** | 보조 인덱스를 데이터 적재 후 비동기로 생성. |
| **낙관적 동시성** | 모든 배치·DDL을 `40001` 재시도로 감쌈. |
| **트리거/저장 프로시저/이벤트 없음** | **UNSUPPORTED** — 애플리케이션으로 재구현(스케줄 이벤트는 EventBridge/Lambda). |
| **네이티브 파티셔닝 없음** | DSQL이 자동 분산; 파티션 테이블은 MANUAL. |
| **클러스터당 하나의 DB** | 다중 DB 소스는 MANUAL(스키마로 통합하거나 클러스터 분리). |

### 호환성 분류

Evaluation은 모든 객체를 다음 중 하나로 분류합니다:

- **AUTO** — 사람 개입 없이 자동 변환.
- **MANUAL** — 변환되지만 사람 결정이나 앱 측 변경 필요(FK, `AUTO_INCREMENT`, CI collation,
  파티셔닝, 초대형 LOB, `ENUM`/`SET`, 생성 컬럼, `ON UPDATE` 타임스탬프, 다중 DB).
- **UNSUPPORTED** — 자동 변환 불가(트리거, 루틴, 이벤트, PK 없음, 공간/미지원 타입, `DECIMAL`
  정밀도 > 38, 테이블당 컬럼 > 255, DB당 테이블 > 1000, FULLTEXT/SPATIAL 인덱스).

선택적 **AI 어시스트**(Amazon Bedrock, 기본 꺼짐)가 `MANUAL`/`UNSUPPORTED` 항목 변환을 제안할 수
있지만 — 제안은 **검토 전용**이며 명시적 승인 없이는 적용되지 않고, AI는 CDC 데이터 경로에 **절대**
들어가지 않습니다.

---

**다음:** [5. Validation →](05-validation.md)
