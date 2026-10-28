# 4. CDC 동작 방식과 DSQL 제약 처리

_언어: [English](../en/04-cdc-and-dsql-constraints.md) | **한국어** | [日本語](../ja/04-cdc-and-dsql-constraints.md)_

> **이전:** [3. Full Load](03-full-load.md)
>
> **CDC에서 컬럼을 빼려면** [13.8 CDC 캡처 목록](13-excluding-tables-and-columns.md#138-cdc-캡처-목록)을 보세요.

**CDC**(Change Data Capture)는 거의 무중단 전환을 위한 **선택적** 스트리밍 파이프라인입니다. Full
Load가 기존 행을 복사한 뒤, CDC는 소스의 모든 신규 insert/update/delete를 반영해 DSQL을 계속 최신
상태로 유지합니다. 덕분에 긴 중단 대신 최소 다운타임으로 전환할 수 있습니다.

CDC가 필요한 경우는 **대규모 또는 지속적** 마이그레이션뿐입니다. 짧은 동결이 허용되는 일회성
전환이라면 Full Load만으로 충분합니다.

---

## 4.1 파이프라인

<p align="center"> <img src="../../images/architecture-cdc-pipeline.png" alt="CDC 파이프라인: 소스 변경 로그(MySQL binlog / PostgreSQL의 논리 복제 슬롯을 통한 WAL) → Debezium 소스 커넥터 → Amazon MSK(PK로 키잉한 테이블별 토픽 + DLQ) → 커스텀 DSQL 싱크 커넥터 → Aurora DSQL" width="900"> </p>

소스 변경 로그는 세 단계를 거칩니다. 두 커넥터 모두 **관리형 MSK Connect**에서 실행되며, 도구는
**자체 싱크 컴퓨트를 돌리지 않고** 컨트롤 플레인 역할만 합니다(구성 작성, 재개 지점 확립, 모니터링).

- **Debezium 소스 커넥터(MySQL 또는 PostgreSQL)**가 소스 변경 로그 — MySQL은 바이너리 로그,
  PostgreSQL은 논리 복제 슬롯을 통한 WAL(write-ahead log) — 를 읽습니다. 소스는 읽기 전용으로
  유지되며, 허용된 예외는 두 가지입니다: PostgreSQL 소스에서는 도구가 마이그레이션 대상 테이블에만
  한정된 논리 슬롯과 publication을 생성(하고 해체 시 삭제)하고, `gtid_mode=ON`이 아닌 MySQL
  소스에서는 Full load + CDC 실행이 시작 지점을 기록한 직후 `FLUSH BINARY LOGS`를 한 번
  실행합니다([§3.5](03-full-load.md#35-워터마크--cdc로의-다리)).
- **Amazon MSK**(Kafka)가 내구성 있는 백본 역할을 합니다. **테이블당 토픽 1개**, 기본 키로 키잉(한
  행의 모든 변경이 한 파티션에 순서대로 유지), 그리고 DLQ 토픽, 유휴 구간에도 커밋된 소스 위치를
  전진시키는 **하트비트** 토픽, MySQL에서만 Debezium **스키마 히스토리** 토픽으로 구성됩니다.
  재시작에 빈틈이 없는 것은 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안뿐입니다(§4.3).
- **커스텀 DSQL 싱크 커넥터**(이 프로젝트가 소유한 Java Kafka Connect 플러그인)가 변경을 DSQL에
  적용합니다. 표준 JDBC 싱크는 DSQL의 단기 IAM 토큰으로 인증할 수 없고, 낙관적 동시성
  충돌(`SQLSTATE 40001`)이나 DSQL이 거부하는 행도 처리하지 못합니다(§4.4). CDC 스택은 배포될 때의
  플러그인을 그대로 쓰므로, 이 버전의 도구에 들어 있는 것보다 오래된 싱크 플러그인을 실행하는 동안
  CDC 단계가 그렇게 알려 주며(*This CDC infrastructure runs an older sink plugin*), **Delete CDC
  infrastructure** 후 다시 배포하면 들어 있는 플러그인이 등록됩니다.

---

## 4.2 CDC는 스키마가 아니라 데이터를 복제 — 중요

CDC는 **행 수준 데이터 변경**(insert/update/delete)을 복제하며, SQL 문이나 **DDL**은 복제하지
않습니다: 소스의 `ALTER TABLE`, `CREATE`, `DROP`은 **전파되지 않고**, Aurora DSQL 스키마는 **Schema
Conversion**이 만든 그대로 유지됩니다.

**CDC 중 소스 스키마를 바꾸면**, Aurora DSQL에 대응하는 변경을 직접 하되(추가되거나 이름이 바뀐
컬럼은 드리프트 배너의 **Fix target schema…**가 해 줍니다) Schema Conversion으로는 하지 마세요:
CDC가 스트리밍하는 동안에는 그 Apply가 **Replace**를 포함해 차단됩니다(Replace는 테이블과 이미
로드된 행을 함께 삭제하며, 그 행은 **Drop & reload**로만 되살릴 수 있습니다). 소스보다 먼저 Aurora
DSQL에 NULL을 허용하는 컬럼을 추가하는 것은 안전합니다.

타깃이 일치할 때까지, 타깃이 받을 수 없는 행은 **DLQ**로 **dead-letter** 처리되며 재시도되지 않고,
그런 테이블이 마지막 dead-letter 레코드 이후 CHECKSUM을 통과할 때까지 Cut over는 잠긴 상태로
남습니다([§5.5](05-validation.md#55-cdc-dead-letter와-컷오버)). 어떤 변경은 dead letter를 전혀
만들지 않습니다: 아래
[데드레터를 남기지 않는 변경](#데드레터를-남기지-않는-변경-소스-스키마-재확인)을 참고하세요.

### CDC 중 소스 스키마 드리프트 감지·처리

소스 `ALTER`를 직접 눈치챌 필요가 없습니다. 데드레터 레코드가 소스 형태 변경을 시사하면 CDC 단계에
**"소스 스키마 변경 감지"** 배너가 표시됩니다. Aurora DSQL에는 `ALTER COLUMN … TYPE`과
`SET NOT NULL`이 없고 기본 키를 그 자리에서 바꿀 수 없으므로, 아래 각 행은 Aurora DSQL이 지원하는
복구 방법을 안내합니다. 도구는 타깃 데이터를 스스로 삭제하거나 다시 쓰지 않습니다.

| 배너 행 | SQLSTATE | 흔한 원인 | Aurora DSQL에서의 복구 |
|---|---|---|---|
| column added or renamed at the source | `42703` | `ADD COLUMN` / `RENAME COLUMN` | **Fix target schema…**(아래) |
| a NOT NULL column got no value (재확인이 확인하면 "column dropped at the source") | `23502`, 컬럼을 보내지 않음 | `DROP COLUMN`, 제외된 컬럼, 또는 PostgreSQL에서 큰(TOAST된) 값을 바꾸지 않은 업데이트 | 삭제된 컬럼이면 **Drop & reload** 전에 `DROP COLUMN`(그 컬럼이 있는 동안 Drop & reload는 거부됨; 기본 키 컬럼은 삭제 불가), 아니면 `ALTER COLUMN … DROP NOT NULL` |
| a NULL sent for a NOT NULL column (확인되면 "NULL rule changed at the source") | `23502`, NULL을 보냄 | 소스 컬럼이 이제 NULL을 허용 | `ALTER COLUMN … DROP NOT NULL`(값은 유지되며, 다시 NOT NULL로 만들 수 없음) |
| column type changed at the source | `42804`(삭제 시 `42883`) | 호환되지 않는 타입 변경 | 다시 만들기: **Replace** + **Drop & reload** |
| a value its Aurora DSQL column cannot hold exactly | `22001`, `22003`, 그리고 싱크의 거부 | 넓어진 소스 타입이나 Schema Conversion에서 고른 더 좁은 타입 | 같은 방식으로 다시 만들기 |
| a CHECK constraint rejected the row | `23514` | MySQL `ENUM`이나 PostgreSQL enum에 추가된 값(둘 다 `text` + `CHECK`로 이식됨) | 사유에 적힌 이름으로 `DROP CONSTRAINT` 후 `ADD CONSTRAINT … CHECK (…) NOT VALID`, 트랜잭션당 한 문 |
| the change records' key no longer matches the Aurora DSQL primary key (빨간색: 모든 insert와 update가 유실됨) | `42P10` | 소스 기본 키가 바뀌었거나, 테이블을 다른 키로 다시 적용함 | 키를 되돌리거나 다시 만들기(Step 1 재실행, **Replace**, **Drop & reload**; 재키잉된 테이블은 **Delete CDC infrastructure**와 새 배포도 필요) |

**CDC only**에는 리로드할 Full Load 단계가 없습니다: 표에서 **Drop & reload**라고 한 곳은 Aurora
DSQL에서 테이블을 직접 고치고 그동안 밀쳐둔 행을 바로잡거나(타입 변경이면 새 타입의 컬럼을 추가해
값을 복사하고 이전 컬럼을 삭제한 뒤 새 컬럼의 이름을 바꿉니다. 트랜잭션당 최대 3,000행) **Start
over**를 사용하세요.

**Fix target schema…**(추가되거나 이름이 바뀐 컬럼용)는 정확한 문을 보여 주고, 승인했을 때만
적용합니다(트랜잭션당 1 DDL). 그러면 그 테이블의 CDC가 재개됩니다.

- **새 컬럼**은 추가됩니다(NULL 허용, 기본값 없음). **이름이 바뀐 컬럼**은 이름 변경으로 보일 때
  값을 유지한 채 이름이 바뀝니다(예: Step 1 인벤토리의 같은 위치에 같은 타입으로 이전 이름이 있거나,
  대소문자만 다른 경우). 그렇지 않으면 추가된 컬럼으로 제안되며, 소스에 더 이상 없는 Aurora DSQL
  컬럼을 짚는 경고가 함께 표시됩니다. 대화 상자가 잘못 짐작했다면 취소하고 직접 고치세요.
- **추가하지 않는 것:** **생성(generated)** 컬럼, Aurora DSQL이 담을 수 없는 타입(대화 상자가 직접
  추가하는 방법을 알려 줌), 제외 설정이 막는
  컬럼([§13.8](13-excluding-tables-and-columns.md#138-cdc-캡처-목록)): 캡처 목록이 바뀔 때까지 그
  행은 계속 데드레터로 갑니다. **ENUM** 컬럼은 `CHECK` 없이 `text`로 추가되며, 경고에 그 `CHECK`를
  추가하는 문이 나옵니다.
- **이름이 바뀌었거나, 삭제되었거나, 바뀐 기본 키·CDC 키 컬럼**은 건드리지 않습니다: CDC는 그 키로
  행을 쓰고 Aurora DSQL은 기본 키를 그 자리에서 바꿀 수 없으므로, 대화 상자는 대신 다시 만드는
  단계(위 표의 마지막 행)를 안내합니다.

**이미 밀쳐둔 행을 백필하려면:**

- **Full Load + CDC:** Step 1(Evaluation) 재실행 → Stop CDC → **Drop & reload**로 테이블
  리로드(Schema Conversion에서 그 DDL을 편집하지 않았다면 새 키·타입·NULL 규칙·ENUM 값으로 다시
  만듦) → Start CDC → Validation에서 재확인(그 테이블은 CHECKSUM이 자동).
- **CDC only:** Aurora DSQL에서 행을 직접 고치거나(트랜잭션당 최대 3,000행) **Start over**를
  사용하세요. 데드레터 큐 카드는 각 레코드의 기본 키를 알려 주지만 키 값은 정수나 UUID일 때만 보여
  주므로(그 밖의 값은 숨김), 키 값이 숨겨지는 테이블에는 Start over를 사용하세요.

더 이상 존재하지 않는 타깃 테이블(`42P01`), 고유 키 충돌(`23505`), 싱크가 바인딩할 수 없는
값(`07006`)도 배너 행이 생기며(*Stop CDC and reload these tables*, **CDC only**에서는 *Stop CDC and
repair these tables*), 복구는 같습니다. 먼저 타깃에 테이블이 있는지 확인하세요(없으면 Schema
Conversion에서 다시 적용). 타깃에서 삭제된 테이블은 삭제 전에 적용된 모든 행도 잃었고 어떤
데드레터도 그 행을 나열하지 않으므로, 그 테이블에는 Start over를 사용하세요. `23505` 행은 고유
인덱스 이름과 생길 수 있는 원인을 알려 줍니다(§4.4와 §4.6 참고). 소스가 그 `UNIQUE` 인덱스를
삭제하거나 완화했다면(CDC는 이를 복제하지 않음) Aurora DSQL에서도 삭제하세요.

그 밖의 데드레터는 자체 배너 행이 생기지 않고 데드레터 큐 카드에만 나타나며, SQLSTATE가 있으면 함께
표시됩니다:

- Aurora DSQL에 비해 너무 큰 값([§4.5](#45-1-mib-값당-한도-그리고-dlq));
- 싱크가 변환할 수 없는 값. 예: PostgreSQL의 `timetz[]`, `bytea[]` 또는 제약 없는 `numeric[]` 컬럼.
  Evaluation이 이런 컬럼을 미리 알려 줍니다(`CDC_UNCARRYABLE_COLUMN`. 대신 NULL로 도착하는 컬럼도
  함께: `interval[]`, `money[]`, `tsvector`, 그리고 pgvector의 `vector` 같은 대부분의 확장 타입;
  [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계));
- 소스 기본 키가 삭제된 뒤의 키 없는 변경 레코드(MySQL 또는 PostgreSQL): 소스 스키마 재확인이
  "primary key changed at the source"로 보고함;
- 그 밖의 모든 거부(예: `22P02`, `22007`/`22008`, `22021`, `23503`, `54000`).

테이블에 소스 DDL을 실행한 뒤에는 그 테이블을 CHECKSUM으로 검증하세요.

**컬럼이 정확히 담을 수 없는 값**(MySQL·PostgreSQL 소스)은 쓰지 않고 거부합니다. 싱크는 각 숫자를
Aurora DSQL 컬럼과 대조하므로, `boolean` 컬럼에 0이나 1이 아닌 숫자를 `true`/`false`로 조용히
저장하거나(MySQL `TINYINT(1)`은 소스 `ALTER`가 전혀 없어도 127까지 담을 수 있음), 더 적은 소수
자릿수나 정수로 반올림하는 일이 없습니다. 그 행은 데드레터로 가고, CDC 단계에 테이블과 컬럼 이름이
적힌 **"Rows set aside: a value does not fit its Aurora DSQL column"** 배너가 표시됩니다(값은 절대
적히지 않음). Aurora DSQL 자체도 `varchar`/`char` 컬럼보다 긴 문자열(`22001`)이나 정수·`numeric`
컬럼의 범위를 벗어난 숫자(`22003`)를 거부하며, 그런 행도 같은 배너 행을 받습니다. 검사하지 않아
여전히 반올림되는 것: `numeric` 컬럼에 들어가는 `FLOAT`/`DOUBLE` 값, 그리고 정밀도 없이 선언된
PostgreSQL `numeric`(Schema Conversion이 경고하듯 `numeric(18,6)`으로 저장됨). Aurora DSQL은 컬럼
타입을 그 자리에서 바꿀 수 없으므로, 값을 담을 수 있는 타입으로 테이블을 다시 만드세요(예:
`TINYINT(1)`은 `smallint`로 **Replace**한 뒤 **Drop & reload**; **CDC only**: 배너 표 아래 설명
참고).

**소스 `TRUNCATE`**(MySQL·PostgreSQL 소스)는 복제되지 않습니다. Aurora DSQL에는 `TRUNCATE` 문이
없고, 그 이벤트는 지울 행을 가리키지 않기 때문입니다. 대신 싱크가 그 이벤트를 데드레터로 보내고(잘린
테이블당 1건), CDC 단계에는 테이블 이름이 적힌 **"Source TRUNCATE was not replicated"** 배너가
표시되며, 그 테이블의 이후 변경은 계속 스트리밍되고, 타깃에는 소스가 지운 행이 그대로
남습니다(Validation이 extra 행으로 보고). 복구 방법:

- **Full Load + CDC:** Stop CDC → Full Load 단계에서 **Drop & reload**로 테이블 리로드(CDC가 중지된
  동안에만 제공됨; **Append**는 없는 행만 추가하므로 남은 행을 지우지 못함) → Start CDC →
  Validation에서 그 테이블 재확인.
- **CDC only:** 리로드할 Full Load 단계가 없습니다. Stop CDC → Aurora DSQL 테이블에서 소스에 더 이상
  없는 행을 트랜잭션당 최대 3,000행씩 직접 삭제 → Start CDC → Validation에서 그 테이블 재확인. 단일
  정수 키 테이블이라면 Validation의 **Reconcile every record**가 extra 행 수를 세고 그 키 일부를
  보여 줍니다.

워크로드가 주기적으로 TRUNCATE하는 테이블(스테이징·큐 테이블 등)은 TRUNCATE할 때마다 이 상황이
생기므로, Aurora DSQL에 그 데이터가 필요 없다면 마이그레이션에서 제외하세요. 파티션 관리(MySQL의
`TRUNCATE PARTITION`·`DROP PARTITION`, PostgreSQL에서 파티션을 drop·detach·truncate하거나 이미 행이
든 파티션을 attach하는 것)는 변경 이벤트 없이 행을 바꾸므로 CDC는 아무것도 보고하지 않고 extra(또는
누락된) 행은 Validation에서만 보입니다. 이런 테이블도 같은 방법으로 복구하거나, 컷오버까지 파티션
관리를 멈추세요.

**드리프트 중의 지연.** 타깃에 컬럼·테이블·스키마가 없거나, 키가 바뀌었거나, 컬럼 타입이 바뀌었거나,
`NOT NULL` 컬럼에 값이 오지 않아 Aurora DSQL이 어떤 테이블의 행을 거부하면, 싱크는 그 테이블의 이후
변경만 트랜잭션당 한 행씩 적용하고 다른 테이블은 계속 배치로 적용합니다. 다른 테이블도 같은 싱크를
공유하므로 복구하는 동안 stream lag이 늘어날 수 있습니다. 복구하면 그 테이블의 INSERT나 UPDATE가
하나라도 적용되는 즉시, 또는 마지막 거부 후 60초가 지나면 배치 적용으로 돌아갑니다. 값 단위
거부(유니크·`CHECK` 위반, 너무 길거나 범위를 벗어난 값, 바인딩할 수 없는 값)는 그 테이블을 한 행씩
적용으로 바꾸지 않습니다. 그런 행이 든 변경 배치는 여전히 다른 테이블의 행까지 포함해 트랜잭션당 한
행씩 다시 적용됩니다.

### 데드레터를 남기지 않는 변경: 소스 스키마 재확인

어떤 소스 DDL은 Aurora DSQL이 행을 하나도 거부하지 않은 채 CDC가 쓰는 값을 바꿉니다. 그래서 dead
letter도 배너도 나타나지 않습니다:

- 타깃 컬럼이 nullable이거나 DEFAULT가 있는 **컬럼 삭제**: 새 행에는 NULL이나 DEFAULT가 들어가고,
  갱신된 행은 이전 값을 유지합니다;
- 타깃이 모든 값을 그대로 받아들이는 **타입 변경**(더 넓은 `VARCHAR`나 정수 타입), 또는
  `numeric(p,s)` 타깃에서 `FLOAT`/`DOUBLE`로 바뀐 컬럼(예: `DECIMAL(10,2)` → `DOUBLE`): 이 경우는
  검사하지 않으므로 값이 데드레터 없이 반올림됩니다(그 밖에 반올림되거나 `true`/`false`로 저장될
  값은 데드레터로 갑니다. [위 참고](#cdc-중-소스-스키마-드리프트-감지처리));
- **테이블 삭제·이름 변경**(또는 다른 스키마로 이동): CDC는 배포 당시의 이름으로 테이블을 캡처하므로
  타깃 테이블이 더 이상 바뀌지 않습니다(MySQL에서는 같은 이름으로 다시 만든 테이블이 그대로 흘러
  들어가고, PostgreSQL에서는 이름 변경 후 커넥터가 재시작될 때까지만 이전 타깃으로 변경이 도달함);
- PostgreSQL에서 CDC가 변환할 수 없는 타입(예: `interval[]`, `money[]`)의 **컬럼 추가**, 또는
  **생성(generated) 컬럼**: 값이 아예 도착하지 않습니다;
- **NULL 규칙** 변경, 또는 **기본 키** 변경.

그래서 CDC가 실행되는 동안 도구는 소스에서 캡처 중인 테이블의 정의를 다시 읽고(읽기 전용이며 테이블
데이터는 읽지 않음, CDC 모니터·Validation·Cut over가 열려 있는 동안 최대 5분에 한 번) 마이그레이션의
기준인 Step 1(Evaluation) 인벤토리와 비교합니다: 각 테이블이 아직 있는지, 컬럼 이름(대소문자까지
정확히), 타입, NULL 규칙, 기본 키. 제외한 컬럼, CDC가 캡처하지 않는 생성 컬럼,
기본값·주석·콜레이션·인덱스는 비교하지 않으며, **Fix target schema…**로 추가한 컬럼은 정의의 일부로
봅니다(이름을 바꾼 컬럼은 그렇지 않습니다: Step 1을 다시 실행할 때까지 재확인이 이름 변경을 계속
보고하고 Cut over를 보류합니다). 차이는 CDC 단계에 표시되고([§4.6](#소스-스키마-재확인)), 소스가
다시 Step 1 인벤토리와 일치하고 그 뒤 테이블이 CHECKSUM으로 일치할 때까지 그 테이블의 Cut over를
보류합니다([§5.7](05-validation.md#57-소스-스키마-변경과-컷오버)).

---

## 4.3 무손실 Full Load → CDC 핸드오프

벌크 적재와 스트림 사이에 **누락된 변경도 중복도 없도록** 설계된 부분입니다.

### 동작 방식

1. **Full Load가 스냅샷 지점에서 워터마크를 기록합니다.** MySQL은 binlog file:position, GTID 셋과
   소스의 `gtid_mode`, PostgreSQL은 논리 복제 슬롯을 생성할 때 반환되는 WAL LSN이며, 슬롯
   이름·publication 이름과 함께 기록됩니다([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)).
2. **Start CDC는 바로 그 워터마크에서 재개합니다.** 스냅샷 이후 첫 변경부터 시작하며 — "지금"부터가
   아니고, 행을 다시 읽지도 않습니다. MySQL에서는 도구가 커넥터의 시작 오프셋에 file:position과 GTID
   셋을 시드하고(`gtid_mode=ON`이면 Debezium은 정확히 그 GTID 셋 다음부터 시작), 쓸 수 없는 시작
   지점은 거부합니다([6장](06-limitations.md)). PostgreSQL에서는 Full Load 정합성 지점에서 만든
   슬롯이 곧 워터마크이므로 아무것도 시드하지 않습니다.
3. **싱크는 모든 변경을 PK 기준으로 멱등하게 적용합니다.** 그래서 변경이 겹치거나 재시도돼도 중복이
   생기지 않습니다.

Manual 시작 지점은 binlog file:position과 함께 저장된 GTID 셋을 시드합니다. 버전 0.1.565 이하가
저장한 Manual 시작 지점은 GTID 셋 없이 복원되므로, `gtid_mode=ON`인 소스에서는 Start CDC 전에
**Fetch current position**을 다시 하거나 정확히 그 위치에서 실행을 마친 GTID 셋을 다시 입력하세요.

> **이 설명은 `Full load + CDC` 실행에 해당합니다.** **Full load only** 뒤에 나중에 CDC를 시작해도
> 무손실이지만 동작이 다릅니다. **PostgreSQL**은 과거 위치에 슬롯을 만들 수 없으므로 소스 커넥터가
> 자체 슬롯을 만들고 **선택된 모든 테이블을 먼저 재스냅샷**합니다(소스를 한 번 더 읽음). **MySQL**은
> 바이너리 로그가 히스토리를 보관하므로 재읽기 없이 워터마크에서 시작합니다 — 다만 스트리밍한 적
> 있는 스택에서는 Start CDC가 더 새 시작 지점을 거부하므로, 마이그레이션 유형을 CDC only로 바꾼 뒤
> 그 CDC 단계에서 Delete CDC infrastructure 후 다시 배포하고 Start CDC 하세요(Full load only에는 CDC
> 단계가 없습니다. [3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)). 어느 쪽이든 CDC는
> 워터마크 뒤의 변경만 가져오므로, 그 Full load only 실행이 앞선 적재가 타깃에 남긴 행에
> append했다면 먼저 **Drop & reload**로 Re-run Full Load 하세요. `gtid_mode=ON`이 아닌 소스에서
> MySQL 워터마크는 진행 중인 commit을 기다리지 않고 기록한 binlog 위치입니다: CDC가 따라잡은 뒤
> CHECKSUM 모드로 검증하거나, 정확한 시작 지점이 필요하면 Full Load를 Full load + CDC로 다시
> 실행하세요. 선택 기준은 [10장 §10.1](10-conclusion.md)을 참고하세요.

### CDC를 시작하기 전에

- **MySQL — binlog를 유지하세요.** 워터마크 위치의 binlog가 CDC 시작 시점에 여전히 있어야 합니다.
  Aurora MySQL은 기본적으로 binlog를 24시간만 보관하고 CDC 스택 배포에만 ~15~20분이 걸리므로 **시작
  전에 binlog 보존을 늘리세요**([§1.6](01-setup.md#16-cdc를-위한-소스-준비)). 이미 사라졌다면 **Drop
  & reload**로 Full Load를 다시 실행해 새 워터마크를 얻으세요(스트리밍한 적 있는 스택에서는 Delete
  CDC infrastructure 후 다시 배포한 뒤에).
- **PostgreSQL — 슬롯을 건강하게 유지하세요.** 슬롯은 만들어진 순간부터 WAL을 붙잡아 두므로
  전제조건은 binlog 보존이 아니라 `wal_level=logical`입니다. 슬롯이 너무
  뒤처지면(`wal_status='lost'`) Start CDC가 차단됩니다(아래 "Start CDC가 차단될 때" 참고).
- **Full Load가 끝나게 두세요.** Full Load가 실행되는 동안 CDC 단계는 *Start CDC waits for the Full
  Load to finish*를 표시하고 Start CDC는 비활성화됩니다. 반대로 CDC가 스트리밍하는 동안에는 Full
  Load 단계가 아무것도 실행하지 않습니다. 먼저 Stop CDC 하세요.
- **Full Load가 append했나요?** 타깃에 이미 있던 행을 찾았다면(*already there*; 예: 기본값
  *Append*로 한 재실행) 시작 지점 카드가 *Gapless only if this Full Load did not append*를
  표시합니다. append는 그 행을 그대로 두고 CDC는 워터마크 뒤의 변경만 가져오므로, Start CDC 전에
  **Drop & reload**로 Re-run Full Load 하세요(CDC only라면 그 실행 동안 마이그레이션 유형을 Full
  load + CDC로 바꾸세요). 이 알림은 Start를 막지 않으며, append하지 않은 복사 뒤에도 보일 수 있고,
  Drop & reload 뒤에는 사라집니다. Full Load 대화 상자가 미리 경고합니다(*CDC from this run's new
  start point needs 'Drop & reload' …*).
- **Full Load 뒤에 테이블 집합을 바꾸나요?** [§13.7](13-excluding-tables-and-columns.md)을
  참고하세요.

### 다시 시작하기

**Stop CDC 후의 재시작**은 마지막으로 기록된 위치부터 이어지며 기록된 스키마 히스토리를 재생합니다.
행은 다시 읽지 않습니다. Stop CDC는 싱크를 드레인하지 않고 커넥터를 제거하므로, 싱크가 적용하지 않은
변경은 CDC 토픽에서만 Aurora DSQL에 도달합니다. **재시작에 빈틈이 없는 것은 토픽이 그 변경을 아직
보존하는 동안뿐입니다**(MSK에서의 보존 기간이며, Start CDC는 이를 확인하지 않습니다). **Stop CDC**
대화 상자와 *Ready to resume CDC* 카드가 이를 알려 줍니다.

| 상황 | Start CDC가 하는 일 | 내가 할 일 |
|---|---|---|
| Stop CDC 후 재시작, 또는 CloudFormation이 롤백한 Start 뒤 | 마지막으로 기록된 위치부터 이어 갑니다(PostgreSQL: 슬롯의 위치). 스냅샷을 끝내지 못한 소스 커넥터는 선택한 모든 테이블을 다시 스냅샷합니다. | 토픽이 적용되지 않은 변경을 아직 보존하는 동안은 할 일이 없습니다. 보존 기간이 지났다면 Delete CDC infrastructure 후 다시 배포하고, **Drop & reload**로 Re-run Full Load 한 다음 Start CDC 하세요(CDC only: Delete CDC infrastructure 후 다시 배포하고 Start CDC 하면 선택한 모든 테이블을 스냅샷합니다). |
| 스트리밍한 적 있는 스택에서 더 새 시작 지점(Re-run Full Load 또는 더 새 Manual 지점) | 거부합니다(*This CDC stack cannot start from a newer start point*): 그 스택의 토픽에는 어떤 싱크도 적용하지 않은 변경이 남아 있을 수 있습니다. | Delete CDC infrastructure 후 다시 배포(새 빈 토픽)하고, Re-run Full Load 하거나 Manual 지점을 정한 다음 Start CDC 하세요. |
| 새 시작 지점에서의 첫 Start (MySQL) | 캡처 대상 테이블이 그 시작 지점 이후 바뀌지 않았는지 먼저 검사한 뒤([6장](06-limitations.md)), 행은 다시 읽지 않고 스키마 히스토리를 현재 소스 테이블에서 재구성합니다. | 없음. |

### Start CDC가 차단될 때 (PostgreSQL 소스)

Start CDC는 커넥터를 만들기 전에 publication, 복제 슬롯과 그 위치, 그리고 상속 자식 테이블을
확인합니다. 차단 안내는 먼저 그 원인 자체의 해결 방법을, 이어서 아래 경로를 알려 줍니다.

| 보이는 것 | 할 일 |
|---|---|
| *Replication slot invalidated*, *CDC's replication slot is not at this Full Load's start point*, 없는 publication 또는 슬롯, 선택한 테이블이 빠진 publication | 아래 경로를 따르세요. |
| *CDC's publication does not publish every change type* | publication 소유자로서 소스에서 `ALTER PUBLICATION … SET (publish = 'insert, update, delete')`를 실행한 뒤 아래 경로를 따르세요. publication이 좁혀져 있는 동안 커밋된 변경은 돌아오지 않습니다. |
| 같은 이름의 슬롯이 있지만 CDC가 쓸 수 없음(다른 데이터베이스의 슬롯이거나 논리 `pgoutput` 슬롯이 아님) | 그 슬롯을 지우거나 이 마이그레이션에 자신의 CDC 스택 이름을 주고, 그다음 아래 경로를 따르세요(CDC only라면 Start CDC를 다시 실행). |
| *CDC cannot snapshot a table that has inheritance child tables* | **Drop & reload**로 Re-run Full Load 하세요(커넥터를 전에 시작한 스택에서는 먼저 Delete CDC infrastructure 후 다시 배포하고, Full Load 단계가 없는 CDC only라면 먼저 마이그레이션 유형을 Full load + CDC로 바꾸세요). 또는 테이블 선택을 아직 바꿀 수 있는 동안 부모의 선택을 해제하세요. |
| *CDC's publication and replication slot could not be checked* | 도구가 소스를 읽지 못합니다(예: 앱 재시작 후의 Secrets Manager 소스). Start CDC는 막히지 않지만 이 점검은 실행되지 않습니다. Connect 단계에서 소스 연결을 테스트한 뒤 Start CDC를 다시 열어 점검이 실행되게 하세요. |

**경로.** 이 스택의 커넥터를 시작한 적이 있는지에 따라 다릅니다:

- 커넥터를 **한 번도 시작하지 않은** 스택: **Drop & reload**로 Re-run Full Load 하면 슬롯을 자신의
  스냅샷 지점에 다시 만들고, 그다음 Start CDC 합니다(Full Load 단계가 없는 CDC only에서는 먼저
  마이그레이션 유형을 Full load + CDC로 바꿈). 또는 **Re-snapshot every table instead** — Start CDC
  대화 상자는 재스냅샷하는 Start가 스스로 점검을 통과할 때만 이를 제시하며(그렇지 않으면
  *Re-snapshot every table is not offered here: a Start that re-snapshots is refused too.*라고 하고
  통하는 경로를 알려 줍니다).
- 커넥터를 **전에 시작한** 스택(그 뒤 멈췄거나 마지막 Start가 롤백된 경우): CDC 토픽에 어떤 싱크도
  적용하지 않은 변경이 남아 있을 수 있고 Debezium은 이후 Start에서 스냅샷을 건너뛰므로 재스냅샷은
  제시되지 않고, 경로는 새 토픽부터입니다: Delete CDC infrastructure 후 다시 배포하고, **Drop &
  reload**로 Re-run Full Load 한 다음 Start CDC 하세요. Full Load 단계가 없는 CDC only라면 Delete
  CDC infrastructure 후 다시 배포하고 새 스택에서 Start CDC 하면 선택한 모든 테이블을
  스냅샷합니다(그 사이 소스에서 삭제된 행은 Aurora DSQL에 남고, Validation이 EXTRA 행으로
  보고합니다).

시작 지점 카드의 **Resume from the Full Load's slot instead**는 기록된 재스냅샷(*Manual re-snapshots
instead of resuming from this load's slot*)을 되돌립니다. 슬롯 이름은 CDC 스택 이름에서
만들어지므로, 같은 소스의 마이그레이션마다 자신의 CDC 스택 이름을 쓰세요.

---

## 4.4 싱크가 데이터 경로에서 DSQL 제약을 처리하는 방식

DSQL은 분산형이면서 PostgreSQL 호환이라, 싱크는 기존 MySQL/JDBC 라이터(writer)처럼 동작할 수
없습니다. 제약별 처리 방식은 다음과 같습니다:

| DSQL 제약 | 커스텀 싱크의 처리 |
|---|---|
| **IAM 토큰 인증(비밀번호 없음)** | 단기 IAM 토큰을 TLS 위 비밀번호로 사용하고 만료 전 **갱신**해, 장기 CDC가 만료 토큰에 멈추지 않게 함. |
| **낙관적 동시성(락 없음)** | `SQLSTATE 40001`이면 충돌한 **트랜잭션**을 지수 백오프+지터로 다시 실행(최대 10회): ≤3000행 청크 하나(적용이 멱등이므로 안전), 또는 거부된 행을 격리하려고 행 단위로 적용하는 동안에는 행 하나. 다시 실행할 때마다 실패한 시도를 먼저 롤백해 깨끗한 트랜잭션에서 시작(Aurora DSQL은 커밋 시점뿐 아니라 구문을 실행하는 도중에도 `40001`을 보고할 수 있음). |
| **일시적 서비스 상태(`XX000` "server unavailable")** | Aurora DSQL은 `XX000`을 일시적인 상태로 문서화하므로 싱크는 이를 `40001`과 똑같이 처리: 실패한 시도를 롤백한 뒤 **트랜잭션**을 같은 백오프로 다시 실행하며, 두 오류는 최대 10회 예산을 함께 씀. 그 예산을 넘겨도 계속되면 싱크가 다시 연결하고, Aurora DSQL이 다시 응답할 때까지 Kafka Connect가 배치를 다시 전달하므로 그런 행은 **dead-letter 처리되지 않음**. 그동안 CDC 단계는 싱크가 멈췄다고 표시하고("The sink is not applying changes"), 싱크 커넥터 로그에는 `DSQL apply failed (transient sqlstate=XX000); Connect will retry`가 반복됨. Aurora DSQL이 다시 응답하면 스트림은 저절로 재개됨. `XX000`이 계속 반복되면 AWS Support에 문의. |
| **기본 키 외의 고유 인덱스(`23505`)** | Aurora DSQL은 고유 인덱스를 즉시 검사하므로 다른 행이 아직 그 값을 가진 동안에는 변경이 거부될 수 있음: 기본 키의 `UPDATE`(Debezium은 이를 이전 키의 delete와 새 키의 insert로 복제하는데 둘은 서로 다른 파티션으로 갈 수 있어 insert가 먼저 도착할 수 있음) 또는 한 행에서 다른 행으로 넘어가는 고유 값. 싱크(플러그인 v53 이상)는 그런 변경과 그 파티션의 이후 변경을 모두, 값을 비우는 변경이 적용될 때까지 보류함: 다른 파티션은 모두 계속 적용되는 동안 점점 늘어나는 간격(500 ms에서 10 s까지)으로 다시 시도하며, 최대 5분(Aurora DSQL 장애가 대기를 끊으면 더 길어질 수 있음, 자세한 내용은 [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)). 5분은 보류된 변경마다 적용됨. 대기는 그 테이블의 Stream lag으로 보임(§4.6). 재키잉되지 않은 테이블에서 같은 행의 이후 변경이 그것을 완전히 대체하면(일반 delete, 또는 같은 컬럼의 이미지) 충돌한 변경은 대신 건너뜀(dead-letter 처리하지 않음). 재키잉(Composite key) 테이블에서는 아무것도 건너뛰지 않음. 건너뛴 변경의 행은 그 대체 변경이 적용될 때까지 타깃에서 값이 그대로임. 그래서 건너뛴 변경이 비웠을 값이 필요한 다른 행의 변경은 5분 뒤 dead-letter 처리될 수 있음([§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)에 해당하는 경우가 있음). 5분을 넘기면 SQLSTATE `23505`와 인덱스 이름으로 dead-letter 처리되며(*a unique constraint rejected the row*), dead-letter가 난 모든 테이블과 마찬가지로 Cut over는 그 테이블이 CHECKSUM으로 일치할 때까지 보류됨([§5.5](05-validation.md#55-cdc-dead-letter와-컷오버)). |
| **트랜잭션당 ≤ 3000행** | ≤ 3000행 청크와 최대 8 MiB의 바이트 예산으로 적용하고, 청크당 한 번 `commit()`. 텍스트·JSON·바이너리 값은 모두 그 크기만큼 계산되므로, 큰 JSON 문서 묶음도 Aurora DSQL의 트랜잭션당·메시지당 10 MiB 한도보다 넉넉히 작게 나뉨. |
| **문장 단위 UPDATE/재생 없음** | 모든 변경을 **PK 기준 멱등 upsert/delete**로 적용(`INSERT ... ON CONFLICT (pk) DO UPDATE`, `DELETE ... WHERE pk = ?`, Kafka tombstone). 같은 이벤트를 다시 적용해도 안전. |
| **커넥션 끊김(idle close / 토큰 만료 / 워커 교체)** | 죽은 커넥션이나 half-open(끊겼지만 살아있는 듯 보이는) 커넥션을 감지해 새 토큰으로 재연결하고 **같은 오프셋부터 다시 적용**합니다. 같은 변경을 다시 적용해도 중복이 안 생기므로 안전하며, 레코드를 버리지 않습니다. 연결 오류는 일시적(transient) 오류로 보고 재시도하며, 손상된 행(poison row)으로 오인하지 않습니다. Aurora DSQL이 연결을 여는 것 자체를 거부할 때 — IAM 토큰(`28xxx`), 열린 연결이 너무 많음(`53300`), 연결 속도 초과(`53400`) — 도 마찬가지입니다: 새로 서명한 토큰으로 배치를 다시 시도하며 아무것도 dead-letter 처리하지 않습니다. |
| **외래 키(드레인 후 cut over에서 적용)** | 싱크가 스트리밍하는 동안 타깃에는 외래 키가 없습니다: 싱크는 테이블 간 변경을 부모→자식 순서 없이 적용하므로, 강제되는 키가 있으면 먼저 도착한 자식 행을 거부합니다(`23503`, dead-letter 처리). 키는 최종 드레인 뒤 cut over에서 추가됩니다: `NOT VALID`로 바로(몇 초, 그때부터 모든 신규 쓰기를 강제) 추가하고, 이미 적재된 행은 Aurora DSQL이 백그라운드에서 검증합니다([2장](02-evaluation-and-schema-conversion.md)). 소스의 `ON DELETE` / `ON UPDATE` 동작은 스트림에 오지 않으므로(InnoDB가 엔진 내부에서 적용) 그 동작이 바꾼 자식 행 때문에 그 검증이 실패할 수 있습니다: MySQL 소스라면 **I've cut over**는 이런 키가 검증될 때까지, 또는 그 검사 없이 cut over하기로 선택할 때까지 기다립니다. |

CDC 스택은 배포될 때의 플러그인을 그대로 씁니다: 오래된 싱크 플러그인이 이 표와 다르게 동작하면 CDC
단계가 그렇게 알려 주며(*This CDC infrastructure runs an older sink plugin*), **Delete CDC
infrastructure** 후 다시 배포할 때까지 그대로입니다.

---

## 4.5 1 MiB 값당 한도, 그리고 DLQ

DSQL은 **약 1 MiB를 초과하는 단일 값**(`TEXT`/`bytea`)을 거부합니다. 파이프라인은 초대형 값을 **세
구간**으로 처리합니다:

| 값 크기 | 처리 |
|---|---|
| **≤ 1 MiB** | 정상 적용. |
| **1 MiB – 8 MiB** | 싱크가 쓰기 **전에** 각 값을 측정해 초대형 값을 **DLQ로 격리**(quarantine)합니다(절대 적용 불가). 그 레코드의 테이블에서 나머지는 계속 흐릅니다. |
| **> 8 MiB** | Kafka에 들어갈 수 없음. **캡처 단계에서 제외**해야 초대형 LOB 컬럼이 파이프라인에 닿지 않습니다. 드롭할 컬럼은 Data Migration 단계의 테이블 목록 아래 **Exclude tables & columns**에서 규칙으로 제외합니다 — 그 안의 *Oversized-LOB candidates* 섹션이 Evaluation이 `OVERSIZED_LOB`로 표시하는 컬럼과 같은 집합을 보여 줍니다. 도구가 쓰는 행에서 제외한 컬럼은 NULL입니다. CDC 시작 전에 이미 Aurora DSQL에 있던 행은 그 값을 유지하며(CDC는 제외한 컬럼을 쓰지 않음), Validation이 그 행을 집계합니다. |

### DLQ로 가는 것

초대형 값 외에도, DLQ는 DSQL이 **영구적으로** 거부하는 레코드를 격리합니다 — 타입 불일치, 제약 위반,
없는 타깃 컬럼(예: 전파 안 된 소스 `ALTER` 이후) — 그리고 Aurora DSQL 컬럼이 값을 바꿔 버리기 때문에
싱크가 거부하는 값도 격리합니다([§4.2](#cdc-중-소스-스키마-드리프트-감지처리)). 일시적(transient)
실패(OCC `40001`, Aurora DSQL의 일시적인 `XX000` "server unavailable", 커넥션 끊김)는 **재시도**되며
DLQ로 가지 않습니다. 소스 스키마 변경으로 설명되는 거부는 복구 방법과 함께 배너 행으로
표시되고(§4.2), 스키마와 무관한 값 단위 데이터 오류(예: `22P02`)는 SQLSTATE만 표시됩니다. 값 단위
거부(유니크·`CHECK` 위반, 너무 길거나 범위를 벗어난 값)는 그 주변 행들을 트랜잭션당 한 행씩 적용하게
만들고, insert·update의 고유 위반(`23505`)은 먼저 값을 비우는 변경을 위해 최대 5분 동안
보류되며(§4.4), 그보다 오래 갈 때만 dead-letter 처리됩니다.

### DLQ가 보이는 곳 — Kafka 토픽이 아니라 CloudWatch

싱크는 격리(quarantine)된 레코드를 **CloudWatch** 커넥터 로그 그룹에 로깅하고, 도구 모니터링이 그
줄을 파싱해 UI(테이블별 "Quarantined" 카운트와 단일 다운로드 에러 로그)로 보여 줍니다. 로깅된
사유에는 **SQL 템플릿**(컬럼명 + `?` placeholder)이 들어가며 — **행 값이나 자격증명은 절대 포함하지
않음**. 적용도 DLQ 격리도 불가능한 레코드는 조용히 건너뛰지 않고 태스크를 **시끄럽게 실패**시킵니다.

### Full Load가 격리한 행이 이후 변경되면 CDC는 어떻게 처리하는가

Full Load가 어떤 행을 격리했다면(초대형 값을 쓸 수 없었으므로) 그 행은 **대상에 없고** 소스에는
그대로 있습니다. 이후 같은 행이 변경될 때의 동작은 작업 종류에 따라 갈립니다:

| 없는 행에 대한 소스 작업 | CDC 동작 | 결과 |
|---|---|---|
| `DELETE` | `DELETE … WHERE pk = ?`가 **0행**에 매칭됩니다. 싱크는 0행 delete를 에러로 취급하지 않으므로 조용히 적용·커밋됩니다. | **정상.** 의도한 최종 상태("그 행이 대상에 없음")가 이미 성립합니다. |
| 값을 1 MiB 아래로 줄이는 `UPDATE` | Debezium이 **after-image 전체**를 보내고 싱크가 upsert로 씁니다. 기존 행이 없으므로 행이 **INSERT**됩니다. | **갭이 스스로 치유됩니다.** 조치 불필요. |
| 값이 여전히 초대형인 `UPDATE` | 싱크가 쓰기 **전에** 값을 측정하므로 같은 이유로 **DLQ**에 격리됩니다. | 갭 유지, 단 **가시적**입니다(DLQ 깊이 / 모니터의 "Quarantined"). |
| *다른* 행의 `INSERT` | 영향 없음. | 정상. |

Full Load 이후의 격리 갭은 스스로 알려 주지 않습니다: 소스가 그 행을 다시 건드리지 않으면 CDC가
복제할 것이 없고, 0행 delete는 정상적인 재처리와 구분할 수 없습니다. 이를 보고하는 것은
**Validation(4단계)**이며, 그래서 Full Load 완전성 판정이 그쪽을 가리킵니다. 실무 지침은 동일합니다:
컷오버 전에 초대형 소스 값을 고쳐 CDC를 멈춘 상태에서 해당 테이블을 **Reload**하거나(Stop CDC,
Reload, 그다음 Start CDC 다시: CDC가 스트리밍하는 동안 Full Load 단계는 다시 적재를 실행하지
않습니다, [3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)), 초대형 LOB 컬럼을 캡처 단계에서
제외(§4.5)해 파이프라인에 아예 들어오지 않게 하십시오.

---

## 4.6 CDC 진행 모니터링 (마이그레이션 모니터)

CDC 실행 중에는 Data Migration 화면이 테이블별 실시간 모니터를 보여줍니다. 수동 새로고침 없이
자동으로 갱신되며, 소스에는 **스캔이 없습니다** — 프로덕션 DB에 `COUNT(*)`를 실행하지 않습니다.
컬럼:

| 컬럼 | 의미 |
|---|---|
| **Full Load rows** | 원샷 스냅샷이 테이블에 적재한 행 수. |
| **Inserts** / **Updates** / **Deletes** | Full Load 이후 이 테이블에 적용된 CDC insert·update·delete 누적 수. |
| **Quarantined** | **DLQ**로 격리된 변경 이벤트 수(영구 거부된 행: 타입 불일치, 1 MiB 초과 초대형 값, 제약/스키마 드리프트). |
| **Source rows (est.)** | 스캔 없는 카탈로그 **추정치**. **Target rows** — DSQL 정확 카운트. |
| **Stream lag** | 타깃이 소스보다 **시간상** 얼마나 뒤처졌는지(아래 참고). |
| **Consistency** | 색상 배지: 초록 *consistent* = 카운트 일치 · *replicating…* = 따라잡는 중 · 빨강 *rows missing* = 최신 변경은 도착했으나 중간에 행 유실 · 빨강 *data quarantined* = DLQ에 미적용 이벤트 존재. |

### Stream lag — 행 수가 아니라 실제 시간 척도

**Stream lag**은 종단 간 복제 지연입니다: 각 변경에 대해 싱크가 **적용 시각 − 소스 커밋 시각**을
기록하며, 모니터는 최근 창에서 관찰된 **최악(최대) 지연**(마지막 단일 값이 아님)을 시간으로
표시합니다: `caught up`(1초 미만 / 스트림 소진), `8.5s behind`, `2m 10s behind`, `1h 4m behind`.
실제 **시간** 지연이므로 **모든** 기본 키 타입에서 정확하고, 최신 insert뿐 아니라 update/delete
지연도 반영합니다. 싱크가 **고유 인덱스 때문에 보류한 변경**(§4.4)은 기다리는 동안 그 테이블의
지연으로 집계됩니다. 적용되거나 dead-letter 처리되면 지연이 줄어듭니다. 파티션별 지연은 없습니다.
시간 메트릭이 아직 없으면(구버전 싱크 플러그인이거나 데이터포인트가 아직 없을 때) 모니터는
`N behind (PK)` 또는 `caught up`을 표시하며, 단일 컬럼 정수 PK에서만 동작합니다.

지연 측정은 철저히 **best-effort**이며 복제에 영향을 주지 않습니다. 소스의 마지막 쓰기 후 최소 5분이
지난 뒤의 "Stream lag → caught up"을(보류된 변경의 지연은 나타나기까지 1~2분 걸릴 수 있음)
[cut over](10-conclusion.md)로 넘어가도 안전하다는 신호로 사용하세요.

### 파이프라인 헬스와 change flow

per-table 표 위에 **파이프라인 헬스 카드**가 각 커넥터 상태와 **"change flow"** 상태 줄을 보여줍니다
— *Streaming*, *No changes flowing — idle*, 또는 **"Sink stalled — changes are NOT reaching DSQL"**
— 소스 poll과 싱크 send의 rec/s 게이지 두 개와 함께라 유휴 스트림과 멈춘 스트림을 한눈에 구분합니다.
**라이브 stream-lag 차트**는 최악의 종단 지연을 시간축으로 그립니다.

확정된 stall의 알림은 싱크 커넥터 로그를 가리킵니다. 싱크 send 속도가 회복될 때까지 cut over하지
마세요. 반복되는 줄의 의미:

| 로그 줄 | 의미 | 할 일 |
|---|---|---|
| `Commit of offsets timed out` | 싱크 컨슈머가 그룹에서 빠짐 | Stop CDC 후 Start CDC 해 다시 합류시키세요 |
| `DSQL apply failed (transient sqlstate=XX000)` | 일시적인 Aurora DSQL 상태 | 싱크는 저절로 재개됩니다. 오래가면 AWS Support에 문의 |
| `DSQL apply failed (transient sqlstate=…)`가 `08xxx`, `57xxx` 또는 `null` | 싱크가 클러스터에 닿지 못함 | 클러스터가 아직 있는지, 커넥터 서브넷이 포트 5432로 그 엔드포인트에 닿는지(보안 그룹, NAT 게이트웨이 또는 Aurora DSQL VPC 엔드포인트와 그 DNS), 커넥터의 IAM 역할이 아직 연결할 수 있는지 확인 |
| 같은 줄이 `28xxx`, `53300` 또는 `53400` | Aurora DSQL이 연결을 여는 것을 거부함 | `28xxx`는 IAM 토큰(커넥터의 IAM 역할이 아직 연결할 수 있는지 확인), `53300`은 클러스터의 열린 연결 한도(다른 곳의 유휴 연결을 닫거나 쿼터를 올리기), `53400`은 새 연결 속도. 연결이 열리면 싱크가 재개됩니다 |
| 같은 줄이 `40001` | 다른 무언가가 Aurora DSQL에서 같은 행에 쓰고 있음 | cut over까지 멈추세요 |

커넥터의 태스크가 멈추면 그 행이 빨간색으로 바뀝니다. 커넥터의 CloudWatch 워커 로그에 도구가 아는
실패 — Debezium이 파싱하지 못하는 MySQL DDL 문, 스키마 히스토리 문제, 만들 수 없는 PostgreSQL
publication, 연결할 수 없는 소스 등 — 가 보이면 행에 *Stopped —* 뒤에 원인이 표시되고, 커넥터 아래의
오류 알림이 무엇을 해야 하는지 알려 줍니다. 그렇지 않으면 행에 *Stopped — a task is not running.
Restart required.*가 표시되고, 자세한 내용은 로그 그룹 `/msk-connect/<stack>-cdc`에 있습니다. 그러면
라이프사이클 카드는 CDC가 스트리밍 중이 아니라고 알리고 **Stop CDC**를 제공합니다: Retry CDC는 이미
있는 커넥터를 그대로 두므로 멈춘 커넥터를 다시 시작할 수 없습니다. Stop CDC 후에도 원인은 다음
Start까지 Start CDC 카드에 남습니다.

### DLQ 들여다보기

**DLQ 인스펙터**가 데드레터 레코드를 **Time / Table / SQLSTATE / Reason**으로 나열합니다 —
페이지·필터 지원, 테이블별 분해와 depth 배지 포함 — UI를 벗어나지 않고 poison 행을 트리아지할 수
있습니다. **"Download CDC error log"**로 NDJSON 내보내기. (Reason에는 SQL 템플릿이 담기고 행 값은
없습니다 — §4.5 참고.) 레코드 목록은 메모리에만 있어 앱 재시작 후에는 남지 않습니다. 테이블별 건수,
최신 시각, 드리프트 종류는 남으며, Cut over가 확인하는 것이 바로
이것입니다([§5.5](05-validation.md#55-cdc-dead-letter와-컷오버)).

### 재키잉된 PostgreSQL 테이블: REPLICA IDENTITY 재확인

CDC가 재키잉하는 테이블(소스 기본 키 밖의 컬럼을 더한 복합 타깃 키)은 소스의 `REPLICA IDENTITY`가
테이블과 모든 파티션에서 **FULL**이어야 합니다. FULL 미만이면 `DELETE`가 추가된 컬럼을 싣지 않아
오류도 dead letter도 없이 Aurora DSQL에 행이 남을 수 있고, 추가된 컬럼만 바꾸는 `UPDATE`는 dead
letter로 격리되며(*a unique constraint rejected the row*) 이전 행이 남습니다. 그래서 모니터가
`REPLICA IDENTITY`를 다시 읽고(읽기 전용, 최대 2분에 한 번, MySQL 소스에서는 하지 않음) 다음 알림을
보여 줍니다:

| 알림 | 의미 | 할 일 |
|---|---|---|
| **빨간색 — "Deletes on re-keyed tables can be lost"** | 테이블(또는 그 파티션 중 하나)이 FULL 미만입니다. Cut over가 보류되고, 수정 전에 일어난 delete는 다시 재생되지 않습니다. | 소스에서 `ALTER TABLE <table> REPLICA IDENTITY FULL`을 실행하세요 — 부모는 전파되지 않고 새 파티션은 `DEFAULT`로 시작하므로 각 파티션에도 — 그런 다음 **Check again**을 누르세요. PostgreSQL 18에서 생성 컬럼이 있는 테이블에는 알림이 FULL 대신 **Keep source PK**를 알려 줍니다([6장](06-limitations.md) 참고). |
| **노란색 — "REPLICA IDENTITY is FULL again"** | 수정은 반영되었지만 FULL 미만이던 동안의 delete가 빠져 있을 수 있습니다. Validation이 그 테이블을 자동으로 CHECKSUM으로 비교하고, Cut over는 수정 후에 시작한 일치를 기다립니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버)). 단, 그곳에서 **Continue without this check**를 선택했다면 기다리지 않습니다. | 테이블이 다르면: CDC가 따라잡게 둔 뒤 Stop CDC, Full Load 단계에서 **Drop & reload**로 리로드, Start CDC, 재확인. CDC only에는 Full Load 단계가 없으므로 대신 Aurora DSQL에서 잉여 행을 직접 삭제하세요. FULL 미만이던 테이블을 CDC가 더 이상 재키잉하지 않게 되면(Schema Conversion에서 테이블을 **Keep source PK**로 되돌리고 CDC를 다시 배포했거나, 소스 기본 키가 이제 복합 키를 포함함) 같은 알림이 "REPLICA IDENTITY was below FULL on re-keyed tables"라는 제목으로 나타납니다: 그 전에 유실된 delete에는 여전히 CHECKSUM이 필요합니다. |
| **정보 — "Can't re-check REPLICA IDENTITY on the source"** | 소스 비밀번호는 메모리에만 있으므로 앱 재시작 후에는 도구가 접속할 수 없습니다. | Connect 단계에서 다시 연결하세요. 그동안 이 테이블들을 FULL로 유지하세요. |
| **노란색 — "Can't re-check REPLICA IDENTITY on the source"** | 마지막 읽기가 실패했습니다(2분 뒤 다시 시도). | 그동안 이 테이블들을 FULL로 유지하세요. |

### 소스 스키마 재확인

모니터는 소스에서 캡처 중인 테이블의 정의를 다시
읽고([§4.2](#데드레터를-남기지-않는-변경-소스-스키마-재확인) 참고) DLQ 카드 안이 아니라 그 옆에 다음
알림을 보여 줍니다:

| 알림 | 의미 | 할 일 |
|---|---|---|
| **빨간색 — "Source schema changed on N captured table(s)"** | 테이블마다 한 줄로 무엇이 바뀌었는지(컬럼 삭제·추가·이름 변경, 타입·NULL 규칙·기본 키 변경, 테이블 없음)와 그것이 CDC가 쓰는 행에 미치는 영향을 알려 줍니다. Cut over가 보류됩니다. DLQ 배너가 같은 방식으로 이미 보고하는 변경(그로 인해 거부된 행과 함께)은 그 배너에만 표시되며, 어느 쪽이든 Cut over는 보류됩니다. | 소스가 최종 정의를 갖춘 뒤 테이블을 다시 맞추고(아래) **Check again**을 누르세요. |
| **노란색 — "The source schema changed while CDC ran"** | 소스가 다시 Step 1 인벤토리와 일치하지만(또는 Step 1 재실행이 테이블 정의를 바꿨거나, 테이블이 인벤토리에서 빠짐), 그동안 CDC가 쓴 값이 다를 수 있습니다. | Validation이 이 테이블들을 자동으로 CHECKSUM으로 비교하고, Cut over는 그 시각 이후에 시작한 일치를 기다립니다([§5.7](05-validation.md#57-소스-스키마-변경과-컷오버)). Step 1 인벤토리에서 빠진 테이블은 비교할 수 없으므로, Cut over 단계에서 **Continue without this check**로 확인(acknowledge)하세요. |
| **정보 — "Can't re-check the source schema"** | 소스 비밀번호는 메모리에만 있으므로 앱 재시작 후에는 도구가 접속할 수 없습니다. | Connect 단계에서 다시 연결하세요. |
| **노란색 — "Can't re-check the source schema"** | 마지막 읽기가 실패했거나 캡처 중인 테이블을 하나도 찾지 못했습니다(5분 뒤 다시 시도). | 그동안 마이그레이션 대상 테이블에 DDL을 실행하지 마세요. |

소스가 최종 정의를 갖춘 뒤 테이블을 다시 맞추려면:

| 바뀐 것 | 다시 맞추는 방법 |
|---|---|
| 컬럼 추가·이름 변경 | **Fix target schema…**(그 테이블에 DLQ 배너가 이미 제공하는 경우 제외): 새 컬럼은 추가하고, 이름이 바뀐 컬럼은 값을 유지하도록 이름을 바꿉니다. Validation이 그 컬럼을 비교하도록 이어서 Step 1을 다시 실행하세요. 키 변경이나 기본 키·CDC 키 컬럼의 이름 변경에는 아무것도 바꾸지 않고, 생성 컬럼이나 제외 설정이 막는 컬럼은 추가하지 않습니다(대화 상자가 이유와 할 일을 알려 줍니다). |
| 컬럼 삭제, 또는 타입·NULL 규칙 변경 | 소스의 변경을 되돌리거나: **Full Load + CDC** — Step 1을 다시 실행하세요(이후 재확인은 그것과 비교). Aurora DSQL 테이블도 바뀌어야 한다면 CDC가 따라잡게 둔 뒤(Stream lag: caught up) Stop CDC, 삭제된 컬럼은 Aurora DSQL에서 먼저 삭제(Drop & reload는 새 정의에 없는 타깃 컬럼을 지우지 않음), Full Load 단계에서 **Drop & reload**로 테이블 리로드, 그런 다음 Start CDC. 컬럼 타입이 바뀌었다면 먼저 Schema Conversion에서 새 타입을 지정하세요(Aurora DSQL은 컬럼 타입을 그 자리에서 바꿀 수 없음). CDC가 중지된 동안에는 마이그레이션 대상 테이블에 다른 DDL을 실행하지 마세요. **CDC only** — 소스를 따라야 하는 부분은 Aurora DSQL 테이블을 직접 바꾸고, 변경 이후 CDC가 쓴 행을 바로잡은 뒤(다시 재생되지 않음) Step 1을 다시 실행하세요. |
| 기본 키 | 소스에서 키를 되돌리거나, (Full Load + CDC) Step 1을 다시 실행하고 **Drop & reload**로 리로드해 새 키로 다시 만드세요. 키 컬럼의 이름이 바뀌었거나 삭제되었다면 먼저 Schema Conversion에서 테이블을 **Replace**로 다시 적용하세요(Drop & reload는 이전 키 컬럼을 지우기를 거부하고, Aurora DSQL은 그 컬럼을 삭제할 수 없음). CDC only라면 **Start over**를 사용하세요. CDC 키 컬럼(Composite key)의 이름이 바뀐 경우 알림은 Fix를 제공하지 않고 다시 만드는 단계를 직접 안내합니다. |
| 테이블 이름 변경·삭제 | CDC는 고정된 테이블 목록을 캡처하므로 그 자리에서 다시 맞출 수 없습니다: 소스에서 이름을 되돌리거나 **Start over**로 새 이름으로 마이그레이션하세요. 의도적으로 삭제했다면 Step 1을 다시 실행하세요: 그 테이블은 재확인에서 빠지고, Cut over가 확인(acknowledge)을 요청합니다. |

---

**다음:** [5. Validation →](05-validation.md)
