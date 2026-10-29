# 3. Full Load 동작 방식

_언어: [English](../en/03-full-load.md) | **한국어** | [日本語](../ja/03-full-load.md)_

> **이전:** [2. Evaluation과 Schema Conversion](02-evaluation-and-schema-conversion.md)
>
> **테이블이나 컬럼을 빼려면** [13. 테이블과 컬럼 제외](13-excluding-tables-and-columns.md)를 보세요. 제외한 컬럼은 Aurora DSQL에 NULL로 로드됩니다.

**Full Load**는 기존 행을 소스 데이터베이스 — RDS / Aurora **MySQL** 또는 RDS / Aurora
**PostgreSQL** — 에서 Aurora DSQL로 옮기는 도구 자체의 벌크 복사입니다. Debezium 스냅샷이
**아닙니다**. 테이블을 선택한 뒤 **Data Migration** 단계에서 실행합니다.

> **머릿속 모델:** 로더는 소스를 PK 순서로 한 페이지씩 읽어, 작고 재시도 가능하며 **여러 번 적용해도
> 중복이 생기지 않는** 배치로 DSQL에 씁니다 — 테이블이 아무리 커도 메모리는 한정된 채로 유지되고,
> 중단되면 이어서 재개됩니다.

**가장 짧은 경로:** **Data Migration** 단계에서 테이블을 선택하고 **Start Full Load**를 클릭한 뒤
진행 표를 지켜보다가, quarantine되었거나 실패한 테이블이 있으면
[§3.6](#36-실패를-격리하는-방식--그리고-격리되지-않는-하나)을 따르세요. 그다음은 (선택했다면)
CDC([4장](04-cdc-and-dsql-constraints.md))와 Validation([5장](05-validation.md))입니다.

---

## 3.1 큰 그림

```
Source (MySQL / PostgreSQL)          Aurora DSQL
  │  기본 키 페이지 단위로 읽기      ▲  작고 재시도 가능한 배치
  └──────────►  값 변환  ────────────┘
       (읽기 전용, 일관된 스냅샷)
```

1. **워터마크 캡처**(소스 히스토리의 일관된 지점).
2. PK 페이지 단위로 **행 스트리밍 추출**(큰 테이블은 쓰기가 분산되도록 여러 기본 키 구간으로,
   §3.8).
3. 읽어 들인 각 값을 DSQL 형태로 **변환**.
4. 한정된 배치(재적용해도 중복 없음)로 DSQL에 **동시 적재**: 보조 **인덱스**가 이미 있는 테이블에
   적재합니다(§3.4).

**도구가 소스에 쓰는 내용.** 소스는 일관된 스냅샷 안에서 **읽기 전용**으로 읽히며, 로더는 절대
수정하지 않습니다. 읽기 외에 소스에 손대는 것은 CDC뿐이며, 다음과 같은 경우뿐입니다:

| 실행 | 도구가 소스에 쓰는 내용 |
|---|---|
| `gtid_mode=ON`이 아닌 소스의 MySQL **Full load + CDC** | **`FLUSH BINARY LOGS`** 하나를, CDC 시작 지점을 기록한 직후 어떤 테이블도 읽기 전에 실행합니다. `RELOAD`가 필요하고, 데이터를 쓰지 않으며, 테이블을 잠그지 않고, binlog가 새 파일로 넘어갑니다(§3.5). |
| PostgreSQL **CDC** | 정확히 마이그레이션 대상 테이블로만 한정된 논리 복제 슬롯과 publication(워터마크 시점(§3.5)에 만들고 teardown 시 삭제); 재키잉된 테이블의 `REPLICA IDENTITY FULL`; 그리고 바로잡아야 할 때 publication의 `publish_via_partition_root` 설정. |
| 그 밖의 실행 | 읽기 외에는 없음. |

모두 autocommit이고, 소규모 allowlist로 제한되며, 감사됩니다. 이것이 도구가 어떤 소스에든 수행하는
유일한 쓰기입니다.

PostgreSQL에서는 도구가 쓰기 전에 먼저 확인합니다. 워터마크 시점에 publication을 만들거나 거기에
테이블을 추가하기 전에, 추가할 모든 테이블에 사용 가능한 `REPLICA IDENTITY`가 있는지(그리고
PostgreSQL 18에서는 재키잉된 테이블에 생성 컬럼이 없는지 — 그 테이블에 필요한 `FULL`이 깨지므로)
확인하며, 없으면 실행은 소스에서 아무것도 바꾸기 전에 멈추고 그 테이블과 원인에 맞는 해결 방법을
알려 줍니다. 같은 이름의 복제 슬롯이 사용 중이거나, 다른 데이터베이스에 속하거나, 논리 `pgoutput`
슬롯이 아니면(예: 같은 CDC 스택 이름을 쓰는 다른 마이그레이션의 슬롯) 쓰기 전에 멈추며, 이
데이터베이스의 비활성 슬롯은 이 스택의 이전 슬롯으로 보고 교체합니다. 파티션된 테이블이 있는데
publication의 `publish_via_partition_root`를 설정할 수 없으면, 실행은 슬롯을 만들기 전에 멈추고
publication 소유자로 실행할 `ALTER PUBLICATION` 구문을 알려 줍니다. Start CDC에서는 바로잡기가
실패하거나 세션에 소스 비밀번호가 없어 실행되지 못해도 Start가 멈추지 않으며, 그 로그가 같은 구문을
알려 줍니다. Full Load 이후 선택한 테이블도 추가하기 전에 같은 확인을 거치며, 없으면 그 테이블은
로드되지 않고 그 행에 이유가 표시됩니다([13.7](13-excluding-tables-and-columns.md)).

---

## 3.2 스트리밍 export — 어떤 크기든 한정된 메모리

로더는 `OFFSET`이 아니라 **기본 키 페이지**로 테이블을 읽습니다. 각 페이지는 이전 페이지의 마지막 키
다음부터 키 순서대로 시작하며, 짧은 페이지가 끝을 알릴 때까지 반복합니다. 페이지는 단일 일관 스냅샷
안에서 소스로부터 스트리밍되므로:

- 테이블 전체가 **RAM에 올라오지 않음** — 메모리가 한 페이지로 한정;
- 실행 중인 소스가 계속 바뀌어도 읽기는 **단일 일관 스냅샷**으로 유지됨(나누어 읽는 MySQL 테이블은
  부분마다 스냅샷 하나, [§3.8](#38-병렬-적재)).

**MySQL은 소스 자체 설정과 관계없이 `REPEATABLE READ`로 읽습니다.** 도구는 자신이 여는 세션에만 이를
설정하므로(서버의 `transaction_isolation`은 그대로이고 권한도 필요 없음), `READ-COMMITTED`로 설정된
소스에서도 테이블마다 일관된 스냅샷 하나를 얻습니다. 그래서 각 테이블 읽기는 끝날 때까지 InnoDB read
view 하나를 붙잡습니다. 쓰기 많은 소스에서는 큰 테이블을 읽는 동안 History List Length가
늘어납니다([§7.3](07-performance-and-tuning.md#쓰기-많은-소스의-대형-테이블)).

**기본 키가 필요합니다.** PK 없는 테이블은 페이지를 넘길 수 없어 거부됩니다(Evaluation에서도
`UNSUPPORTED`로 플래그됨 — DSQL도 PK를 요구). 단일·복합 PK 모두 지원합니다 — 복합 키는 단순히
허용되는 수준을 넘어, 단조 증가 키에서 발생하는 쓰기 핫 파티션에 대한 이 도구의 권장
해결책입니다([7장 §7.1](07-performance-and-tuning.md#기본-키-전략--핫-파티션-회피) 참조).

정확하게 페이지를 넘길 수 없는 키 컬럼은 행이 빠진 채로 로드하는 대신, 해결 방법을 알려 주는
메시지와 함께 그 테이블을 멈춥니다. 멤버를 정확히 읽을 수 없거나 소스가 같다고 비교하는(예:
대소문자를 구분하지 않는 collation) MySQL `ENUM`·`SET` 키 컬럼, 그리고 한 페이지를 넘는 테이블에서
**뒤에 다른 키 컬럼이 오는** 단정밀도 MySQL `FLOAT` 키 컬럼이 그렇습니다(소스에서 컬럼을 `DOUBLE`
또는 `DECIMAL`로 바꾸거나 테이블에 다른 기본 키를 준 뒤 그 테이블을 다시 시도하세요). `TIME`·`BIT`
키 컬럼은 다른 컬럼처럼 읽습니다. 소스에서 `ENUM`·`SET` 멤버를 추가한 뒤에는 Evaluation과 Schema
Conversion을 다시 실행해 타깃의 `CHECK`에 그 멤버가 나열되게 하세요.

> **바쁜 프로덕션 소스의 읽기 부하가 걱정되나요?** 여러 테이블을 한꺼번에 로드해도 하나의
> 레버(테이블 병렬수)로 제한되며, 소스를 건강하게 유지하는 구체적 단계가 있습니다 —
> [7장 §7.3 — 소스 부하 최소화](07-performance-and-tuning.md#73-소스-부하-최소화) 참고.

---

## 3.3 행을 읽어 들이면서 타입 변환

행을 한 건씩 읽어 들이는 동안 각 값이 DSQL 저장 형태로 변환됩니다. 이는 Schema Conversion 매핑을
그대로 따르므로 컬럼 타입과 실제 값이 서로 어긋나지 않습니다. **MySQL 소스**가 만들어 내는 예:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → 정수.
- `DATETIME` → `timestamp`, 벽시계를 그대로 복사(시간대를 적용하거나 옮기지 않으므로 다른 시간대의
  소스는 그 시간대의 벽시계를 유지합니다: 2장의 `SOURCE_TIME_ZONE`); `TIMESTAMP` → `timestamptz`, 그
  시점.
- Aurora DSQL에 없는 값인 0이거나 잘못된 `DATE`/`DATETIME`/`TIMESTAMP`(`0000-00-00`, `2024-00-15`,
  `ALLOW_INVALID_DATES`에서의 `2024-02-31`) → CDC 커넥터가 0 날짜에 쓰는 값: NULL, 컬럼의 `DEFAULT`
  또는 `1970-01-01`(그 달에 없는 날도 `1970-01-01`이 되며, CDC는 대신 그런 값에서 멈춥니다.
  [6장 §6.2](06-limitations.md#62-마이그레이션-프로세스-한계)에 자세히 나옵니다). 그러면 그
  테이블에는 컬럼별 개수를 담은 WARNING 줄이 활동 로그에 하나 남습니다(값은 남기지 않음). 기본 키
  컬럼의 그런 값은 저장된 그대로 두며(유니크 키 컬럼의 값도 마찬가지), Aurora DSQL이 이를 거부하므로
  그 행은 격리됩니다.
- `BLOB`/`BINARY`/`VARBINARY` 계열 → `bytea`.

**PostgreSQL 소스**에서는 Full Load 중 MySQL 방식의 값 변환이 **없습니다**: 양쪽 끝이 모두
PostgreSQL이므로 DSQL이 지원하는 값은 그대로 통과합니다. PostgreSQL 타입 처리 — 미지원 타입은
remodel 대상으로 플래그, `numeric` 정밀도 클램핑 — 는 즉석 값 변환이 아니라 **Schema
Conversion**에서 일어납니다.

전체 매핑(및 "값당 1 MiB 한도" 같은 DSQL 제약 처리)은
[2장 §2.3](02-evaluation-and-schema-conversion.md#23-타입-매핑-참조)과 Schema Conversion 단계에
있습니다.

---

## 3.4 배치·재적용 안전·한정 병렬 적재

읽어 들인 행은 곧바로 **멱등 배치 INSERT**로 모여, 한정된 DSQL 커넥션 풀에서 **동시에** 적재됩니다.
도구가 모든 배치를 Aurora DSQL의 트랜잭션당 한도에 맞게 알아서 조절하고, 낙관적 동시성 충돌·끊긴
연결·일시적인 "server unavailable"에 부딪힌 배치는 백오프를 두고 다시 시도합니다(최대 20회).

**다시 돌려도 안전합니다(idempotent).** 클린 적재는 새로 만든 빈 타깃에 넣고, 데이터가 이미 있을 수
있으면(재시도, 또는 이미 행이 있는 테이블에 append) **"기존 건너뛰기"** 모드로 전환합니다 —
insert하되 기존 키는 덮어쓰지 않고 건너뜀 — 그래서 재실행해도 중복이 안 생기고 **더 최신 CDC 적용
행을 덮어쓰지 않습니다.** 다만 CDC가 방금 삭제한 행과 처음부터 없던 행은 구별하지 못하므로, CDC가
타깃으로 스트리밍하는 동안에는 Full Load가 실행되지 않고 Start CDC는 실행 중인 Full Load를
기다립니다([§3.5](#35-워터마크--cdc로의-다리)): 그렇지 않으면 테이블을 복사하는 동안 소스가 삭제한
행이 다시 쓰입니다.

**완전 적재 후 identity 시퀀스를 전진시킵니다.** 테이블 키를 캐시드 identity
컬럼(`GENERATED BY DEFAULT AS IDENTITY`)으로 변환했다면, 적재가 끝날 때 도구가 그 시퀀스를
마이그레이션된 가장 큰 키 다음부터 다시 시작하게 맞춰, **컷오버 후 애플리케이션의 첫 insert가**
마이그레이션된 키와 **충돌하지 않게** 합니다. **완전(또는 gap 수용) 적재에서만** 실행되고 부분/실패
적재에서는 실행되지 않습니다; 평범한 정수 키는 시퀀스가 없어 건드리지 않습니다.

**보조 인덱스가 먼저입니다.** Aurora DSQL에서는 인덱스가 적재된 행 위에서보다 빈 테이블에서 훨씬
빨리 빌드되므로([§7.5](07-performance-and-tuning.md#75-라이브-인프라-실측--71과-72의-근거)), 테이블의
보조 인덱스는 행보다 먼저 존재합니다: Schema Conversion의 적용이 테이블과 함께 인덱스를 만들고,
**Drop & reload**는 다시 만든 테이블에 인덱스를 다시 만든 뒤, 첫 행을 적재하기 전에 Aurora DSQL이 그
빌드를 마치기를 최대 10분 기다립니다. 실행이 행보다 먼저 손대지 못한 인덱스(예: 실행 도중에 다시
만드는 테이블의 인덱스)만 모든 테이블의 데이터가 적재된 뒤에 만들어집니다. 인덱스가 이미 있으므로,
기본 키가 아닌 `UNIQUE` 인덱스가 거부하는 행은 quarantine되고(§3.6) 테이블의 나머지는 적재됩니다.

실패한 인덱스 구문은 다시 시도합니다. 재시도에서 같은 이름의 인덱스가 이미 있으면 생성된 것으로 세지
않고(Aurora DSQL에서 인덱스 이름은 스키마 단위라 다른 테이블의 인덱스일 수도 있기 때문) Full Load
화면에 없는 인덱스가 아니라 **not confirmed**(확인되지 않음)로 표시하므로, 인덱스를 추가하기 전에
타깃을 확인하세요. 화면의 알림은 `UNIQUE` 인덱스가 생성되지 않은 것이 확실할 때만 제약이 빠졌다고
말합니다.

Aurora DSQL은 인덱스를 각각 백그라운드에서 빌드합니다. Full Load는 Drop & reload가 행을 적재하기
전에만, 최대 10분 동안 빌드를 기다리므로, 실행이 끝났다고 보고할 때도 빌드가 아직 진행 중일 수
있습니다. **Data Migration** 단계가 빌드 상황을 보여 줍니다:

| 단계에 표시되는 내용 | 의미 | 할 일 |
|---|---|---|
| *Waiting for Aurora DSQL to build the secondary indexes of N recreated table(s) before loading* | Drop & reload가 빈 테이블에 인덱스를 만들고 빌드를 기다리는 중 | 없음: 빌드가 끝나면, 또는 10분이 지나면 적재가 시작되며, 그때까지 끝나지 않은 빌드는 계속 진행됩니다 |
| *Aurora DSQL is building secondary indexes: n of m built* | 데이터는 적재됨. 인덱스가 필요한 쿼리는 빌드가 끝날 때까지 느림 | 없음: 다음 단계로 넘어가도 됩니다 |
| *Secondary indexes built: all m* | 실행이 제출한 인덱스가 모두 빌드됨 | — |
| *UNIQUE index build failed: a constraint is missing* 또는 *Secondary index builds failed* | Aurora DSQL이 인덱스를 빌드하지 못함. 실패한 `UNIQUE` 인덱스는 빠진 제약 | 컷오버 전에, 알림이 알려 주는 원인을 고치고(`UNIQUE`라면 중복 제거), 테이블에 아직 그 인덱스가 있으면 삭제한 뒤 다시 만드세요 |
| *Secondary indexes not created yet (N table(s))* | 그 테이블들의 인덱스를 제출하기 전에 실행이 끝남(그 도중의 정지나 앱 재시작, 또는 Aurora DSQL 무응답). 빠진 데이터는 없음 | **Retry unfinished tables**: 데이터를 다시 적재하지 않고 인덱스를 제출합니다 |

---

## 3.5 워터마크 — CDC로의 다리

적재 전 도구는 **워터마크**를 캡처합니다: CDC가 시작할 소스 변경 스트림의 지점으로, 어떤 테이블도
읽기 전에 기록됩니다. 모든 워터마크는 **UTC 스냅샷 타임스탬프**와 테이블별 **근사** 행 수(소스를
풀스캔하지 않는 추정치)도 담습니다. 시작 지점 자체는 엔진마다 다릅니다:

- **MySQL** — **binlog 파일 + 위치**, 그리고 **GTID 셋**, 소스의 **`gtid_mode`**, `server_uuid`.
  `gtid_mode=ON`인 소스에서는 CDC가 워터마크의 GTID 셋 다음부터 시작하고, 그 밖에는 그 파일 +
  위치부터 시작합니다. 워터마크는 마이그레이션 대상 테이블마다 컬럼 정의의 짧은 지문도 기록하므로,
  Start CDC가 그 이후 테이블이 바뀌었는지 알 수 있습니다([6장](06-limitations.md) 참조).
- **PostgreSQL** — binlog 좌표가 아니라 **WAL LSN**. 일관성 지점에서 도구가 **publication**(정확히
  마이그레이션 대상 테이블에 대한 `FOR TABLE`, 절대 `FOR ALL TABLES` 아님)과 `pgoutput` **논리 복제
  슬롯**을 생성하고, 슬롯이 반환한 일관 LSN을 슬롯·publication 이름과 함께 기록합니다. 이 LSN이
  WAL을 고정하여 무손실 CDC 핸드오프를 가능하게 합니다. PostgreSQL에는 GTID가 없습니다.

워터마크는 이후 **무손실 CDC 핸드오프**를 가능하게 합니다. 테이블마다 워터마크 이후에 연 자신의 일관
스냅샷으로 읽고, CDC는 워터마크 이후의 모든 변경을 다시 적용합니다. 스냅샷과 CDC 양쪽에 있는 변경은
두 번 적용되지만 기본 키 기준의 멱등 upsert / delete라
무해합니다([4장](04-cdc-and-dsql-constraints.md) 참조). 그래서 워터마크는 스냅샷이 볼 수 없는
트랜잭션을 포함하면 안 되며, MySQL에서는 도구가 이를 다음과 같이 보장합니다:

- `gtid_mode=ON`인 소스에서는 필요한 것이 없습니다. GTID 셋에는 commit이 끝난 트랜잭션만 들어갑니다.
- binlog 파일 + 위치는 binlog에는 들어갔지만 아직 commit되지 않은 트랜잭션을 포함할 수 있습니다.
  그래서 **Full load + CDC** 실행은 위치를 기록한 직후, 어떤 테이블도 읽기 전에
  **`FLUSH BINARY LOGS`** 구문을 실행합니다. MySQL은 새 binlog 파일로 바꾸기 전에 이미 binlog에
  들어간 트랜잭션이 모두 commit될 때까지 기다립니다(보통 수 밀리초, 그동안 새 commit도 기다림).
  **RELOAD** 권한이 필요하고, 테이블은 잠그지 않으며, 활동 로그에 기록됩니다. **enhanced binlog**를
  쓰는 Aurora MySQL에서는 도구가 이 대기에 의존하지 않으므로, 그런 소스의 Full load + CDC 실행에는
  `gtid_mode=ON`이 필요합니다(또는 enhanced binlog를 꺼야 합니다). 사전 조건 **CDC start point can
  be made exact**가 각 Full load + CDC 실행 전에 둘 다 확인합니다.
- 워터마크 이전에 `XA PREPARE`되고 테이블을 읽은 뒤 commit된 트랜잭션은 어느 쪽에도 복사되지
  않으므로, 실행은 발견한 준비 상태의 XA 트랜잭션을 최대 30초 기다리고, 그래도 남아 있으면 멈춥니다.
  MySQL 8.0.19 이상에서는 이 확인에 `XA_RECOVER_ADMIN`이 필요하며, 이 권한이 없으면 실행은 경고를
  기록하고 계속합니다.

**Full load only** 실행은 `FLUSH`를 실행하지 않습니다. 스냅샷만 복사하고 그 이후는 복사하지
않으므로, 모든 변경이 들어가야 한다면 먼저 소스 쓰기를 멈추세요. 따라서 `gtid_mode=ON`이 아닌
소스에서 그 워터마크는 이전 버전의 도구가 기록한 워터마크와 마찬가지로 정확하게 맞춰지지 않으므로,
나중에 그 워터마크에서 Start CDC를 하면 *This start point was recorded without waiting for in-flight
commits*가 표시됩니다. 이 때문에 CDC only에서는 Full Load를 Full load + CDC로 다시 실행합니다. CDC
사전 조건이 충족되지 않으면 — MySQL에서는 바이너리 로깅이 꺼져 있거나 권한이 제한된 경우,
PostgreSQL에서는 `wal_level`이 `logical`이 아니거나 슬롯/publication을 만들 수 없는 경우(복제 권한
누락 또는 standby 엔드포인트) — CDC 핸드오프만 받지 못할 뿐 Full Load는 정상 동작합니다. 예외는
binlog 위치를 정확하게 맞출 수 없는 MySQL **Full load + CDC** 실행입니다(`RELOAD` 없음,
`gtid_mode=ON` 없이 enhanced binlog 사용, 또는 아직 준비 상태인 XA 트랜잭션). 이 실행은 어떤
테이블도 읽기 전에 멈추고 해결 방법을 알려 주므로, 원인을 고친 뒤 다시 시작하세요.

> 워터마크의 행 수는 **의도적으로 근사**입니다(소스 부담 최소화). 정확한 `COUNT(*)`와 체크섬은
> **Validation**([5장](05-validation.md))의 역할이지 Full Load가 아닙니다.

### 새 시작 지점에는 새 CDC 토픽이 필요합니다

**새** 워터마크를 캡처하는 것은 CDC에 대한 결정이기도 하므로, CDC 스택이 **스트리밍한 적 있는**
**Full load + CDC** 마이그레이션 — Start 뒤에 멈췄거나, 커넥터가 그대로 있거나, CloudFormation이
Start를 롤백한 스택 — 에서는 새 워터마크를 가져가는 모든 작업이 **거부됩니다**: **Start Full Load**,
**Re-run Full Load**, **Re-run all tables**는 비활성화되고, 페이지 헤더의 **Run**은 아무것도
시작하지 않습니다. Stop CDC(그리고 롤백된 Start)는 싱크를 드레인하지 않고 제거하므로, 그 싱크의
컨슈머 그룹은 새 스냅샷보다 오래된 변경을 다시 적재한 테이블 위에 다시 적용하고, 그동안 소스는 이전
정지 지점과 새 스냅샷 사이의 변경을 건너뜁니다 — 조용히, CHECKSUM 검증만이 그것을 드러냅니다. 그래서
알림은 대신 경로를 알려 줍니다: **Delete CDC infrastructure** 후 다시 배포(새 스택은 빈 토픽으로
시작), 그다음 **Re-run Full Load**를 **Drop & reload**로(PostgreSQL에서는 복제 슬롯을 자신의 스냅샷
지점에 다시 만듦), 그다음 **Start CDC**.

Full Load 작업이 어떻게 되는지는 CDC 상태에 따라 다릅니다:

| CDC 상태 | 새 시작 지점: **Start Full Load**, **Re-run Full Load**, **Re-run all tables**, 헤더의 **Run** | 원래 시작 지점: **Retry unfinished tables**, **Reload specific tables**, 테이블별 **Reload**, **Change & reload** |
|---|---|---|
| 아직 스트리밍한 적 없음 | 허용 | 허용 |
| **살아 있음**(스트리밍 중) | 비활성: *Stop CDC first (CDC step → Stop CDC), then Delete CDC infrastructure and deploy it again before a new Full Load.* | 비활성: 먼저 Stop CDC를 하고 그 작업을 실행한 뒤 Start CDC를 다시 하세요. CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안(MSK에서의 보존 기간)만 빈틈이 없습니다. |
| **Start**, **Stop** 또는 **Delete CDC infrastructure** 진행 중 | 비활성: 끝나기를 기다리세요(CDC 단계에 진행 상황이 표시됨). | 비활성: 끝나기를 기다리세요. |
| 스트리밍한 뒤 **멈춤** | 거부됨: 위의 경로를 따르세요. | 허용: CDC는 멈춘 곳에서 이어 가며(위와 같음), 다시 적재한 테이블은 수렴합니다. |

Full Load 단계는 **Full load only**와 **CDC only** 마이그레이션을 막지 않지만, 그 유형으로 바꿔도 이
게이트를 우회할 수는 없습니다: MySQL에서는 Start CDC도 거부합니다(아래). **이 세션에서 CDC 상태를
아직 읽지 못했으면**(화면을 막 열었거나 앱을 재시작한 직후), 또는 읽을 수 없었으면 새 시작 지점은
추측하지 않고 **기다립니다**: 알림이 그렇게 말하고 CDC 단계의 **Re-check CDC state**를 알려
줍니다(읽기에 실패했다면 먼저 AWS 접근 권한을 고치세요).

**Start CDC도 더 새 시작 지점을 거부합니다(MySQL).** MySQL 소스에서 Start CDC는 스트리밍한 적 있는
CDC 스택을 마이그레이션 유형과 상관없이, 스택이 마지막으로 시작한 지점보다 새 시작 지점으로(또는
멈춘 스택의 마지막 Start가 시작 지점을 주지 않았다면 어떤 시작 지점으로도) 앞당기는 것을 거부합니다:
예를 들어 Full load + CDC에서 **Full load only**로 바꾼 뒤 Re-run Full Load가 잡은 워터마크나, 더
뒤의 **Manual** 지점입니다. 시작 지점 카드와 Start CDC 카드가 *This CDC stack cannot start from a
newer start point*와 함께 경로 — 위의 경로, CDC only에서는 **Delete CDC infrastructure** 후 다시
배포한 뒤 새 스택에서 **Start CDC** — 를 보여 주고 Start CDC는 비활성화됩니다. 그 지점과 같거나
이전인 시작 지점, 또는 시작 지점이 없는 Start는 스트림을 이어 가며 거부되지 않습니다.

CloudFormation이 롤백한 첫 Start 뒤에도 같은 규칙이 그 Start가 준 시작 지점에 대해 적용됩니다. 그
지점도 얻지 못하면 그 스택에서는 같은 시작 지점을 포함해 모든 시작 지점이 같은 경로와 함께
거부됩니다(*This CDC stack's last start point is not known*).

CDC only 경로는 새 스택을 Full Load의 워터마크에서 시작하므로, 거부된 지점이 그
워터마크(Automatic)이면 그 경로에는 조건이 하나 있습니다: 그 Full Load가 타깃에 이미 있던 행에
append했다면 그 Start 전에 **Drop & reload**로 다시 실행하세요. CDC only에는 Full Load 단계가
없으므로 그 실행 동안 마이그레이션 유형을 Full load + CDC로 바꿉니다. 그 실행의 Full Load가 CDC 시작
지점을 정확하게 맞춥니다(append는 그 행을 그대로 두고, 워터마크에서 시작한 CDC는 그 뒤의 변경만
가져옵니다). 새 스택에서는 그 Start가 거부되지 않으며, 이 세션이 그 Full Load가 앞선 복사본 위에
append했을 수 있음을 보여 주면 CDC 단계가 그 Start 전에 그 조건을 알려
줍니다([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)).

PostgreSQL에는 이런 거부가 없습니다: Start CDC는 Full Load의 워터마크와 상관없이 스택 자체의 복제
슬롯에서 이어 가고, Full load only 실행은 그 슬롯을 건드리지 않으므로, 스택의 소스 커넥터가
스트리밍했다면 CDC는 Stop CDC 후의 모든 재시작처럼(위) 멈춘 곳에서 이어 갑니다(그렇지 않으면
Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다).

---

## 3.6 실패를 격리하는 방식 — 그리고 격리되지 않는 하나

이 부분은 잘 이해해 두세요. 두 실패 동작이 의도적으로 다릅니다.

### 행 단위 quarantine (테이블은 계속 적재)

적재 시 **Aurora DSQL이 특정 행을 거부**하면(예: 1 MiB 초과 값, 또는 기본 키가 아닌 `UNIQUE`
인덱스의 중복 값 같은 제약 위반), 로더는 테이블을 실패 처리하지 **않습니다.** 문제 행만 분리해
**quarantine**하고(그 **기본 키와 사유만 기록 — 값은 절대 기록 안 함**) 나머지를 적재합니다.
quarantine된 행은 다운로드 가능한 에러 로그에 나타납니다.

Aurora DSQL의 일시적 상태는 거부가 아닙니다: 동시성 충돌, 끊긴 연결, "server unavailable"은 배치
전체를 재시도하며(§3.4), 재시도를 다 써도 해결되지 않은 배치는 실패로 처리됩니다 — 테이블은 **Retry
unfinished tables**로 다시 시도할 수 있고, 그 때문에 quarantine되는 행은 없습니다.

### 테이블 전체 실패(table-fatal)

값을 **조용히 바꾸지 않고는 쓸 수 없으면**, 그 테이블의 적재는 컬럼, 그 타입, 규칙만 적힌
메시지(값은 절대 적히지 않음)와 함께 **시끄럽게** 멈춥니다:

- `boolean`에 매핑된 `TINYINT(1)`이 `{0,1}` 밖의 값(예: `2`)을 가질 때(MySQL 소스만);
- 두 엔진 모두에서, `numeric(p,s)` 컬럼이 유지하는 것보다 소수 자릿수가 많은 값, 또는 정수 컬럼에
  들어가는 소수. Step 1 이후 소스 타입이 넓어졌거나 Schema Conversion에서 더 좁은 타입을 골랐을 때
  생깁니다. `numeric` 컬럼에 들어가는 `FLOAT`/`DOUBLE` 소스 값은 검사하지 않고 계속 반올림됩니다;
- 00:00:00–23:59:59.999999를 벗어난 MySQL `TIME`.

의도된 동작이며, 읽는 중에 발견되어 DSQL에 묻기도 전이므로 행 단위 quarantine이 **아닙니다.** 도구는
`2`를 `true`로 뭉개거나 `1.2345`를 `1.23`으로 반올림해 데이터를 조용히 손상시키지 않습니다: 눈에
보이고 고칠 수 있는 실패가 조용히 틀린 값보다 낫습니다.

### 실행 수준 판정

행을 **하나라도** quarantine한 테이블은 **실패**(미완료)로 보고되며, 실행은 성공으로 보고되지
않습니다 — 미완료 적재가 깨끗한 적재로 오인되지 않습니다 — **단, "격리된 행 수용하고 계속"을
명시적으로 선택하면** gap을 인지한 채 실행이 완료됩니다(Validation에는 여전히 표시). 재시도 가능한
테이블 실패는 항상 차단합니다. 다운로드 에러 로그가 어떤 PK가 왜 quarantine됐는지 정확히 나열합니다.

### 대처 방법

| 상황 | 할 일 |
|---|---|
| 행이 quarantine되었다 | 다운로드한 에러 로그(기본 키와 사유만, 값은 없음)를 보고 그 행을 고치거나, **"격리된 행 수용하고 계속"**을 선택해 gap을 인지한 채 실행을 완료하세요(Validation에는 여전히 표시됩니다). |
| 재시도를 다 쓰고 테이블이 실패했다(Aurora DSQL의 일시적 상태) | **Retry unfinished tables**. 그 때문에 quarantine되는 행은 없습니다. |
| 값을 조용히 바꾸지 않고는 쓸 수 없어 테이블이 멈췄다 | Schema Conversion에서 값을 담을 수 있는 타입을 컬럼에 지정하거나(소스 타입이 바뀌었다면 먼저 Step 1을 다시 실행한 뒤 **Replace**로 다시 적용), 소스 데이터를 고치거나 컬럼을 제외한 다음 그 테이블을 재시도하세요. |
| 실행이 중단되었다(정지, 크래시, 태스크 교체) | **Retry unfinished tables**: 끝난 테이블은 유지되고 끝나지 않은 테이블은 처음부터 다시 적재되며, 원래 워터마크(CDC 핸드오프 지점)를 재사용합니다([§3.7](#37-재개-가능성)). |
| *Secondary indexes not created yet*(빠진 데이터 없음) | **Retry unfinished tables**: 데이터를 다시 적재하지 않고 그 테이블들의 빠진 인덱스만 제출합니다(§3.4). |
| 새 복사본과 새 시작 지점이 필요하다 | **Re-run Full Load**에서 **Drop & reload**를 선택하세요. CDC 스택이 스트리밍한 적 있는 Full load + CDC 마이그레이션이라면 먼저 [새 시작 지점에는 새 CDC 토픽이 필요합니다](#새-시작-지점에는-새-cdc-토픽이-필요합니다)를 보세요. |

---

## 3.7 재개 가능성

중단된 Full Load가 재개하는 단위는 PK 범위가 아니라 **테이블**입니다:

- 끝난 테이블은 `DONE`으로 기록되어 유지;
- 정지·크래시·태스크 교체 뒤에는 끝나지 않은 테이블마다 "기존 건너뛰기" 모드(§3.4)로, 또는 **Drop &
  reload**면 테이블을 먼저 다시 만든 뒤 **처음부터 다시 적재** — 그래서 끝나기 직전에 중단된 아주 큰
  테이블도 처음부터 다시 읽음.

**Retry unfinished tables** 경로는 미완료 테이블만 대기 상태로 리셋하고, **원래 워터마크를
재사용**하며, 완료된 테이블은 유지합니다 — 끝난 테이블을 다시 적재하거나 CDC 핸드오프 지점을 잃지
않습니다.

**적재 후 테이블 체크/해제.** **Full load only**에서는 테이블 집합을 바꿀 수 있고, **Full load +
CDC**에서는 CDC를 처음 시작하기 전까지 바꿀 수 있습니다. 규칙과 점검은
[13.7](13-excluding-tables-and-columns.md)에 있습니다. 로드 후에 선택한 테이블도 같은 재시도가
로드하지만, 원래 워터마크는 그 테이블을 포함하지 않으므로 CDC 핸드오프는 원래 집합에만 유효합니다:
Re-run Full Load가 현재 집합에 대한 새 워터마크를 잡을 때까지(또는 그 이후 선택한 테이블의 체크를
해제할 때까지) 마이그레이션 유형은 Full load only에 머뭅니다. 그 재실행은 **Drop & reload**로
하세요: 그러면 CDC가 그 워터마크에서 시작하는데, *Append*는 앞선 적재가 쓴 행을 그 뒤 소스의
UPDATE와 DELETE 없이 그대로 둡니다. Full load + CDC에서 로드 후에 체크한 테이블은, CDC가 Full Load
이후의 공백 없이 그 테이블을 캡처할 수 있게 되면 Retry unfinished tables가 원래 워터마크 아래에서
로드합니다. MySQL에서 Full Load가 시작 지점 정의를 기록하지 않은 테이블(이전 버전의 Full Load, 또는
로드 후에 변환된 테이블)은 대신 Re-run all tables가 로드하며, 이때 **Drop & reload**를 선택하세요:
모든 테이블에 대해 새 시작 지점을 잡는데, append는 이미 로드된 행을 그대로 두기 때문입니다. 로드
후에 체크를 해제한 테이블은 Aurora DSQL에 그대로 남고 더 이상 단계를 붙잡지 않습니다.

> **명령줄에서 Full Load 실행 (선택).** 동일한 벌크 로더를 CLI 스크립트로도 쓸 수 있습니다 —
> `scripts/run_full_load.py`(먼저 계획 출력, 그다음 `--yes`; 선택적으로 `--clean`, CDC 워터마크
> 캡처용 `--watermark-out`) — 웹 UI 없이 자동화나 대용량 실행에 유용합니다.
> [`scripts/README.md`](../../../scripts/README.md) 참고. 소스는 읽기 전용(`--watermark-out`을 쓰면
> Full load + CDC 실행이 되므로, 소스가 `gtid_mode=ON`이 아니면 워터마크 직후 `FLUSH BINARY LOGS`를
> 실행하며 여기에는 `RELOAD`가 필요함, §3.5)이며, 로드는 UI와 똑같이 멱등입니다.

---

## 3.8 병렬 적재

테이블은 병렬로 적재되며, 각 테이블은 자체 worker 프로세스에서 실행되므로 Full Load가 실행되는
태스크의 vCPU를 모두 쓸 수 있습니다. `table_parallelism`(*Tables in parallel*)이 동시에 실행되는
worker 수를 정합니다. 큰 테이블은 기본 키 구간으로도 나누어 읽습니다: Aurora DSQL은 테이블의 키 범위
하나를 약 20,000 rows/s로 쓰므로, 여러 키 범위로 나누면 쓰기가
빨라집니다([§7.2](07-performance-and-tuning.md#72-병렬수-튜닝)).

### 동작 방식

- **소형 테이블**(분할 기준 미만이거나 선행 기본 키 컬럼이 정수가 아닌 경우): 테이블당 worker 1개,
  리더 1개.
- **대형 테이블**(선행 PK 컬럼이 정수인 경우 — 단일 정수 PK, 또는 `(tenant_id, id)`처럼 선행 컬럼이
  정수인 복합 PK)은 K개의 기본 키 구간으로 나누어 읽습니다. K는
  *Reader shards per large table*(**Settings → Full Load**, `FULL_LOAD_READER_SHARDS`)이 정합니다:
  `0` = 자동(기본값): 추정 5,000,000행부터 2,500,000행마다 구간 하나, 최대 32(10M 행: 구간 4개);
  `1` = 꺼짐; `2`–`32` = 정확히 그 수(아래). 추정치는 시작 지점의 스캔 없는 추정치이며,
  `TABLE_PARALLELISM`이 1이면 아무것도 나누지 않습니다.
  - **PostgreSQL**: 모든 구간을 스냅샷 하나에서 읽고 테이블의 worker들이 그 스냅샷을 공유하므로,
    리더 하나일 때와 마찬가지로 복사본은 한 시점의 것입니다. 실행 전체의 추정 행 수 중 그 테이블이
    차지하는 몫만큼의 worker(최대 `TABLE_PARALLELISM`)가 테이블을 읽으며, worker마다 연속된 구간
    묶음을 읽습니다.
  - **MySQL**: 리더 세션 하나가 스냅샷 하나에서 모든 구간을 구간마다 한 페이지씩 번갈아 읽으므로,
    Full load only와 Full load + CDC 모두에서 리더 하나일 때와 마찬가지로 복사본은 한 시점의
    것입니다. 테이블은 소스 연결 하나와 worker 하나를 씁니다.
- **2부터 32까지를 명시하면** 추정 행 수가 `SHARD_MIN_ROWS` 이상인 테이블에 정확히 그 수의 구간을
  주고 같은 방식으로 읽습니다. 예외는 MySQL의 별도 스냅샷 부분입니다:
  - **MySQL**: 시작 지점에 binlog 파일과 위치가 있는 **Full load + CDC**에서만, 그리고 Aurora
    DSQL에서 모든 unique 키가 선행 키 컬럼을 포함하는 테이블만: 그러면 각 구간이 부분이 되어, 부분마다
    자신의 worker가 자신의 스냅샷에서 읽습니다. MySQL은 스냅샷을 공유할 수 없으므로, 읽는 동안 키가
    바뀐 행은 두 번 복사되거나 전혀 복사되지 않을 수 있습니다. 모든 읽기보다 먼저 잡은 시작
    지점([§3.5](#35-워터마크--cdc로의-다리))에서 시작한 CDC가 그 이후의 모든 변경을 재생해 이를
    바로잡습니다. 그 밖의 MySQL 테이블은 위와 같이 세션 하나가 읽습니다.
- **소스 연결이 끊기면** 세션 하나로 읽는 테이블은 새 스냅샷에서 전체를 다시 읽고, PostgreSQL 구간은
  공유 스냅샷을 다시 읽습니다. MySQL 부분은 그 자리에서 다시 읽지 않습니다: 테이블은 그
  이유(*range 2 of 4*)와 함께 *Failed*가 되며, **Retry unfinished tables**가 다시 적재합니다.
- **동시에 적재되는 테이블 또는 테이블의 부분은 최대 *Tables in parallel*개**이며, 읽을 행이 가장
  많은 것부터 실행됩니다.

### 나누어 읽은 테이블에는 CDC의 재생이 필요합니다(MySQL)

이 절은 위의 별도 스냅샷 부분에만 해당하며, 그러려면 *Reader shards per large table*을 2 이상으로
명시해야 합니다: 기본 설정의 실행은 어떤 테이블도 부분으로 나누어 읽지 않습니다. CDC가 재생하기
전까지 나누어 읽은 MySQL 테이블은 어긋난(torn) 상태이므로, 그 재생에서 테이블을 빼내는 경로는 모두
테이블 이름과 함께 거부되며, 거부를 끝내는 방법을 안내합니다:

- 적재 후 테이블 목록에서 **체크 해제**하거나, *Exclude tables & columns*의 테이블 규칙으로 선택
  해제하기: 체크박스가 잠기며, 잠금 아이콘에 이유가 표시됩니다.
- 적재 후 **마이그레이션 유형을 Full load only로 바꾸기**(CDC only에서도: 그때 경로는 Full Load
  단계가 테이블을 다시 적재하는 Full load + CDC를 거칩니다).
- **더 나중의 시작 지점에서 Start CDC**: Full Load 워터마크보다 나중인 Manual 위치(직접 입력하거나
  *Fetch current position*으로 채운 것). Automatic, 또는 워터마크와 같거나 그보다 이른 Manual 지점은
  모든 읽기를 재생하므로 거부되지 않으며, 멈춘 스트림을 재개하는 Start도 거부되지 않습니다.
- 테이블을 선택 해제할 수 없을 때의 **Deploy CDC infrastructure, Start CDC, Retry CDC**: 그런
  테이블을 선택 해제할 수 없는 테이블로 명시하고 Full load only를 제시하지 않습니다.

**거부를 끝내는 방법.** Settings에서 *Reader shards per large table*을 0(자동) 또는 1(꺼짐)로
설정하고 Full Load 단계가 그 테이블에 보여 주는 컨트롤로 다시 적재한 뒤(리더 세션 하나, 스냅샷 하나 —
테이블이 끝나지 않았으면 **Retry unfinished tables**, 모든 테이블이 적재된 뒤에는
**Reload specific tables**) 작업을 수행합니다. Full load only와 더 나중의 시작 지점에는 두 번째
방법이 있습니다: CDC가 테이블을 재생한 뒤 *Fast sweep*을 끄고 **CHECKSUM** 모드로 Validation을
실행합니다: 일치하면 사본이 증명되어 거부가 끝납니다. 테이블의 체크 해제에는 CDC를 시작하기 전에
다시 적재하는 방법만 있습니다: 재생에는 CDC가 시작되어야 하고, CDC의 첫 Start부터 테이블 집합이
고정되기 때문입니다(Start over로만 바꿀 수 있음, [§13.7](13-excluding-tables-and-columns.md)).

테이블의 표시는 두 가지 중 하나로 지워집니다: 그 테이블을 다시 만드는 적재(**Drop & reload**), 또는
그 테이블의 마지막 적재가 끝난 뒤 시작된 Validation의 **CHECKSUM** 비교가 일치하는 경우입니다.
그래서 리더 하나로 읽는 Retry나 Reload는 그런 테이블을, Append / Drop & reload 선택과 관계없이 삭제
후 다시 적재합니다(대화 상자가 이유를 표시). 모든 테이블의 재실행(**Re-run Full Load** 또는 **Re-run
all tables**)도 선택이나 Reader shards 설정과 관계없이 그런 테이블을 삭제 후 다시 적재합니다:
재실행은 새 시작 지점을 잡고, 그 지점부터의 CDC는 그 전에 일어난 변경을 재생하지 않기 때문입니다.

Start CDC는 스트리밍했더라도 거부를 끝내지 않습니다: 도구는 그것만으로 CDC가 테이블을 재생했는지 알
수 없습니다. 스트리밍하지 못한 Start(워커가 거부했거나 커넥터가 실패한 경우) 뒤에는
**Automatic**(또는 Full Load 워터마크와 같거나 그보다 이른 Manual 지점)에서 CDC를 다시 시작하세요:
그것이 재생이며 거부되지 않습니다. CDC가 따라잡은 뒤에는 CHECKSUM 일치가 Full load only와 더 나중의
시작 지점에 대한 거부를 끝냅니다.

### 튜닝

대규모 마이그레이션에서는 *Tables in parallel*을 태스크의 vCPU 수에 맞추세요(프로덕션 트래픽을
처리하는 소스라면 2–4에서 시작: [§7.3](07-performance-and-tuning.md#73-소스-부하-최소화)). 분할
자체에는 설정이 필요 없습니다. 관련
설정(`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`, `FULL_LOAD_READER_SHARDS`)과 각각의
한도, 그리고 소스 부하와의 관계는
[7장 §7.2 — 병렬수 튜닝](07-performance-and-tuning.md#72-병렬수-튜닝)에 모여 있습니다. 이 설계가
실제로 낸 처리량은 [Appendix: 성능 테스트 결과](12-performance-test-results.md)에 있습니다.

---

**다음:** [4. CDC와 DSQL 제약 →](04-cdc-and-dsql-constraints.md)
