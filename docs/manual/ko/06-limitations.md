# 6. 한계

_언어: [English](../en/06-limitations.md) | **한국어** | [日本語](../ja/06-limitations.md)_

> **이전:** [5. Validation](05-validation.md)

계획에 반드시 반영해야 하는 **실제로 강제되는** 한계입니다. 대부분은 Aurora DSQL 자체에서 비롯됩니다(분산형이라
수평 확장에 맞지 않는 기능을 의도적으로 제외했기 때문), 일부는 배포 방식에서 비롯됩니다. 어느 것도 런타임에서
갑자기 튀어나오지 않습니다. 도구가 **Evaluation** 단계에서 플래그하고, 처리할 수 있는 것은 처리하며, 처리할 수
없는 것은 **요란하게** 실패시킵니다.

---

## 6.1 Aurora DSQL 기능 한계 (스키마가 이에 맞아야 함)

| 한계 | 결과 | 도구 동작 |
|---|---|---|
| **외래 키(지원·강제됨)** | DSQL은 강제되는 외래 키를 지원합니다. 다만 참조/피참조 테이블에 대한 DML에는 추가 읽기 비용이 있고, `CASCADE`/`SET NULL`/`SET DEFAULT` 동작은 트랜잭션당 3,000행 한도에 포함됩니다. | FK를 제거하지 않고 **보존** — 적재 후 실행할 `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`로 렌더링(`CREATE TABLE` 밖)해 데이터 적재 뒤 다시 생성하며, CDC 마이그레이션은 cut over 시점에 적용. Evaluation은 이를 **권고(RECOMMENDED, 필수 작업 아님)**로 분류하고, 원하면 떼어 낼 수도 있음(`preserve_foreign_keys=False`). |
| **소스 `CHECK` 제약** | 소스의 `CHECK` 제약은 타깃으로 변환되지 않음(예: MySQL `CHECK`, 8.0.16+). | DDL에서 드롭되고 **MANUAL** 플래그 — DSQL 호환 `CHECK`를 직접 다시 추가하거나 앱에서 강제. (도구가 MySQL `ENUM`이나 PostgreSQL enum(둘 다 `text`로 변환됨)용으로 생성하는 `CHECK … IN (...)`은 영향 없음. 나중에 소스 타입에 추가된 값은 그 `CHECK`를 바꿀 때까지 `23514`로 데드레터됨; [§4.2](04-cdc-and-dsql-constraints.md#cdc-중-소스-스키마-드리프트-감지처리) 참고.) |
| **기본 키 필수** | PK 없는 테이블은 마이그레이션 불가. | **UNSUPPORTED** 플래그; Full Load도 차단(keyset export에 PK 필요). |
| **트리거/저장 프로시저/함수/스케줄 이벤트 없음** | 서버 측 로직은 옮겨지지 않음. | **UNSUPPORTED** — 애플리케이션으로 재구현(이벤트 → EventBridge/Lambda). |
| **네이티브 파티셔닝 없음** | DSQL이 직접 자동 분산. | 파티션 테이블 **MANUAL**(파티셔닝 제거). |
| **열당 1 MiB 한도** | 열 하나의 값이 **1 MiB**를 넘으면 저장할 수 없습니다 — 타입과 무관합니다(MySQL `TEXT`/`BLOB`/`JSON`, PostgreSQL `text`/`bytea`/`json`): `54000 ERROR: maximum column size exceeded`. **행** 전체는 **2 MiB**가 한도이고(`maximum row size exceeded`), 쓰기 트랜잭션 하나는 10 MiB입니다. ([Aurora DSQL 쿼터](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)) | 한도를 넘는 값은 행 단위로 에러 로그(Full Load) / **DLQ**(CDC)로 **격리(quarantine)**됩니다. Kafka 메시지 상한을 넘길 수 있는 컬럼은 **캡처 단계에서 제외**해야 합니다. 초과 값을 담을 수 있는 컬럼은 `OVERSIZED_LOB`로 표시됩니다: 바이너리/문서 컬럼(MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`)은 LOSS — 그 타입을 고른 것이 큰 값을 담겠다는 의도이기 때문입니다. 길이 제한 없는 PostgreSQL `text`/`varchar`는 RECOMMENDATION — 그것이 평범한 짧은 문자열의 관용적 표기이기도 하기 때문입니다. CHECK로 리터럴 집합에 제한된 컬럼은 아예 표시되지 않습니다. |
| **`DECIMAL` 정밀도 > 38** | 더 높은 정밀도 미지원. | Evaluation은 **UNSUPPORTED**(`NUMERIC_PRECISION`)로 표시하지만, 변환된 DDL을 적용하면 Schema Conversion이 **`numeric(38,37)`로 clamp**(손실)하며 경고 — 스케일도 37로 제한. |
| **공간/geometry 타입** | MySQL 소스에서는 `bytea`로 대체(원본 WKB가 Full Load·CDC 전 구간에서 그대로 보존됨); PostgreSQL 소스는 자동 대체 **안 함**. | MySQL 소스: 변환기가 각 컬럼을 `bytea`로 자동 대체하고, 평가기가 해당 테이블을 검토 대상 **MANUAL**로 표시. PostgreSQL 소스: geometric 타입(point/line/lseg/box/path/polygon/circle)은 Evaluation(`PG_UNSUPPORTED_TYPE`)과 Schema Conversion 양쪽에서 **UNSUPPORTED**로 표시되고 `text`로의 재모델링이 제안됨 — DDL 적용 전에 직접 재모델링해야 함. (자동 대체가 아니라 수동 재모델링이 필요한 그 밖의 PG 전용 타입: 배열, network `inet`/`cidr`/`macaddr`, `xml`, `money`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange, composite, pgvector.) |
| **FULLTEXT / SPATIAL 인덱스** | 미지원. | **UNSUPPORTED**. |
| **테이블당 ≤ 255 컬럼, DB당 ≤ 1000 테이블** | 초과 시 미지원. | **UNSUPPORTED**(`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT`). |
| **테이블당 ≤ 24 인덱스** (PK가 포함되므로 보조 인덱스는 ≤ 23; MySQL 소스는 64) | 초과분 `CREATE INDEX ASYNC`가 Full Load로 모든 행을 쓴 **뒤에** 실패. | 계획 단계에서 **MANUAL**(`TOO_MANY_INDEXES`)로 표시하고 Schema Conversion에도 동일 노트. |
| **기본 키·인덱스의 컬럼 수 ≤ 8** (MySQL 소스는 16) | 더 넓은 키는 error 54011로 실패. | PK가 넓으면 **UNSUPPORTED**(`CREATE TABLE` 자체가 거부되어 아무 데이터도 적재되지 않음), 인덱스가 넓으면 **MANUAL**(`TOO_MANY_KEY_COLUMNS`). 변환은 적재 후 반드시 실패할 DDL을 내보내지 않고 그 인덱스를 **생략**하며, 어떤 인덱스인지 노트에 명시. |
| **키 합산 크기 ≤ 1 KiB** (PK 각각, 인덱스 각각) | DDL이 아니라 **`INSERT`/`UPDATE` 시점의 값**에 대해 검사 — 따라서 실제 키가 너무 긴 행만 실패(error 54000 `key size too large`). | *선언된* 폭이 이를 넘을 수 있으면 변환이 경고(**MANUAL** 권장)하며, MySQL 소스의 utf8mb4 기준 문자당 4바이트로 계산해 해당 키와 최악의 크기를 명시. 차단하지 않음 — 선언은 넓어도 실제 값이 짧으면 정상 마이그레이션되므로. |
| **클러스터당 하나의 DB** | DSQL은 다중 DB가 아니라 스키마로 구성. | MySQL 소스의 경우 다중 DB 소스는 **MANUAL**(MySQL DB 하나가 DSQL 스키마 하나에 매핑됨 — 스키마로 통합하거나 클러스터 분리). PostgreSQL 소스는 하나의 데이터베이스에 연결하며, 그 비시스템 스키마가 이미 DSQL 스키마로 직접 매핑되므로(정규화된 `schema.table`) 다중 DB 통합 단계가 없음. |
| **`TRUNCATE` 없음; 트랜잭션당 DDL 한 개; 낙관적 동시성** | 기존 단일 노드 RDBMS(MySQL 또는 PostgreSQL 소스)와 다른 쓰기/DDL 의미. | 투명하게 처리: TRUNCATE 대신 DROP+재생성, 단일 DDL 단위, 필요한 곳마다 `40001` 재시도. |
| **IAM 토큰 인증(비밀번호 없음); 단기 토큰** | 정적 DB 비밀번호 없음. | 도구(와 CDC 싱크)가 IAM 토큰을 자동 발급·갱신. |

> **스키마 설계의 결론:** 대형 blob, 서버 측 로직, 매우 높은 정밀도의 숫자는 DSQL에
> 맡기기 *전에* 데이터베이스 밖의 애플리케이션으로 옮기세요. 어떤 객체에 이 작업이 필요한지는
> Evaluation이 정확히 알려 줍니다.

---

## 6.2 마이그레이션 프로세스 한계

- **단일 리전 — 크로스 리전 마이그레이션 없음.** 도구는 DSQL을 지원하는 어느 리전에서나 동작하지만,
  **소스와 타깃은 동일 리전에 있어야 하며** 선택적 CDC 파이프라인은 단일 리전/VPC에서 실행됩니다. 크로스
  리전은 지원되지 않습니다.
- **CDC는 소스 측 사전 요건이 있음(엔진별).** 사전 점검 게이트가 이 조건이 충족될 때까지 **CDC를
  차단**합니다. **Full Load만 한다면 이 중 아무것도 필요 없습니다.**
  - **MySQL 소스:** 바이너리 로깅이 **ROW 포맷, 전체 행 이미지**(full row image)로 켜져 있고
    (`binlog_format=ROW`, `binlog_row_image=FULL`) **복제 권한**을 가진 사용자가 있어야 합니다.
    또한 Full Load 워터마크 시점의 로그가 CDC가 시작될 때까지 남아 있도록 **binlog 보존 기간을 늘려야**
    합니다(Aurora MySQL은 기본적으로 24시간만 보존). RDS/Aurora에서는 `my.cnf`가 아니라 파라미터 그룹과
    `mysql.rds_set_configuration`으로 설정합니다 — [§1.1](01-setup.md#11-사전-요구사항) 참조.
  - **PostgreSQL 소스:** `wal_level=logical`(RDS/Aurora: 사용자 지정 DB/클러스터 파라미터 그룹에서 정적
    파라미터 `rds.logical_replication=1`을 설정한 뒤 재부팅; 자체 관리: `wal_level=logical` 설정 후 재시작);
    **복제 권한을 가진 사용자**(superuser, `REPLICATION` 역할 속성, 또는 RDS/Aurora의 `rds_replication`
    멤버십); 소스는 standby/reader가 아니라 클러스터의 **WRITER**여야 함(`pg_is_in_recovery()=false`);
    모든 복제 대상 테이블은 사용 가능한 **`REPLICA IDENTITY`**를 가져야 함(PK가 있으면 기본값, 없으면
    `ALTER TABLE … REPLICA IDENTITY FULL` 또는 인덱스 identity — `NOTHING`은 거부되며 publisher에서
    `UPDATE`/`DELETE` 오류). binlog 보존에 해당하는 개념은 없습니다: 도구가 Full Load 정합성 지점에서
    **논리 복제 슬롯**과 **정확히 마이그레이션 대상 테이블만을 범위로 하는 publication**을 생성하므로, WAL은
    보존 기간이 아니라 슬롯에 의해 고정됩니다(`wal_status`를 관찰하세요 — 비활성 슬롯은 소스 디스크를 가득
    채울 수 있음). 복제 슬롯 / `max_wal_senders` 여유는 점검됩니다(차단하지 않음).
  - **PostgreSQL 소스, 재키잉(re-keyed) 테이블:** CDC가 재키잉하는 테이블 — Schema Conversion의
    **Composite key**, 또는 소스 기본 키 밖의 컬럼을 더하도록 편집한 타깃 키 — 은 Start 시점만이 아니라
    **CDC 실행 내내 테이블과 모든 파티션에 `REPLICA IDENTITY FULL`**이 필요합니다(나중에 만든 파티션은
    `DEFAULT`로 시작). FULL 미만이면 그 테이블의 `DELETE`가 오류도 dead letter도 없이 Aurora DSQL에 행을
    남길 수 있습니다 — Aurora DSQL이 소스 기본 키에 대한 테이블의 고유 인덱스를 다 만들기 전까지, 그리고 이전
    버전이 배포한 CDC 인프라에서는 언제나 — 그리고 추가된 키 컬럼을 바꾸는 `UPDATE`는 dead letter로 격리되고
    이전 행이 남습니다. 사전 점검이 Start를
    차단합니다. Data Migration, Validation 또는 Cut over 단계가 열려 있는 동안 도구는 최대 2분마다 이를
    다시 읽습니다: CDC 모니터에 빨간 알림이 나타나고, 다시 FULL이 되고 테이블이 CHECKSUM으로 일치할 때까지
    Cut over가 보류됩니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버) 참고). 앱 재시작
    후에는 재확인에 소스 비밀번호가 다시 필요합니다(Connect 단계에서 다시 연결). 두 번의 재확인 사이에
    내려갔다가 되돌아온 경우는 보이지 않으므로 FULL을 계속 유지하세요.
- **이전 버전이 배포한 CDC 인프라의 재키잉 테이블**은 변경 레코드를 계속 복합 키로 키잉합니다(Start CDC
  로그가 그렇다고 알려 줍니다). 그러면 선행 키 컬럼의 `UPDATE` — 또는 행을 다른 선행 값으로 delete한 뒤 다시
  insert하는 것 — 는 이전 키의 delete와 새 키의 insert로 복제되며, 토픽의 파티션이 둘 이상이면(캡처 테이블이
  8개 미만이거나, 큰 테이블에 2개 또는 4개가 주어진 경우) 어느 순서로든 적용될 수 있습니다. insert가 먼저
  오면 dead letter로 격리되고(*a unique constraint rejected the row*) delete가 행을 지우므로, 테이블을 리로드할
  때까지 그 행이 빠집니다. Cut over는 그 테이블이 CHECKSUM으로 일치할 때까지 보류합니다. 이 테이블들을 소스
  기본 키로 키잉하려면 CDC 인프라를 삭제(Delete CDC infrastructure)하고 다시 배포하세요. 이 작업은 스트림
  위치도 삭제합니다: MySQL에서는 Start CDC가 Full Load 워터마크부터 binlog를 다시 읽고(CDC only: 소스의 현재
  위치에서 시작하므로 Aurora DSQL이 마지막으로 적용한 변경 이후의 변경은 복제되지 않음), PostgreSQL에서는 복제
  슬롯이 drop되므로 다음 Start가 모든 테이블을 다시 읽으며(Full load + CDC:
  **"Re-snapshot every table instead"**를 선택) 그 사이 소스에서 삭제된 행은 Aurora DSQL에 남습니다.
  [13장](13-excluding-tables-and-columns.md#집합이-잠기는-이유)의 "삭제 후 다시 CDC 시작"과 "CDC only 스택을
  삭제하는 대가"를 참고하세요. 두 엔진 모두 해당합니다.
- **CDC는 데이터를 복제할 뿐 DDL은 복제하지 않음.** CDC 도중 소스에서 발생한 스키마 변경은 DSQL로
  **전파되지 않으므로** Aurora DSQL에 대응하는 변경을 직접 해야 합니다: 추가되거나 이름이 바뀐 컬럼은
  **Fix target schema…**, 이제 NULL을 허용하는 컬럼은 `ALTER … DROP NOT NULL`, 타입·키 변경은 CDC를 중지한
  상태에서 다시 만들기(**Replace** + **Drop & reload**. CDC only에는 별도의 복구 방법이 있음). CDC가 스트리밍하는
  동안에는 Schema Conversion의 Apply가 Replace를 포함해 차단됩니다
  ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요) 참조).
  그중 일부는 dead letter 없이 CDC가 쓰는 값을 바꾸므로(nullable 컬럼 삭제, 타입 확장, 테이블 이름 변경 등),
  CDC 모니터·Validation·Cut over 단계가 열려 있는 동안 도구가 캡처 중인 테이블의 정의를 최대 5분에 한 번
  다시 읽고 Step 1 인벤토리와 다르면 Cut over를 보류합니다
  ([§5.7](05-validation.md#57-소스-스키마-변경과-컷오버) 참고). 이 재확인이 보지 못하는 것:
  - 두 번의 읽기 사이에 되돌려진 변경, 또는 CDC 모니터·Validation·Cut over 페이지가 하나도 열려 있지 않을 때
    일어난 변경(다음에 열릴 때 보임);
  - 모양이 같은 `DROP TABLE` + `CREATE TABLE`(MySQL에는 비교할 안정적인 테이블 식별자가 없음), 또는 값이
    추가된 PostgreSQL enum(`ALTER TYPE … ADD VALUE`);
  - 기본값·주석·콜레이션·인덱스·생성 컬럼 식(비교하지 않음);
  - 도구가 그 테이블을 처음 확인했을 때 이미 Step 1 인벤토리에 들어 있던 변경(CDC 실행 전의 Step 1 재실행은
    기준이 됨);
  - 앱 재시작 후, Connect 단계에서 소스를 다시 연결하기 전까지의 모든 것(비밀번호는 메모리에만 있음);
  - 범위가 제한된 읽기(서버 8초, 소켓 10초)로 끝내지 못하는 매우 큰 MySQL 5.7 카탈로그: 이때 CDC 모니터와
    Cut over가 읽기 실패를 보고합니다.
- **PostgreSQL: CDC를 멈춘 동안의 DDL.** `pgoutput`에는 DDL이 없으므로 그 DDL을 재생하는 것이 없습니다. 위의
  소스 스키마 재확인은 바뀐 키나 NULL 규칙을 다음 읽기에서 보고하지만, CDC가 이어서 진행될 때 밀린 변경이 이미
  테이블의 *현재* 키와 NULL 규칙으로 적용되었을 수 있다는 것, 즉 그 변경이 이전의 변경에도 이미 적용된 것처럼
  처리되었을 수 있다는 것은 알려 주지 못합니다. 스트리밍 중의 변경과 같은 후속 조치를 Aurora DSQL에서 하고,
  Cut over 전에 그 테이블을 CHECKSUM으로 검증하세요.
- **소스 `TRUNCATE`는 CDC가 적용하지 않음.** Aurora DSQL에는 `TRUNCATE`가 없으므로 CDC는 그 이벤트를
  데드레터로 보내고 CDC 단계에 테이블 이름을 표시합니다. 남은 행을 지우고(Full Load + CDC는 Drop & reload,
  CDC only는 직접 삭제) 그 테이블이 CHECKSUM으로 일치할 때까지 Cut over가 보류됩니다
  ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요) 참조). MySQL의
  `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION`은 변경 이벤트를 전혀 만들지 않으며, 남은 행은
  Validation에서만 보입니다. 이전 버전의 도구에서 업그레이드할 때 이미 스트리밍 중이던 MySQL 파이프라인은
  Stop CDC → Start CDC를 한 번 거친 뒤부터 `TRUNCATE`를 감지합니다.
- **MySQL: CDC 시작 지점과 그 지점을 쓰는 Start 사이의 스키마 변경.** 새 시작 위치를 시드하는 MySQL
  Start는 Debezium을 `snapshot.mode=recovery`로 실행합니다: Full Load 워터마크나 Manual binlog 시작
  지점에서의 첫 Start(세션에 Full Load 워터마크가 없는 MySQL CDC only의 첫 시작은 Manual 지점이 필요),
  **Re-run Full Load**나 더 새 Manual 지점 이후의 Start, 그리고 **Delete CDC infrastructure** 후 다시
  배포한 뒤의 첫 Start — 이때는 **원래** 워터마크부터 다시 읽습니다. `recovery`에서 Debezium은 테이블의
  **현재** 정의로 스키마 히스토리를 다시 만든 뒤 그 이전 위치부터 binlog를 읽고, 각 행의 값을 위치 순서로
  컬럼에 대응시킵니다. 따라서 그 사이에 캡처 대상 테이블에 커밋된 스키마 변경(`ALTER TABLE` 등)은
  스트림을 깨뜨립니다: 컬럼 추가·삭제는 Start할 때마다 커넥터를 멈추고, 컬럼 이름 변경도 커넥터를
  멈추며, 컬럼 수가 그대로인 변경(길이나 DECIMAL 정밀도 이외의 타입 변경, 순서 변경, 삭제한 컬럼을 다른
  컬럼으로 대체, 문자 집합 변환)은 **아무 오류 없이** 밀리거나 NULL인 값을 씁니다.
  - **사전 검사.** Full Load 워터마크와 Manual 지점의 **Fetch current position**은 위치를 읽기 직전에
    캡처 대상 테이블마다 짧은 지문(컬럼 순서, 타입, NULL 규칙, 문자 집합, 기본 키)을 기록합니다. 그런
    Start 전에 Start CDC는 테이블을 이 지문과 비교하고, 바뀐 테이블을 하나하나 명시하며 **거부**합니다.
    검사는 소스를 읽으므로 소스 연결이 필요합니다(Connect 단계에서 소스 연결을 다시 테스트할 때까지
    거부합니다). 시작 지점에 지문이 없으면 — 이전 버전 도구가 기록한 지점, 직접 입력한 위치, 정의를 읽지
    못했던 지점 — **경고**만 합니다. `CHAR`, `VARCHAR`, `VARBINARY` 컬럼의 길이 변경이나 같은 스케일에서의
    `DECIMAL` 정밀도 변경은 똑같이 디코딩되므로 거부하지 않습니다. 소스 스키마 재확인
    ([§4.2](04-cdc-and-dsql-constraints.md#데드레터를-남기지-않는-변경-소스-스키마-재확인))은 여전히
    이를 알리며, Aurora DSQL 컬럼을 넓혀야 할 수 있습니다. 정수 타입 확장이나 `ENUM` 값 추가는 여전히
    거부됩니다: 지문으로는 이를 실제로 잘못 디코딩되는 축소나 순서 변경과 구별할 수 없기 때문입니다.
  - **검사가 볼 수 없는 것.** CDC가 캡처하지 **않는** 테이블의 컬럼 이름 변경(Debezium은 모든 테이블의
    정의를 추적하므로 그것을 재생하다 실패합니다), 커넥터를 만드는 동안의 변경, 그리고 그 실행이
    `recovery` 모드인 동안 태스크가 자동 재시작되기 전의 변경. recovery로 만들어진 커넥터는 Stop CDC까지 그
    모드를 유지하므로, 이 구간은 처음 몇 분이 아니라 그 실행 전체입니다. Stop CDC 뒤의 재시작은 보통
    `schema_only`로 재개되므로 영향을 받지 않습니다. 단, 그 스키마 히스토리가 없으면 그 Start는 히스토리를
    `recovery`로 다시 만들며(아래 "Stop CDC 후의 재시작은 다릅니다" 참고), 이 구간은 그렇게 만들어진 실행에도
    적용됩니다. 이름 변경이나 컬럼 추가·삭제는 커넥터를 멈추고, CDC 단계가 원인을 알려
    줍니다 — Start CDC 오류에서, 또는 시작된 뒤에 멈췄다면 그 커넥터의 Pipeline health 행에서
    ([§4.6](04-cdc-and-dsql-constraints.md#파이프라인-헬스와-change-flow)). 컬럼 수가 그대로인 변경은
    커넥터를 멈추지 않습니다: 이전 행이 오류 없이 밀리거나 NULL인 값으로 쓰입니다. 소스 스키마 재확인
    ([§4.2](04-cdc-and-dsql-constraints.md#데드레터를-남기지-않는-변경-소스-스키마-재확인))은 타입 변경과
    다른 컬럼으로 대체된 컬럼을 알리고 Cut over를 보류합니다. 순서 변경과 문자 집합 변환은 어느 검사도 보지
    못하므로, 그런 변경 뒤에는 CHECKSUM으로 검증하세요.
  - **다시 Start, Retry CDC, Debezium의 "recovery mode"는 결코 도움이 되지 않습니다** — 같은 행을 다시
    재생할 뿐입니다. 시작 지점을 변경 이후로 옮기세요: CDC를 Stop하고, 소스 인벤토리를 새로 읽은 뒤(1단계
    Evaluation 재실행) Schema Conversion에서 바뀐 테이블을 갱신해 다시 로드가 새 정의로 테이블을 만들고
    읽게 한 다음 — DDL을 직접 편집했거나 키 전략을 고른 테이블은 그 편집이 유지되므로 다시 하거나
    초기화하고, 삭제·이름 변경된 컬럼은 먼저 Aurora DSQL에서 삭제하거나 그 테이블을 Replace로 다시
    적용해야 합니다(Drop & reload는 새 정의에 없는 대상 컬럼을 지우지 않으려고 해당 테이블을 거부합니다) —
    **"Drop & reload"로 Re-run Full Load**를 실행하세요(기본값 *Append*는 기존 행을 유지하므로 그 사이의
    UPDATE/DELETE가 사라지고, *Reload specific tables*와 *Retry*는 원래 워터마크를 유지합니다). 새 실행의
    워터마크는 변경 이후이므로 Start CDC가 오프셋을 그 위치로 앞당깁니다. CDC only 실행에는 다시 돌릴 Full
    Load가 없으므로, 바뀐 테이블을 Aurora DSQL에서 갱신한 다음 같은 스택에서 **Manual**, **Fetch current
    position**, **Use this start point**를 선택하세요(Start는 멈춘 스택의 오프셋을 되감지는 않지만 앞으로는
    이동합니다. 멈춘 스택에서는 시작 지점 카드의 **Manual — start from a later binlog position instead**
    아래에 있습니다). 그러면 그 지점 이전에 캡처 대상 테이블에 생긴 변경은 복제되지 않습니다: "Drop &
    reload"로 다시 로드하거나(Full load + CDC) Aurora DSQL에서 바로잡고(CDC only), CHECKSUM으로 검증하세요.
    다시 로드하면 CDC 시작 지점이 Automatic으로 돌아가므로, Start CDC 전에 Manual을 다시 선택하세요(가져온
    지점은 유지됩니다).
  - **Stop CDC 후의 재시작은 다릅니다.** 기록된 위치에서 이어 가며 **기록된 스키마 히스토리를
    재생**하므로(`snapshot.mode=schema_only`), CDC를 멈춘 동안의 DDL은 스트리밍 중의 변경처럼 순서대로
    적용되고 같은 후속 조치가 필요합니다
    ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)).
    그 히스토리가 없으면 Start가 그렇다고 알리며 실패합니다. 커넥터가 아직 남아 있으면 Stop CDC(또는
    **Clean up leftover connector**)로 먼저 제거하세요 — 남아 있는 커넥터에 대한 Start나 Retry CDC는 그
    스냅샷 모드를 유지하므로 같은 방식으로 실패합니다. 그러면 다음 Start가 같은 검사를 거친 뒤 히스토리를
    다시 만듭니다(`recovery`). 이 검사는 테이블을 원래 시작 지점과 비교하므로, Stop 전에 한 변경이라도 그
    Start를 거부합니다.
- **MySQL: Debezium이 파싱하지 못하는 DDL 문은, 소스 서버의 어디에서 실행되든 CDC를 멈춤.** Debezium은
  binary log의 모든 DDL 문을 자신의 MySQL 문법(Debezium 2.7)으로 파싱합니다 — 캡처 대상만이 아니라 서버의
  모든 데이터베이스와 테이블에 대해, 저장 루틴·트리거·뷰·계정 문까지 포함해 — 무엇이든 필터링하기 전에
  그렇게 하며, 도구의 설정은 파싱하지 못하는 문을 건너뛰지 않고 멈춥니다. 소스 커넥터는 그 문 바로 앞에서
  멈추고 그 위치는 문을 지나 앞으로 나아갈 수 없습니다: Retry CDC도, Stop CDC 후의 Start CDC도 다시 그
  문에서 멈춥니다. CDC 단계가 원인을 알려 줍니다 — Start CDC 오류에서, 또는 시작된 뒤라면 그 커넥터의
  Pipeline health 행과 오류 알림에서
  ([§4.6](04-cdc-and-dsql-constraints.md#파이프라인-헬스와-change-flow)) — 그리고 따옴표로 묶인 값을 가린
  채 그 문의 앞부분을 보여 줍니다.
  - **해결 방법.** CDC를 Stop한 다음, 그 문 이후의 시작 지점을 CDC에 주세요: 모든 테이블에 대해
    **"Drop & reload"로 Re-run Full Load**를 실행하거나(Full load + CDC; 그 문이 캡처 대상 테이블을
    바꿨다면 먼저 1단계 Evaluation과 Schema Conversion을 다시 실행), **Manual** → **Fetch current
    position** → **Use this start point**를 선택하세요. 그러면 CDC가 멈춘 지점과 그 지점 사이의 변경은
    복제되지 않으므로, 그 테이블들을 "Drop & reload"로 다시 로드하고(CDC only: Aurora DSQL에서 바로잡기)
    CHECKSUM으로 검증하세요. 다시 로드하면 시작 지점이 Automatic으로 돌아가므로, Start CDC 전에 Manual을
    다시 선택하세요. 커넥터 로그의 *binlog reader near position*을 시작 지점으로 쓰지 마세요: 그것은
    실패한 문이 시작되는 위치입니다.
  - **시작 지점을 옮겨도 도움이 되지 않는 경우.** 소스로부터 Debezium의 스키마를 만드는 Start — 첫
    Start, 새 시작 지점에서의 Start, 없어진 스키마 히스토리를 다시 만드는 Start — 는 서버의 모든 테이블의
    현재 정의를 읽습니다. 어떤 테이블의 정의 자체가 문법이 지원하지 않는 구문을 쓰면, 그런 Start는 시작
    지점과 상관없이 매번 그 정의를 읽다가 멈춥니다: 소스에서 그 정의를 바꾼 뒤 Stop CDC와 Start CDC를
    하거나, 그 서버는 Full load only로 마이그레이션하세요. 다시 실행되는 문(스키마 마이그레이션 도구,
    예약된 작업)은 실행될 때마다 CDC를 멈춥니다.
  - **건너뛰기 옵션 없음.** 도구는 Debezium의 `schema.history.internal.skip.unparseable.ddl`을 제공하지
    않습니다: 건너뛴 문은 Debezium의 테이블 정의를 조용히 틀리게 남길 수 있어 이후의 행이 오류 없이 잘못된
    컬럼에 쓰일 수 있고, 같은 설정은 Debezium이 재생하지 못하는 컬럼 이름 변경(위)도 건너뛰어 그 멈춤을
    잘못된 값으로 바꿉니다. PostgreSQL 소스는 영향을 받지 않습니다: 그 변경 스트림에는 DDL이 없습니다.
- **캐스케이드 FK 동작은 CDC로 복제되지 않음.** 서버 측 `ON DELETE/UPDATE CASCADE`(및 `SET NULL`/
  `SET DEFAULT`) 동작은 소스 엔진 *내부*에서 발생해 변경 스트림에 잡히지 않을 수 있으므로 CDC가 적용할 수
  없습니다 — 소스가 캐스케이드한 자식 행이 타깃에 orphan으로 남습니다(MySQL 소스의 경우 InnoDB가 binlog에
  남기지 않은 채 실행). 이 orphan은 조용히 어긋나 버리는 대신 **cut over
  시점의 외래 키 적용(`ADD CONSTRAINT`)을 차단**하며(Validation의 **고아 검사**가 먼저 잡아냄), 그래서
  cut over 전에 정리하게 됩니다. Evaluation이 해당 테이블을 표시합니다.
- **CDC는 배포되어 있는 동안 과금됨.** 스트리밍 파이프라인(MSK Serverless + MSK Connect, NAT 게이트웨이를
  만들었다면 그것까지)은 실행되는 내내 비용이 발생합니다. Cut over 후에는 cdc-stack을 내리세요. Full Load만
  수행하면 스트리밍 인프라가 프로비저닝되지 않습니다.
- **타깃 컬럼이 정확히 담을 수 없는 값은 변환하지 않고 거부합니다.** 대상은 다음과 같습니다:
  - `boolean` 컬럼에 들어가는 0이나 1이 아닌 숫자(`2`를 담은 MySQL `TINYINT(1)`, 또는 CDC에서 정수 타입으로
    바뀐 PostgreSQL `boolean`);
  - `numeric(p,s)` 컬럼이 유지하는 것보다 많은 소수 자릿수;
  - 정수 컬럼에 들어가는 소수.

  보통 Step 1 이후 소스 타입이 넓어졌거나 Schema Conversion에서 더 좁은 타입을 골랐을 때 생깁니다. Full Load는
  그 테이블을 중단합니다(테이블 전체 실패). CDC는 그 행을 데드레터로 보내고 배너를 표시합니다. 이전 버전의
  도구로 배포한 CDC 인프라는 삭제하고 다시 배포하기 전까지 여전히 바뀐 값을 씁니다. Schema Conversion에서
  값을 담을 수 있는 타입을 컬럼에 지정하고(**Replace**로 다시 적용) 리로드하거나, 소스 데이터를 정리하세요.
  정밀도 없는 `numeric` 타깃(Schema Conversion이 경고하는 Aurora DSQL 기본값 `numeric(18,6)`)은 설계상
  PostgreSQL의 제약 없는 `numeric`을 여전히 소수 6자리로 반올림합니다. 검사하지 않는 경우: `numeric` 컬럼에
  들어가는 `FLOAT`/`DOUBLE` 값.
- **PostgreSQL 17/18 소스 — 버전별 엣지 3가지(어느 것도 마이그레이션을 막지 않음).** PG 13–18을
  지원하며, 17·18에서 유의할 점이 3가지 있습니다:
  - **`interval`의 `infinity`/`-infinity`(PG17 신규).** Aurora DSQL은 PostgreSQL-16 호환이라 그
    `interval` 입력 파서가 이 값을 거부합니다. 따라서 *무한* `interval`을 담은 행은 Full Load 중
    **격리(quarantine)**되어(격리 카운트에 표시됨) Validation에서 count/checksum 불일치로 드러납니다 —
    조용히 변형되는 일은 없습니다. 유한 interval과 `timestamp`/`timestamptz`의 `infinity`는 영향받지
    않습니다. 해당 컬럼을 마이그레이션하기 전에 그런 행을 재모델링(예: 유한 sentinel 값)하세요.
  - **VIRTUAL 생성 컬럼(PG18의 기본 종류).** Aurora DSQL은 `GENERATED ALWAYS AS (expr) **STORED**`를
    지원하고 값을 스스로 유지하므로 STORED 컬럼은 **보존**됩니다 — 다시 계산할 것도, 드리프트도
    없습니다(쓰기에서 그 컬럼을 빼면 되고, Full Load와 CDC 싱크가 그렇게 합니다. DSQL은 값을 명시하면
    거부합니다). 제약은 **VIRTUAL** 쪽입니다: DSQL이 받지 않으므로 일반 컬럼으로 생성되고
    **MANUAL/LOSS**로 표시됩니다. Full Load가 소스의 계산값을 실체화하므로 타깃은 처음엔 정확하지만
    이후엔 아무것도 유지하지 않고, VIRTUAL 컬럼은 논리 복제 자체가 불가해 CDC도 나르지 않습니다.
    컷오버 전에 애플리케이션에서 다시 계산하세요. 어느 쪽이든 **apply 전에** 결정해야 합니다: DSQL은
    컬럼에서 식을 DROP할 수는 있어도 추가(ADD)할 수는 없습니다.
  - **CDC 커넥터 커버리지.** 번들된 Debezium PostgreSQL 커넥터의 테스트 매트릭스는 PG16까지입니다.
    `pgoutput` 스트리밍은 17/18에서도 동작하지만(논리 복제 프로토콜은 그대로이고 드라이버는 안정적인
    와이어 프로토콜로 연결됨), 17/18 CDC는 **best-effort**로 취급하고 프로덕션에 의존하기 전에 종단 간
    검증(Full Load 워터마크 → 슬롯 재개 → DSQL 싱크 → Validation 체크섬)을 하세요. PG18에서는
    `idle_replication_slot_timeout`가 `0`(비활성)인지도 확인해, 유휴 구간에 CDC 슬롯이 자동
    무효화되지 않도록 하세요.

---

## 6.3 배포 한계 (AWS 호스팅 형태)

- **단일 태스크 컨트롤 플레인.** 도구는 **하나의** ECS Fargate 태스크로 배포되며, 하나를 초과해 올리면
  안 됩니다. AWS 호스팅 배포에서는 **작업/세션 상태가 모두 관리형 S3 버킷**(`jobs/` + `sessions/` 프리픽스)에
  durable하게 보관되므로, 태스크가 교체돼도(배포, 크래시) 진행 상태를 잃지 않고 **재개**합니다 — 재개는
  마지막으로 기록된 상태 전이 지점으로 복귀하며, 진행 중이던 정확한 배치는 아닙니다. 단일 태스크 제한은
  상태 durability가 아니라 **단일 writer 오케스트레이션**(CDC 배포 조율 + 인메모리 세션 상태) 때문입니다;
  임시 스토리지의 로컬 SQLite는 로컬 개발 폴백일 뿐입니다.
- **앱 자체 인증 없음.** 앱은 ALB의 **선택적 Cognito** 게이트에 의존합니다. 인터넷에 노출되는 배포라면
  Cognito를 켜세요. 배포 템플릿은 Cognito 없이 인터넷에 노출하는 안전하지 않은 조합을 막습니다.
- **자격증명은 세션별로만, 메모리에만 존재.** 절대 디스크에 남지 않으며, 재시작하면 소스/타깃 연결 정보를
  다시 입력해야 합니다.

---

**다음:** [7. 성능과 튜닝 →](07-performance-and-tuning.md)
