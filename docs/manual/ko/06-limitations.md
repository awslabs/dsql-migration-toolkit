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
| **행 수준 보안 없음**(PostgreSQL 소스) | 테이블의 정책(`CREATE POLICY`, `ENABLE`/`FORCE ROW LEVEL SECURITY`)이 Aurora DSQL에는 존재하지 않음: 컷오버 후에는 테이블을 읽거나 쓸 수 있는 모든 역할이 모든 행에 접근함. | 각 정책의 `USING`/`WITH CHECK` 식과 함께 **UNSUPPORTED**(`PG_ROW_LEVEL_SECURITY`)로 표시 — 조건을 애플리케이션에서 강제하세요. 소스는 `row_security = off`로 읽으므로(`pg_dump`와 같음), 정책이 적용되는 마이그레이션 사용자는 일부 행만 복사·검증하는 대신 해당 테이블에서 거부됩니다. 슈퍼유저, `BYPASSRLS` 역할, 또는 테이블 소유자(`FORCE ROW LEVEL SECURITY`가 아닐 때)여야 하며, 사전 점검이 이를 확인합니다(`ROW_SECURITY`). |
| **시퀀스와 identity 컬럼은 `bigint`** | Aurora DSQL은 둘 다 지원하지만 시퀀스는 항상 `bigint`이고 `CACHE`는 1 또는 65536 이상이며, identity는 `bigint` 컬럼에만 허용됨(적용 후에는 컬럼 타입을 바꿀 수 없음). | 어떤 컬럼도 소유하지 않는 PostgreSQL 시퀀스는 Schema Conversion이 만들지 않음: Evaluation이 실행할 `CREATE SEQUENCE`와 함께 **MANUAL**(`PG_STANDALONE_SEQUENCE`)로 나열하며, 컷오버 때 소스의 최종 값 다음으로 옮겨야 합니다(`setval`) — 도구는 동기화하지 않습니다. 키가 아닌 `serial`/identity 컬럼은 값 생성을 잃습니다(`NON_KEY_SEQUENCE`). 유지하려면 적용 전에 타깃 DDL에서 컬럼을 `bigint … AS IDENTITY`로 선언하세요. |
| **네이티브 파티셔닝 없음** | DSQL이 직접 자동 분산. | 파티션 테이블 **MANUAL**(파티셔닝 제거). 테이블 상속도 없음: PostgreSQL 상속(`INHERITS`) 부모와 각 자식 테이블은 별개의 테이블로, 선택한 것만 마이그레이션됨. Full Load와 Validation, CDC 모니터의 `MAX(pk)` 확인은 부모 자신의 행만 읽으므로(`FROM ONLY`, CDC가 캡처하는 것과 같음) 모든 행이 자식에 있는 부모(파티셔닝 용도의 상속)는 행을 하나도 로드하지 않고, Aurora DSQL에서 그 테이블은 자신의 행만 가짐. 자식 테이블에는 자체 기본 키가 필요함(PostgreSQL은 기본 키를 상속하지 않음). 사전 요구사항 검사는 모든 마이그레이션 유형에서 그런 부모마다 경고하고(*Inheritance child tables are migrated as tables of their own*: 부모 자신의 행만 옮기며, 각 자식은 선택한 경우 별개의 테이블로 마이그레이션됨), 이 마이그레이션의 Start CDC가 그 부모를 스냅샷하게 되면(예: 모든 CDC only Start, 다시 스냅샷, Manual 시작 지점; [§4.3](04-cdc-and-dsql-constraints.md)) 실패로 판정하며, Start CDC는 그 스냅샷을 거부하고 CDC only에서는 Deploy CDC infrastructure도 거부함(*CDC cannot snapshot a table that has inheritance child tables*): Debezium의 스냅샷이 자식 테이블의 행을 부모 테이블에 복사하고, CDC는 그 행을 결코 바꾸지 않기 때문. 도구가 소스를 아예 읽지 못하면 Start CDC는 거부하지 않고 PostgreSQL 검사가 실행되지 않았다고 경고함([§4.3](04-cdc-and-dsql-constraints.md)). CDC only에서는 부모의 선택을 해제하고(각 자식은 여전히 별개의 테이블로 마이그레이션됨), Full load + CDC에서는 CDC가 스냅샷 없이 Full Load의 스냅샷 지점에서 시작함. |
| **열당 1 MiB 한도** | 열 하나의 값이 **1 MiB**를 넘으면 저장할 수 없습니다 — 타입과 무관합니다(MySQL `TEXT`/`BLOB`/`JSON`, PostgreSQL `text`/`bytea`/`json`): `54000 ERROR: maximum column size exceeded`. **행** 전체는 **2 MiB**가 한도이고(`maximum row size exceeded`), 쓰기 트랜잭션 하나는 10 MiB입니다. ([Aurora DSQL 쿼터](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)) | 한도를 넘는 값은 행 단위로 에러 로그(Full Load) / **DLQ**(CDC)로 **격리(quarantine)**됩니다. Kafka 메시지 상한을 넘길 수 있는 컬럼은 **캡처 단계에서 제외**해야 합니다. 초과 값을 담을 수 있는 컬럼은 `OVERSIZED_LOB`로 표시됩니다: 바이너리/문서 컬럼(MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`)은 LOSS — 그 타입을 고른 것이 큰 값을 담겠다는 의도이기 때문입니다. 길이 제한 없는 PostgreSQL `text`/`varchar`는 RECOMMENDATION — 그것이 평범한 짧은 문자열의 관용적 표기이기도 하기 때문입니다. CHECK로 리터럴 집합에 제한된 컬럼은 아예 표시되지 않습니다. |
| **`DECIMAL` 정밀도 > 1000** | 더 높은 정밀도 미지원(DSQL 문서상 최대 정밀도 1000, 스케일 -1000..1000). | Evaluation은 **UNSUPPORTED**(`NUMERIC_PRECISION`)로 표시하지만, 변환된 DDL을 적용하면 Schema Conversion이 **최대값으로 clamp**(손실)하며 경고. 어느 소스도 이를 넘을 수 없으므로(MySQL은 `DECIMAL`을 65,30으로 제한하고, PostgreSQL 자체의 `numeric` 한도도 같은 1000) 안전장치일 뿐입니다. |
| **공간/geometry 타입** | MySQL 소스에서는 `bytea`로 대체(원본 WKB가 Full Load·CDC 전 구간에서 그대로 보존됨); PostgreSQL 소스는 자동 대체 **안 함**. | MySQL 소스: 변환기가 각 컬럼을 `bytea`로 자동 대체하고, 평가기가 해당 테이블을 검토 대상 **MANUAL**로 표시. PostgreSQL 소스: geometric 타입(point/line/lseg/box/path/polygon/circle)은 `text`(정규 텍스트)로 타입이 바뀌며, 대체 타입이 있는 다른 PG 전용 타입도 같습니다 — 배열 → `jsonb`, network `inet`/`cidr`/`macaddr`, `xml`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange → `text`, `money` → `numeric(23,4)`, enum → `CHECK`가 붙은 `text`. 각각 Evaluation(`PG_UNSUPPORTED_TYPE`)과 Schema Conversion 양쪽에서 **MANUAL**: 데이터는 옮겨지고 애플리케이션이 새 타입에 맞춰야 함. 대체 타입이 없는 composite 타입, domain, pgvector는 **UNSUPPORTED**로 남으며 DDL 적용 전에 재모델링해야 함. |
| **FULLTEXT / SPATIAL 인덱스** | 미지원. | 생성하지 않으며 **MANUAL**(`UNSUPPORTED_INDEX_TYPE`): 테이블과 데이터는 옮겨지고, 그 인덱스가 하던 검색은 데이터베이스 밖(예: Amazon OpenSearch Service)으로 옮겨야 함. |
| **테이블당 ≤ 255 컬럼; 데이터베이스당 테이블 ≤ 1,000, 스키마 ≤ 10, 뷰 ≤ 5,000** | 초과 시 `CREATE`가 실패(컬럼은 `54011`, 데이터베이스 한도는 `54000`). | **UNSUPPORTED**(`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT` / `SCHEMA_COUNT_LIMIT` / `VIEW_COUNT_LIMIT`). 데이터베이스 단위 개수는 타깃 클러스터에 이미 있는 것을 포함하며, 하나라도 넘으면 준비도 점수가 **Blocked**가 됩니다. |
| **테이블당 ≤ 24 인덱스** (PK가 포함되므로 보조 인덱스는 ≤ 23; MySQL 소스는 64) | 초과분 `CREATE INDEX ASYNC`가 Full Load로 모든 행을 쓴 **뒤에** 실패. | 계획 단계에서 **MANUAL**(`TOO_MANY_INDEXES`)로 표시하고 Schema Conversion에도 동일 노트. |
| **기본 키·인덱스의 컬럼 수 ≤ 8** (MySQL 소스는 16) | 더 넓은 키는 error 54011로 실패. | PK가 넓으면 **UNSUPPORTED**(`CREATE TABLE` 자체가 거부되어 아무 데이터도 적재되지 않음), 인덱스가 넓으면 **MANUAL**(`TOO_MANY_KEY_COLUMNS`). 변환은 적재 후 반드시 실패할 DDL을 내보내지 않고 그 인덱스를 **생략**하며, 어떤 인덱스인지 노트에 명시. |
| **키 합산 크기 ≤ 1 KiB** (PK 각각, 인덱스 각각) | DDL이 아니라 **`INSERT`/`UPDATE` 시점의 값**에 대해 검사 — 따라서 실제 키가 너무 긴 행만 실패(error 54000 `key size too large`). | *선언된* 폭이 이를 넘을 수 있으면 변환이 경고(**MANUAL** 권장)하며, MySQL 소스의 utf8mb4 기준 문자당 4바이트로 계산해 해당 키와 최악의 크기를 명시. 차단하지 않음 — 선언은 넓어도 실제 값이 짧으면 정상 마이그레이션되므로. |
| **클러스터당 하나의 DB** | DSQL은 다중 DB가 아니라 스키마로 구성. | MySQL 소스의 경우 각 MySQL 데이터베이스가 자동으로 자체 DSQL 스키마가 되며(`RECOMMENDED`로 표시), 데이터베이스가 10개를 넘으면 위의 스키마 한도를 초과합니다(`SCHEMA_COUNT_LIMIT`). PostgreSQL 소스는 하나의 데이터베이스에 연결하며, 그 비시스템 스키마가 이미 DSQL 스키마로 직접 매핑되므로(정규화된 `schema.table`) 다중 DB 통합 단계가 없음. |
| **`TRUNCATE` 없음; 트랜잭션당 DDL 한 개; 낙관적 동시성** | 기존 단일 노드 RDBMS(MySQL 또는 PostgreSQL 소스)와 다른 쓰기/DDL 의미. | 투명하게 처리: TRUNCATE 대신 DROP+재생성, 단일 DDL 단위, 필요한 곳마다 `40001` 재시도. |
| **IAM 토큰 인증(비밀번호 없음); 단기 토큰** | 정적 DB 비밀번호 없음. | 도구(와 CDC 싱크)가 IAM 토큰을 자동 발급·갱신. |

> **스키마 설계의 결론:** 대형 blob, 서버 측 로직, 매우 높은 정밀도의 숫자는 DSQL에
> 맡기기 *전에* 데이터베이스 밖의 애플리케이션으로 옮기세요. 어떤 객체에 이 작업이 필요한지는
> Evaluation이 정확히 알려 줍니다.

---

## 6.2 마이그레이션 프로세스 한계

- **새 CDC 시작 지점에는 새 CDC 토픽이 먼저 필요합니다.** 이 장에서 CDC에 더 새로운 시작 지점을 주기
  위해 **Re-run Full Load를 "Drop & reload"로**(또는 **Re-run all tables**) 실행하거나 더 새 **Manual**
  시작 지점을 쓰라고 안내하는 곳은 모두, 이 마이그레이션의 CDC 스택이 스트리밍한 적 있는 동안 — Start
  뒤에 멈췄거나, 커넥터가 그대로 있거나, CloudFormation이 Start를 롤백한 동안 — 그 시작 지점이
  **거부됩니다**. Full Load 단계가 재실행을 거부하고(Full load + CDC), MySQL 소스에서는 Start CDC가
  마이그레이션 유형과 상관없이 스택이 마지막으로 시작한 지점보다 새 시작 지점을 거부하므로, **Full load
  only**를 거쳐 돌아와도 우회되지 않습니다. 먼저 **Delete CDC infrastructure** 후 다시 배포하고, 그다음
  재실행(또는 Manual 지점), 그다음 **Start CDC**
  ([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)). 그러지 않으면 그 스택의 싱크가 새 시작
  지점보다 오래된 변경을 다시 적재한 테이블 위에 다시 적용하고, 그동안 소스는 CDC가 멈춘 곳과 새 지점
  사이의 변경을 건너뜁니다. Full load only에는 CDC 단계가 없으므로, 그 유형을 거쳐 가는 중이라면 먼저
  마이그레이션 유형을 바꾼 뒤 CDC 단계에서 삭제하고 다시 배포하세요. MySQL에서 CloudFormation이 롤백한
  첫 Start 뒤에는 롤백이 스택의 파라미터를 되돌리므로, Start CDC는 다음 Start를 앱이 기록한 그 Start의
  시작 지점과, 앱을 재시작한 뒤에는 스택의 CloudFormation 이벤트에 아직 기록된 그 지점과 비교합니다: 그
  지점보다 뒤의 시작 지점을 거부하고(그 지점에서 다시 시작하는 것은 거부하지 않음), 둘 다 그 지점을 주지
  못하면(예를 들어 앱 안에서 Kafka를 시드하는 단일 EC2 호스트에서 재시작한 뒤) 그 스택에서 모든 시작
  지점을 거부합니다.
  PostgreSQL 소스의 Start CDC에는 이런 거부가 없습니다:
  스택 자체의 복제 슬롯에서 이어 가고 Full load only 실행은 그 슬롯을 그대로 두므로, 스택의 소스 커넥터가
  스트리밍했다면 CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만
  빈틈이 없고, 다시 적재한 테이블은 수렴합니다. 그렇지 않으면 Debezium이 먼저 선택한 모든 테이블을
  스냅샷하므로, 다시 적재한 뒤 그 스냅샷 전에 소스에서 삭제된 행은 타깃에 남고, Validation이 EXTRA 행으로
  보고합니다. **Reload specific tables**와 **Retry unfinished tables**는 원래 시작 지점을 유지하므로
  영향을 받지 않습니다. Manual 지점은 새 시작 지점입니다: CDC가 멈춘 곳과 그 지점 사이의 변경은 새
  스택에서도 복제되지 않으므로, 아래 각 해결 방법이 말하는 대로 그 테이블을 다시 적재하거나 바로잡고
  CHECKSUM으로 검증하세요.
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
    모든 복제 대상 테이블은 사용 가능한 **`REPLICA IDENTITY`**를 가져야 함: `DEFAULT`에는 `DEFERRABLE`이
    아닌 유효한 기본 키가 필요하고(PostgreSQL은 키가 삭제된 뒤에도 `DEFAULT`를 유지하므로 검사는 소스의
    카탈로그를 읽음), `USING INDEX`에는 유효한 고유 인덱스가 필요하며, `FULL`은 어떤 테이블에도 동작하지만
    PostgreSQL 18에서 생성 컬럼이 있는 테이블은 예외임: PostgreSQL 18은 publication이 게시하지 않는 생성
    컬럼이 identity에 들어간 게시 테이블의 `UPDATE`/`DELETE`를 거부하고(CDC의 publication은 생성 컬럼을
    게시하지 않음), 그래서 거기서는 `FULL`도, 생성 컬럼이 있는 키도 쓸 수 없음. 파티션된 테이블은 모든 파티션에
    하나씩 필요함.
    없으면 publication에 들어간 뒤 그 테이블의 모든
    `UPDATE`/`DELETE`가 publisher에서 실패하므로 CDC 사전 요구사항 검사가 실패로 판정하고 Full Load는
    소스에서 아무것도 바꾸기 전에 멈춤(publication도 슬롯도 만들지 않음). 이 거부는 모두 그 원인을
    해결하는 방법을 알려 줌(사전 점검은 테이블 이름을 따옴표로 감싼 구문도 함께 보여 줌): 새 기본 키는
    `DEFAULT`만 고치며, `DEFERRABLE` 키는 `NOT DEFERRABLE`로 다시 추가하거나 `FULL`로 고치고(PostgreSQL은 이를 그 자리에서
    바꿀 수 없음), `NOTHING`이나 인덱스가 사라진 `USING INDEX`는
    `ALTER TABLE … REPLICA IDENTITY`(`FULL`, 또는 기본 키가 있으면 `DEFAULT`)로만 바뀌며, PostgreSQL 18이 거부할
    테이블에는 어떤 해결 방법도 `FULL`을 권하지 않음. binlog 보존에 해당하는 개념은
    없습니다: 도구가 Full Load 정합성 지점에서 **논리 복제 슬롯**과 **정확히 마이그레이션 대상 테이블만을
    범위로 하는 publication**을 생성하므로, WAL은 보존 기간이 아니라 슬롯에 의해 고정됩니다(`wal_status`를
    관찰하세요 — 비활성 슬롯은 소스 디스크를 가득 채울 수 있음). 복제 슬롯 / `max_wal_senders` 여유는
    점검됩니다(차단하지 않음).
  - **PostgreSQL 소스, 재키잉(re-keyed) 테이블:** CDC가 재키잉하는 테이블 — Schema Conversion의
    **Composite key**, 또는 소스 기본 키 밖의 컬럼을 더하도록 편집한 타깃 키 — 은 Start 시점만이 아니라
    **CDC 실행 내내 테이블과 모든 파티션에 `REPLICA IDENTITY FULL`**이 필요합니다(나중에 만든 파티션은
    `DEFAULT`로 시작). FULL 미만이면 그 테이블의 `DELETE`가 오류도 dead letter도 없이 Aurora DSQL에 행을
    남길 수 있습니다 — Aurora DSQL이 소스 기본 키에 대한 테이블의 고유 인덱스를 다 만들기 전까지, 그리고 이전
    버전이 배포한 CDC 인프라에서는 언제나 — 그리고 같은 경우에 추가된 키 컬럼을 바꾸는 `UPDATE`는 dead letter로
    격리되고 이전 행이 남습니다. 사전 점검이 Start를
    차단합니다. PostgreSQL 18에서 생성 컬럼이 있는 재키잉 테이블은 FULL을 유지할 수 없으므로(그 `UPDATE`/`DELETE`가
    소스에서 실패함, 위 참고) 사전 점검, Full Load, 그리고 Full Load 후 선택한 테이블의 로드가 쓰기 전에
    거부합니다: Schema Conversion에서 그 테이블을 **Keep source PK**로 되돌리세요(그런 테이블에는 아래 CDC 모니터
    알림도 FULL 대신 그 해결 방법을 알려 줌: 그다음 테이블을 리로드하거나, CDC only에서는 **Start over**로 다시
    마이그레이션). Data Migration, Validation 또는 Cut over 단계가 열려 있는 동안 도구는 최대 2분마다 이를
    다시 읽습니다: CDC 모니터에 빨간 알림이 나타나고, 다시 FULL이 되고 테이블이 CHECKSUM으로 일치할 때까지
    Cut over가 보류됩니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버) 참고). 앱 재시작
    후에는 재확인에 소스 비밀번호가 다시 필요합니다(Connect 단계에서 다시 연결). 두 번의 재확인 사이에
    내려갔다가 되돌아온 경우는 보이지 않으므로 FULL을 계속 유지하세요.
- **기본 키 변경과 행 사이를 옮겨 가는 고유 값.** Debezium은 기본 키의 `UPDATE`를 이전 키의 delete와 새 키의
  insert로 복제합니다. 둘의 키가 달라 서로 다른 파티션에 도착할 수 있고(파티션이 둘 이상인 토픽: 캡처 테이블이
  8개 미만이거나, 큰 테이블에 2개 또는 4개가 주어진 경우) 어느 순서로든 적용될 수 있습니다. 한 행에서 다른
  행으로 넘어가는 고유 값(다른 계정으로 옮긴 이메일 주소)도 마찬가지입니다. 기본 키 외에 고유 인덱스가 있는
  테이블에서는 그러면 insert가 이전 행과 부딪히고, Aurora DSQL은 이를 즉시 거부합니다(`23505`). 이 버전이 배포한
  CDC 인프라(싱크 플러그인 v53 이상)는 그 변경과 그 파티션의 이후 변경을 모두, 다른 행의 변경이 적용될 때까지
  최대 5분(그 변경을 보류한 싱크 태스크의 일시적 실패가 더 나중이면 그때부터 다시 세므로 그 태스크가 겪는
  Aurora DSQL 장애는 그 시간을 다 써 버리지 않음. 다른 싱크 태스크에서 다른 행의 변경만 늦추는 장애는 다 써
  버릴 수 있음) 동안 보류하며,
  그동안 다른 파티션은 계속 흐르고 그 테이블의 Stream lag이 대기를 보여 줍니다. 5분은 보류된 변경마다
  적용되므로 한 파티션의 충돌은 하나씩 차례로 기다립니다: 따라잡기 중에 그런 충돌이 여럿이면 그 파티션은
  하나에 약 5분씩 늦어집니다. 재키잉되지 않은 테이블에서는 같은 배치나 그 파티션의 다음 배치에 그 변경을 완전히
  대체하는 같은 행의 이후 변경이 있으면 그 변경은 대신 건너뜁니다.
  다음 경우에는 그래도 dead-letter 처리됩니다(*a
  unique constraint rejected the row*, 인덱스 이름 포함): 값을 비우는 변경이 5분 넘게 뒤처진 경우(긴 따라잡기
  중 파티션 간 지연이 크게 벌어질 때), 재시작이나 스트림 재읽기(CDC 인프라 삭제 후 재배포 뒤의 재읽기 등) 후
  재생된 행의 이전 변경이 그 행의 이후 변경이 이미 쓴 값과 부딪히고 그 이후 변경이 같은 파티션에서 배치 하나보다
  더 뒤에 있는 경우, 타깃이 스트림 밖에서 이미 그 값을 가진 경우, 다른 행의 건너뛴 변경이 비웠을 값이 그 변경에
  필요한데 그 행의 대체 변경이 그 파티션에서 그 뒤에 줄 서 있거나, 그 대체 변경 자신이 이 변경의 행이 가진 값을
  기다리는 경우(이때는 둘 다 dead-letter 처리됨. 여러 행을 거쳐 넘어가는 고유 값: 예컨대 b가 아직 다른 행에 있을
  때 X가 a를 b로 바꾸고, 이어서 Z가 z를 a로 바꾸고, X가 다시 바뀌면 X의 첫 변경은 건너뛰므로 X는 마지막 변경이
  적용될 때까지 타깃에서 a를 가짐), PostgreSQL 트랜잭션이
  `DEFERRABLE` 고유 제약 아래에서 행끼리 고유 값을 맞바꾼 경우(두 변경이 서로를 기다리므로 둘 다
  dead-letter 처리됨: 5분 뒤, 두 변경이 같은 파티션에 있으면 약 10분 뒤), 재키잉(Composite key) 테이블에서 그
  행 자신의 이후 변경이 이미 충돌을 무의미하게 만든 경우(기본 키 변경과 그 되돌림, 재생된 이전 이미지,
  반영된 `XX000` 커밋의 재실행: 거기서는 아무것도 건너뛰지 않으므로 5분을 기다린 뒤, 타깃은 결국 맞게 되는데도
  dead-letter 처리됨), 그리고 재키잉 테이블의 복합 키 원인(아래와 §4.6). 이전 버전이 배포한 CDC 인프라에서는
  insert가 즉시 dead-letter 처리되고 그 뒤 이전 행의 delete가 행을 지우므로, 테이블을 리로드할 때까지 그 행이
  빠집니다. 어느 쪽이든 Cut over는 그 테이블이 CHECKSUM으로 일치할 때까지 보류하며, 일치하면 CDC 단계가 그
  테이블을 *Verified by CHECKSUM*으로 표시합니다. 열린 소스 스키마나 REPLICA IDENTITY 발견 사항이 아직
  Cut over에서 그 테이블을 붙잡고 있으면 그 알림이 그 발견 사항을 알려 주며, 그 발견 사항은 자체의 해결
  방법을 따릅니다(dead letter 때문에 리로드할 필요는 없음). 복구: Stop CDC, Full Load 단계에서 그 테이블을
  **Drop & reload**로 리로드(CDC only: Aurora DSQL에서 행을 직접 고침), Start CDC, 그다음 검증. 소스가 그
  `UNIQUE` 인덱스를 삭제하거나 완화해도 같은 dead letter가 생깁니다(CDC는 DDL을 복제하지 않음): Aurora
  DSQL에서도 그 인덱스를 삭제하고, Full load + CDC에서는 리로드가 인덱스를 다시 만들지 않도록 먼저 Step
  1(Evaluation)을 다시 실행하세요. 대기 동작을 받으려면 CDC 인프라를 삭제(Delete CDC
  infrastructure)하고 다시 배포하세요. 이 작업은 스트림 위치도 삭제합니다(다음 항목 참고). 두 엔진 모두
  해당합니다.
- **이전 버전이 배포한 CDC 인프라의 재키잉 테이블**은 변경 레코드를 계속 복합 키로 키잉합니다(Start CDC
  로그가 그렇다고 알려 줍니다). 그러면 선행 키 컬럼의 `UPDATE` — 또는 행을 다른 선행 값으로 delete한 뒤 다시
  insert하는 것 — 는 이전 키의 delete와 새 키의 insert로 복제되며, 토픽의 파티션이 둘 이상이면(캡처 테이블이
  8개 미만이거나, 큰 테이블에 2개 또는 4개가 주어진 경우) 어느 순서로든 적용될 수 있습니다. insert가 먼저
  오면 dead letter로 격리되고(*a unique constraint rejected the row*) delete가 행을 지우므로, 테이블을 리로드할
  때까지 그 행이 빠집니다. Cut over는 그 테이블이 CHECKSUM으로 일치할 때까지 보류합니다. 이 테이블들을 소스
  기본 키로 키잉하려면 CDC 인프라를 삭제(Delete CDC infrastructure)하고 다시 배포하세요. 이 작업은 스트림
  위치도 삭제합니다: MySQL에서는 Start CDC가 Full Load 워터마크부터 binlog를 다시 읽고(CDC only: 소스의 현재
  위치에서 시작하므로 Aurora DSQL이 마지막으로 적용한 변경 이후의 변경은 복제되지 않음), PostgreSQL에서는 복제
  슬롯이 drop되므로 다음 Start가 모든 테이블을 다시 읽으며(Full load + CDC: Start CDC 대화 상자가
  제시하면 **"Re-snapshot every table instead"**를 선택) 그 사이 소스에서 삭제된 행은 Aurora DSQL에 남습니다.
  [13장](13-excluding-tables-and-columns.md#집합이-잠기는-이유)의 "삭제 후 다시 CDC 시작"과 "CDC only 스택을
  삭제하는 대가"를 참고하세요. 두 엔진 모두 해당합니다. (다른 고유 인덱스가 있는 모든 테이블의 기본 키
  변경: 위 항목.)
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
  `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` — 그리고 PostgreSQL에서 파티션 drop·detach·truncate나
  이미 행이 든 파티션의 attach — 은 변경 이벤트를 전혀 만들지 않으며, 남은(또는 누락된) 행은 Validation에서만 보입니다. 컷오버까지 파티션 관리를 멈추세요. 이전 버전의 도구에서 업그레이드할 때 이미 스트리밍 중이던 MySQL 파이프라인은
  Stop CDC → Start CDC를 한 번 거친 뒤부터 `TRUNCATE`를 감지합니다.
- **MySQL: CDC 시작 지점과 그 지점을 쓰는 Start 사이의 스키마 변경.** 새 시작 위치를 시드하는 MySQL
  Start는 Debezium을 `snapshot.mode=recovery`로 실행합니다: Full Load 워터마크나 Manual binlog 시작
  지점에서의 첫 Start(세션에 Full Load 워터마크가 없는 MySQL CDC only의 첫 시작은 Manual 지점이 필요),
  그리고 **Delete CDC infrastructure** 후 다시 배포한 뒤의 첫 Start — 이때는 **원래** 워터마크부터 다시
  읽습니다. 스트리밍한 적 있는 스택에서는 **Re-run Full Load**의 새 시작 지점이나 더 새 Manual 지점이
  인프라를 다시 배포할 때까지 거부됩니다(§6.2 첫 항목). `recovery`에서 Debezium은 테이블의
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
    재생할 뿐입니다. 시작 지점을 변경 이후로 옮기세요. CDC를 Stop하세요. 스트리밍한 적 있는 스택(Stop CDC
    후의 재시작, 또는 실행 도중 멈춘 커넥터)이면 먼저 **Delete CDC infrastructure** 후 다시 배포하세요:
    새 빈 토픽을 얻으며, 그때까지 Full Load 단계는 새 시작 지점을, Start CDC는 더 새 시작 지점을
    거부합니다. 소스 인벤토리를 새로 읽은 뒤(1단계
    Evaluation 재실행) Schema Conversion에서 바뀐 테이블을 갱신해 다시 로드가 새 정의로 테이블을 만들고
    읽게 한 다음 — DDL을 직접 편집했거나 키 전략을 고른 테이블은 그 편집이 유지되므로 다시 하거나
    초기화하고, 삭제·이름 변경된 컬럼은 먼저 Aurora DSQL에서 삭제하거나 그 테이블을 Replace로 다시
    적용해야 합니다(Drop & reload는 새 정의에 없는 대상 컬럼을 지우지 않으려고 해당 테이블을 거부합니다) —
    **"Drop & reload"로 Re-run Full Load**를 실행하세요(기본값 *Append*는 기존 행을 유지하므로 그 사이의
    UPDATE/DELETE가 사라지고, *Reload specific tables*와 *Retry*는 원래 워터마크를 유지합니다). 새 실행의
    워터마크는 변경 이후이며, 새 스택에서의(또는 스택의 첫) Start CDC가 그 지점에서 시작합니다. CDC only
    실행에는 다시 돌릴 Full Load가 없으므로, 바뀐 테이블을 Aurora DSQL에서 갱신한 다음 — 스트리밍한
    스택이면 Delete CDC infrastructure 후 다시 배포한 뒤 — **Manual**, **Fetch current position**, **Use
    this start point**를 선택하고 Start CDC 하세요. 그러면 그 지점 이전에 캡처 대상 테이블에 생긴 변경은 복제되지 않습니다: "Drop &
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
- **MySQL에서 `gtid_mode=ON`이면: CDC는 시작 지점의 GTID 셋 다음부터 시작.** Debezium은 소스의
  `gtid_mode`가 정확히 `ON`일 때만 GTID로 위치를 잡습니다: 그때는 시작 지점과 함께 기록된 GTID 셋에 없는
  모든 트랜잭션을 스트리밍하고, 시작 지점의 binlog `file:position`은 무시합니다(`OFF`, `OFF_PERMISSIVE`,
  `ON_PERMISSIVE`이면 `file:position`을 씁니다). 그래서 `ON`인 소스에서는 GTID 셋이 곧 시작 지점이며,
  Start CDC는 먼저 `gtid_mode`를 읽고 — Start 대화 상자에서, 그리고 무엇이든 바꾸기 전에 다시(여기서 거부하면
  Start가 *Start CDC blocked: …*로 실패하고, CDC 단계가 실패한 Start 아래에 해결 방법을 포함한 알림 전체를
  보여 줌. 활동 로그도 알림 전체를 기록하며, 로그 한 줄보다 긴 알림은 `(1/2) …`, `(2/2) …`처럼 이어지는 줄로
  기록함) — 쓸 수 없는 시작 지점은 거부합니다:
  - **GTID 셋이 없음.** Debezium이 MySQL에 보관 중인 바이너리 로그 전체를 다시 보내게 해, 시작 지점 이전의
    변경을 다시 적용합니다. **Fetch current position**은 위치와 GTID 셋을 함께 기록합니다. 직접 입력한
    위치라면 소스가 정확히 그 위치에서 실행을 마친 GTID 셋(예: 덤프가 기록한 것)도 함께 입력하세요. 버전
    0.1.565 이하가 저장한 Manual 시작 지점은 GTID 셋 없이 복원됩니다(그 버전들은 가져온 셋을 그 뒤에 입력한
    어떤 위치 옆에도 남겨 두었습니다): 다시 가져오거나, 정확히 그 위치에서 실행을 마친 셋을 다시 입력하세요.
  - **`gtid_mode`가 `ON`이 아닐 때 기록된 시작 지점**(그런 지점이나 GTID 셋이 없는 시작 지점에서
    시작한 스택의 Stop CDC 후 재시작, 그리고 마지막 시작이 `gtid_mode`가 `ON`이 아닐 때였던 스택의
    모든 시작 포함): 다음 항목을 참조하세요.
  - **소스가 실행한 적 없는 GTID 셋**(다른 서버에서 읽었거나 잘못 입력함), **또는 소스가 그 이후의
    바이너리 로그를 이미 삭제한 셋**: CDC에 새 시작 지점을 주세요 — **"Drop & reload"로 Re-run Full
    Load**, 또는 **Manual** → **Fetch current position** → **Use this start point**(그러면 두 지점 사이의
    캡처 대상 테이블 변경은 복제되지 않으므로 "Drop & reload"로 다시 로드하고 — CDC only: Aurora DSQL에서
    바로잡고 — CHECKSUM으로 검증하세요) — 보존 기간이 끝나서였다면 먼저 binlog 보존을 늘리세요. 대신
    GTID 셋을 소스와 대조하는 검사 자체가 실패했다면(연결이 끊김, 읽기 시간 초과) 시작 지점에 문제가 있다고
    알려진 것은 없습니다: 도구에서 소스에 연결할 수 있는지 확인한 뒤 Start CDC를 다시 실행하세요.
  - 시작 지점에 GTID 셋이 없거나 `gtid_mode`가 `ON`이 아닐 때 기록됐는데, 또는 Start CDC가 그 모드를
    기록해야 하는데 — `gtid_mode` 기록이 있는 스택의 모든 시작 전, 그리고 스택의 첫 시작 전 —
    **`gtid_mode`를 읽을 수 없음**(이 세션에 소스 연결이 없음 — 비밀번호는 메모리에만 보관되므로
    앱을 재시작하면 지워집니다 — 또는 읽기 실패): Connect 단계에서 소스 연결을 다시 테스트한 뒤
    Start CDC를 다시 실행하세요.

  Manual 카드에서 **Fetch current position** 후 위치를 고치면 — 위치를 비워 두거나 잘못 입력해도 —
  가져온 GTID 셋은 버려지고(가져온 위치에서 읽은 것이므로), GTID 셋을 고치면 기록된 테이블 정의가
  버려지므로 위의 시작 지점 검사는 경고만 합니다. 입력하는 GTID 셋은 MySQL GTID 셋이어야 합니다
  (`<server UUID>:<interval>`을 쉼표로 구분, 예: `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100`):
  Start CDC는 그 밖의 텍스트 — 함께 복사된 따옴표나 `SET` 문 등 — 를 모든 `gtid_mode`에서 거부합니다.
  Debezium이 그 값을 커밋하는 모든 오프셋에 써 넣고 다음 재시작 때 멈추기 때문입니다.
- **MySQL: `gtid_mode`는 Full Load 전에 정하고 cut over까지 유지.** `gtid_mode`가 `ON`이 아닐 때 기록된
  시작 지점으로는 소스가 `ON`이 된 뒤 CDC를 시작할 수 없습니다: MySQL은 GTID 없이 쓰인 변경을 GTID로
  보낼 수 없으므로 그 변경에서 커넥터를 멈추거나, 오류 없이 건너뜁니다. Start CDC는 그런 시작을 거부하며,
  그런 지점이나 GTID 셋이 없는 시작 지점에서 시작한 스택의 Stop CDC 후 재시작도 거부합니다
  (`file:position`으로 스트리밍했으므로 이어 갈 GTID 셋이 없음). Start CDC는 시작 오프셋을 스택이 마지막
  Start 때 남긴 자체 기록에서 읽으므로, 세션에 더 이상 시작 지점이 없어도(연결한 스택, 초기화한 세션)
  마찬가지입니다. 그 시작 지점을 유지하려면 cut over까지 `gtid_mode`를 `ON_PERMISSIVE`로
  되돌리거나(그러면 CDC는 `file:position`으로 스트리밍), CDC에 새 시작 지점을 주세요 — Stop CDC 후의
  재시작(스트리밍한 적 있는 스택)이라면 먼저 **Delete CDC infrastructure** 후 다시 배포한 뒤: **"Drop &
  reload"로 Re-run Full Load**, 또는 **Manual** → **Fetch current position** → **Use this start point**
  후 그 사이에 바뀐 테이블을 "Drop & reload"로 다시 로드하고(CDC only: Aurora DSQL에서 바로잡기) Start
  CDC 한 다음 CHECKSUM으로 검증하세요. CDC가 스트리밍하는 동안 `gtid_mode`를 바꾸면 커넥터의 다음 자동 재시작 때
  적용되어 커넥터를 멈추거나 오류 없이 변경을 건너뛸 수 있고, 도구는 이를 막을 수 없습니다: 먼저 Stop
  CDC 하세요. Start CDC는 또 소스 커넥터를 시작할 때마다 소스의 `gtid_mode`를 CDC 스택에 기록합니다(Stop
  CDC는 그 기록을 유지합니다). `ON`인 소스에서는 마지막 시작이 `gtid_mode`가 `ON`이 아닐 때였던 스택의
  시작을 모두 거부합니다 — Stop CDC 후 재시작은 물론 새 시작 지점도 거부하는데, Start CDC는 스택이
  마지막으로 시작한 지점보다 새 시작 지점을 받지 않고(§6.2 첫 항목), 오프셋 시더는 새 지점이 커밋된
  오프셋보다 뒤가 아니면 커밋된 오프셋을 그대로 두기 때문입니다. 그런 스택은 그 시작부터
  `file:position`으로 스트리밍했으므로 커밋된 오프셋의 GTID 셋은 거기서 더 나아가지 않았습니다. 재시작이라면
  cut over까지 `gtid_mode`를 `ON_PERMISSIVE`로 유지하고, 아니면 **Delete CDC infrastructure** 후 다시
  배포하고 Start CDC 하세요: 새 스택에는 커밋된 오프셋이 없으므로 CDC는 시작
  지점에서 정확히 시작합니다 — 가지고 있는 시작 지점이 기록될 때 이미 `gtid_mode`가 `ON`이었고 그 뒤로
  계속 `ON`이었다면 그 지점에서, 아니면 위와 같이 새 시작 지점에서. 마지막 시작이 `gtid_mode`가
  `ON`일 때였던 스택은 세션에 더 이상 그 시작 지점이 없어도 GTID로 이어 갑니다. 이 기록은 CDC가
  스트리밍하는 동안의 `gtid_mode` 변경(위와 같이 먼저 Stop CDC 하세요)도, 실패해 롤백된 Start도 알 수
  없습니다(그러면 스택은 그 이전 시작의 기록을 유지하므로, `gtid_mode`를 바꾸기 전에 다시 시작하세요).
  버전 0.1.566 이하가 마지막으로 시작한 스택에는 기록이 없으며, 어떤 시작이 모드를 읽기 전까지는 — 그
  시작을 포함해 — Start CDC는 기록된 것 — 시작 지점과 함께 기록된 모드, 그리고 스택이 마지막으로 시작한
  시작 오프셋(과 그 GTID 셋) — 만으로 판단할 뿐, 그 뒤 스택이 어떻게 스트리밍했는지는 알 수 없습니다.
  그래서 그런 스택에서는 `gtid_mode`를 다시 `ON`으로 바꾼 뒤의 Start가 다음 경우에 거부되지 않습니다:
  `ON`일 때 기록된 지점에서 시작했지만 `ON`이 아닌 동안(예: GTID 없이 쓰인 변경으로 실패한 뒤의
  `ON_PERMISSIVE`) 스트리밍한 경우, `ON`이 아닐 때 입력한 GTID 셋에서 시작한 경우, 마지막 시작에 줄 시작
  지점이 없었던 경우(시작 지점이 없는 세션에서의 재시작). 이런 스택은 가지고 있는 GTID 셋 다음부터 이어
  가며 이미 적용한 변경을 다시 보내거나, GTID 없이 쓰인 변경에서 멈추거나, 오류 없이 건너뜁니다. 그런
  소스는 cut over까지 `ON`이 아닌 상태로 두거나, 새 스택에서(먼저 Delete CDC infrastructure 후 다시
  배포) 위와 같이 CDC에 새 시작 지점을 주세요. 버전 0.1.565 이하가 기록한 시작 지점에는 모드가 기록되지
  않습니다: 그 뒤 `gtid_mode`를 `ON`으로 바꿨다면 새 시작 지점을 정하세요(스트리밍한 스택이면 먼저 Delete
  CDC infrastructure 후 다시 배포).
- **MySQL에서 `gtid_mode=ON`이면: 버전 0.1.565 이하가 시작한 파이프라인.** 그 버전들은 Debezium이 시작
  기준으로 삼는 GTID 셋을 비웠기 때문에, `gtid_mode=ON`인 소스에서 MySQL이 모든 Start, Stop 후의 모든
  Start, 커넥터의 모든 자동 재시작 때마다 보관 중인 바이너리 로그 전체를 다시 보냈습니다. 쓰기는
  수렴했지만(싱크는 멱등) 예전 값이 다시 적용됐고, 고유 키 충돌은 데드레터가 될 수 있었으며, 보관된 로그
  어디에든 — 시작 지점 이전이라도 — 파싱할 수 없는 DDL이 있으면 모든 Start가 멈췄습니다. 실행 중인
  파이프라인은 **Stop CDC** → **Start CDC**를 한 번 할 때까지 그 동작을 유지하며, 그 뒤에는 멈춘 곳에서
  이어 갑니다(Stop 때 싱크가 적용하지 않은 변경은 CDC 토픽에서 오므로 토픽이 그 변경을 아직 보존하는
  동안만 빈틈이 없습니다 — [§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)). Manual 지점(CDC only 포함)에서 시작한 파이프라인은 그 Start가 *the source has purged
  binary logs CDC's GTID set does not cover*로 실패할 수 있습니다: 그 버전들은 시작 시점에 소스가 이미
  삭제한 트랜잭션을 빼고 GTID 셋을 기록했지만, 소스에는 CDC가 멈춘 이후의 변경이 모두 남아 있을 수
  있습니다. Stop CDC 후 cut over까지 소스의 `gtid_mode`를 `ON_PERMISSIVE`로 바꾸고(RDS와 Aurora에서는
  파라미터 그룹 변경) Start CDC를 하세요: Debezium이 멈춘 곳부터 `file:position`으로 재개하며, CDC가
  멈출 때 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안(MSK 보존 기간)은 빈틈 없이 이어
  갑니다(binlog 보존 기간도 끝났다면 그 Start는 대신 실패합니다). 또는 메시지대로 CDC에 새 시작 지점을
  주세요: 이 스택은 스트리밍했으므로 먼저 **Delete CDC infrastructure** 후 다시 배포한 뒤, 모든 테이블에
  대해 **"Drop & reload"로 Re-run Full Load**(CDC only: **Manual** → **Fetch current position** → **Use
  this start point** 후 그 사이에 바뀐 테이블을 Aurora DSQL에서 바로잡기), 그다음 Start CDC 하고
  CHECKSUM으로 검증하세요. Manual 지점에 GTID 셋이 없으면 그런 Start는 무엇이든 바꾸기 전에 거부됩니다: 메시지를 따르세요.
  그런 Start가 중간에 실패했다면 토픽에 다시 보내진 예전 변경이 남아 있고 싱크가 그 일부를 적용했을 수
  있습니다: **Delete CDC infrastructure** 후 다시 배포하고, 모든 테이블에 대해 **"Drop & reload"로
  Re-run Full Load**를 한 뒤(CDC only: **Manual** → **Fetch current position** → **Use this start
  point** 후 Aurora DSQL에서 테이블을 바로잡기 — 싱크가 다시 적용한 행은 다시 쓰일 때까지 예전 값을
  유지합니다) Start CDC를 하고 CHECKSUM으로 검증하세요. 이전 버전으로 `gtid_mode=ON` 소스에서 실행한
  파이프라인은 cut over 전에 CHECKSUM으로 검증하세요.
- **MySQL: Debezium이 파싱하지 못하는 DDL 문은, 소스 서버의 어디에서 실행되든 CDC를 멈춤.** Debezium은
  binary log의 모든 DDL 문을 자신의 MySQL 문법(Debezium 2.7)으로 파싱합니다 — 캡처 대상만이 아니라 서버의
  모든 데이터베이스와 테이블에 대해, 저장 루틴·트리거·뷰·계정 문까지 포함해 — 무엇이든 필터링하기 전에
  그렇게 하며, 도구의 설정은 파싱하지 못하는 문을 건너뛰지 않고 멈춥니다. 소스 커넥터는 그 문 바로 앞에서
  멈추고 그 위치는 문을 지나 앞으로 나아갈 수 없습니다: Retry CDC도, Stop CDC 후의 Start CDC도 다시 그
  문에서 멈춥니다. CDC 단계가 원인을 알려 줍니다 — Start CDC 오류에서, 또는 시작된 뒤라면 그 커넥터의
  Pipeline health 행과 오류 알림에서
  ([§4.6](04-cdc-and-dsql-constraints.md#파이프라인-헬스와-change-flow)) — 그리고 따옴표로 묶인 값을 가린
  채 그 문의 앞부분을 보여 줍니다. 시작 지점 이후의 문만 CDC를 멈춥니다(버전 0.1.565 이하는
  `gtid_mode=ON`인 소스에서 보관된 바이너리 로그를 처음부터 다시 읽었으므로 그 이전의 문도 CDC를 멈출 수
  있었습니다 — 위 참조).
  - **해결 방법.** CDC를 Stop한 다음 **Delete CDC infrastructure** 후 다시 배포하세요: 스택이
    스트리밍했으므로 새 시작 지점에는 새 빈 토픽이 필요합니다(그때까지 Full Load 단계는 새 시작 지점을,
    Start CDC는 더 새 시작 지점을 거부합니다). 그다음 그 문 이후의 시작 지점을 CDC에 주세요: 모든
    테이블에 대해 **"Drop & reload"로 Re-run Full Load**를 실행하거나(Full load + CDC; 그 문이 캡처 대상
    테이블을 바꿨다면 먼저 1단계 Evaluation과 Schema Conversion을 다시 실행), **Manual** → **Fetch
    current position** → **Use this start point**를 선택하세요. 그러면 CDC가 멈춘 지점과 그 지점 사이의
    변경은 복제되지 않으므로, 그 테이블들을 "Drop & reload"로 다시 로드하고(CDC only: Aurora DSQL에서
    바로잡기) CHECKSUM으로 검증하세요. 다시 로드하면 시작 지점이 Automatic으로 돌아가므로, 새 스택에서
    **Start CDC** 하기 전에 Manual을 다시 선택하세요. 커넥터 로그의 *binlog reader near position*을 시작 지점으로 쓰지 마세요: 그것은
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
    **격리(quarantine)**되어(격리 카운트에 표시됨) Validation에서 count/checksum 불일치로 드러납니다.
    하지만 CDC는 이 값을 조용히 **0** interval(`00:00:00`)로 옮깁니다: Debezium의 드라이버가 `infinity`를
    0으로 읽으며, CHECKSUM 검증에서만 드러납니다. 무한 `interval` 행은 해당 컬럼을 마이그레이션하거나 CDC를
    시작하기 전에 재모델링(예: 유한 sentinel 값)하세요. 유한 interval은 영향받지 않고,
    `date`/`timestamp`/`timestamptz`의 `infinity`/`-infinity`도 마찬가지입니다: Full Load와 CDC(싱크 플러그인
    v54 이상, 스트리밍이든 스냅샷이든) 모두 `infinity`로 저장합니다. 이전 플러그인은 그런 `timestamptz` 값을
    처음 만나면 싱크 태스크를 멈추고, `timestamp`의 `+infinity`를 294247년으로 저장했으며, `-infinity`와
    date infinity는 dead-letter 처리했습니다: **Delete CDC infrastructure** 후 다시 배포하고, 태스크가 멈추지
    않았더라도 그런 테이블을 다시 적재(Drop & reload, 또는 다음 Start가 제안하는 재스냅샷)한 뒤 CHECKSUM으로
    검증하세요.
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
