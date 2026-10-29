# 6. 한계

_언어: [English](../en/06-limitations.md) | **한국어** | [日本語](../ja/06-limitations.md)_

> **이전:** [5. Validation](05-validation.md)

계획에 반드시 반영해야 하는 **실제로 강제되는** 한계입니다. 대부분은 Aurora DSQL 자체에서
비롯됩니다(분산형이라 수평 확장에 맞지 않는 기능을 의도적으로 제외했기 때문), 일부는 배포 방식에서
비롯됩니다. 어느 것도 런타임에서 갑자기 튀어나오지 않습니다. 도구가 **Evaluation** 단계에서
플래그하고, 처리할 수 있는 것은 처리하며, 처리할 수 없는 것은 **요란하게** 실패시킵니다.

---

## 6.1 Aurora DSQL 기능 한계 (스키마가 이에 맞아야 함)

> **스키마 설계의 결론:** 대형 blob, 서버 측 로직, 매우 높은 정밀도의 숫자는 DSQL에 맡기기 *전에*
> 데이터베이스 밖의 애플리케이션으로 옮기고, 연쇄(cascade) 팬아웃은 DSQL의 트랜잭션당 한도 안에
> 두세요. 어떤 객체에 이 작업이 필요한지는 Evaluation이 정확히 알려 줍니다.

아래는 Aurora DSQL이 지원하지 않거나 상한을 두는 항목입니다. Evaluation이 각각을 표시하며(괄호 안이
finding), 어떻게 대처할지는 [§2.4](02-evaluation-and-schema-conversion.md#24-evaluation-항목-참조)에
있습니다.

**마이그레이션을 막는 것**(**Unsupported**)

| Aurora DSQL | 어떻게 되나, 무엇을 하나 |
|---|---|
| 기본 키가 필요합니다(`NO_PRIMARY_KEY`) | 기본 키가 없는 테이블은 마이그레이션할 수 없습니다. Full Load는 기본 키로 페이지를 나눠 읽고 CDC는 키 기준으로 변경을 적용하기 때문입니다. |
| 트리거, 저장 프로시저, 함수, 스케줄 이벤트가 없습니다(`TRIGGER_UNSUPPORTED`, `PROC_PLPGSQL`, `EVENT_UNSUPPORTED`) | 서버 측 로직은 옮겨지지 않습니다. 애플리케이션으로 재구현하세요(이벤트 → EventBridge/Lambda). |
| 행 수준 보안이 없습니다(PostgreSQL 소스, `PG_ROW_LEVEL_SECURITY`) | 컷오버 후에는 테이블을 읽거나 쓸 수 있는 모든 역할이 모든 행에 접근하므로 각 정책의 조건을 애플리케이션에서 강제하세요. 마이그레이션 사용자는 정책을 우회할 수 있어야 하며(슈퍼유저, `BYPASSRLS`, 또는 테이블 소유자 — `FORCE ROW LEVEL SECURITY`일 때는 제외), 그렇지 않으면 일부 행만 복사하는 대신 해당 테이블이 거부됩니다(사전 점검 `ROW_SECURITY`). |
| 테이블당 최대 255컬럼, 데이터베이스당 테이블 1,000개·스키마 10개·뷰 5,000개(`TOO_MANY_COLUMNS`, `TABLE_COUNT_LIMIT`, `SCHEMA_COUNT_LIMIT`, `VIEW_COUNT_LIMIT`) | 초과 시 `CREATE`가 실패합니다(컬럼은 `54011`, 데이터베이스 한도는 `54000`). 데이터베이스 단위 개수는 타깃 클러스터에 이미 있는 것을 포함하며, 하나라도 넘으면 준비도 점수가 **Blocked**가 됩니다. |
| 기본 키·인덱스의 컬럼 수는 최대 8개(MySQL 소스는 16개; `TOO_MANY_KEY_COLUMNS`) | 더 넓은 기본 키는 거부되어(error `54011`) 아무 데이터도 적재되지 않습니다. 더 넓은 인덱스는 적재 후 반드시 실패할 DDL을 내보내는 대신 생략되고 **Review needed**로 표시됩니다. |

**컷오버 후 동작이 달라지는 것**(**Review needed**)

| Aurora DSQL | 어떻게 되나, 무엇을 하나 |
|---|---|
| 소스 `CHECK` 제약(`CHECK_CONSTRAINT_DROPPED`) | 변환되지 않고(예: MySQL `CHECK`, 8.0.16+) DDL에서 제외됩니다. DSQL 호환 `CHECK`를 다시 추가하거나 규칙을 애플리케이션에서 강제하세요. enum용으로 생성하는 `CHECK … IN (...)`은 영향이 없지만, 나중에 소스 enum에 추가된 값은 그 `CHECK`를 바꿀 때까지 `23514`로 데드레터됩니다([§4.2](04-cdc-and-dsql-constraints.md#cdc-중-소스-스키마-드리프트-감지처리)). |
| `EXCLUDE` 제약, `WITHOUT OVERLAPS` 키, `PERIOD`가 없습니다(PostgreSQL 소스, `PG_EXCLUSION_CONSTRAINT`, `PG_TEMPORAL_KEY`, `PG_FK_PERIOD`) | Aurora DSQL에는 셋 다 없으므로 컷오버 후에는 그런 규칙에 어긋나는 행이 받아들여집니다. `EXCLUDE` 제약과 `FOREIGN KEY … PERIOD`는 생성되지 않고, `WITHOUT OVERLAPS` 키는 일반 키로 생성되어 정확한 중복만 거부합니다. 규칙은 애플리케이션에서 강제하거나(동시 쓰기들이 함께 갱신하는 행에서 충돌하게 하세요) 설계를 바꾸세요. `WITHOUT OVERLAPS` 기본 키가 있는 큰 테이블을 적재하기 전에는 소스에서 그 키 컬럼들에 btree 인덱스를 추가하세요. |
| 지연 가능한 유니크 키가 없습니다(PostgreSQL 소스, `PG_DEFERRABLE_KEY`) | Aurora DSQL은 지연 가능한 유니크 키를 지원하지 않으므로 키가 즉시 검사됩니다. 두 행이 잠시 같은 키 값을 갖게 하는 쓰기(값 맞바꾸기, 번호 다시 매기기)가 소스에서는 받아들여졌더라도 실패할 수 있습니다. CDC를 쓰면 `DEFERRABLE` 기본 키는 `REPLICA IDENTITY`도 되지 못합니다. `REPLICA IDENTITY DEFAULT`인 테이블은 키를 `NOT DEFERRABLE`로 다시 만들거나 테이블을 `REPLICA IDENTITY FULL`로 설정할 때까지 CDC 사전 요구사항 검사가 실패로 판정합니다. |
| 시퀀스와 identity 컬럼은 `bigint`입니다(`PG_STANDALONE_SEQUENCE`, `NON_KEY_SEQUENCE`) | 시퀀스는 항상 `bigint`이고 `CACHE`는 1 또는 65536 이상이며, identity는 `bigint` 컬럼에만 허용됩니다(적용 후에는 컬럼 타입을 바꿀 수 없음). 어떤 컬럼도 소유하지 않는 시퀀스는 만들어지지 않고 키가 아닌 `serial`/identity 컬럼은 값 생성을 잃습니다. 유지하는 방법은 [§2.4](02-evaluation-and-schema-conversion.md#24-evaluation-항목-참조)에 있습니다(컬럼은 적용 전에 타깃 DDL에서 `bigint … AS IDENTITY`로 선언). |
| 테이블 상속이 없습니다(PostgreSQL 소스) | PostgreSQL 상속(`INHERITS`) 부모와 각 자식 테이블은 별개의 테이블로, 선택한 것만 마이그레이션됩니다. Full Load와 Validation은 CDC가 캡처하는 것과 같이 부모 자신의 행만 읽으므로 모든 행이 자식에 있는 부모(파티셔닝 용도의 상속)는 행을 하나도 로드하지 않고, 자식 테이블에는 자체 기본 키가 필요합니다(PostgreSQL은 기본 키를 상속하지 않음). 사전 요구사항 검사는 그런 부모마다 경고하고, Start CDC는 그 스냅샷을 거부합니다(*CDC cannot snapshot a table that has inheritance child tables*, [§4.3](04-cdc-and-dsql-constraints.md)). CDC only에서는 부모의 선택을 해제하세요(각 자식은 여전히 별개의 테이블로 마이그레이션됩니다). Full load + CDC에서는 CDC가 스냅샷 없이 Full Load의 스냅샷 지점에서 시작합니다. |
| 공간 타입과 PostgreSQL 전용 타입(`SPATIAL_TYPE`, `PG_UNSUPPORTED_TYPE`) | MySQL 소스의 공간 컬럼은 `bytea`가 됩니다(원본 WKB 보존). PostgreSQL에서 Aurora DSQL에 대응이 없는 타입은 값을 보존하는 대체 타입을 받으며(배열 → `jsonb`, `money` → `numeric(23,4)`, geometric·network·enum 등 나머지 → `text`) **Review needed**로 평가됩니다: 데이터는 옮겨지고 애플리케이션이 맞춥니다([§2.3](02-evaluation-and-schema-conversion.md#23-타입-매핑-참조)). 대체 타입이 없는 composite 타입, domain, pgvector는 **Unsupported**로 남습니다. |
| FULLTEXT·SPATIAL 인덱스(`UNSUPPORTED_INDEX_TYPE`) | 생성하지 않으며 **Review needed**로 표시됩니다. 테이블과 데이터는 옮겨지지만, 그 인덱스가 하던 검색은 데이터베이스 밖(예: Amazon OpenSearch Service)으로 옮겨야 합니다. |

**크기·개수 상한**

| Aurora DSQL | 어떻게 되나, 무엇을 하나 |
|---|---|
| 값은 열당 1 MiB, 행은 2 MiB, 쓰기 트랜잭션은 10 MiB(`OVERSIZED_LOB`) | 열 하나의 값이 **1 MiB**를 넘으면 타입과 무관하게 저장할 수 없습니다: `54000 ERROR: maximum column size exceeded`. 행이 **2 MiB**를 넘어도 저장할 수 없습니다(`maximum row size exceeded`; [Aurora DSQL 쿼터](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)). 한도를 넘는 값은 행 단위로 **격리(quarantine)**되고(Full Load는 에러 로그, CDC는 **DLQ**), Kafka 메시지 상한을 넘길 수 있는 컬럼은 **캡처 단계에서 제외**해야 합니다. 바이너리/문서 컬럼(MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`)은 `OVERSIZED_LOB`로 LOSS, 길이 제한 없는 PostgreSQL `text`/`varchar`는 RECOMMENDATION으로 표시됩니다. |
| 테이블당 인덱스 최대 24개: PK가 포함되므로 보조 인덱스는 23개(MySQL 소스는 64개; `TOO_MANY_INDEXES`) | Schema Conversion은 23개를 유지하고 나머지는 빼며 노트에 그 이름을 적습니다. 그래서 테이블은 그 인덱스 없이 적재되고, 그중 하나에 의존하던 쿼리는 스캔으로 대체됩니다. 계획 단계에서 **Review needed**로 표시됩니다: 적용하기 전에 유지할 인덱스를 고르세요. |
| 키 합산 크기 최대 1 KiB(기본 키마다, 인덱스마다) | DDL이 아니라 **`INSERT`/`UPDATE` 시점의 값**에 대해 검사되므로 실제 키가 너무 긴 행만 실패합니다(error `54000` `key size too large`). *선언된* 폭이 이를 넘을 수 있으면 변환이 경고하며(`utf8mb4`는 문자당 4바이트로 계산) 해당 키와 최악의 크기를 명시하지만, 차단하지는 않습니다. |

**설계상 다른 점**(도구가 처리)

| Aurora DSQL | 어떻게 되나, 무엇을 하나 |
|---|---|
| 외래 키는 강제되며 런타임 비용이 있습니다(`PG_FK_NOT_ENFORCED`, `PG_FK_PERIOD`) | 참조하는/참조되는 테이블에 대한 DML에는 **추가 읽기** 비용이 있고, 동시 충돌은 재시도 가능한 `40001`이며, `CASCADE`/`SET NULL`/`SET DEFAULT` 동작은 트랜잭션당 **3,000행** 한도에 포함됩니다(3,000행을 넘는 연쇄는 실패). 자식 행 수가 무제한일 수 있으면 `NO ACTION`/`RESTRICT`를 쓰세요. 도구는 적재 후 키를 `NOT VALID`로 적용하며([외래 키](02-evaluation-and-schema-conversion.md#외래-키)), 대신 키를 떼어 내고(`preserve_foreign_keys=False`) 앱에서 정합성을 강제할 수도 있습니다. PostgreSQL 키는 `MATCH FULL`과 `DEFERRABLE` / `INITIALLY DEFERRED`를 유지하며, `NOT ENFORCED`로 선언한 PostgreSQL 18 키는 Aurora DSQL이 강제하게 되므로 생성하지 않고 `FOREIGN KEY … PERIOD`도 생성하지 않습니다. |
| 네이티브 파티셔닝이 없습니다(`PARTITIONED_TABLE`, 권장사항) | Aurora DSQL이 직접 데이터를 분산하므로 Schema Conversion이 파티셔닝을 제거하고 파티션은 하나의 테이블이 됩니다(CDC 중에는 컷오버까지 파티션 관리를 중지). |
| `TRUNCATE` 없음, 트랜잭션당 DDL 한 개, 낙관적 동시성, IAM 토큰 인증(비밀번호 없음, 단기 토큰) | 도구가 모두 알아서 처리합니다: `TRUNCATE` 대신 DROP+재생성, 단일 DDL 단위, 필요한 곳마다 `40001` 재시도, IAM 토큰 자동 발급·갱신(CDC 싱크 포함). |

---

## 6.2 마이그레이션 프로세스 한계

### 시작 지점, 리전, 소스 사전 요구사항

- **새 CDC 시작 지점에는 새 CDC 토픽이 먼저 필요합니다.** 이 장에서 CDC에 더 새로운 시작 지점을 주기
  위해 **Re-run Full Load를 "Drop & reload"로**(또는 **Re-run all tables**) 실행하거나 더 새
  **Manual** 시작 지점을 쓰라고 안내하는 곳은 모두, 이 마이그레이션의 CDC 스택이 스트리밍한 적 있는
  동안 — Start 뒤에 멈췄거나, 커넥터가 그대로 있거나, CloudFormation이 Start를 롤백한 동안 — 그 시작
  지점이 **거부됩니다**. 먼저 **Delete CDC infrastructure** 후 다시 배포하고, 그다음 재실행(또는
  Manual 지점), 그다음 **Start CDC** ([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)). 그러지
  않으면 그 스택의 싱크가 새 시작 지점보다 오래된 변경을 다시 적재한 테이블 위에 다시 적용합니다.

  | | 동작 |
  |---|---|
  | **Full load + CDC** | Full Load 단계가 재실행을 거부합니다. |
  | **MySQL 소스** | Start CDC가 마이그레이션 유형과 상관없이 스택이 마지막으로 시작한 지점보다 새 시작 지점을 거부하므로, **Full load only**를 거쳐 돌아와도 우회되지 않습니다. |
  | **Full load only** | Full load only에는 CDC 단계가 없으므로, 그 유형을 거쳐 가는 중이라면 먼저 마이그레이션 유형을 바꾼 뒤 CDC 단계에서 삭제하고 다시 배포하세요. |
  | **PostgreSQL 소스** | PostgreSQL 소스의 Start CDC에는 이런 거부가 없습니다: 스택 자체의 복제 슬롯에서 이어 가고 Full load only 실행은 그 슬롯을 그대로 두므로, 스택의 소스 커넥터가 스트리밍했다면 CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만 빈틈이 없고, 다시 적재한 테이블은 수렴합니다. 그렇지 않으면 Debezium이 먼저 선택한 모든 테이블을 스냅샷하므로, 다시 적재한 뒤 그 스냅샷 전에 소스에서 삭제된 행은 타깃에 남고, Validation이 EXTRA 행으로 보고합니다. |
  | **Reload specific tables**, **Retry unfinished tables** | 원래 시작 지점을 유지하므로 이 거부는 적용되지 않습니다(CDC가 스트리밍하는 동안에는 모든 Full Load처럼 Stop CDC를 기다립니다). |

  Manual 지점은 새 시작 지점입니다: CDC가 멈춘 곳과 그 지점 사이의 변경은 새 스택에서도 복제되지
  않으므로, 아래 각 해결 방법이 말하는 대로 그 테이블을 다시 적재하거나 바로잡고 CHECKSUM으로
  검증하세요.

- **단일 리전 — 크로스 리전 마이그레이션 없음.** 도구는 DSQL을 지원하는 어느 리전에서나 동작하지만,
  **소스와 타깃은 동일 리전에 있어야 하며** 선택적 CDC 파이프라인은 단일 리전/VPC에서 실행됩니다.
  크로스 리전은 지원되지 않습니다.

- **CDC는 소스 측 사전 요건이 있음(엔진별).** 사전 점검 게이트가 이 조건이 충족될 때까지 **CDC를
  차단**합니다. **Full Load만 한다면 이 중 아무것도 필요 없습니다.** 설정 방법:
  [§1.6](01-setup.md#16-cdc를-위한-소스-준비).
  - **MySQL**: 바이너리 로깅이 **ROW 포맷, 전체 행 이미지**(full row image)로 켜져
    있고(`binlog_format=ROW`, `binlog_row_image=FULL`), **복제 권한**을 가진 사용자가 있어야
    합니다(소스가 `gtid_mode=ON`이 아니면 Full load + CDC 실행에는 `RELOAD`도 필요). 또한 Full Load
    워터마크 시점의 로그가 CDC가 시작될 때까지 남아 있도록 **binlog 보존 기간을 늘려야**
    합니다(Aurora MySQL은 기본적으로 24시간만 보존).
  - **PostgreSQL**: `wal_level=logical`, **복제 권한을 가진 사용자**, 소스는 standby/reader가 아니라
    클러스터의 **WRITER**, 그리고 복제 대상 테이블마다(파티션된 테이블은 모든 파티션마다) 사용
    가능한 **`REPLICA IDENTITY`**: `DEFAULT`에는 `DEFERRABLE`이 아닌 유효한 기본 키가 필요하고,
    `USING INDEX`에는 유효한 고유 인덱스가 필요하며, `FULL`은 어떤 테이블에도 동작하지만 PostgreSQL
    18에서 생성 컬럼이 있는 테이블은 예외임(PostgreSQL 18은 publication이 게시하지 않는 생성 컬럼이
    identity에 들어간 게시 테이블의 `UPDATE`/`DELETE`를 거부하고 CDC의 publication은 생성 컬럼을
    게시하지 않으므로, 거기서는 `FULL`도, 생성 컬럼이 있는 키도 쓸 수 없음). 없으면 CDC 사전
    요구사항 검사가 그 테이블을 실패로 판정하고 Full Load는 소스에서 아무것도 바꾸기 전에 멈추며, 각
    거부는 실행할 구문과 함께 해결 방법을 알려 줌. 도구가 만드는 **논리 복제 슬롯**이 보존 기간이
    아니라 WAL을 고정합니다 — `wal_status`를 관찰하세요, 비활성 슬롯은 소스 디스크를 가득 채울 수
    있습니다.

- **PostgreSQL 소스, 재키잉(re-keyed) 테이블:** CDC가 재키잉하는 테이블 — Schema Conversion의
  **Composite key**, 또는 소스 기본 키 밖의 컬럼을 더하도록 편집한 타깃 키 — 은 Start 시점만이
  아니라 **CDC 실행 내내 테이블과 모든 파티션에 `REPLICA IDENTITY FULL`**이 필요합니다(나중에 만든
  파티션은 `DEFAULT`로 시작). FULL 미만이면 그 테이블의 `DELETE`가 오류도 dead letter도 없이 Aurora
  DSQL에 행을 남길 수 있고, 추가된 키 컬럼을 바꾸는 `UPDATE`는 dead letter로 격리되고 이전 행이
  남습니다. 사전 점검이 Start를 차단합니다. PostgreSQL 18에서 생성 컬럼이 있는 재키잉 테이블은
  FULL을 유지할 수 없으므로(그 `UPDATE`/`DELETE`가 소스에서 실패함, 위 참고) 사전 점검, Full Load,
  그리고 Full Load 후 선택한 테이블의 로드가 쓰기 전에 거부합니다: Schema Conversion에서 그 테이블을
  **Keep source PK**로 되돌리세요(그런 테이블에는 아래 CDC 모니터 알림도 FULL 대신 그 해결 방법을
  알려 줌: 그다음 테이블을 리로드하거나, CDC only에서는 **Start over**로 다시 마이그레이션). Data
  Migration, Validation 또는 Cut over 단계가 열려 있는 동안 도구는 최대 2분마다 이를 다시 읽습니다:
  CDC 모니터에 빨간 알림이 나타나고, 다시 FULL이 되고 테이블이 CHECKSUM으로 일치할 때까지 Cut over가
  보류됩니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버) 참고). 앱 재시작 후에는
  재확인에 소스 비밀번호가 다시 필요합니다(Connect 단계에서 다시 연결). 두 번의 재확인 사이에
  내려갔다가 되돌아온 경우는 보이지 않으므로 FULL을 계속 유지하세요.

### 기본 키 변경과 고유 값

- **기본 키 변경과 행 사이를 옮겨 가는 고유 값(두 엔진 모두).** 기본 키의 `UPDATE`는 이전 키의
  delete와 새 키의 insert로 CDC에 도착하며, 파티션이 둘 이상인 토픽(캡처 테이블이 8개 미만이거나, 큰
  테이블에 2개 또는 4개가 주어진 경우)에서는 서로 다른 파티션에 도착해 어느 순서로든 적용될 수
  있습니다. 한 행에서 다른 행으로 넘어가는 고유 값(다른 계정으로 옮긴 이메일 주소)도 마찬가지입니다.
  기본 키 외에 고유 인덱스가 있는 테이블에서는 그러면 insert가 이전 행과 부딪히고, Aurora DSQL은
  이를 즉시 거부합니다(`23505`).
  - **이 버전이 배포한 CDC 인프라**(싱크 플러그인 v53 이상): 싱크는 그 변경과 그 파티션의 이후
    변경을 모두, 다른 행의 변경이 적용될 때까지 최대 **5분** 동안 **보류**하며, 그동안 다른 파티션은
    계속 흐르고 그 테이블의 Stream lag이 대기를 보여 줍니다. 재키잉되지 않은 테이블에서는 그 변경을
    완전히 대체하는 같은 행의 이후 변경이 있으면 그 변경은 대신 건너뜁니다.
  - **그래도 dead-letter 처리되는 경우**(*a unique constraint rejected the row*, 인덱스 이름 포함):
    값을 비우는 변경이 5분 넘게 뒤처진 경우(긴 따라잡기 중 파티션 간 지연이 크게 벌어질 때); 타깃이
    스트림 밖에서 이미 그 값을 가진 경우; 변경끼리 서로를 기다리는 경우(여러 행을 거쳐 넘어가는 고유
    값, 또는 `DEFERRABLE` 고유 제약 아래에서 고유 값을 맞바꾼 PostgreSQL 트랜잭션); 재시작이나
    스트림 재읽기 후 재생된 행의 이전 변경이 그 행의 이후 변경이 이미 쓴 값과 부딪히는 경우; 또는
    재키잉(Composite key) 테이블에서 그 행 자신의 이후 변경이 이미 충돌을 무의미하게 만든
    경우(타깃은 결국 맞게 되지만 그 변경은 5분을 기다린 뒤 dead-letter 처리됨).

  어느 쪽이든 Cut over는 그 테이블이 CHECKSUM으로 일치할 때까지 보류하며(일치하면 CDC 단계가 그
  테이블을 *Verified by CHECKSUM*으로 표시합니다). **복구:** Stop CDC, Full Load 단계에서 그
  테이블을 **Drop & reload**로 리로드(CDC only: Aurora DSQL에서 행을 직접 고침), Start CDC, 그다음
  검증. 소스가 그 `UNIQUE` 인덱스를 삭제하거나 완화해도 같은 dead letter가 생깁니다(CDC는 DDL을
  복제하지 않음): Aurora DSQL에서도 그 인덱스를 삭제하고, Full load + CDC에서는 리로드가 인덱스를
  다시 만들지 않도록 먼저 Step 1(Evaluation)을 다시 실행하세요.

- **이전 버전이 배포한 CDC 인프라의 재키잉 테이블**은 변경 레코드를 계속 복합 키로 키잉합니다(Start
  CDC 로그가 그렇다고 알려 줍니다). 그러면 선행 키 컬럼의 `UPDATE` — 또는 행을 다른 선행 값으로
  delete한 뒤 다시 insert하는 것 — 는 이전 키의 delete와 새 키의 insert로 복제되며 어느 순서로든
  적용될 수 있습니다. insert가 먼저 오면 dead letter로 격리되고(*a unique constraint rejected the
  row*) delete가 행을 지우므로, 테이블을 리로드할 때까지 그 행이 빠집니다(이전 버전이 배포한 CDC
  인프라에서는 모든 고유 충돌이 위의 대기 없이 즉시 dead-letter 처리됩니다). Cut over는 그 테이블이
  CHECKSUM으로 일치할 때까지 보류합니다. 이 테이블들을 소스 기본 키로 키잉하고 대기 동작을 받으려면
  CDC 인프라를 삭제(**Delete CDC infrastructure**)하고 다시 배포하세요. 이 작업은 스트림 위치도
  삭제합니다: MySQL에서는 Start CDC가 Full Load 워터마크부터 binlog를 다시 읽고(CDC only: 소스의
  현재 위치에서 시작하므로 Aurora DSQL이 마지막으로 적용한 변경 이후의 변경은 복제되지 않음),
  PostgreSQL에서는 복제 슬롯이 drop되므로 다음 Start가 모든 테이블을 다시 읽으며(Full load + CDC:
  Start CDC 대화 상자가 제시하면 **"Re-snapshot every table instead"**를 선택) 그 사이 소스에서
  삭제된 행은 Aurora DSQL에 남습니다.
  [13장](13-excluding-tables-and-columns.md#집합이-잠기는-이유)의 "삭제 후 다시 CDC 시작"과 "CDC
  only 스택을 삭제하는 대가"를 참고하세요.

### CDC 중 소스 스키마 변경

- **CDC는 데이터를 복제할 뿐 DDL은 복제하지 않음.** CDC 도중 소스에서 발생한 스키마 변경은 DSQL로
  **전파되지 않으므로** Aurora DSQL에 대응하는 변경을 직접 해야 합니다:

  | 소스 변경 | Aurora DSQL에서 할 일 |
  |---|---|
  | 컬럼 추가·이름 변경 | **Fix target schema…** |
  | 이제 NULL을 허용하는 컬럼 | `ALTER … DROP NOT NULL` |
  | 타입·키 변경 | CDC를 중지한 상태에서 테이블 다시 만들기(**Replace** + **Drop & reload**. CDC only에는 별도의 복구 방법이 있음). |

  CDC가 스트리밍하는 동안에는 Schema Conversion의 Apply가 Replace를 포함해 차단됩니다
  ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요) 참조).
  일부 변경은 dead letter 없이 CDC가 쓰는 값을 바꾸므로(nullable 컬럼 삭제, 타입 확장, 테이블 이름
  변경 등), CDC 모니터·Validation·Cut over 단계가 열려 있는 동안 도구가 캡처 중인 테이블의 정의를
  최대 5분에 한 번 다시 읽고 Step 1 인벤토리와 다르면 Cut over를 보류합니다
  ([§5.7](05-validation.md#57-소스-스키마-변경과-컷오버) 참고). 이 재확인이 보지 못하는 것:

  - 두 번의 읽기 사이에 되돌려진 변경, 또는 CDC 모니터·Validation·Cut over 페이지가 하나도 열려 있지
    않을 때 일어난 변경(다음에 열릴 때 보임);
  - 모양이 같은 `DROP TABLE` + `CREATE TABLE`(MySQL에는 비교할 안정적인 테이블 식별자가 없음), 또는
    값이 추가된 PostgreSQL enum(`ALTER TYPE … ADD VALUE`);
  - 기본값·주석·콜레이션·인덱스·생성 컬럼 식(비교하지 않음);
  - 도구가 그 테이블을 처음 확인했을 때 이미 Step 1 인벤토리에 들어 있던 변경(CDC 실행 전의 Step 1
    재실행은 기준이 됨);
  - 앱 재시작 후, Connect 단계에서 소스를 다시 연결하기 전까지의 모든 것(비밀번호는 메모리에만
    있음); 그리고 범위가 제한된 읽기로 끝내지 못하는 매우 큰 MySQL 5.7 카탈로그에서는 CDC 모니터와
    Cut over가 읽기 실패를 보고합니다.

- **PostgreSQL: CDC를 멈춘 동안의 DDL.** `pgoutput`에는 DDL이 없으므로 그 DDL을 재생하는 것이
  없습니다. 위의 소스 스키마 재확인은 바뀐 키나 NULL 규칙을 다음 읽기에서 보고하지만, CDC가 이어서
  진행될 때 밀린 변경이 이미 테이블의 *현재* 키와 NULL 규칙으로 적용되었을 수 있다는 것, 즉 그
  변경이 이전의 변경에도 이미 적용된 것처럼 처리되었을 수 있다는 것은 알려 주지 못합니다. 스트리밍
  중의 변경과 같은 후속 조치를 Aurora DSQL에서 하고, Cut over 전에 그 테이블을 CHECKSUM으로
  검증하세요.

- **소스 `TRUNCATE`는 CDC가 적용하지 않음.** Aurora DSQL에는 `TRUNCATE`가 없으므로 CDC는 그 이벤트를
  데드레터로 보내고 CDC 단계에 테이블 이름을 표시합니다. 남은 행을 지우고(Full Load + CDC는 Drop &
  reload, CDC only는 직접 삭제) 그 테이블이 CHECKSUM으로 일치할 때까지 Cut over가 보류됩니다
  ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요) 참조).
  MySQL의 `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` — 그리고 PostgreSQL에서 파티션
  drop·detach·truncate나 이미 행이 든 파티션의 attach — 은 변경 이벤트를 전혀 만들지 않으며,
  남은(또는 누락된) 행은 Validation에서만 보입니다. 컷오버까지 파티션 관리를 멈추세요.

- **MySQL: CDC 시작 지점과 그 지점을 쓰는 Start 사이의 스키마 변경은 스트림을 깨뜨립니다.** 이전
  binlog 위치에서 시작하는 Start — Full Load 워터마크나 Manual binlog 시작 지점에서의 첫 Start,
  그리고 **Delete CDC infrastructure** 후 다시 배포한 뒤의 첫 Start(**원래** 워터마크부터 다시 읽음)
  — 는 테이블의 **현재** 정의를 기준으로 binlog를 재생하며, 각 행의 값을 위치 순서로 컬럼에
  대응시킵니다. 그 사이의 컬럼 추가·삭제·이름 변경은 Start할 때마다 커넥터를 멈추고(CDC 단계가
  원인을 알려 줍니다 — Start CDC 오류에서, 또는 시작된 뒤에 멈췄다면 그 커넥터의 Pipeline health
  행에서, [§4.6](04-cdc-and-dsql-constraints.md#파이프라인-헬스와-change-flow)), 컬럼 수가 그대로인
  변경(길이나 DECIMAL 정밀도 이외의 타입 변경, 순서 변경, 삭제한 컬럼을 다른 컬럼으로 대체, 문자
  집합 변환)은 **아무 오류 없이** 밀리거나 NULL인 값을 씁니다.
  - **사전 검사.** Full Load 워터마크와 Manual 지점의 **Fetch current position**은 캡처 대상
    테이블마다 짧은 지문(컬럼 순서, 타입, NULL 규칙, 문자 집합, 기본 키)을 기록합니다. 그런 Start
    전에 Start CDC는 테이블을 이 지문과 비교하고, 바뀐 테이블을 하나하나 명시하며
    **거부**합니다(소스 연결이 필요하므로 그 때문에 거부하면 Connect 단계에서 소스 연결을 다시
    테스트하세요). 시작 지점에 지문이 없으면 — 이전 버전 도구가 기록한 지점, 직접 입력한 위치,
    정의를 읽지 못했던 지점 — **경고**만 합니다. `CHAR`, `VARCHAR`, `VARBINARY` 컬럼의 길이 변경이나
    같은 스케일에서의 `DECIMAL` 정밀도 변경은 똑같이 디코딩되므로 거부하지 않습니다. 소스 스키마
    재확인 ([§4.2](04-cdc-and-dsql-constraints.md#데드레터를-남기지-않는-변경-소스-스키마-재확인))은
    여전히 이를 알리며, Aurora DSQL 컬럼을 넓혀야 할 수 있습니다. 정수 타입 확장이나 `ENUM` 값
    추가는 여전히 거부됩니다.
  - **검사가 볼 수 없는 것.** CDC가 캡처하지 **않는** 테이블의 컬럼 이름 변경, 그리고 커넥터를
    만드는 동안이나 태스크가 자동 재시작되기 전에 한 변경(이전 위치에서 시작한 실행은 Stop CDC까지
    그 위험에 노출됩니다). 소스 스키마 재확인은 타입 변경과 다른 컬럼으로 대체된 컬럼을 알리고 Cut
    over를 보류하지만, 순서 변경과 문자 집합 변환은 어느 검사도 보지 못하므로, 그런 변경 뒤에는
    CHECKSUM으로 검증하세요.
  - **다시 Start, Retry CDC, Debezium의 "recovery mode"는 결코 도움이 되지 않습니다** — 같은 행을
    다시 재생할 뿐입니다. 시작 지점을 변경 이후로 옮기세요:
    - CDC를 Stop하세요. 스트리밍한 적 있는 스택(Stop CDC 후의 재시작, 또는 실행 도중 멈춘
      커넥터)이면 먼저 **Delete CDC infrastructure** 후 다시 배포하세요: 새 빈 토픽을 얻으며,
      그때까지 Full Load 단계는 새 시작 지점을, Start CDC는 더 새 시작 지점을 거부합니다.
    - 1단계 Evaluation을 다시 실행하고 Schema Conversion에서 바뀐 테이블을 갱신해 다시 로드가 새
      정의로 테이블을 만들고 읽게 하세요 — DDL을 직접 편집했거나 키 전략을 고른 테이블은 그 편집이
      유지되므로 다시 하거나 초기화하고, 삭제·이름 변경된 컬럼은 먼저 Aurora DSQL에서 삭제하거나 그
      테이블을 Replace로 다시 적용해야 합니다.
    - **Full load + CDC:** **"Drop & reload"로 Re-run Full Load**를 실행하세요(기본값 *Append*는
      기존 행을 유지하므로 그 사이의 UPDATE/DELETE가 사라집니다). 새 실행의 워터마크는 변경
      이후이며, 새 스택에서의(또는 스택의 첫) Start CDC가 그 지점에서 시작합니다. **CDC only:** 다시
      돌릴 Full Load가 없으므로, 바뀐 테이블을 Aurora DSQL에서 갱신한 다음 **Manual**, **Fetch
      current position**, **Use this start point**를 선택하고 Start CDC 하세요. 그러면 그 지점
      이전에 캡처 대상 테이블에 생긴 변경은 복제되지 않습니다: "Drop & reload"로 다시
      로드하거나(Full load + CDC) Aurora DSQL에서 바로잡고(CDC only), CHECKSUM으로 검증하세요. 다시
      로드하면 CDC 시작 지점이 Automatic으로 돌아가므로, Start CDC 전에 Manual을 다시
      선택하세요(가져온 지점은 유지됩니다).
  - **Stop CDC 후의 재시작은 다릅니다.** 기록된 위치에서 이어 가며 **기록된 스키마 히스토리를
    재생**하므로, CDC를 멈춘 동안의 DDL은 스트리밍 중의 변경처럼 순서대로 적용되고 같은 후속 조치가
    필요합니다
    ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)). 그
    히스토리가 없으면 Start가 그렇다고 알리며 실패합니다. 커넥터가 아직 남아 있으면 Stop CDC(또는
    **Clean up leftover connector**)로 먼저 제거하세요 — 남아 있는 커넥터에 대한 Start나 Retry CDC는
    그 스냅샷 모드를 유지하므로 같은 방식으로 실패합니다. 그러면 다음 Start가 같은 검사를 거친 뒤
    히스토리를 다시 만듭니다. 이 검사는 테이블을 원래 시작 지점과 비교하므로, Stop 전에 한
    변경이라도 그 Start를 거부합니다.

### MySQL `gtid_mode`

- **MySQL에서 `gtid_mode=ON`이면: CDC는 시작 지점의 GTID 셋 다음부터 시작.** Debezium은 소스의
  `gtid_mode`가 정확히 `ON`일 때만 GTID로 위치를 잡습니다: 그때는 시작 지점과 함께 기록된 GTID 셋에
  없는 모든 트랜잭션을 스트리밍하고, 시작 지점의 binlog `file:position`은 무시합니다(`OFF`,
  `OFF_PERMISSIVE`, `ON_PERMISSIVE`이면 `file:position`을 씁니다). 그래서 `ON`인 소스에서는 GTID
  셋이 곧 시작 지점이며, Start CDC는 먼저 `gtid_mode`를 읽고 — Start 대화 상자에서, 그리고 무엇이든
  바꾸기 전에 다시(여기서 거부하면 Start가 *Start CDC blocked: …*로 실패하고, CDC 단계가 실패한
  Start 아래에 해결 방법을 포함한 알림 전체를 보여 줌) — 쓸 수 없는 시작 지점은 거부합니다:

  | 시작 지점 | 할 일 |
  |---|---|
  | **GTID 셋이 없음**(직접 입력한 위치, 또는 버전 0.1.565 이하가 저장한 Manual 시작 지점 — GTID 셋 없이 복원됩니다): MySQL이 보관 중인 바이너리 로그 전체를 다시 보내 시작 지점 이전의 변경을 다시 적용하게 됨 | **Fetch current position**은 위치와 GTID 셋을 함께 기록합니다. 직접 입력한 위치라면 소스가 정확히 그 위치에서 실행을 마친 GTID 셋(예: 덤프가 기록한 것)도 함께 입력하세요. 0.1.565 이하가 저장한 Manual 지점은 다시 가져와야 합니다. |
  | **`gtid_mode`가 `ON`이 아닐 때 기록된 시작 지점**(그런 지점이나 GTID 셋이 없는 시작 지점에서 시작한 스택의 Stop CDC 후 재시작, 그리고 마지막 시작이 `gtid_mode`가 `ON`이 아닐 때였던 스택의 모든 시작 포함) | 다음 항목을 참조하세요. |
  | **소스가 실행한 적 없는 GTID 셋**(다른 서버에서 읽었거나 잘못 입력함), **또는 소스가 그 이후의 바이너리 로그를 이미 삭제한 셋** | CDC에 새 시작 지점을 주세요 — **"Drop & reload"로 Re-run Full Load**, 또는 **Manual** → **Fetch current position** → **Use this start point**(그러면 두 지점 사이의 캡처 대상 테이블 변경은 복제되지 않으므로 "Drop & reload"로 다시 로드하고 — CDC only: Aurora DSQL에서 바로잡고 — CHECKSUM으로 검증하세요) — 보존 기간이 끝나서였다면 먼저 binlog 보존을 늘리세요. 대신 GTID 셋을 소스와 대조하는 검사 자체가 실패했다면(연결이 끊김, 읽기 시간 초과) 시작 지점에 문제가 있다고 알려진 것은 없습니다: 도구에서 소스에 연결할 수 있는지 확인한 뒤 Start CDC를 다시 실행하세요. |
  | **`gtid_mode`를 읽을 수 없음**(이 세션에 소스 연결이 없음 — 비밀번호는 메모리에만 보관되므로 앱을 재시작하면 지워집니다 — 또는 읽기 실패) | Connect 단계에서 소스 연결을 다시 테스트한 뒤 Start CDC를 다시 실행하세요. |

  Manual 카드에서 **Fetch current position** 후 위치를 고치면 — 위치를 비워 두거나 잘못 입력해도 —
  가져온 GTID 셋은 버려지고(가져온 위치에서 읽은 것이므로), GTID 셋을 고치면 기록된 테이블 정의가
  버려지므로 위의 시작 지점 검사는 경고만 합니다. 입력하는 GTID 셋은 MySQL GTID 셋이어야 합니다
  (`<server UUID>:<interval>`을 쉼표로 구분, 예: `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100`):
  Start CDC는 그 밖의 텍스트 — 함께 복사된 따옴표나 `SET` 문 등 — 를 모든 `gtid_mode`에서
  거부합니다.

- **MySQL: `gtid_mode`는 Full Load 전에 정하고 cut over까지 유지.** `gtid_mode`가 `ON`이 아닐 때
  기록된 시작 지점으로는 소스가 `ON`이 된 뒤 CDC를 시작할 수 없습니다: MySQL은 GTID 없이 쓰인 변경을
  GTID로 보낼 수 없으므로 그 변경에서 커넥터를 멈추거나, 오류 없이 건너뜁니다. 그 시작 지점을
  유지하려면 cut over까지 `gtid_mode`를 `ON_PERMISSIVE`로 되돌리거나(그러면 CDC는
  `file:position`으로 스트리밍), CDC에 새 시작 지점을 주세요 — Stop CDC 후의 재시작(스트리밍한 적
  있는 스택)이라면 먼저 **Delete CDC infrastructure** 후 다시 배포한 뒤: **"Drop & reload"로 Re-run
  Full Load**, 또는 **Manual** → **Fetch current position** → **Use this start point** 후 그 사이에
  바뀐 테이블을 "Drop & reload"로 다시 로드하고(CDC only: Aurora DSQL에서 바로잡기) Start CDC 한
  다음 CHECKSUM으로 검증하세요. 새 스택에는 커밋된 오프셋이 없으므로 CDC는 시작 지점에서 정확히
  시작합니다 — 가지고 있는 시작 지점이 기록될 때 이미 `gtid_mode`가 `ON`이었고 그 뒤로 계속
  `ON`이었다면 그 지점에서, 아니면 위와 같이 새 시작 지점에서.
  - **Start CDC가 거부하는 것.** 그런 시작 지점에서의 시작, 그리고 그런 지점이나 GTID 셋이 없는 시작
    지점에서 시작한 스택의 Stop CDC 후 재시작(`file:position`으로 스트리밍했으므로 이어 갈 GTID 셋이
    없음) — 세션에 더 이상 시작 지점이 없어도(연결한 스택, 초기화한 세션) 마찬가지입니다. Start
    CDC는 소스 커넥터를 시작할 때마다 소스의 `gtid_mode`를 CDC 스택에 기록하며(Stop CDC는 그 기록을
    유지합니다), `ON`인 소스에서는 마지막 시작이 `gtid_mode`가 `ON`이 아닐 때였던 스택의 시작을 모두
    거부합니다 — Stop CDC 후 재시작은 물론 새 시작 지점도 거부하는데, 그 스택은 그 시작부터
    `file:position`으로 스트리밍했으므로 커밋된 오프셋의 GTID 셋이 거기서 더 나아가지 않았기
    때문입니다.
  - **CDC가 스트리밍하는 동안에는 `gtid_mode`를 바꾸지 마세요.** 커넥터의 다음 자동 재시작 때
    적용되어 커넥터를 멈추거나 오류 없이 변경을 건너뛸 수 있고, 도구는 이를 막을 수 없습니다: 먼저
    Stop CDC 하세요.
  - **이전 버전.** 버전 0.1.566 이하가 마지막으로 시작한 스택에는 기록이 없고(어떤 시작이 모드를
    읽을 때까지), 0.1.565 이하가 기록한 시작 지점에는 모드가 기록되지 않습니다: 그런 소스는 cut
    over까지 `ON`이 아닌 상태로 두거나, 새 스택에서(먼저 Delete CDC infrastructure 후 다시 배포)
    CDC에 새 시작 지점을 주세요.
- **MySQL에서 `gtid_mode=ON`이면: 버전 0.1.565 이하가 시작한 파이프라인.** 그 버전들은 Debezium이
  시작 기준으로 삼는 GTID 셋을 비웠기 때문에, `gtid_mode=ON`인 소스에서 MySQL이 모든 Start와
  커넥터의 모든 자동 재시작 때마다 보관 중인 바이너리 로그 전체를 다시 보냈습니다. 쓰기는
  수렴했지만(싱크는 멱등) 예전 값이 다시 적용됐고, 고유 키 충돌은 데드레터가 될 수 있었으며, 보관된
  로그 어디에든 — 시작 지점 이전이라도 — 파싱할 수 없는 DDL이 있으면 모든 Start가 멈췄습니다. 실행
  중인 파이프라인은 **Stop CDC** → **Start CDC**를 한 번 할 때까지 그 동작을 유지하며, 그 뒤에는
  멈춘 곳에서 이어 갑니다(Stop 때 싱크가 적용하지 않은 변경은 CDC 토픽에서 오므로 토픽이 그 변경을
  아직 보존하는 동안만 빈틈이 없습니다 —
  [§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)). Manual 지점(CDC only
  포함)에서 시작한 파이프라인은 그 Start가 *the source has purged binary logs CDC's GTID set does
  not cover*로 실패할 수 있습니다: Stop CDC 후 cut over까지 소스의 `gtid_mode`를 `ON_PERMISSIVE`로
  바꾸고(RDS와 Aurora에서는 파라미터 그룹 변경) Start CDC를 하세요 — 멈춘 곳부터 `file:position`으로
  재개하며, CDC가 멈출 때 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안은 빈틈 없이 이어
  갑니다(binlog 보존 기간도 끝났다면 그 Start는 대신 실패합니다). 또는 메시지대로 CDC에 새 시작
  지점을 주세요: 이 스택은 스트리밍했으므로 먼저 **Delete CDC infrastructure** 후 다시 배포한 뒤,
  모든 테이블에 대해 **"Drop & reload"로 Re-run Full Load**(CDC only: **Manual** → **Fetch current
  position** → **Use this start point** 후 그 사이에 바뀐 테이블을 Aurora DSQL에서 바로잡기), 그다음
  Start CDC 하고 CHECKSUM으로 검증하세요. 이전 버전으로 `gtid_mode=ON` 소스에서 실행한 파이프라인은
  cut over 전에 CHECKSUM으로 검증하세요.

### CDC가 파싱하지 못하는 MySQL DDL 문

- **MySQL: Debezium이 파싱하지 못하는 DDL 문은, 소스 서버의 어디에서 실행되든 CDC를 멈춤.**
  Debezium은 binary log의 모든 DDL 문을 자신의 MySQL 문법으로 파싱합니다 — 캡처 대상만이 아니라
  서버의 모든 데이터베이스와 테이블에 대해, 저장 루틴·트리거·뷰·계정 문까지 포함해 — 무엇이든
  필터링하기 전에 그렇게 하며, 도구의 설정은 파싱하지 못하는 문을 건너뛰지 않고 멈춥니다. 소스
  커넥터는 그 문 바로 앞에서 멈추고 그 위치는 문을 지나 앞으로 나아갈 수 없습니다: Retry CDC도, Stop
  CDC 후의 Start CDC도 다시 그 문에서 멈춥니다. CDC 단계가 원인을 알려 줍니다 — Start CDC 오류에서,
  또는 시작된 뒤라면 그 커넥터의 Pipeline health 행과 오류 알림에서
  ([§4.6](04-cdc-and-dsql-constraints.md#파이프라인-헬스와-change-flow)) — 그리고 따옴표로 묶인 값을
  가린 채 그 문의 앞부분을 보여 줍니다. 시작 지점 이후의 문만 CDC를 멈춥니다.
  - **해결 방법.** CDC를 Stop한 다음 **Delete CDC infrastructure** 후 다시 배포하세요: 스택이
    스트리밍했으므로 새 시작 지점에는 새 빈 토픽이 필요합니다(그때까지 Full Load 단계는 새 시작
    지점을, Start CDC는 더 새 시작 지점을 거부합니다). 그다음 그 문 이후의 시작 지점을 CDC에 주세요:
    모든 테이블에 대해 **"Drop & reload"로 Re-run Full Load**를 실행하거나(Full load + CDC; 그 문이
    캡처 대상 테이블을 바꿨다면 먼저 1단계 Evaluation과 Schema Conversion을 다시 실행), **Manual** →
    **Fetch current position** → **Use this start point**를 선택하세요. 그러면 CDC가 멈춘 지점과 그
    지점 사이의 변경은 복제되지 않으므로, 그 테이블들을 "Drop & reload"로 다시 로드하고(CDC only:
    Aurora DSQL에서 바로잡기) CHECKSUM으로 검증하세요. 다시 로드하면 시작 지점이 Automatic으로
    돌아가므로, 새 스택에서 **Start CDC** 하기 전에 Manual을 다시 선택하세요. 커넥터 로그의 *binlog
    reader near position*을 시작 지점으로 쓰지 마세요: 그것은 실패한 문이 시작되는 위치입니다.
  - **시작 지점을 옮겨도 도움이 되지 않는 경우.** 어떤 테이블의 정의 자체가 문법이 지원하지 않는
    구문을 쓰면, 소스로부터 스키마를 만드는 모든 Start(첫 Start, 새 시작 지점에서의 Start, 없어진
    스키마 히스토리를 다시 만드는 Start)는 시작 지점과 상관없이 그 정의를 읽다가 멈춥니다: 소스에서
    그 정의를 바꾼 뒤 Stop CDC와 Start CDC를 하거나, 그 서버는 Full load only로 마이그레이션하세요.
    다시 실행되는 문(스키마 마이그레이션 도구, 예약된 작업)은 실행될 때마다 CDC를 멈춥니다.
  - **건너뛰기 옵션 없음.** 도구는 CDC가 파싱하지 못하는 문을 건너뛰는 방법을 제공하지 않습니다:
    건너뛴 문은 이후의 행이 오류 없이 잘못된 컬럼에 쓰이게 할 수 있습니다. PostgreSQL 소스는 영향을
    받지 않습니다: 그 변경 스트림에는 DDL이 없습니다.

### 외래 키

- **캐스케이드 FK 동작은 CDC로 복제되지 않음.** 서버 측 `ON DELETE/UPDATE CASCADE`(및 `SET NULL`/
  `SET DEFAULT`) 동작은 소스 엔진 *내부*에서 발생해 변경 스트림에 잡히지 않을 수 있으므로 CDC가
  적용할 수 없습니다 — 소스가 캐스케이드한 자식 행이 타깃에 orphan으로 남습니다(MySQL 소스의 경우
  InnoDB가 binlog에 남기지 않은 채 실행). 도구는 **cut over 시점**에 외래 키를 다시 생성합니다(복제
  중에는 하지 않음): 키는 여전히 바로 추가되지만(`NOT VALID`) 이 orphan 행들에서 Aurora DSQL의
  **적재된 행 검증이 실패**합니다 — cut-over 화면이 그 키를 알리고 행 수를 세어 나열하며, MySQL
  소스라면 **I've cut over**는 키가 검증될 때까지, 또는 그 검사 없이 cut over하기로 선택할 때까지
  기다립니다 — 그리고 Validation의 **고아 검사**도 이를 보고하므로 조용히 어긋나지 않습니다.
  Evaluation이 해당 테이블을 **MANUAL**로 표시합니다. 자동 동작은 애플리케이션의 명시적 자식 행
  문으로 바꾸고, 어긋남이 잡히도록 **최종 cut-over 비교 전에 소스 쓰기를 멈추세요**.
- **Aurora DSQL이 검증하지 못한 외래 키는 `NOT VALID`로 남음.** 키는 자리를 잡고 모든 신규 쓰기에
  강제됩니다: 없는 부모를 가리키는 삽입이나 키를 바꾸는 갱신은 거부되고(`23503`), 아직 참조되는 부모
  행의 삭제나 키 변경도 거부됩니다(`NO ACTION` / `RESTRICT`; `CASCADE`와 `SET NULL`은 평소대로
  동작). 이미 있는 행은 다시 검사하지 않습니다: 그런 행의 키가 아닌 컬럼을 갱신하거나, 삭제하거나,
  있는 부모를 가리키게 하는 것은 되지만, 삭제 후 다시 삽입하는 식으로 저장하는 것은 새 orphan과
  마찬가지로 거부됩니다. 행을 고치고(트랜잭션당 최대 3,000행씩) **Validate again**이 성공할 때까지
  키는 증명되지 않습니다.

### 용량과 비용

- **CDC 파이프라인 하나가 캡처하는 테이블 수에는 한도가 있음: 일반적인 이름이면 약 80–160개.**
  CloudFormation은 파라미터 값 하나를 4,096바이트로 제한하고, CDC 스택은 캡처할 테이블(테이블 목록과
  싱크의 토픽 목록)을 파라미터로 받으므로 파이프라인 하나는 최대 약 4,097 ÷ (L + 11)개의 테이블을
  캡처합니다. L은 정규화된 테이블 이름(MySQL은 `database.table`, PostgreSQL은 `schema.table`)의 평균
  바이트 길이입니다: 15바이트 이름이면 약 155개, 20이면 130개, 30이면 100개, 40이면 80개입니다.
  Evaluation은 1,000개 테이블까지 허용하고, Full load only에는 이런 한도가 없습니다.
  - **Deploy CDC infrastructure와 Start CDC는 이보다 큰 캡처를 거부합니다.** 무엇이든 업로드하거나
    만들기 전에 거부합니다(*Too many tables for one CDC pipeline*, 파라미터와 그 크기를 명시).
    Deploy를 클릭하기 전까지 배포 양식은 최악의 경우 파라미터가 맞지 않을 수 있으면
    경고합니다(*Possibly too many tables for one CDC pipeline*).
  - **Full load + CDC에서는 Full Load를 막지 않습니다.** 적재 전에 Full Load 단계가 경고하고(*Too
    many tables for one CDC pipeline*, 또는 *Possibly too many tables for one CDC pipeline*) 선택한
    테이블 중 약 몇 개를 파이프라인 하나가 캡처하는지 알려 줍니다. 그래도 모든 테이블을 적재한 뒤,
    CDC를 시작하기 전에 CDC가 캡처할 필요가 없는 테이블의 선택을 해제할 수 있습니다
    ([13장](13-excluding-tables-and-columns.md)의 "Full Load 후 테이블 집합 바꾸기 (Full load +
    CDC)" 참고): 그 테이블은 적재된 그대로 Aurora DSQL에 남고, CDC와 Validation은 그 테이블을
    건너뜁니다. 적재가 별도 스냅샷 부분으로 나누어 읽는 MySQL
    테이블(*Reader shards per large table*을 2 이상으로 명시, [§3.8](03-full-load.md#38-병렬-적재))은
    예외입니다: 리더 하나로 다시 적재하기(Drop & reload) 전에는 선택을 해제할 수 없습니다.
  - 그 밖에는 이 CDC 파이프라인에서 캡처하는 테이블을 줄이거나(테이블 목록에서 선택 해제), Full load
    only를 사용하세요.
- **MySQL: 나누어 읽은 테이블은 CDC가 재생해야 일관됨.** *Reader shards per large table*을 2 이상으로
  명시하면, binlog 시작 지점이 있는 Full load + CDC 적재는 큰 MySQL 테이블을 기본 키 구간으로 나누어
  구간마다 자신의 스냅샷으로 읽습니다([§3.8](03-full-load.md#38-병렬-적재)). Full Load의 시작 지점에서
  시작한 CDC가 그 테이블을 재생하기 전까지는, 읽는 동안 키가 바뀐 행이 Aurora DSQL에 두 번 있거나
  아예 없을 수 있고, 두 구간에 걸친 트랜잭션은 절반만 반영될 수 있습니다 — 행 수는 같은 채로:
  ROW_COUNT 검증은 이를 보지 못하고, CHECKSUM은 봅니다. 그래서 그런 테이블을 그 재생 없이 남기는
  경로(선택 해제, Full load only, 더 나중의 시작 지점에서 Start CDC)는 테이블을 리더 하나로 다시
  적재할 때까지(Drop & reload: Append는 그 행을 그대로 둡니다) 모두 거부됩니다. Full load only와 더
  나중의 시작 지점은 CHECKSUM 모드의 Validation이 그 테이블의 일치를 확인해도 허용됩니다. 선택
  해제에는 CDC를 시작하기 전의 다시 적재가 필요합니다: CDC의 첫 Start부터 테이블 집합은 고정됩니다.
  기본 설정에서는 어떤 테이블도 이렇게 읽지 않으며, Full load only 실행의 테이블과 선행 키 컬럼을
  포함하지 않는 unique 키가 있는 테이블도 마찬가지입니다: 그런 테이블은 리더 세션 하나가 스냅샷
  하나에서 읽습니다.
- **CDC는 배포되어 있는 동안 과금됨.** 스트리밍 파이프라인(MSK Serverless + MSK Connect, NAT
  게이트웨이를 만들었다면 그것까지)은 실행되는 내내 비용이 발생합니다. Cut over 후에는 cdc-stack을
  내리세요. Full Load만 수행하면 스트리밍 인프라가 프로비저닝되지 않습니다.

### Aurora DSQL이 담을 수 없는 값

- **타깃 컬럼이 정확히 담을 수 없는 값은 변환하지 않고 거부합니다.** 대상은 다음과 같습니다:
  - `boolean` 컬럼에 들어가는 0이나 1이 아닌 숫자(`2`를 담은 MySQL `TINYINT(1)`, 또는 CDC에서 정수
    타입으로 바뀐 PostgreSQL `boolean`);
  - `numeric(p,s)` 컬럼이 유지하는 것보다 많은 소수 자릿수;
  - 정수 컬럼에 들어가는 소수.

  보통 Step 1 이후 소스 타입이 넓어졌거나 Schema Conversion에서 더 좁은 타입을 골랐을 때 생깁니다.
  Full Load는 그 테이블을 중단합니다(테이블 전체 실패). CDC는 그 행을 데드레터로 보내고 배너를
  표시합니다. 이전 버전의 도구로 배포한 CDC 인프라는 삭제하고 다시 배포하기 전까지 여전히 바뀐 값을
  씁니다. Schema Conversion에서 값을 담을 수 있는 타입을 컬럼에 지정하고(**Replace**로 다시 적용)
  리로드하거나, 소스 데이터를 정리하세요. 정밀도 없는 `numeric` 타깃(Schema Conversion이 경고하는
  Aurora DSQL 기본값 `numeric(18,6)`)은 설계상 PostgreSQL의 제약 없는 `numeric`을 여전히 소수
  6자리로 반올림합니다. 검사하지 않는 경우: `numeric` 컬럼에 들어가는 `FLOAT`/`DOUBLE` 값.

- **이전 버전이 배포한 CDC 인프라의 1582-10-15 이전 날짜·시각과 MySQL의 0–99년.** Full Load는
  `DATETIME`, `timestamp`, `timestamptz`, `DATE`, `date`를 그대로 저장합니다. MySQL이 문서화한
  최솟값 `1000-01-01`과, .NET의 `DateTime.MinValue`나 Go의 영값 `time.Time`이 쓰는 `0001-01-01`도
  마찬가지이며, CDC도 싱크 플러그인 v54부터 그렇게 합니다.
  - 이전 싱크는 1582-10-15 이전의 `DATETIME`·`timestamp`·`timestamptz` 값을 율리우스력으로 며칠
    어긋나게 썼고(`1000-01-01`을 `0999-12-27`로, `0001-01-01`을 `0001-01-03`으로), 그레고리력
    개력으로 건너뛴 열흘(1582-10-05~1582-10-14)의 `DATE`·`date` 값은 열흘 뒤의 날짜로
    썼습니다(`1582-10-10`을 `1582-10-20`으로). 이전 MySQL 소스는 `DATETIME`·`DATE`의 0–99년을
    2000–2069년 또는 1970–1999년으로 읽었습니다(`0001-01-01`을 `2001-01-01`로). dead-letter도 없고
    ROW_COUNT도 그대로 일치하므로 CHECKSUM에서만 드러납니다. **Delete CDC infrastructure** 후 다시
    배포하고, 그런 테이블을 다시 적재(Drop & reload)한 뒤 CHECKSUM으로 검증하세요.
  - 다만 PostgreSQL 소스에서 Debezium 스냅샷이 복사하는 행(모든 CDC only Start, 다시 스냅샷, Manual
    시작 지점; [§4.3](04-cdc-and-dsql-constraints.md))은 싱크에 닿기 전에 JDBC로 읽히므로, 싱크
    플러그인과 관계없이 1582-10-05~1582-10-14의 `date`나 `timestamp`는 열흘 뒤의 값으로 저장되고 BC
    값은 기원전(BC) 표시를 잃으며(`0044-03-15 BC`가 `0044-03-15`로), 1582-10-15 이전의
    `timestamptz`는 율리우스력으로 읽혀 며칠 어긋납니다(`1000-01-01`은 닷새 뒤, `0001-01-01`은 이틀
    앞). 그런 테이블은 **Full load + CDC**로 마이그레이션하고(슬롯 핸드오프는 스냅샷을 하지 않음)
    CHECKSUM으로 검증하세요.
- **MySQL의 0 날짜와 잘못된 날짜.** MySQL은 Aurora DSQL에 없는 날짜를 저장할 수 있습니다: 0 날짜
  `0000-00-00`, 월이나 일이 0인 날짜(`2024-00-15`), 0 `TIMESTAMP`, 그리고
  `ALLOW_INVALID_DATES`에서는 그 달에 없는 날 (`2024-02-31`)입니다. `sql_mode`에 strict 모드,
  `NO_ZERO_DATE` 또는 `NO_ZERO_IN_DATE`가 없거나 컬럼의 `DEFAULT`가 0 날짜이면 이런 값이 들어옵니다.
  Evaluation의 `ZERO_DATES` 항목([2장 §2.1](02-evaluation-and-schema-conversion.md))이 날짜 컬럼과,
  그런 값을 가진 행을 세는 테이블별 쿼리를 보여 줍니다. Full Load는 그런 값을 번들 CDC
  커넥터(Debezium)가 바이너리 로그를 읽어 쓰는 그대로 쓰므로, 어느 경로가 쓴 행이든 같습니다:
  - `DATE`나 `DATETIME` 컬럼의 0 날짜(연·월·일 중 하나가 0)는 컬럼이 NULL을 허용하면 NULL이 되고,
    `NOT NULL` 컬럼에서는 컬럼의 `DEFAULT`가 날짜이면(`CURRENT_TIMESTAMP` 같은 식은 제외) 그 값이,
    아니면 `1970-01-01`(`DATETIME`에서는 `1970-01-01 00:00:00`)이 됩니다;
  - 0 `TIMESTAMP`는 컬럼의 NULL 허용 여부와 관계없이 `1970-01-01 00:00:00` UTC가 됩니다;
  - 그 달에 없는 날은 `1970-01-01`(`1970-01-01 00:00:00`)이 됩니다. Debezium이 읽지 못하는 날짜 대신
    넣는 값입니다(번들 커넥터는 대신 그런 값에서 멈춥니다. 아래 참고).

  그런 값이 있던 테이블에는 컬럼별 개수와 무엇으로 바뀌었는지를 담은 WARNING 줄이 활동 로그에 하나
  남습니다(값은 남기지 않음). Validation은 소스 값을 같은 방식으로 바꾼 뒤 checksum을 계산하므로,
  그런 값만 다른 테이블은 일치하고, 그 밖의 차이는 그대로 드러납니다. 그대로 옮겨지지 않는 것:

  - **CDC는 그 달에 없는 날에서 멈춥니다.** 소스 커넥터는 그런 날짜를 가진 행의 바이너리 로그 변경을
    읽지 못합니다 — 마이그레이션하는 테이블만이 아니라 바이너리 로그가 기록하는 모든 테이블에서 —
    그래서 그 변경에서 멈추고, 다시 시작할 때마다 같은 변경에서 또 멈춥니다. 그런 행을 고치는 것
    자체가 그런 변경이므로, CDC의 시작 지점이 기록되기 전에 고치세요: **Full load + CDC**에서는 Full
    Load 전, **CDC only**에서는 **Start CDC** 전입니다. 그 지점 이후에 고쳤다면 **Start CDC** 전에
    CDC에 더 새로운 시작 지점을 주세요 ("Drop & reload"로 **Re-run Full Load**). CDC가 이미 그런
    값에서 멈췄다면 더 새로운 시작 지점에서만 계속됩니다(이 절의 첫 항목).
  - **그런 날짜를 가진 기본 키.** Full Load는 키 컬럼의 값을 저장된 그대로 둡니다: 고정된
    날짜(`1970-01-01` 또는 컬럼의 `DEFAULT`)로 쓰면 그런 키를 가진 행들이 하나의 키로 겹치기
    때문입니다 — 서로의 키, 또는 그 날짜를 가진 행의 키로. Aurora DSQL은 저장된 값을
    거부하므로(`22008`) 그런 행은 격리되고, Validation이 그 차이를 보고합니다. CDC는 그런 행의
    변경을 고정된 날짜의 키로 쓰므로, 그 키를 가진 행을 덮어쓰거나 삭제할 수 있습니다. Full Load
    전에 소스에서 그런 키를 고치세요.
  - **그런 컬럼에 걸린 유니크 키.** Full Load는 유니크 키 컬럼의 값도 저장된 그대로 둡니다: 고정된
    날짜로 쓰면 서로 다른 0 날짜나 잘못된 날짜가 하나의 값이 되거나 다른 행이 가진 값이 되어, 로드가
    그 중복에서 실패하기 때문입니다. Aurora DSQL은 저장된 값을 거부하므로(`22008`) 그런 행은
    격리되고, Validation이 그 차이를 보고합니다. CDC는 그런 행의 변경을 고정된 날짜로 쓰며, 다른
    행이 그 값을 가지고 있으면 dead-letter로 보냅니다(`23505`). 먼저 소스에서 그런 값을 고치세요.
  - **애플리케이션의 SQL.** Aurora DSQL은 `INSERT`, `UPDATE`나 `WHERE`에 들어온 그런 날짜를 거부하고
    (`22008`), 그런 날짜와의 비교가 찾던 행 — soft delete 관용구
    `deleted_at = '0000-00-00 00:00:00'`, 또는 `deleted_at = 0`(MySQL은 날짜와 비교되는 숫자 0을
    제로 날짜로 읽습니다. Aurora DSQL은 날짜를 숫자와 비교하지 않습니다, `42883`) — 은 위와 같이
    이제 NULL, 컬럼의 `DEFAULT` 또는 `1970-01-01`을 가집니다. 컷오버 전에 그런 날짜를 쓰는 코드를
    바꾸고, 대신 `IS NULL`이나 그 값과 비교하세요. 그런 날짜와 비교하는 뷰는 Schema Conversion이
    보고하고(그 `CREATE VIEW`는 실패합니다), 그런 문장은 Query Converter가 보고합니다.

### PostgreSQL 관련 사항

- **PostgreSQL: CDC는 대부분의 확장 타입을 옮기지 못함.** 번들 CDC 커넥터(Debezium)는
  확장(extension)이 정의한 타입을 이름으로만 알아보며, 아는 것은 여섯 가지입니다: `citext`,
  `hstore`, `ltree`, `isbn`, 그리고 PostGIS `geometry`와 `geography`(및 그 배열). 그 밖의 확장 타입
  컬럼은 모든 변경 이벤트에서 빠집니다. 예: pgvector의 `vector`, `halfvec`, `sparsevec`, `cube`,
  `seg`, `lquery`, `isbn`이 아닌 `isn` 타입. 그러면 CDC가 삽입한 행은 그 컬럼에 NULL(또는 그
  `DEFAULT`)을 받고 업데이트는 그 컬럼을 그대로 두며, 데드레터는 생기지 않습니다. 그 컬럼이 Aurora
  DSQL에서 `DEFAULT` 없는 `NOT NULL`이면 대신 Aurora DSQL이 CDC가 그 테이블에 쓰는 모든 삽입과
  업데이트를 거부합니다(`23502`, 데드레터 처리). Full Load는 그 컬럼을 옮기므로 CDC로 도착한 행만
  달라지고(CDC only에서는 스냅샷의 행도), Validation CHECKSUM이 그것을 보여 줍니다. Evaluation
  (`CDC_UNCARRYABLE_COLUMN`)은 흔히 쓰는 확장의 그런 컬럼을 짚어 주고, CDC 사전 요구사항 검사
  (*Every column's type can be carried by CDC*)는 소스 카탈로그를 읽어 그 모두를 찾습니다. CDC가
  옮기지 못하는 내장 타입과
  같습니다([§4.2](04-cdc-and-dsql-constraints.md#cdc-중-소스-스키마-드리프트-감지처리)). CDC를
  배포하기 전에 정하세요: 컬럼을 제외하거나([13장](13-excluding-tables-and-columns.md)), 소스에서
  CDC가 옮기는 타입(`text`)으로 바꾸거나, 테이블을 빼고 Full load only로 마이그레이션하거나, 그
  차이를 받아들이고 컷오버 전에 그 테이블의 쓰기를 멈춘 채 **Drop & reload**로 다시 로드하세요. 그런
  타입(또는 CDC가 옮기지 못하는 다른 타입)의 기본 키 컬럼은 더 나쁩니다: CDC는 그 컬럼을 각 변경의
  키에서도 빼므로 테이블의 변경을 나머지 키 컬럼만으로 적용하고, DELETE는 그 값을 공유하는 타깃 행을
  모두 지웁니다. 키 컬럼은 제외할 수 없으므로 CDC 사전 요구사항 검사가 그런 테이블의 마이그레이션을
  막습니다(**Full load + CDC**에서는 Full Load 전부터): 테이블을 빼고 Full load only로
  마이그레이션하거나, 소스에서 키 컬럼을 CDC가 옮기는 타입으로 바꾸세요. Schema Conversion에서 그
  컬럼을 뺀 기본 키로 바꾼 테이블은 막지 않습니다: CDC는 그 테이블의 변경을 그 키로 적용하고, 그
  컬럼은 같은 타입의 다른 컬럼처럼 다룹니다(다만 소스 기본 키 컬럼은 제외할 수 없습니다).
- **PostgreSQL 17/18 소스 — 버전별 엣지 4가지(어느 것도 마이그레이션을 막지 않음).** PG 13–18을
  지원하며, 17·18에서 유의할 점이 4가지 있습니다:
  - **`interval`의 `infinity`/`-infinity`**(PG17 신규): Aurora DSQL의 `interval` 입력은 이 값을
    거부합니다. 따라서 *무한* `interval`을 담은 행은 Full Load 중 **격리(quarantine)**되어(격리
    카운트에 표시됨) Validation에서 count/checksum 불일치로 드러납니다. 하지만 CDC는 이 값을 조용히
    **0** interval(`00:00:00`)로 옮기며, CHECKSUM 검증에서만 드러납니다. 유한 interval은 영향받지
    않고, `date`/`timestamp`/`timestamptz`의 `infinity`/`-infinity`도 마찬가지입니다: Full Load와
    CDC(싱크 플러그인 v54 이상, 스트리밍이든 스냅샷이든) 모두 `infinity`로 저장합니다. **할 일:**
    무한 `interval` 행은 해당 컬럼을 마이그레이션하거나 CDC를 시작하기 전에 재모델링(예: 유한
    sentinel 값)하세요. 이전 버전이 배포한 CDC 인프라라면 **Delete CDC infrastructure** 후 다시
    배포하고, 태스크가 멈추지 않았더라도 그런 테이블을 다시 적재(Drop & reload, 또는 다음 Start가
    제안하는 재스냅샷)한 뒤 CHECKSUM으로 검증하세요.
  - **VIRTUAL 생성 컬럼**(PG18의 기본 종류): Aurora DSQL은 `GENERATED ALWAYS AS (expr) **STORED**`를
    지원하고 값을 스스로 유지하므로 STORED 컬럼은 **보존**됩니다(쓰기에서 그 컬럼을 빼면 되고, Full
    Load와 CDC 싱크가 그렇게 합니다). **VIRTUAL** 컬럼은 일반 컬럼으로 생성되고 **MANUAL/LOSS**로
    표시됩니다: Full Load가 소스의 계산값을 실체화하므로 타깃은 처음엔 정확하지만 이후엔 아무것도
    유지하지 않고, VIRTUAL 컬럼은 논리 복제 자체가 불가해 CDC도 나르지 않습니다. **할 일:** 컷오버
    전에 애플리케이션에서 다시 계산하세요. **apply 전에** 결정해야 합니다: DSQL은 컬럼에서 식을
    DROP할 수는 있어도 추가(ADD)할 수는 없습니다.
  - **`NOT NULL … NOT VALID`**(PG18 신규): 그 컬럼에는 제약 이전의 NULL이 남아 있을 수 있고, 소스는
    그 행들을 검사하지 않았습니다: Aurora DSQL에서는 `NOT NULL` 없이 생성되어 Full Load가 그 행들을
    적재하며, **MANUAL/LOSS**(`PG_NOT_NULL_NOT_VALID`)로 표시됩니다. 컷오버 후에는 타깃이 그 컬럼의
    NULL을 받아들이고, Aurora DSQL은 나중에 그 컬럼에 `NOT NULL`을 추가할 수 없습니다. **할 일:**
    규칙을 유지하려면 Schema Conversion이 그 테이블을 적용하기 전에 소스에서 그 행들에 값을 넣고
    `ALTER TABLE … VALIDATE CONSTRAINT`를 실행한 뒤 Evaluation을 다시 실행하세요.
  - **CDC 커넥터 커버리지**: 번들된 Debezium PostgreSQL 커넥터 자체의 테스트 매트릭스는
    PG16까지이지만, CDC는 PG17/18에서도 종단 간 테스트되었습니다. **할 일:** PG18에서는
    `idle_replication_slot_timeout`가 `0`(비활성)인지도 확인해, 유휴 구간에 CDC 슬롯이 자동
    무효화되지 않도록 하세요.

---

## 6.3 배포 한계 (AWS 호스팅 형태)

- **단일 태스크 컨트롤 플레인.** 도구는 **하나의** ECS Fargate 태스크로 배포되며, 하나를 초과해
  올리면 안 됩니다. AWS 호스팅 배포에서는 **작업/세션 상태가 모두 관리형 S3 버킷**(`jobs/` +
  `sessions/` 프리픽스)에 durable하게 보관되므로, 태스크가 교체돼도(배포, 크래시) 진행 상태를 잃지
  않고 **재개**합니다 — 재개는 마지막으로 기록된 상태 전이 지점으로 복귀하며, 진행 중이던 정확한
  배치는 아닙니다. 단일 태스크 제한은 상태 durability가 아니라 **단일 writer 오케스트레이션**(CDC
  배포 조율 + 인메모리 세션 상태) 때문입니다; 임시 스토리지의 로컬 SQLite는 로컬 개발 폴백일
  뿐입니다.
- **앱 자체 인증 없음.** 앱은 ALB의 **선택적 Cognito** 게이트에 의존합니다. 인터넷에 노출되는
  배포라면 Cognito를 켜세요. 배포 템플릿은 Cognito 없이 인터넷에 노출하는 안전하지 않은 조합을
  막습니다.
- **자격증명은 세션별로만, 메모리에만 존재.** 절대 디스크에 남지 않으며, 재시작하면 소스/타깃 연결
  정보를 다시 입력해야 합니다.

---

**다음:** [7. 성능과 튜닝 →](07-performance-and-tuning.md)
