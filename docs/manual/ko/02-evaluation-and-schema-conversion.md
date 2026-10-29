# 2. Evaluation과 Schema Conversion

_언어: [English](../en/02-evaluation-and-schema-conversion.md) | **한국어** | [日本語](../ja/02-evaluation-and-schema-conversion.md)_

> **이전:** [1. 설정](01-setup.md)

연결과 데이터 이동 사이에는, 마이그레이션을 시행착오가 아니라 **결정론적이고 예측 가능하게** 만들어
주는 두 단계가 있습니다:

```
Connect → [ 1. Evaluation → 2. Schema Conversion ] → 3. Data Migration → 4. Validation → 5. Cut over
```

- **Evaluation**은 *"내 소스 데이터베이스에서 무엇이 Aurora DSQL로 옮겨지고 무엇이 안 되며, 각각
  작업량은 얼마인가?"* 에 답합니다.
- **Schema Conversion**은 소스 스키마를 Aurora DSQL DDL로 바꾸고, 그 DDL을 검토·편집할 수 있게 한 뒤
  타깃에 적용합니다.

**가장 짧은 경로:** **Run evaluation** → 준비도 등급과 **Unsupported** 항목
확인([§2.1](#21-evaluation--호환성-평가)) → 각 항목을 고치거나
결정([§2.4](#24-evaluation-항목-참조)에 대처 방법) → **Generate DDL for selected** → 검토 후
**Apply**([§2.2](#22-schema-conversion--dsql-ddl-생성적용)).

두 단계 모두 소스를 읽기만 하며, Schema Conversion은 적용할 때에만 타깃에 씁니다. 소스 엔진이
무엇이든(MySQL 또는 PostgreSQL), **이 단계는 Aurora DSQL이 받아들이지 않을 것을 단 한 행도 옮기기
전에 알게 되는 지점입니다.** 건너뛰지 마세요.

단계는 두 소스 엔진에서 같습니다. 서로 다른 곳은 이 장의 짧은 노트나 표가 어떻게 다른지 밝히며,
[§2.3](#23-타입-매핑-참조)은 각 엔진의 모든 컬럼 타입을 매핑하고, [§2.4](#24-evaluation-항목-참조)는
항목과 대처 방법을 정리합니다.

---

## 2.1 Evaluation — 호환성 평가

**Run evaluation**을 클릭하세요. 도구가 소스 스키마(MySQL에서는 Connect에서 지정한 데이터베이스,
비워 두었다면 모든 데이터베이스; PostgreSQL에서는 연결된 데이터베이스의 모든 비시스템 스키마)와
타깃에 이미 있는 것을 읽은 뒤 호환성 리포트를 보여 줍니다. 어느 쪽에서도 아무것도 바꾸지 않습니다.
실행 중에는 테이블마다 진행 상황을 보여 주고, **Cancel evaluation**은 읽고 있는 테이블을 마친 뒤
실행을 멈추며, 다시 실행하면 성공할 때까지 이전 리포트가 화면에 남습니다. 마이그레이션 사용자에게
필요한 권한은 [§1.1](01-setup.md#11-사전-요구사항)에 있습니다.

Evaluation은 소스의 **서버 설정**도 한 번 읽습니다(설정만 읽으며 테이블은 스캔하지 않습니다). 읽은
설정은 Evaluation 화면의 소스 객체 수 아래(**Source server settings**, 접힌 상태)에 보여
줍니다(내려받는 리포트에는 이 목록이 들어가지 않으며, 설정을 평가하는 각 항목이 그 값을 인용합니다).
이 설정은 테이블 정의에는 드러나지 않는, 애플리케이션이 실행되는 조건을 알려 줍니다: 시계 함수가
읽는 시간대와, MySQL에서는 `sql_mode`입니다. 이를테면 이 모드는 Aurora DSQL에 해당하는 값이 없는 0
날짜를 소스가 담을 수 있는지를 정합니다(`ZERO_DATES`,
[§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)). 이 읽기로는 애플리케이션이 스스로 설정하는
값, 즉 세션의 `SET`(`SET sql_mode`, `SET TIME ZONE`)이나 드라이버가 연결할 때 보내는 옵션을 볼 수
없으므로 그런 값은 직접 확인하세요. 평가되는 설정이 여러분에게 무엇을 뜻하는지는 아래
[소스 시간대와 서버 설정](#소스-시간대와-서버-설정)에서 설명합니다.

### 모든 객체가 하나의 분류를 받음

Evaluation이 평가하는 각 객체는 세 가지 분류 중 하나를 받습니다. 화면에는 레이블이, 내보낸
리포트에는 괄호 안의 값이 나옵니다.

| 분류 | 의미 | 할 일 |
|---|---|---|
| **Automatic** (`AUTO`) | 아무 조치 없이 변환됩니다. 기본 키가 있는 일반 테이블이 그렇습니다. | 없음. |
| **Review needed** (`MANUAL`) | 변환되지만 무언가를 잃거나 다르게 동작합니다: 버려지는 대소문자 무시 collation, `ENUM`, 큰 LOB나 JSON 컬럼, `ON UPDATE` 타임스탬프. | 결정하세요: 그대로 받아들이거나, Schema Conversion에서 타깃 DDL을 바꾸거나, 컷오버 전에 애플리케이션을 바꿉니다. |
| **Unsupported** (`UNSUPPORTED`) | 그대로는 옮길 수 없습니다: 기본 키가 없는 테이블, 트리거·루틴·이벤트, 바이너리 컬럼의 기본 키, Aurora DSQL 데이터베이스 하나가 담을 수 있는 것보다 많은 객체. | 마이그레이션하기 전에 소스를 바꾸거나, 그 객체를 데이터베이스 밖에서 다시 만드세요. |

항목은 **Recommended** 노트일 수도 있습니다. 이때 변환은 완전하고 정확하며, 노트는 Aurora DSQL에서
잘 돌리기 위한 조언입니다: 무시하면 성능을 잃지만 정확성은 잃지 않고, 객체의 작업량이나 준비도
점수에도 아무것도 더하지 않습니다. 가장 자주 보게 되는 두 가지는 계속 값을 생성하는 생성
키([기본 키 전략 선택](#기본-키-전략-선택))와 보존되는 외래 키([외래 키](#외래-키))입니다.

객체는 자기 항목 중 가장 엄격한 분류를 받으며(**Recommended** 노트는 세지 않습니다), 모든 항목이 그
객체 아래에 나열됩니다. 어떤 규칙에도 걸리지 않는 객체는 **Automatic**입니다.

### 리포트 읽기

리포트는 위에서 아래로 읽습니다:

- **Migration readiness**는 객체별 작업을 0~100으로 평가하고 등급을 매깁니다.
- **Compatibility by object kind**는 종류마다 **Automatic**, **Review needed**, **Unsupported** 객체
  수를 셉니다.
- **Objects by importance (most critical first)** 목록은 모든 객체를 종류별로 묶어 나열합니다.
  객체를 펼치면 항목마다 **Risk**(플래그된 이유. 조언이면 **Note**)와 **Recommendation**(할 일)이,
  그리고 객체의 작업량(**Simple (< 2h)**, **Medium (2–6h)**, **Significant (> 6h)** 중 하나)이
  보입니다. **Classification**·**Estimated manual effort** 필터로 목록을 좁힐 수 있고, 검색 상자는
  객체 이름이나 규칙 id에 맞춰집니다.
- **Target analysis (Aurora DSQL)** 섹션은 타깃에 이미 있는
  객체(**Objects already exist on target**)와 그곳에서 다른 객체가 차지한
  이름(**Names held on the target by another object**)을 나열하므로, 적용이 예기치 않은 충돌로
  도중에 실패하지 않습니다. 각각을 건너뛸지 교체할지는 적용할 때
  선택합니다([검토, 편집, 적용](#검토-편집-적용)).
- **Export:** **HTML**, **JSON**, **Text** 리포트에는 모든 객체와 타깃 분석이 들어 있습니다. 이
  리포트는 결정론적이며 AI 출력을 담지 않습니다.

**준비도 점수.** **Automatic** 객체는 비용이 0이고, 그 외 객체는 작업량 가중치만큼 비용이
듭니다(Simple 1, Medium 3, Significant 7; 추정치가 없는 **Unsupported** 객체는 Significant로 계산).
**Recommended** 노트는 비용이 없습니다. 점수는 100에서 최악의 경우 대비 그 비용의 비율을 뺀 값이며,
작업이 필요한 객체가 하나라도 있으면 100이 되지 않습니다. 이 점수는 작업의 순위를 정하고 계획하는 데
쓰고, 소요 시간 추정으로 쓰지는 마세요. 등급은 blocker가 먼저 정하므로, 큰 소스라도 blocker가 평균에
묻히지 않습니다:

| 등급 | 조건 |
|---|---|
| **Blocked** (빨강) | 마이그레이션이 Aurora DSQL 데이터베이스 한도를 넘음: 한 데이터베이스에 테이블 1,000개, 스키마 10개 또는 뷰 5,000개 초과(타깃 클러스터에 이미 있는 것 포함). 해결하기 전에는 다른 것이 의미가 없으므로 점수와 관계없이 이 등급이 됩니다. |
| **Unsupported objects** (앰버) | **Unsupported** 객체가 하나 이상 있고, 점수만으로는 Ready 또는 Low effort인 경우. (점수가 더 낮으면 아래 작업량 등급이 그대로 유지됩니다.) |
| **Ready** / **Low effort** (초록) | 점수 ≥ 90 / ≥ 70이고 **Unsupported** 객체가 없음. |
| **Moderate effort** / **Significant effort** (앰버) | 점수 ≥ 45 / ≥ 1. |
| **High effort** (빨강) | 점수 0. |

### 리포트 활용법

심각도 순으로 위에서부터 처리하세요:

1. **모든 Unsupported 항목을 해결하세요**(`UNSUPPORTED`). 이 항목들은 마이그레이션을 가로막습니다.
   기본 키가 없으면 추가하고, 트리거·루틴·이벤트는 애플리케이션으로 옮기며(스케줄은 Amazon
   EventBridge Scheduler로), Aurora DSQL이 인덱싱할 수 없는 기본 키를 가진 테이블은 키를 다시
   잡으세요(re-key). 각각 무엇을 할지는 [§2.4](#24-evaluation-항목-참조)에 있습니다.
2. **모든 Review needed 항목을 결정하세요**(`MANUAL`). 각 항목은 무엇을 잃거나 무엇이 바뀌는지와 할
   일을 알려 줍니다: 그대로 받아들이거나, Schema Conversion에서 타깃 DDL을 바꾸거나, 컷오버 전에
   애플리케이션을 바꿉니다. 초대형 LOB·JSON 컬럼(`OVERSIZED_LOB`)은 먼저 Data Migration 사전
   점검(또는 **Check sizes**, [§13.5](13-excluding-tables-and-columns.md#135-초대형-lob-후보와-check-sizes))을
   실행하고, 값이 이미 한도를 넘는 경우에만 조치하세요: 그 내용을 외부 스토리지(예: Amazon S3)로
   옮기거나 컬럼을 제외합니다(그러면 도구가 그 컬럼에 NULL을 씁니다).
3. **Automatic 항목**(`AUTO`)은 할 일이 없고, **Recommended** 노트는 선택적인 조언입니다.

> **팁:** 한 가지 수정이 적용되는 모든 객체를 보려면, 목록의 검색 상자에 그 규칙 id(예:
> `BYTEA_KEY`)를 입력하세요.

### 가장 자주 만나는 항목

- **기본 키가 없는** 테이블(`NO_PRIMARY_KEY`)이나 바이너리 컬럼의 기본 키(`BYTEA_KEY`): 소스에서 키를
  추가하거나 다시 잡으세요.
- **트리거, 루틴, 이벤트**: 마이그레이션되지 않으니 로직을 애플리케이션에서 다시 만드세요.
- **텍스트 비교가 달라짐**(`CI_COLLATION`): Aurora DSQL은 텍스트를 바이트로 비교하므로, 대소문자를
  무시하던 MySQL collation에서는 `=`, `LIKE`, `UNIQUE`가 대소문자를 구분하게 됩니다.
- **버려진 기본값**, 또는 MySQL `ON UPDATE CURRENT_TIMESTAMP`(`DEFAULT_DROPPED`,
  `ON_UPDATE_TIMESTAMP`): 애플리케이션이 값을 설정하게 하세요.
- MySQL에서 `boolean`이 되는 `TINYINT(1)`(`TINYINT_BOOLEAN`)과 0이거나 잘못된 날짜(`ZERO_DATES`).

모든 항목을 소스 엔진별로, 대처 방법과 함께 [§2.4](#24-evaluation-항목-참조)에 정리했습니다. AI 어시스트는 항목을
설명하거나 수정안을 초안으로 써 줍니다([§2.5](#25-ai-어시스트-선택)).

---

## 2.2 Schema Conversion — DSQL DDL 생성·적용

평가 결과를 바탕으로 Schema Conversion이 소스 스키마를 Aurora DSQL DDL로 바꿔 타깃에 적용합니다:

1. **Object browser**에서 객체를 **선택**합니다: 스키마나 객체를 체크하고(또는 **Select all**),
   **Generate DDL for selected**를 클릭합니다.
2. 각 객체의 DDL을 **검토**하고, 기본 키 전략을 고르고, 항목이 요구하는 곳에서 타깃 DDL을
   편집합니다.
3. DDL을 타깃에 **적용**합니다. 객체 하나씩 적용하거나 한 번에 모두 적용할 수 있습니다.

**Reset all**은 생성된 DDL과 편집 내용을 버리므로, 새로 선택해 생성할 수 있습니다. 선택은 타깃에
어떤 객체를 만들지를 정합니다. 대신 테이블이나 컬럼을 데이터 마이그레이션에서 빼려면 Data
Migration의 **Exclude tables & columns...** 기능을
쓰세요([13장](13-excluding-tables-and-columns.md)): 제외한 컬럼은 Aurora DSQL에 그대로 남고, 도구가
쓰는 모든 행에서 그 컬럼은 NULL입니다. 타깃 테이블이 이미 있다면(앞서 적용했거나 직접 만든 경우)
**Skip conversion & continue to Data Migration**이 다음 단계를 엽니다. Data Migration은 타깃
테이블이 있는 테이블만 적재합니다.

### 변환이 대신 해 주는 것

변환의 대부분은 여러분이 할 일이 없습니다. MySQL 스키마는 Aurora DSQL의 PostgreSQL 방언으로
번역되고, PostgreSQL 스키마는 거의 그대로 유지됩니다. 컬럼 타입을 간단히 정리하면 다음과
같습니다(전체 매핑은 [§2.3](#23-타입-매핑-참조)에 있습니다):

| | MySQL 소스 | PostgreSQL 소스 |
|---|---|---|
| 바뀌는 것 | 각 타입이 Aurora DSQL 타입으로 매핑됩니다: `TINYINT(1)` → `boolean`, `DATETIME` → `timestamp`, `BLOB` → `bytea`, `ENUM` → CHECK가 붙은 `text`. | 대부분의 타입은 그대로입니다. Aurora DSQL에 없는 타입은 같은 데이터를 담는 타입으로 바뀝니다: 배열 → `jsonb`, `money` → `numeric(23,4)`, `inet` → `text`. |
| Review needed | `TINYINT(1)`, `BIT`, `YEAR`, `TIME`, `ENUM`/`SET`, 공간 타입, `MEDIUMTEXT`/`LONGTEXT`/`MEDIUMBLOB`/`LONGBLOB`/`JSON`, 대소문자 무시 collation | 정밀도 없는 `numeric`, 바뀌는 모든 타입, `bytea`/`json`/`jsonb`, 너무 긴 `varchar(n)`/`char(n)`, `C` 이외의 collation |
| Unsupported | — | composite 타입, domain, 사용자 정의 range, 확장 타입(`citext`, `hstore`, PostGIS, pgvector), 시스템 타입(`oid`, `pg_lsn`, …) |
| 전체 매핑 | [MySQL → Aurora DSQL](#mysql--aurora-dsql) | [PostgreSQL → Aurora DSQL](#postgresql--aurora-dsql) |

변환이 하는 나머지 일:

- **기본값**은 그대로 옮겨지며, 이것이 가장 중요한 경우는 `NOT NULL` 컬럼입니다(그 컬럼을 생략한
  `INSERT`가 계속 동작합니다). Aurora DSQL에 대응이 없는 기본값은 드롭되고
  보고되며(`DEFAULT_DROPPED`), MySQL의 `ON UPDATE CURRENT_TIMESTAMP`는 재현되지 않습니다.
- **시계 기본값**(과 시계를 읽는 MySQL 뷰)은 자동으로 소스의 시간대로
  쓰입니다([소스 시간대와 서버 설정](#소스-시간대와-서버-설정)).
- **생성 컬럼.** PostgreSQL `STORED` 생성 컬럼은 유지되며, Aurora DSQL이 그 값을 계산합니다(쓰기는
  그 컬럼을 빼야 하며, Full Load와 CDC가 그렇게 합니다). MySQL 생성 컬럼과 PostgreSQL `VIRTUAL` 생성
  컬럼은 일반 컬럼이 됩니다: 값은 복사되지만, 컷오버 후에는 아무것도 그 값을 계산하지 않습니다.
  MySQL 식은 번역해서 타깃 DDL에 `GENERATED ALWAYS AS (…) STORED`로 다시 추가할 수 있습니다.
  적용하기 전에 결정하세요: Aurora DSQL은 나중에 생성 컬럼을 일반 컬럼으로 바꿀 수는 있지만, 이미
  있는 컬럼을 생성 컬럼으로 만들 수는 없습니다.
- **뷰.** MySQL 뷰는 MySQL의 날짜·문자열·숫자·JSON 동작까지 포함해 번역되고, PostgreSQL 뷰는
  PostgreSQL이 저장한 그대로 복사됩니다. 표시된 뷰는 적용하기 전에 나란히 놓고
  검토하세요(`VIEW_NOT_CONVERTED`, 또는 결과가 다르다는 노트. PostgreSQL에서는 타입이 바뀌는 컬럼을
  읽는 뷰의 `VIEW_REMODELLED_COLUMN`).
- **인덱스**는 테이블과 함께, 행이 적재되기 전에 만들어집니다(`CREATE INDEX ASYNC`, Aurora DSQL이
  백그라운드에서 빌드합니다). MySQL FULLTEXT·SPATIAL 인덱스는 생성되지 않고, PostgreSQL GIN이나 GiST
  인덱스는 더 이상 원래의 검색에 쓰이지 않는 btree가 됩니다.
  PostgreSQL `UNIQUE … NULLS NOT DISTINCT` 인덱스는 `NULLS NOT DISTINCT`를 유지합니다.
- **파티셔닝**은 제거됩니다: Aurora DSQL이 기본 키로 행을 스스로 분산하며, 파티션들은 하나의
  테이블이 됩니다.
- **여러 MySQL 데이터베이스**는 하나뿐인 Aurora DSQL 데이터베이스 안에서 각각 하나의 스키마가
  됩니다.
- **이름**은 따옴표로 감싸 철자 그대로 유지됩니다(대소문자가 섞인 이름에 주의하세요:
  `MIXED_CASE_IDENTIFIERS`).
- **외래 키**는 유지되며, 적재 후에 적용됩니다([외래 키](#외래-키)).

### 기본 키 전략 선택

각 테이블 카드에는 **Primary key** 선택이 있습니다. 적용하기 전에 정하세요: 테이블이 Aurora DSQL에
만들어지고 나면 키를 바꿀 수 없으며, 나중에 바꾸려면 CDC를 멈춘 상태에서 **Replace**와 다시 적재가
필요합니다([최종 상태](#최종-상태)).

| 전략 | 제공되는 경우 | 애플리케이션에 주는 의미 |
|---|---|---|
| **Server-generated (IDENTITY)** | 단일 컬럼 생성 키(MySQL `AUTO_INCREMENT`, PostgreSQL `serial` 또는 identity)일 때. 키가 `bigint`에 들어가면 기본값 | Aurora DSQL이 캐시된 `bigint` identity로 키를 계속 생성합니다: 값에 빈틈이 생기고 순서가 느슨해집니다. Full Load는 소스의 id를 유지합니다. MySQL에서는 insert 관용구 세 가지가 동작하지 않게 됩니다: 키에 명시적 `NULL`이나 `0` 넣기, `LAST_INSERT_ID()`(`RETURNING` 사용), 그리고 애플리케이션이 직접 지정한 id(identity를 진행시키지 않습니다). |
| **Keep source PK** | 항상. 소스가 생성하지 않는 키와, `bigint`가 담을 수 없는 MySQL `BIGINT UNSIGNED` 키의 기본값 | 키는 타입을 유지하며, Aurora DSQL에서는 아무것도 키를 생성하지 않습니다: 소스가 생성하던 키라면 애플리케이션이 insert마다 값을 지정해야 합니다. |
| **Composite key** | 테이블에 키 앞에 둘 고분산 컬럼이 있을 때 | **Leading column (high-cardinality)** 선택란에서 고른 컬럼이 키 앞에 붙어(예: `(customer_id, id)`) 쓰기를 Aurora DSQL 파티션 전체에 분산합니다([§7.1](07-performance-and-tuning.md#기본-키-전략--핫-파티션-회피)). 애플리케이션의 쿼리·조인·upsert는 전체 키를 써야 하며, UNIQUE 인덱스가 원래 키의 유일성을 유지합니다. CDC를 쓰면 모든 키 컬럼이 계속 캡처되어야 합니다. 값이 절대 바뀌지 않는 컬럼을 고르세요: 도구는 이를 검사할 수 없습니다. |

Full Load가 완전히 끝난 뒤와 컷오버 때 다시 한 번, 도구는 각 identity를 적재된 id 다음으로 옮기므로
컷오버 후 애플리케이션의 첫 insert가 충돌하지
않습니다([§3.4](03-full-load.md#34-배치재적용-안전한정-병렬-적재),
[§10.3](10-conclusion.md#103-cut-over-애플리케이션을-dsql로-전환)).

### 외래 키

Aurora DSQL은 외래 키를 강제하며, 도구는 기본적으로 외래 키를
유지합니다(**Generate DDL for selected** 옆의 **Preserve foreign keys**). Schema Apply는 외래 키를
만들지 않습니다: 적재는 부모·자식 테이블을 부모→자식 순서 없이 쓰고, CDC는 테이블 간 변경을 순서
없이 적용하므로, 강제되는 키가 있으면 단지 먼저 도착했을 뿐인 행을 거부하게 됩니다. 각 테이블의 키는
별도의 읽기 전용 창인 **Foreign keys — applied by a separate action, not at Schema Apply**에
나타나며, **Schema Apply 직후 외래 키가 없는 타깃은 정상이며 실패가 아닙니다.**

**언제 적용되는가:**

- **Full load only** — 적재가 끝난 뒤 Data Migration 단계의 명시적인 **Apply foreign keys** 동작으로
  적용합니다(적재 종료 시 자동으로 적용되지 않습니다).
- **CDC** — Cut over 단계에서, 최종 드레인 후 애플리케이션을 재지정(repoint)하기 전에
  적용합니다([§10.3](10-conclusion.md#103-cut-over-애플리케이션을-dsql로-전환)). 다시 실행해도
  안전합니다.

**먼저 추가하고, 나중에 검증합니다.** 각 키는 Aurora DSQL이 받아들이는 유일한 형태인 `NOT VALID`로
추가됩니다: 행을 하나도 읽지 않고 추가되므로 자식 테이블 크기와 상관없이 몇 초면 끝나며, 그 순간부터
모든 신규 쓰기를 강제합니다. 그다음 Aurora DSQL이 이미 적재된 행을 백그라운드에서 검증합니다.

**검증이 실패하면.** 키는 `NOT VALID` 상태로 그대로 남아 이를 깨는 신규 쓰기를 계속 거부하지만, 이미
적재된 행 중 일부가 Aurora DSQL에 없는 부모 행을 가리킵니다. 화면은 키를 깨는 행 수와 그 행을
나열하는 쿼리를 보여 줍니다. Aurora DSQL에서 트랜잭션당 최대 3,000행씩 나눠 행을 고치고(없는 부모
행을 추가하거나, 행이 있는 부모나 NULL을 가리키게 하거나, 삭제), **Validate again**을 클릭하세요.

**추가하기 전에 세는 키.** 부모 쪽이 설계상 Aurora DSQL에 다 있지 않은 키(참조 컬럼이 제외되었거나,
부모 또는 자식 테이블이 마이그레이션되지 않는 키)는 먼저 셉니다. 자식 행이 Aurora DSQL에 없는 부모
행을 가리키면, 그 키는 이유와 행 수를 밝히는 노트와 함께 건너뜁니다: 추가하면 그런 부모를 가리키는
애플리케이션의 쓰기를 거부하게 되기 때문입니다.

**직접 추가하거나 이름을 바꾼 키.** 타깃 DDL에서 추가하거나 이름을 바꾼 외래 키도 다른 키와 똑같이
적용·검증되며, 이름은 Aurora DSQL이 저장하는 대로 읽습니다: 따옴표 없는 이름은
소문자로(`FK_Orders_Cust`는 `fk_orders_cust`).

**비용.** 강제되는 키는 관련 테이블의 모든 쓰기에 읽기를 더하며(쓰기가 많은 테이블은 먼저
벤치마크하세요), 충돌은 재시도 가능한 `40001`로 돌아옵니다. `CASCADE`, `SET NULL`, `SET DEFAULT`
동작은 Aurora DSQL의 트랜잭션당 3,000행 한도에 포함되므로 그보다 큰 cascade는 실패합니다: 자식 행
수에 상한이 없는 곳에서는 `NO ACTION`이나 `RESTRICT`를 택하세요.

**CDC를 쓰는 MySQL 소스: cascade는 공백입니다.** MySQL은 `ON DELETE` / `ON UPDATE`의 `CASCADE`,
`SET NULL`, `SET DEFAULT`를 엔진 내부에서 수행하고 그 자식 변경은 바이너리 로그에 남지 않으므로,
CDC는 고아 자식 행이나 낡은 자식 행을 남길 수 있습니다(`FK_CASCADE_CDC_GAP`, **Review needed**). 그
동작을 명시적인 자식 행 문장으로 바꾸거나, 최종 비교 전에 소스 쓰기를 멈추세요. 컷오버에서
**I've cut over**는 그런 키와, MySQL이 Aurora DSQL보다 느슨하게 맞추는 텍스트 컬럼의 키(대소문자
무시 collation, 또는 `VARCHAR`에서 `utf8mb4_bin` 같은 PAD SPACE collation)가 검증될 때까지, 또는 그
검사 없이 컷오버하기로 선택할 때까지 기다립니다([§10.3](10-conclusion.md#103-cut-over-애플리케이션을-dsql로-전환)).

**PostgreSQL 소스.** 키는 선언한 절을 유지합니다: `MATCH FULL`, `DEFERRABLE`, 그리고
`ON DELETE SET NULL (…)`의 컬럼 목록 등입니다. `NOT ENFORCED`나 `PERIOD`로 선언한 PostgreSQL 18 키는
생성되지 않습니다(`PG_FK_NOT_ENFORCED`, `PG_FK_PERIOD`).

**무결성을 애플리케이션에서 강제하고 싶다면?** **Preserve foreign keys**를 끄세요. 그러면 도구가
모든 경로에서 외래 키를 빼지만, 관계는 Validation의 고아 행 검사를 위해 계속 유지합니다. 이 선택은
다시 연결해도 기억됩니다.

### 검토, 편집, 적용

**Generate DDL for selected** 후에는 각 객체가 소스 DDL과 Aurora DSQL DDL을 나란히 보여 주고 변환이
바꾼 부분을 강조하며, **Conversion warnings**와 **Recommendations**를 나열합니다.

- 항목에 따라 조치하려면 타깃 DDL을 **Edit**로 편집하고(예: 다른 숫자를 담는 `TINYINT(1)` 컬럼을
  `smallint`로 변경) **Done**을 클릭합니다. **Reset to generated**는 편집 내용을 버립니다.
- AI 어시스트를 켜면 AI DBA가 변경 초안을 만들어 줄 수
  있습니다([선택적 AI 어시스트](#선택적-ai-어시스트-amazon-bedrock)).
- **Apply to target**은 객체 하나를 적용합니다. 객체가 타깃에 이미 있으면 도구가 그 자리에서
  **Replace**, **Skip**, **Cancel** 중 하나를 묻습니다: DDL을 바꾼 뒤 테이블을 다시 적용하는 방법이
  이것입니다(Skip은 이전 테이블을 그대로 둡니다).
- **Apply generated DDL to target** 카드는 생성된 모든 객체를 적용합니다.
  **When an object already exists on the target**에서 **Skip if the object already exists**(기본값)
  또는 **Replace existing objects (destructive)** 중 하나를 고른 뒤
  **Apply all N generated objects to target**을 클릭하세요. Replace는 이미 있는 각 객체를 삭제하고
  다시 만들므로 테이블은 행을 잃습니다: 도구가 삭제할 것을 나열하고 **Confirm REPLACE**를
  요청합니다.
- **Apply results**는 각 객체를 생성됨, 건너뜀, 실패로 보여 줍니다. 실패한 객체의 DDL을 고치고
  **Retry failed (N)** 버튼을 클릭하세요.

외래 키는 적용에 포함되지 않으며([외래 키](#외래-키)), CDC가 타깃으로 스트리밍하는 동안에는 적용이
차단됩니다([최종 상태](#최종-상태)).

### 최종 상태

Schema Conversion 후에는 **Aurora DSQL 타깃 스키마가 고정**됩니다: Full Load가 쓰고 CDC가
스트리밍하는 스키마입니다. CDC는 이후 소스 DDL을 **전파하지 않으므로**, CDC 도중에 소스 스키마를
바꾸면 Aurora DSQL에 대응하는 변경을 직접 해야 합니다 — CDC가 스트리밍하는 동안 여기서
다시 적용하는 방식이 아닙니다(그동안 Apply는 **Replace**를 포함해 차단되며, Replace는 테이블을
삭제합니다). CDC를 중지한 상태에서 타입·키 변경은 다시 만들기입니다: 여기서 **Replace**, 그다음
**Drop & reload** ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)
참조).

### 애플리케이션 쿼리 확인

Schema Conversion은 스키마를 변환할 뿐, 애플리케이션이 보내는 SQL은 변환하지 않습니다. MySQL 쿼리를
변환하고, 타깃에서 쿼리를 읽기 전용으로 테스트하고, AI 어시스트를 켰다면 튜닝까지 하려면 사이드바의
선택 도구인 **Query Converter**를 쓰세요([9장](09-query-validation.md)). 어느 엔진이든
`SELECT … FOR UPDATE`는 확인하세요: Aurora DSQL의 낙관적 동시성에서는 다르게 동작합니다.

---

## 2.3 타입 매핑 (참조)

Schema Conversion이 각 소스 컬럼 타입에 대해 쓰는 것과, 그에 대해 알아 둘 점입니다. Aurora DSQL
자체의 한도(키, 인덱스, 컬럼, 값 크기, 데이터베이스당 객체 수)는
[6장 §6.1](06-limitations.md#61-aurora-dsql-기능-한계-스키마가-이에-맞아야-함)에 있습니다.

### MySQL → Aurora DSQL

Schema Conversion은 각 컬럼을 아래의 Aurora DSQL 타입으로 쓰고, Full Load와 CDC는 각 값을 같은
방식으로 저장합니다: CDC는 모든 MySQL 타입을 옮깁니다. 값을 그대로 저장할 수 없으면(`2`를 담은
`TINYINT(1)`, 음수 `TIME`), 다른 값을 쓰는 대신 Full Load는 그 테이블을 실패시키고 CDC는 그 행을
dead-letter 처리합니다. MySQL의 표시 폭(`INT(11)`)과 `ZEROFILL`은 MySQL이 숫자를 출력하는 방식만
바꾸므로 드롭됩니다.

#### 정수

<!-- type-map: mysql -->
| MySQL 타입 | Aurora DSQL 타입 | 분류 | 알아 둘 점 |
|---|---|---|---|
| `TINYINT`, `TINYINT UNSIGNED` | `smallint` | AUTO | PostgreSQL에는 1바이트 정수가 없습니다. |
| `TINYINT(1)` (`BOOL`, `BOOLEAN`) | `boolean` | MANUAL `TINYINT_BOOLEAN` | MySQL의 boolean 관례입니다: `0`과 `1`은 `false`와 `true`가 됩니다. 그 밖의 값은 거부됩니다. 컬럼에 다른 숫자가 있다면 적용하기 전에 Schema Conversion에서 타입을 `smallint`로 바꾸세요. `TINYINT(1) UNSIGNED`는 일반 `smallint`입니다. |
| `SMALLINT` | `smallint` | AUTO | |
| `SMALLINT UNSIGNED`, `MEDIUMINT`, `MEDIUMINT UNSIGNED`, `INT` (`INTEGER`) | `integer` | AUTO | 필요한 곳은 넓혀서 모든 값이 들어갑니다. |
| `INT UNSIGNED`, `BIGINT` | `bigint` | AUTO | |
| `BIGINT UNSIGNED` | `numeric(20,0)` | AUTO | 더 넓은 정수 타입이 없습니다: `numeric(20,0)`은 18,446,744,073,709,551,615까지 모든 값을 담습니다. |
| `BIT(n)` | `smallint`, `integer`, `bigint` or `numeric(20,0)` | MANUAL `BIT_TYPE` | Aurora DSQL에는 비트 문자열 타입이 없습니다. 컬럼은 모든 `BIT(n)` 값을 담는 가장 작은 타입(`BIT(15)`까지 `smallint`, `BIT(31)`까지 `integer`, `BIT(63)`까지 `bigint`)을 받으며, 비트가 나타내는 숫자를 저장합니다(`b'101'`은 `5`). 비트 연산자나 `b'…'` 리터럴을 쓰는 쿼리는 다시 작성하세요. |

**자동 증가 키.** 기본 키 전체인 `AUTO_INCREMENT` 컬럼은 `bigint GENERATED BY DEFAULT AS IDENTITY`가
되어 Aurora DSQL이 계속 값을 생성합니다(더 이상 빈틈이 없거나 엄격히 증가하지는 않습니다). 복합 키에
속한 컬럼이나 `BIGINT UNSIGNED` 키는 값 생성 없이 타입을 유지하므로 애플리케이션이 값을 지정해야
합니다. `BIGINT UNSIGNED` 키에는 대신 **Server-generated (IDENTITY)** 전략을 고를 수 있습니다:
그러면 `bigint`가 되며, 키가 9,223,372,036,854,775,807보다 큰 행은 적재되지 않습니다.
[기본 키 전략 선택](#기본-키-전략-선택)을 참고하세요.

#### 고정소수점과 부동소수점

<!-- type-map: mysql -->
| MySQL 타입 | Aurora DSQL 타입 | 분류 | 알아 둘 점 |
|---|---|---|---|
| `DECIMAL(p,s)`, `DECIMAL(p,s) UNSIGNED` (`NUMERIC`, `DEC`, `FIXED`) | `numeric(p,s)` | AUTO | 정밀도와 스케일이 유지됩니다: MySQL의 최대인 `DECIMAL(65,30)`도 Aurora DSQL의 한도 1000 안에 넉넉히 들어갑니다. `UNSIGNED`는 드롭되므로 타깃은 음수도 받아들입니다. |
| `FLOAT`, `FLOAT(M,D)`, `FLOAT UNSIGNED` | `real` | AUTO | `(M,D)`와 `UNSIGNED`는 드롭됩니다: 값은 정확히 복사되지만, 새 값은 더 이상 소수 `D`자리로 반올림되거나 음수일 때 거부되지 않습니다. |
| `DOUBLE`, `DOUBLE(M,D)`, `DOUBLE UNSIGNED` (`REAL`, `DOUBLE PRECISION`) | `double precision` | AUTO | `FLOAT`과 같습니다. |

#### 날짜와 시간

<!-- type-map: mysql -->
| MySQL 타입 | Aurora DSQL 타입 | 분류 | 알아 둘 점 |
|---|---|---|---|
| `DATE` | `date` | AUTO | 0이거나 잘못된 날짜(`0000-00-00`, `2024-02-30`)에 해당하는 Aurora DSQL 값은 없습니다: 소스가 그런 값을 담을 수 있다면, Full Load와 CDC가 그 값을 어떻게 처리하는지 Evaluation의 `ZERO_DATES` 항목이 알려 줍니다. |
| `DATETIME`, `DATETIME(fsp)` | `timestamp` | AUTO | 같은 벽시계 값입니다: 시간대를 적용하거나 옮기지 않습니다. 소수 초는 유지됩니다. 0 날짜: `DATE`와 같습니다. |
| `TIMESTAMP`, `TIMESTAMP(fsp)` | `timestamptz` | AUTO | 같은 시점을 UTC로 담습니다. 소수 초는 유지됩니다. 0 날짜: `DATE`와 같습니다. |
| `TIME`, `TIME(fsp)` | `time` | MANUAL `TIME_TYPE` | MySQL `TIME`은 기간(`-838:59:59`부터 `838:59:59`까지)이고, Aurora DSQL `time`은 하루 중의 시각입니다. `00:00:00`보다 작거나 `24:00:00`보다 큰 값은 거부됩니다. 컬럼이 기간을 담는다면 적재하기 전에 Schema Conversion에서 타입을 `interval`로 바꾸세요(키나 인덱스가 그 컬럼을 쓰면 `text`로). |
| `YEAR` | `smallint` | MANUAL `YEAR_TYPE` | 연도 숫자(`2024`)로 저장됩니다. |

#### 문자열, 바이너리, 구조화

<!-- type-map: mysql -->
| MySQL 타입 | Aurora DSQL 타입 | 분류 | 알아 둘 점 |
|---|---|---|---|
| `CHAR(n)` | `char(n)` | AUTO | |
| `VARCHAR(n)` | `varchar(n)` | AUTO | |
| `TINYTEXT`, `TEXT` | `text` | AUTO | |
| `MEDIUMTEXT`, `LONGTEXT` | `text` | MANUAL `OVERSIZED_LOB` | Aurora DSQL의 1 MiB 한도(압축된 크기 기준)를 넘는 값은 저장할 수 없습니다: Full Load는 그 행을 격리하고 CDC는 dead-letter 처리합니다. Data Migration 사전 점검이 적재 전에 그런 값을 찾아 줍니다. |
| `BINARY(n)`, `VARBINARY(n)`, `TINYBLOB`, `BLOB` | `bytea` | AUTO | 바이트가 정확히 복사됩니다. |
| `MEDIUMBLOB`, `LONGBLOB` | `bytea` | MANUAL `OVERSIZED_LOB` | 1 MiB를 넘는 값은 저장할 수 없습니다(Aurora DSQL은 `bytea`를 압축하지 않습니다): Full Load는 그 행을 격리하고 CDC는 dead-letter 처리합니다. |
| `ENUM('a','b',…)` | `text`, with a CHECK on the values | MANUAL `ENUM_SET_TYPE` | CHECK가 허용 값을 유지합니다. 정렬과 비교는 선언 순서가 아니라 알파벳순이 되며, MySQL의 빈 문자열 오류 값(`''`)은 거부됩니다. |
| `SET('a','b',…)` | `text` | MANUAL `ENUM_SET_TYPE` | 쉼표로 구분한 멤버(`'a,b'`)로 저장되며 CHECK는 없습니다: 허용 값이 더 이상 강제되지 않습니다. |
| `JSON` | `json` | MANUAL `OVERSIZED_LOB` | 같은 JSON 텍스트입니다. 1 MiB(압축 후)를 넘는 문서는 저장할 수 없습니다: Full Load는 그 행을 격리하고 CDC는 dead-letter 처리합니다. |

**대소문자 무시 collation.** 대부분의 MySQL 텍스트 컬럼이 이런 collation을
씁니다(`utf8mb4_0900_ai_ci`가 MySQL 8의 기본값). collation은 옮겨지지 않습니다: Aurora DSQL은
텍스트를 대소문자를 구분해 비교하므로, 모든 값이 정확히 마이그레이션되더라도 `=`, `LIKE`,
`ORDER BY`, `UNIQUE`가 다르게 동작합니다(`bob` 옆의 `Bob`을 거부하던 `UNIQUE` 컬럼이 둘 다
받아들입니다). Evaluation은 그런 테이블을 MANUAL로 표시합니다(`CI_COLLATION`). `_bin`이나 `_cs`
collation은 이미 대소문자를 구분하므로 표시되지 않습니다.

#### 기타

<!-- type-map: mysql -->
| MySQL 타입 | Aurora DSQL 타입 | 분류 | 알아 둘 점 |
|---|---|---|---|
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION` | `bytea` | MANUAL `SPATIAL_TYPE` | Aurora DSQL에는 공간 타입이 없습니다. geometry는 SRID 없이 WKB 바이트로 유지되며, 공간 함수와 `SPATIAL` 인덱스는 쓸 수 없습니다. |

**Aurora DSQL이 인덱싱할 수 없는 컬럼.** `bytea` 컬럼(`BINARY`, `VARBINARY`, `BLOB` 계열 타입 또는
공간 타입에서 온 것)은 기본 키나 인덱스의 일부가 될 수 없습니다: 그런 컬럼을 포함한 기본 키는
테이블을 UNSUPPORTED로 만들고, 그런 컬럼의 인덱스는 생성되지 않습니다(`BYTEA_KEY`). `interval`
컬럼도 마찬가지이므로, 키에 쓰이면서 기간을 담는 `TIME` 컬럼은 `text`로 바꿉니다.

### PostgreSQL → Aurora DSQL

Aurora DSQL은 PostgreSQL 호환이므로 대부분의 타입은 그대로 유지됩니다. Aurora DSQL에 없는 타입은
같은 데이터를 담는 타입으로 바뀌거나(MANUAL: 애플리케이션이 새 타입을 따라야 합니다), 그런 타입이
없으면 그대로 두고 Schema Conversion에서 컬럼을 바꿀 때까지 테이블이 UNSUPPORTED로 평가됩니다. Full
Load는 변환된 컬럼의 모든 값을 옮깁니다.

**CDC** 열은 CDC가 Full Load와 다른 곳을 알려 줍니다. Evaluation은 이런 컬럼을
`CDC_UNCARRYABLE_COLUMN`으로 보고하며, CDC로 복제할 때에만 문제가 됩니다:

- *NULL로 도착*: 컬럼이 CDC의 변경 이벤트에 없으므로, CDC가 삽입하는 행은 NULL(또는 컬럼의 기본값)을
  받고 갱신은 이전 값을 남깁니다. 기본값이 없는 `NOT NULL` 컬럼이면 그런 변경이 대신 실패하며,
  어떻게 되는지는 `CDC_UNCARRYABLE_COLUMN` 항목이 알려 줍니다.
- *dead-letter 처리*: 그 컬럼을 설정하는 모든 변경이 dead-letter 처리되어 타깃에 도달하지 않습니다.
- *바뀐 값으로 도착*: CDC가 Full Load와 다른 값을 씁니다.

*NULL로 도착*과 *바뀐 값으로 도착*은 CHECKSUM 검증만 알아챕니다. 각각을 피하는 방법은
[4장](04-cdc-and-dsql-constraints.md)과 [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)에
있습니다.

#### 정수

<!-- type-map: postgres -->
| PostgreSQL 타입 | Aurora DSQL 타입 | 분류 | CDC | 알아 둘 점 |
|---|---|---|---|---|
| `smallint`, `integer`, `bigint` (`int2`, `int4`, `int8`) | `smallint`, `integer`, `bigint` | AUTO | — | 같은 타입입니다. |

**serial과 identity 컬럼.** `smallserial`, `serial`, `bigserial`과 identity 컬럼은 시퀀스가 값을
공급하는 이 정수 타입들입니다. 기본 키 전체인 컬럼은 `bigint GENERATED BY DEFAULT AS IDENTITY`가
되어 Aurora DSQL이 계속 값을 생성합니다([기본 키 전략 선택](#기본-키-전략-선택) 참고). 복합 키에
속하거나 다른 컬럼에 있으면 시퀀스가 옮겨지지 않으므로 애플리케이션이 값을 지정해야
합니다(Evaluation은 그런 키가 아닌 컬럼을 알려 줍니다: `NON_KEY_SEQUENCE`).

#### 고정소수점과 부동소수점

<!-- type-map: postgres -->
| PostgreSQL 타입 | Aurora DSQL 타입 | 분류 | CDC | 알아 둘 점 |
|---|---|---|---|---|
| `numeric(p,s)` (`decimal(p,s)`) | `numeric(p,s)` | AUTO | — | 정밀도와 스케일이 유지됩니다(Aurora DSQL은 PostgreSQL처럼 1000까지 허용합니다). |
| `numeric` (`decimal`), with no precision | `numeric` | MANUAL `PG_UNCONSTRAINED_NUMERIC` | — | Aurora DSQL은 이를 `numeric(18,6)`으로 저장합니다: 소수 6자리를 넘는 값은 반올림되고, 정수부가 12자리를 넘는 값은 들어가지 않습니다. 컬럼에 그런 값이 있다면 적용하기 전에 Schema Conversion에서 명시적인 `numeric(p,s)`를 지정하세요. |
| `real`, `double precision` (`float4`, `float8`) | `real`, `double precision` | AUTO | — | 같은 타입입니다. |
| `money` | `numeric(23,4)` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 통화 형식의 텍스트가 아니라 정확한 금액이 유지됩니다. 통화 형식 지정과 `money` 산술은 애플리케이션으로 옮겨 갑니다. |

#### 날짜와 시간

<!-- type-map: postgres -->
| PostgreSQL 타입 | Aurora DSQL 타입 | 분류 | CDC | 알아 둘 점 |
|---|---|---|---|---|
| `date`, `time without time zone`, `timestamp without time zone`, `timestamp with time zone` (`time`, `timestamp`, `timestamptz`) | `date`, `time`, `timestamp`, `timestamptz` | AUTO | — | 같은 타입과 정밀도입니다. |
| `time with time zone` (`timetz`) | `timetz` | AUTO | — | 기본 키나 인덱스의 일부가 될 수 없습니다(`PG_UNINDEXABLE_COLUMN`). |
| `interval`, with or without fields or a precision (`interval day to second`, `interval(3)`) | `interval` | AUTO | — | 필드는 유지되고, 정밀도는 선언에서 드롭됩니다(값에서는 드롭되지 않습니다). 기본 키나 인덱스의 일부가 될 수 없습니다(`PG_UNINDEXABLE_COLUMN`). |

#### 문자열, 바이너리, 구조화

<!-- type-map: postgres -->
| PostgreSQL 타입 | Aurora DSQL 타입 | 분류 | CDC | 알아 둘 점 |
|---|---|---|---|---|
| `character varying(n)`, `character varying`, `character(n)`, `text` (`varchar(n)`, `varchar`, `char(n)`) | `varchar(n)`, `varchar`, `char(n)`, `text` | AUTO | — | 같은 타입입니다. Aurora DSQL에서 값은 `varchar`에 최대 65,535바이트, `char`에 4,096바이트, `text`에 1 MiB까지 담깁니다. 길이 제한이 없는 컬럼에 대해서는 Evaluation이 크기 확인을 권장합니다(`OVERSIZED_LOB`). |
| `character varying(n)` with n over 65535, `character(n)` with n over 4096 | `text` | MANUAL `PG_CHARACTER_LENGTH` | — | Aurora DSQL은 그런 선언 길이를 거부합니다. `text`는 1 MiB까지 모든 값을 담지만 길이 제한은 더 이상 강제되지 않습니다: 애플리케이션이 그 제한에 의존한다면 `CHECK (length(col) <= n)`을 추가하세요. |
| `bytea` | `bytea` | MANUAL `OVERSIZED_LOB` | — | 바이트가 정확히 복사됩니다. 1 MiB를 넘는 값은 저장할 수 없으며(Aurora DSQL은 `bytea`를 압축하지 않습니다), 이 컬럼은 기본 키나 인덱스의 일부가 될 수 없습니다(`BYTEA_KEY`). |
| `boolean`, `uuid` (`bool`) | `boolean`, `uuid` | AUTO | — | 같은 타입입니다. |
| `json`, `jsonb` | `json`, `jsonb` | MANUAL `OVERSIZED_LOB` | — | 1 MiB(압축 후)를 넘는 문서는 저장할 수 없습니다: Full Load는 그 행을 격리하고 CDC는 dead-letter 처리합니다. 기본 키나 인덱스의 일부가 될 수 없습니다(`PG_UNINDEXABLE_COLUMN`). |
| `xml` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 문서 텍스트는 유지되지만, `xpath()` 같은 XML 함수는 더 이상 적용되지 않습니다. |

#### 기타

<!-- type-map: postgres -->
| PostgreSQL 타입 | Aurora DSQL 타입 | 분류 | CDC | 알아 둘 점 |
|---|---|---|---|---|
| A user-defined enum (`CREATE TYPE … AS ENUM`) | `text`, with a CHECK on the values | MANUAL `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL에는 `CREATE TYPE`이 없습니다. CHECK가 허용 값을 유지하고 기본값은 텍스트로 유지되며, 정렬은 enum의 순서가 아니라 알파벳순이 됩니다. |
| A composite type (`CREATE TYPE … AS (…)`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL로 도착 | Aurora DSQL에는 `CREATE TYPE`이 없습니다. Schema Conversion에서 컬럼을 `text`로 바꾸세요(그러면 Full Load가 `(1,abc)` 같은 값의 텍스트 형태를 복사합니다). |
| A domain (`CREATE DOMAIN`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL은 domain을 지원하지만 도구는 domain을 만들지 않습니다: 먼저 Aurora DSQL에 domain을 만들거나, Schema Conversion에서 컬럼을 domain의 기본 타입(과 그 CHECK)으로 바꾸세요. CDC는 그 컬럼을 기본 타입으로 다룹니다. |
| `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 텍스트 형태(`[1,5)`)로 유지되며, range 연산자(`@>`, `&&`)와 함수(`lower()`, `upper()`)는 더 이상 적용되지 않습니다. |
| `int4multirange`, `int8multirange`, `nummultirange`, `tsmultirange`, `tstzmultirange`, `datemultirange` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL로 도착 | range와 같습니다(텍스트 형태 `{[1,3),[5,7)}`). |
| A user-defined range (`CREATE TYPE … AS RANGE`) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL로 도착 | Schema Conversion에서 컬럼을 `text`로 바꾸세요. |
| `point` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 텍스트 형태(`(1,2)`)로 유지되며, 기하 연산자는 더 이상 적용되지 않습니다. |
| `line`, `lseg`, `box`, `path`, `polygon`, `circle` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL로 도착 | `point`와 같습니다. |
| `inet`, `cidr`, `macaddr`, `macaddr8` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 정규화된 주소 텍스트가 유지되며, 네트워크 연산자와 함수는 더 이상 적용되지 않습니다. |
| `bit(n)`, with n of 2 or more | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | — | 비트 문자열(`'10101010'`)로 유지되며, 비트 연산자는 더 이상 적용되지 않습니다. |
| `bit(1)` (`bit`), `bit varying(n)` (`varbit(n)`) | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | 바뀐 값으로 도착 | 위와 같습니다. CDC는 `bit(1)` 값을 `true`/`false`로 쓰고, n보다 짧은 `bit varying(n)` 값은 앞에 0을 채워 씁니다. |
| `bit varying` (`varbit`), with no length | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | dead-letter 처리 | 위와 같습니다. |
| `tsvector`, `tsquery` | `text` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL로 도착 | 텍스트로 유지되며, 전문 검색 연산자와 인덱스는 더 이상 적용되지 않습니다. |
| `integer[]`, `text[]` and other arrays | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | — | JSON 배열로 저장됩니다(`{1,2}`는 `[1, 2]`가 됩니다): 배열 연산자와 첨자는 JSON 연산자로 바꾸세요. 배열 컬럼을 키로 하는 테이블은 적용에 실패합니다(`PG_UNINDEXABLE_COLUMN`). |
| `numeric[]` (no precision), `time with time zone[]`, `bytea[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | dead-letter 처리 | 배열과 같습니다. `numeric[]`에는 소스에서 정밀도를 지정하세요(`numeric(p,s)[]`는 CDC가 옮깁니다). |
| `interval[]`, `money[]`, `xml[]`, `point[]`, `bit(n)[]`, `bit varying(n)[]` | `jsonb` | MANUAL `PG_UNSUPPORTED_TYPE` | NULL로 도착 | 배열과 같습니다. 이 표에서 *NULL로 도착*으로 표시한 다른 타입의 배열도 NULL로 도착합니다. |
| `"char"`, `name`, `oid` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | PostgreSQL 시스템 타입입니다. Schema Conversion에서 컬럼을 `char(1)`, `varchar(63)` 또는 `bigint`로 바꾸세요. |
| `jsonpath`, `pg_lsn`, `xid`, `regclass` (and `regtype`, `regproc`, …) | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL로 도착 | PostgreSQL 시스템 타입입니다. Schema Conversion에서 컬럼을 `text`로 바꾸세요. |
| Extension types: `citext`, `hstore`, `ltree`, PostGIS `geometry` and `geography` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | — | Aurora DSQL에는 확장(extension)이 없습니다. Schema Conversion에서 컬럼을 `text`로 바꾸세요(그러면 Full Load가 값의 텍스트 형태를 복사합니다). 이때 `citext` 컬럼은 대소문자를 구분해 비교합니다. |
| Extension types: pgvector `vector`, `halfvec`, `sparsevec`, and `cube` | — | UNSUPPORTED `PG_UNSUPPORTED_TYPE` | NULL로 도착 | Schema Conversion에서 컬럼을 `jsonb`나 `text`(`sparsevec`은 `text`)로 바꾸세요. Aurora DSQL에서 어떤 타입이든 CDC는 이 컬럼을 옮길 수 없습니다. |

**Collation.** Aurora DSQL은 텍스트를 `C` collation으로만 비교합니다. 다른 collation을 가진
컬럼(`PG_COLLATION_DROPPED`)이나 기본 collation이 `C`가 아닌
데이터베이스(`PG_DATABASE_COLLATION`)는, 모든 값이 정확히 마이그레이션되더라도 옮긴 뒤에는 텍스트를
다르게 정렬하고 비교합니다.

**Aurora DSQL이 인덱싱할 수 없는 컬럼.** `bytea`, `json`, `jsonb`, `interval`, `time with time zone`
컬럼과 배열(이제 `jsonb`)은 기본 키나 인덱스의 일부가 될 수 없습니다: 그런 컬럼을 포함한 기본 키는
테이블 적용을 실패시키고, 그런 컬럼의 인덱스는 생성되지 않습니다(`BYTEA_KEY`,
`PG_UNINDEXABLE_COLUMN`).

---

## 2.4 Evaluation 항목 참조

Evaluation이 보고하는 항목을 소스 엔진별로 정리한 참조이며, 소스 시간대와 서버 설정도 담고 있습니다.
리포트를 처리하는 동안 찾아보는 용도로 쓰세요([리포트 활용법](#리포트-활용법)).

### 자주 나오는 항목과 대처 방법

가장 자주 만나게 되는 항목을, 여러분에게 뜻하는 바에 따라 묶었습니다. 화면에서는 각 항목이 관련된
객체와 값도 밝히고 전체 조언을 보여 줍니다.
소스 엔진별로 나누었으니 **두 엔진 공통**과 여러분의 엔진 블록만 읽으세요.

시간대와 평가되는 MySQL 설정(`SOURCE_TIME_ZONE`, `SQL_MODE`, `EXPLICIT_DEFAULTS_FOR_TIMESTAMP`,
`AUTO_INCREMENT_STEP`)은 다음 절에서 설명합니다.

#### 두 엔진 공통

**마이그레이션을 가로막음** (Unsupported)

| 항목 | 할 일 |
|---|---|
| 기본 키 없음(`NO_PRIMARY_KEY`) | 소스에서 기본 키를 추가하세요(항목이 승격할 수 있는 유니크 키를 알려 줍니다): 도구는 모든 테이블을 그 키로 적재·복제·검증합니다. |
| 트리거, 저장 프로시저와 함수, 이벤트; PostgreSQL materialized view와 foreign table(`TRIGGER_UNSUPPORTED`, `PROC_PLPGSQL`, `EVENT_UNSUPPORTED`, `PG_UNSUPPORTED_RELATION`) | 마이그레이션되지 않습니다. 로직은 애플리케이션에, 스케줄은 데이터베이스 밖에(예: Amazon EventBridge Scheduler와 AWS Lambda) 다시 만들고, materialized view는 작업(job)이 갱신하는 테이블로 다시 만드세요. |
| Aurora DSQL이 인덱싱할 수 없는 컬럼의 기본 키(`BYTEA_KEY`, `PG_UNINDEXABLE_COLUMN`) | 소스에서 테이블의 키를 다시 잡으세요. 예를 들어 그 값의 텍스트나 해시 형태를 키로 씁니다. 그런 컬럼은 MySQL `BINARY`, `VARBINARY`, `BLOB`과 공간 타입(Aurora DSQL에서는 모두 `bytea`), 그리고 PostgreSQL `bytea`, `json`, `jsonb`, `interval`, `time with time zone`과 배열입니다. 그런 컬럼에 대한 인덱스는 생성되지 않으므로(**Review needed**), `UNIQUE` 인덱스였다면 더 이상 유일성을 강제하지 않습니다. |
| Aurora DSQL 데이터베이스 한도 초과(`TABLE_COUNT_LIMIT`, `SCHEMA_COUNT_LIMIT`, `VIEW_COUNT_LIMIT`) | 타깃 데이터베이스가 이미 담고 있는 것을 포함해 테이블 1,000개, 스키마 10개 또는 뷰 5,000개를 넘게 되며, 점수는 **Blocked**가 됩니다. 마이그레이션을 여러 클러스터로 나누세요. |
| 테이블이나 이름의 한도 초과(`TOO_MANY_COLUMNS`, `TOO_MANY_KEY_COLUMNS`, `IDENTIFIER_LENGTH`, `NAME_COLLISION`) | 컬럼이 255개를 넘거나, 기본 키가 8개를 넘는 컬럼으로 되어 있거나, (MySQL에서) 이름의 앞 63바이트가 같은 경우입니다: 그런 컬럼이 둘이면 테이블의 `CREATE TABLE`이 실패하고, 그런 테이블이나 뷰가 둘이면 어느 쪽도 생성되지 않습니다. 소스에서 테이블이나 이름을 바꾸세요. |

**데이터를 바꾸거나 거부함** (Review needed)

| 항목 | 할 일 |
|---|---|
| Aurora DSQL의 1 MiB 한도를 넘는 값(`OVERSIZED_LOB`) | 적재 전에 크기를 확인하세요([리포트 활용법](#리포트-활용법)의 2단계): Full Load는 그런 값이 있는 행을 격리하고, CDC는 그 행을 dead-letter 처리합니다. |
| Aurora DSQL에 없는 타입이 바뀜(`PG_UNSUPPORTED_TYPE`, `ENUM_SET_TYPE`, `BIT_TYPE`, `YEAR_TYPE`, `SPATIAL_TYPE`, `PG_CHARACTER_LENGTH`) | 데이터는 대체 타입으로 마이그레이션됩니다(PostgreSQL 배열은 `jsonb`, enum은 CHECK가 붙은 `text`, `money`는 `numeric(23,4)`, MySQL 공간 컬럼은 `bytea`): 애플리케이션을 그 타입에 맞추세요. |
| 텍스트 비교가 달라짐(`CI_COLLATION`, `PG_COLLATION_DROPPED`, `PG_DATABASE_COLLATION`) | Aurora DSQL은 텍스트를 바이트로 비교하며(`C` collation), 소스의 collation은 옮겨지지 않습니다. MySQL의 대소문자 무시 collation이었다면 `=`, `LIKE`, `ORDER BY`, `UNIQUE`가 대소문자를 구분하게 되고(`Bob`과 `bob`은 서로 다른 값), PostgreSQL의 다른 collation이었다면 텍스트의 정렬 순서가 달라집니다. 그런 쿼리와 키를 검토하세요. |

**컷오버 전에 애플리케이션 변경이 필요함** (Review needed)

| 항목 | 할 일 |
|---|---|
| 버려진 기본값, 또는 MySQL `ON UPDATE CURRENT_TIMESTAMP`(`DEFAULT_DROPPED`, `ON_UPDATE_TIMESTAMP`) | 애플리케이션이 값을 설정하게 하세요: 그 컬럼을 생략한 `INSERT`는 NULL을 저장하고(`NOT NULL` 컬럼이면 거부되고), 갱신은 더 이상 타임스탬프를 새로 고치지 않습니다. 버려진 기본값은 적용한 뒤 다시 추가할 수도 있습니다. |
| 일반 컬럼이 되는 생성 컬럼(`GENERATED_COLUMN`) | 컷오버 후에는 아무것도 그 값을 계산하지 않습니다: 애플리케이션에서 계산하거나, 적용하기 전에 식을 다시 추가하세요([변환이 대신 해 주는 것](#변환이-대신-해-주는-것)). PostgreSQL `STORED` 컬럼은 유지됩니다. |
| 소스의 `CHECK` 제약(`CHECK_CONSTRAINT_DROPPED`) | 옮겨지지 않습니다. Aurora DSQL이 받아들이는 제약은 적재 후 다시 추가하거나(변환 노트가 그 문장을 제시합니다), 애플리케이션에서 규칙을 강제하세요. |
| 도구가 재현할 수 없는 뷰(`VIEW_NOT_CONVERTED`, `VIEW_UNSUPPORTED_SQL`, `VIEW_REMODELLED_COLUMN`) | 변환된 그대로는 적용에 실패하거나 다른 결과를 돌려줍니다: 적용하기 전에 타깃 DDL을 편집하거나, 적재 후 직접 다시 만드세요. |
| 그대로 옮겨지지 않는 인덱스(`UNSUPPORTED_INDEX_TYPE`, `PG_INDEX_NOT_PRESERVED`, `EXPRESSION_INDEX`, `TOO_MANY_INDEXES`) | 전문(full-text)·공간·GIN·GiST 검색은 데이터베이스 밖(예: Amazon OpenSearch Service)으로 옮기고, 식 인덱스는 적재 후 다시 만들며(`CREATE INDEX ASYNC`), 보조 인덱스는 테이블당 23개 이하로 유지하세요. |

#### MySQL 전용

**데이터를 바꾸거나 거부함** (Review needed)

| 항목 | 할 일 |
|---|---|
| `TINYINT(1)`이 `boolean`이 됨(`TINYINT_BOOLEAN`) | 0이나 1이 아닌 값은 거부됩니다: Full Load는 테이블을 중단하고, CDC는 그 행을 dead-letter 처리합니다. 컬럼에 다른 숫자가 있다면 적용하기 전에 타깃 타입을 `smallint`로 바꾸세요. |
| 기간(duration)으로 쓰는 `TIME`(`TIME_TYPE`) | 00:00:00보다 작거나 24:00:00보다 큰 값은 거부됩니다. 기간을 담는 컬럼이면 적재하기 전에 타깃 타입을 `interval`로 바꾸세요. |
| 0이거나 잘못된 날짜(`ZERO_DATES`) | Aurora DSQL에는 그런 값이 없으므로, Full Load와 CDC는 대신 NULL, 컬럼의 기본값 또는 `1970-01-01`을 씁니다([§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)). 항목이 제시하는 쿼리로 그런 값을 세고, 적재 전에 소스에서 고치세요. |

**컷오버 전에 애플리케이션 변경이 필요함** (Review needed)

| 항목 | 할 일 |
|---|---|
| 대소문자가 섞인 이름(`MIXED_CASE_IDENTIFIERS`) | Aurora DSQL은 따옴표 없는 이름을 소문자로 바꾸므로, `CustomerId`를 따옴표 없이 쓰는 SQL은 아무것도 찾지 못합니다. 그런 이름은 철자 그대로 따옴표로 감싸거나, Schema Conversion 전에 소스에서 소문자로 이름을 바꾸세요(컷오버 전에 타깃에서 바꾸지는 마세요). |
| CDC에서의 외래 키 cascade 동작(`FK_CASCADE_CDC_GAP`) | CDC는 MySQL의 cascade를 복제하지 않습니다: [외래 키](#외래-키)를 참고하세요. |

#### PostgreSQL 전용

**마이그레이션을 가로막음** (Unsupported)

| 항목 | 할 일 |
|---|---|
| 행 수준 보안(`PG_ROW_LEVEL_SECURITY`) | 컷오버 후에는 그 테이블에 접근하는 모든 역할이 모든 행에 접근합니다: 정책은 애플리케이션에서 강제하세요(항목이 정책을 나열합니다). 모든 행을 읽는 사용자(슈퍼유저, `BYPASSRLS` 역할, 또는 `FORCE ROW LEVEL SECURITY`가 아닌 테이블의 소유자)로 마이그레이션하세요. 그렇지 않으면 적재가 그 테이블에서 멈춥니다. |
| 대체 타입이 없는 컬럼 타입(`PG_UNSUPPORTED_TYPE`) | composite 타입, domain, 확장 타입(`citext`, `hstore`, PostGIS, pgvector) 또는 시스템 타입입니다: 적용하기 전에 Schema Conversion에서 컬럼의 타깃 타입을 바꾸세요(예: `text`로). |

**데이터를 바꾸거나 거부함** (Review needed)

| 항목 | 할 일 |
|---|---|
| 정밀도 없는 `numeric`(`PG_UNCONSTRAINED_NUMERIC`) | `numeric(18,6)`으로 저장되므로 소수 6자리를 넘으면 반올림되고, 정수부는 최대 12자리입니다. 값에 그 이상이 필요하다면 적용하기 전에 컬럼에 명시적인 `numeric(p,s)`를 지정하세요. |
| CDC가 옮길 수 없는 컬럼(`CDC_UNCARRYABLE_COLUMN`) | CDC를 쓸 때만 해당합니다: 값이 NULL이나 바뀐 값으로 도착하거나, 행이 dead-letter 처리됩니다. CDC를 배포하기 전에 소스에서 컬럼의 타입을 바꾸거나, 컬럼을 제외하거나, 그 테이블을 Full load only로 옮기세요. |

**컷오버 전에 애플리케이션 변경이 필요함** (Review needed)

| 항목 | 할 일 |
|---|---|
| 키가 아닌 `serial`이나 identity 컬럼(`NON_KEY_SEQUENCE`) | 타깃에서는 값을 생성하는 것이 없으므로, 그 컬럼을 생략한 `INSERT`는 거부되거나(`23502`) NULL을 허용하는 컬럼이면 NULL을 저장합니다. 애플리케이션이 값을 지정하게 하거나, 적용하기 전에 타깃 DDL에서 컬럼을 `bigint GENERATED BY DEFAULT AS IDENTITY`로 선언하고(나중에는 타입을 바꿀 수 없습니다) 컷오버 전에 적재된 값 다음으로 재시작하세요. |
| 어떤 컬럼도 소유하지 않는 시퀀스(`PG_STANDALONE_SEQUENCE`) | 생성되지 않습니다. 컷오버 전에 만들고(항목이 그 문장을 제시합니다), 컷오버 때 소스의 마지막 값 다음으로 옮기세요: 도구는 이를 동기화하지 않습니다([§10.3](10-conclusion.md#103-cut-over-애플리케이션을-dsql로-전환)). |
| Aurora DSQL이 선언대로 강제할 수 없는 규칙(`PG_EXCLUSION_CONSTRAINT`, `PG_TEMPORAL_KEY`, `PG_FK_PERIOD`) | `EXCLUDE` 제약, `WITHOUT OVERLAPS` 키, `FOREIGN KEY … PERIOD`는 컷오버 후 강제되지 않습니다: 애플리케이션에서 규칙을 강제하거나 설계를 바꾸세요. `WITHOUT OVERLAPS` 기본 키를 가진 큰 테이블은 적재하기 전에 소스에서 그 키 컬럼들에 btree 인덱스를 추가하세요. |
| `DEFERRABLE` 기본 키나 유니크 키(`PG_DEFERRABLE_KEY`) | 즉시 검사되므로, 두 행이 잠시 같은 키 값을 갖게 하는 쓰기(값 맞바꾸기, 번호 다시 매기기)는 실패합니다. CDC를 쓰면 그런 기본 키는 테이블의 `REPLICA IDENTITY`가 될 수 없습니다: `REPLICA IDENTITY DEFAULT`인 테이블은 소스에서 키를 `NOT DEFERRABLE`로 다시 만들거나 테이블을 `REPLICA IDENTITY FULL`로 설정할 때까지 CDC 사전 요구사항 검사에서 실패합니다. |
| 설치된 확장(`PG_EXTENSION_UNSUPPORTED`) | Aurora DSQL에는 확장(extension)이 없습니다: 애플리케이션의 확장 호출을 바꾸세요(`gen_random_uuid()`는 내장되어 있습니다). |

#### 조언, 그리고 불완전한 리포트

**조언만** (Recommended). 마이그레이션 자체를 위해 할 일은 없습니다:

- `AUTO_INCREMENT`: 키가 계속 값을 생성합니다([기본 키 전략 선택](#기본-키-전략-선택)). Aurora
  DSQL에서 아무것도 그 값을 생성하지 않게 되면, 이 항목은 대신 **Review needed**입니다.
- `FK_PRESERVED`: 외래 키가 유지되며 적재 후에 적용됩니다([외래 키](#외래-키)).
- `PARTITIONED_TABLE`: 파티션들이 하나의 테이블이 되며, 데이터는 잃지 않습니다. CDC를 쓰면
  컷오버까지 파티션 관리(drop, truncate, detach, attach)를 멈추세요: CDC는 그런 변경을 옮기지
  못합니다.
- `MULTIPLE_DATABASES`(MySQL): 각 데이터베이스가 자체 스키마가 됩니다. 애플리케이션이 스키마로
  한정한 이름을 쓰게 하거나, `search_path`를 설정하세요.
- `NAME_COLLISION`(인덱스): 같은 스키마의 다른 테이블·뷰·인덱스가 쓰는 이름의 인덱스는
  `<table>_<index>`로 생성됩니다.
- `PG_FK_NOT_ENFORCED`(PostgreSQL 18): 키를 생성하지 않으므로 타깃은 소스처럼 동작합니다. Aurora
  DSQL이 그런 키를 강제하게 하려면(Aurora DSQL에 그런 형태가 없는 `PERIOD`로 선언한 키는 제외) 그
  키의 `ALTER TABLE … ADD CONSTRAINT … NOT VALID`를 테이블의 타깃 DDL에 추가하세요.

**리포트가 불완전하면,** 그렇다고 알려 줍니다. MySQL에서는 마이그레이션 사용자가 나열할 수 없는
객체(`SOURCE_VISIBILITY`: `TRIGGER`, `EVENT` 또는 `SHOW_ROUTINE` 권한이 없는 경우이며, 앞의 두
권한은 그 객체를 변경하는 것도 허용합니다)와 정의를 읽을 수 없는 뷰(`VIEW_DEFINITION_UNREADABLE`:
`SHOW VIEW` 권한이 없는 경우)입니다. PostgreSQL에서는 실패한 카탈로그
읽기(`PG_CATALOG_UNREAD`)입니다. 두 엔진 모두에서는 Evaluation이 카탈로그를 읽는 동안 사라진
테이블(`CATALOG_CHANGED_DURING_READ`)입니다. 보지 못한 객체는 평가되지도 마이그레이션되지도
않습니다: 빠진 권한을 부여하거나(또는 관리자로서 그 객체를 검토하거나), Evaluation을 다시
실행하세요.

### 소스 시간대와 서버 설정

Aurora DSQL 세션은 다른 시간대를 설정하지 않는 한 UTC로 실행되지만, 시간대 없는 컬럼은 쓰인 그대로
소스의 벽시계를 담습니다. 그래서 Evaluation이 읽은 시간대가 UTC가 아니면, Schema Conversion은 시계를
읽는 곳마다 그 시간대를 자동으로 써 넣습니다: `DATETIME`(`timestamp`) 기본값은
`DEFAULT (now() AT TIME ZONE 'Asia/Seoul')`이 되고, MySQL 뷰의 `NOW()`는 그 시간대의 시계를
읽습니다. PostgreSQL 뷰는 그대로 복사되므로, 항목은 시계를 읽는 뷰를 검토할 수 있도록 나열합니다.

`SOURCE_TIME_ZONE` 항목(**Review needed**)은 해당 컬럼과 뷰를 알려 줍니다. 여러분이 할 일은
애플리케이션에 있습니다: 애플리케이션이 세션 시간대에 의존한다면(그런 컬럼에 `NOW()`나
`CURRENT_DATE`를 쓰거나 그런 컬럼과 비교한다면), 새 Aurora DSQL 연결마다
`SET TIME ZONE 'Asia/Seoul'`을 실행하거나 SQL에 시간대를 직접 쓰게 하세요. 시간대를 판별할 수 없으면
권고 항목이 그렇다고 알리며, 변환은 UTC를 가정합니다: 소스가 UTC가 아니라면 Schema Conversion에서 그
기본값들을 편집하고 애플리케이션에서 시간대를 설정하세요. 읽는 시간대는 애플리케이션 세션이 기본으로
받는 시간대입니다: MySQL에서는 GLOBAL `time_zone`(또는 파라미터 그룹의 `init_connect`가 설정하는
값), PostgreSQL에서는 기본 `TimeZone`이며, `ALTER DATABASE`나 `ALTER ROLE` 기본값은 포함하지만
마이그레이션 사용자 자신의 기본값은 포함하지 않습니다.

MySQL 소스에서는 설정 세 가지가 더 평가되며, 각각 데이터베이스 수준의 **Review needed**
항목입니다(`ZERO_DATES`는 위의 표에 있습니다):

| 소스의 설정 | 항목 | Aurora DSQL에서의 의미 |
|---|---|---|
| `ONLY_FULL_GROUP_BY`가 없는 `sql_mode` | `SQL_MODE` | `GROUP BY`로 정해지지 않는 컬럼을 선택하는 뷰나 쿼리는 거부됩니다(`42803`). 뷰라면 Schema Conversion이 적용할 때입니다. 같은 항목이 `ANSI_QUOTES`와 `PIPES_AS_CONCAT`도 표시합니다: 이 모드에서는 Query Converter에 붙여 넣는 SQL이 다른 뜻이 됩니다. |
| `explicit_defaults_for_timestamp=OFF` | `EXPLICIT_DEFAULTS_FOR_TIMESTAMP` | MySQL에서는 `NOT NULL`로 선언된 `TIMESTAMP` 컬럼에 NULL을 쓰면 현재 시각이 저장되었지만, Aurora DSQL은 그 쓰기를 거부합니다. 애플리케이션이 시각을 쓰게 하거나, `INSERT`에서 그 컬럼을 빼세요. |
| 1보다 큰 `auto_increment_increment` | `AUTO_INCREMENT_STEP` | Aurora DSQL의 identity는 1씩 증가합니다: 그 증가폭에 의존하는 로직(writer별 키 범위)은 애플리케이션으로 옮기세요. |

---

## 2.5 AI 어시스트 (선택)

AI 어시스트는 두 단계 모두에서 쓸 수 있습니다. 이 절은 무엇을 하는지와 켜는 방법을 설명합니다.

### 선택적 AI 어시스트 (Amazon Bedrock)

AI 어시스트는 선택 사항이며 기본적으로 꺼져 있고, Amazon Bedrock에서만 동작합니다. 변환 중 사람의
판단이 필요한 부분을 돕습니다:

- **Evaluation**에서는 항목 옆의 **Ask AI about this finding** 아이콘이 그 항목에 대한 **AI DBA**
  패널을 엽니다.
- **Schema Conversion**에서는 변환 경고 옆의 AI 아이콘이나
  **Ask AI DBA about converting this object**가 그 객체에 대한 AI DBA를 엽니다. 답변에 DDL이 있으면
  **Use as target DDL**이 그 문장들을 객체의 타깃 DDL로 복사하며, 다른 편집과 마찬가지로 검토하고
  적용합니다. 트리거·루틴·이벤트에 대해서는 **Ask AI DBA how to reimplement these**가 각각을 다시
  만드는 방법을 알려 줍니다.

AI 어시스트는 결정론적 변환을 대체하지 않고(AI를 꺼도 워크플로는 같습니다), 여러분이 적용하기 전에는
타깃에 닿지 않으며, 스키마와 DDL 메타데이터만 보고 Full Load나 CDC의 행 데이터는 절대 보지 않습니다.

**켜는 방법:**

1. **Connect** 화면에서 **AI Assist**를 펼치고 **Enable AI Assist**를
   켭니다(**Bedrock settings**에서 모델과 리전 설정). API 키 입력란은 없습니다: AI 어시스트는 도구의
   AWS 자격증명으로 Bedrock을 호출하며, 이 자격증명에는 선택한 모델(기본값
   `global.anthropic.claude-sonnet-5`)에 대한 `bedrock:InvokeModel`과
   `bedrock:InvokeModelWithResponseStream`이 필요합니다. AWS에 호스팅한 배포에서는 스택도 이를 켜야
   합니다:
   [Enable AI-assisted conversion (optional)](../../../deploy/DEPLOYMENT.md#enable-ai-assisted-conversion-optional)을
   참고하세요. 호출마다 Bedrock 비용과 약간의 지연이 추가됩니다.
2. **Verify AI access** 사전 점검(preflight)을 사용하세요. 이 자격증명으로 설정된 모델을 호출해 보는
   비차단 점검입니다(유효한 AWS 프로필만으로는 `bedrock:InvokeModel`이 **보장되지 않음**). Bedrock이
   그 모델을 사용할 수 없다고 하거나 모델 약관 외의 이유로 거부하면 점검은 대체(fallback) 모델도
   시도하고, 대체 모델이 응답하면 Model ID가 그 모델로 바뀌며 결과는 두 모델을 모두 밝히는 경고가
   됩니다.

**모델이 계정에서 활성화되지 않았다면.** Bedrock의 Anthropic 모델은 AWS Marketplace
약관(agreement)을 통해 계정마다 한 번 활성화됩니다. 그러면 AI DBA 패널과 Connect 화면의 AI Assist
상태가 *the configured model is not enabled for this AWS account yet* 이라고 알립니다(최초
사용(first-time use) 양식이 제출되지 않은 경우에는 대신 *not enabled or not available* 로 보고될 수
있습니다). AWS Marketplace 권한이 있는 사람이 Amazon Bedrock 콘솔에서 모델 약관을 수락하고(양식을
요구하면 제출), 약 2분 기다린 뒤 **Verify AI access**를 다시 실행하세요(명령줄 절차는
`deploy/DEPLOYMENT.md`에 있습니다). 새 계정에서는 통과가 일시적일 수 있습니다: 약관이 없어 호출이
멈추기 전까지 최대 약 15분 동안은 호출이 성공할 수 있습니다.

---

**다음:** [3. Full Load →](03-full-load.md)
