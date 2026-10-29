# 13. 테이블과 컬럼 제외

_언어: [English](../en/13-excluding-tables-and-columns.md) | **한국어** | [日本語](../ja/13-excluding-tables-and-columns.md)_

> **이전:** [부록: 성능 테스트 결과](12-performance-test-results.md)

소스의 모든 것이 Aurora DSQL에 도달해야 하는 것은 아닙니다. 개인 데이터(국가 식별 번호, 비밀번호
해시, 생년월일)를 타깃으로 옮기지 말아야 할 수 있습니다. 대형 객체(LOB) 컬럼에는 Aurora DSQL의 값당
한도를 넘는 값이 있을 수 있습니다. 그런 행은 그대로 두면 로드 시 격리(quarantine)되고, CDC도 적용할
수 없습니다(4장 §4.5). 또 감사(audit) 테이블이나 스테이징 테이블처럼 아예 필요 없는 테이블도 있을 수
있습니다. 이 모든 것은 한 곳, **Data Migration** 단계의 **Exclude tables & columns** 팝업에서
고릅니다. 모든 마이그레이션 유형(Full load only, Full load + CDC, CDC only)에서 똑같이 동작하며,
체크박스를 하나씩 체크하는 대신 짧은 텍스트 규칙으로 제외 대상을 씁니다.

> **머릿속 모델:** 규칙을 쓰면 Save가 그것을 하나의 제외 집합으로 만듭니다. 모든 경로가 이 하나의
> 집합을 읽습니다: Full Load, Start CDC, CDC 캡처 목록, 사전 점검, Validation, Cut over. 제외한
> **테이블**은 이관되지 않습니다. 제외한 **컬럼**은 Aurora DSQL에 그대로 존재하지만, 도구가 쓰는
> 모든 행에서 그 컬럼은 **NULL**입니다.

---

## 13.1 제외가 하는 일

| | 테이블 제외 | 컬럼 제외 |
|---|---|---|
| 규칙 | `exclude table shop.audit_log` | `exclude shop.users.ssn` |
| 도구에서 | 테이블 트리에서 그 테이블의 선택이 해제됩니다. 직접 체크를 해제한 것과 같습니다. 로드되지 않고 CDC도 캡처하지 않습니다. | 테이블은 그대로 이관됩니다. 도구는 이 컬럼의 소스 값을 Aurora DSQL에 절대 쓰지 않습니다. |
| Aurora DSQL에서 | 그대로 남습니다. 로드되지 않으며 **절대 drop되지 않으므로**, 이미 있던 테이블은 행을 유지합니다. | 컬럼은 (Schema Conversion이 정의한 대로) 테이블에 남습니다. 첫 행을 쓰기 전에 DEFAULT 없는 nullable로 바뀌므로, 도구가 쓰는 모든 행은 그 컬럼에 NULL을 저장합니다(§13.6). |
| Validation | 비교하지 않습니다. | 행은 평소처럼 세고 비교합니다. 제외 컬럼은 값을 비교하지 않지만, Validation이 그 컬럼이 NULL임을 증명합니다(§13.9). |

먼저 이해해 둘 점:

- **DEFAULT가 아니라 NULL.** 제외 컬럼은 타깃 컬럼의 DEFAULT로 채워지지 않고 NULL로 로드됩니다.
  DEFAULT 없는 NOT NULL 컬럼도 제외할 수 있는 이유이기도 합니다: 도구가 먼저 NOT NULL을 제거합니다.
  NOT NULL 제거는 **단방향**입니다(§13.4).
- **Save만으로는 Aurora DSQL에서 아무것도 바뀌지 않습니다.** 타깃 변경은 로드, Start CDC 또는 Change
  & reload가 실행될 때만 일어나며, 각 동작은 그 변경을 먼저 보여 줍니다.
- **절대 제외할 수 없는 컬럼이 있습니다.** 키 컬럼, 보존된 외래 키가 참조하는 컬럼, Aurora DSQL이
  계산하는 컬럼은 거부되며, 사유가 항상 표시됩니다(§13.4).
- **데이터가 이동하면 집합이 고정됩니다.** Full Load 후, 또는 CDC 스택이 배포되면 제외 설정이
  잠깁니다. 그래도 바꿀 수는 있지만, §13.7의 경로를 통해서만 가능합니다. Full load only에서는 Full
  Load 후에도 이관할 테이블을 바꿀 수 있고, Full load + CDC에서는 CDC를 시작하기 전까지 테이블을
  빼거나 추가할 수 있으며, 추가한 테이블은 먼저 Full Load 단계에서 로드되는데, 보통 Retry unfinished
  tables가 Full Load의 시작 지점 아래에서 로드합니다(§13.7).

---

## 13.2 Exclude tables & columns 팝업

### 위치

**Data Migration** 단계에서 테이블 목록의 **Tables to migrate** 바로 아래에 **Exclude tables &
columns...** 버튼이 있습니다. 그 옆의 상태 줄이 지금 무엇이 제외되어 있는지 알려 줍니다:
`Nothing excluded`, 저장된 집합(`Rules exclude 2 tables, 5 columns`; 직접 체크를 해제한 테이블은
세지 않습니다), 또는 집합을 더 이상 직접 편집할 수 없을 때의 `... excluded - locked`(§13.7). 두 가지
상태는 사용자의 조치를 요구합니다:

| 상태 줄 | 할 일 |
|---|---|
| `Your rules now ... - review` (경고) | 저장된 규칙이 현재 스키마에 대해 다르게 해석됩니다(예: 새 컬럼이 와일드카드에 매칭됨). 이런 드리프트는 **절대 저절로 적용되지 않습니다**: 팝업을 열고 Save해야 받아들여집니다. Start Full Load 다이얼로그도 이 경고를 반복합니다. |
| `N saved exclusions no longer apply. Starting is blocked until you review them.` (빨강, **Review** 포함) | 저장된 항목이 거부되었거나, 사라진 컬럼처럼 소스에 더 이상 없는 것을 가리킵니다(§13.4): **Review**를 클릭하세요. |

선택한 테이블에 Aurora DSQL의 값 한도를 넘을 수 있는 컬럼이 있으면 두 번째 줄이 나타납니다(§13.5).
크기 점검(**Check sizes** 또는 사전 점검)이 그런 값을 측정하면 이 줄은 경고로 바뀝니다.

### 규칙 작성과 점검

팝업에는 규칙 편집기(한 줄에 규칙 하나, **?** 아이콘이 예시를 보여 줌)와, 그 아래에 입력을 멈추고 약
300 ms 뒤 갱신되는 실시간 미리보기가 있습니다. 미리보기는 각 문제를 줄 번호와 함께(수정 방법이
있으면 원클릭 수정과 함께) 나열하고, 각 규칙이 무엇에
매칭되는지(`L3  matches 4 columns - 1 kept by include on L5 - 2 blocked (primary key, computed)`)
보여 주며, 저장하기 전에 결과를 미리 보여 줍니다: 규칙이 선택 해제하는 테이블, 제외되는
컬럼("migrated as NULL"), 거부된 모든 컬럼과 그 사유(§13.4), 정확한 `ALTER` 구문(**Changes to Aurora
DSQL**, §13.6), 그리고 CDC 마이그레이션 유형에서는 **CDC capture list**(`ColumnExcludeList`, 최대
4,096바이트, §13.8). **Review all N items**는 관련된 모든 것을 표 하나로 보여 주며 **All / Excluded
/ Blocked / Warning**으로 필터할 수 있습니다. **Oversized-LOB candidates**는 §13.5입니다.

다음 문제 중 하나라도 남아 있으면 **Save는 비활성 상태로 남습니다**:

- 구문 오류;
- 아무것에도 매칭되지 않는 이름(미리보기가 대소문자만 다른 이름을, 없으면 최대 세 개의 유사한 이름을
  제안합니다);
- 잠겨 있는 동안 테이블 집합을 바꾸게 되는 테이블 규칙, 또는 삭제한 테이블 규칙(§13.7);
- 이름에 `:`가 들어간 테이블의 컬럼에 매칭되는 컬럼 규칙(§13.10);
- Aurora DSQL 테이블에 없고 제외되지도 않은 소스 컬럼. 로드가 42703으로 확실히 실패하기
  때문입니다(§13.4). 수정은 그 컬럼에 대한 `exclude` 줄을 덧붙이지만, 직접 클릭해야 합니다: 컬럼은
  절대 자동으로 마이그레이션에서 빠지지 않습니다;
- 압축 형태로도 4,096바이트를 넘는 CDC 캡처 목록(§13.8);
- 선택된 테이블을 하나도 남기지 않는 규칙.

수정은 자기 줄만 다시 쓰거나, 집합 전체에 대한 것이면(42703 사례) 규칙 줄을 덧붙입니다. Save는
미리보기가 입력한 내용을 따라잡을 때까지 기다리므로, 보지 못한 내용을 저장하는 일은 없습니다.

### 저장, 취소, 여러 탭

- **Save**는 규칙 텍스트를 해석된 결과와 함께 저장합니다: 제외 컬럼, 테이블 선택, 그리고 규칙이 선택
  해제한 테이블. Aurora DSQL에서는 아무것도 바꾸지 않습니다.
- **Esc와 배경 클릭으로는 팝업이 닫히지 않으므로**, 실수로 편집 내용을 잃지 않습니다.
  **Cancel**(또는 닫기 아이콘)은 편집 내용을 버립니다.
- **브라우저 탭마다 팝업은 최대 하나입니다.** 팝업을 연 뒤 제외 설정이 바뀌었거나(다른 탭에서,
  attach한 스택을 통해, 또는 실행이나 배포가 시작되어) 잠금이 걸리면, Save는 거부되고 입력한
  텍스트는 유지됩니다.
- 규칙, 규칙이 선택 해제한 테이블, 다시 체크한 테이블, 보류 중인 줄(§13.7)은 세션과 함께 저장되고
  세션과 함께 복원됩니다.

### 팝업이 읽기 전용일 때

제외 설정이 잠기면 팝업은 읽기 전용으로 열립니다. 다음을 보여 줍니다:

- 잠금 원인과 이 세션에서 쓸 수 있는 조치;
- 저장된 규칙, 보류 중인 줄, 그리고 저장된 각 항목이 지금 무엇을 하는지에 대한 메모;
- 배포된 스택이라면 배포된 그대로의 캡처 목록. 스택이 이미 스트리밍했고 세션의 목록이 그것과
  다르면(그래서 Start CDC가 거부하게 되면) 두 목록을 나란히 보여 줍니다.

팝업이 제공할 수 있는 버튼은 **Change & reload**와 **Change migration type**입니다. 두 번째 버튼은
마이그레이션 유형 선택기로 스크롤할 뿐이며, 도구가 대신 유형을 바꾸는 일은 없습니다. §13.7 참고.

---

## 13.3 규칙 언어 참조

| 줄 | 의미 |
|---|---|
| `exclude table schema.table` | 테이블을 마이그레이션에서 뺍니다(선택 해제). |
| `exclude schema.table.column` | 컬럼을 NULL로 이관합니다. |
| `exclude type:name[,name...]` | 이 **소스** 데이터 타입의 모든 컬럼을 제외합니다. |
| `include ...` | 위 세 형태 중 무엇이든 예외로 씁니다: 매칭되는 `exclude`를 취소합니다. |
| `# ...` | 주석. 단독 줄이나 규칙 뒤에 씁니다. 빈 줄은 무시됩니다. |

MySQL 소스에서 `schema`는 데이터베이스 이름입니다. PostgreSQL에서는 스키마입니다.

**이름과 키워드.**

- `exclude`, `include`, `table`, `type:`는 대소문자를 구분하지 않습니다. **이름은 대소문자를
  구분합니다.** `shop.Users`는 `shop.users`에 매칭되지 않으며, 오류가 올바른 철자를 제안합니다.
- 컬럼 규칙은 항상 세 부분입니다. `table` 없는 두 부분 이름은 오류이며, 그 수정은 `table`을
  삽입합니다.
- 한 줄에 규칙 하나를 씁니다. 쉼표 목록은 `type:` 뒤에만 허용되며, 공백을 포함할 수 없습니다:
  `type:longtext,longblob`처럼 쓰고, `type:longtext, longblob`처럼 쓰지 않습니다. 두 번째 형태의
  오류는 **Remove the spaces**를 제안합니다.
- 규칙 뒤에 주석이 아닌 텍스트가 있으면 오류이며, 그 수정은 그 텍스트를 주석으로 바꿉니다.

**와일드카드.** `*`가 유일한 와일드카드입니다. **한 이름 부분 안에서** 빈 문자열을 포함한 임의의
문자열에 매칭되며, `.`를 절대 넘지 않습니다:

- `exclude table shop.audit_*`는 `shop`에서 이름이 `audit_`로 시작하는 모든 테이블에 매칭됩니다;
- `exclude shop.*.password_hash`는 모든 `shop` 테이블의 그 컬럼에 매칭됩니다;
- `exclude *.*.ssn`은 모든 `ssn` 컬럼에 매칭됩니다.

`_`, `%`, `?`는 평범한 문자입니다. 따옴표로 감싼 `"*"`는 문자 그대로의 별표이며, 타입 이름에는
와일드카드를 쓸 수 없습니다.

**따옴표.** 이름 부분에 공백, `.`, `,`, `*`, `#`, `"`가 들어 있으면 큰따옴표로 감쌉니다. 따옴표
안에서 `"`는 `""`로 씁니다:

```
exclude "my schema"."order.items".notes
exclude shop.reviews."say ""hi"""
```

줄바꿈이 들어간 이름은 규칙으로 쓸 수 없습니다. 대신 그 테이블을 제외하거나, Schema Conversion에서
타깃 DDL을 바꾸세요.

**줄 순서와 상관없이 include가 우선합니다.** `include`와 `exclude`가 같은 항목에 매칭되면 그 항목은
유지됩니다. 줄 순서는 전혀 상관없습니다(AWS DMS selection rule과 다름). 테이블과 컬럼은 따로
다룹니다: `include table`은 테이블 규칙만 취소하고, 컬럼 또는 `type:` include는 컬럼 규칙만
취소합니다.

**범위.** 컬럼 규칙과 `type:` 규칙은 선택된 테이블에서만 효과가 있습니다. 한 줄의 매칭이 모두 선택
해제된 테이블에 있으면 그 줄에 `only unselected tables` 플래그가 붙습니다. 같은 Save에서 테이블
규칙이 선택 해제하는 테이블에 대한 컬럼 규칙은 효과가 없습니다.

**테이블 규칙은 자신이 선택 해제한 것을 기억합니다.** `exclude table s.t`는 트리에서 그 테이블들의
선택을 해제하고, 팝업은 규칙이 어느 테이블을 선택 해제했는지 기록합니다. 나중에 그 규칙을
지우면(또는 `include table`을 추가하면) 정확히 그 테이블들만 다시 선택됩니다. 직접 체크를 해제한
테이블은 해제된 채로 남습니다. 규칙이 선택 해제한 테이블을 트리에서 다시 체크하면 의도적인 재정의로
취급됩니다: 그 행과 팝업이 이를 이름으로 표시하지만, 규칙도 함께 바꾸지 않으면 다음 Save가 다시 선택
해제합니다.

**`type:` 키.** `type:` 규칙은 하나의 키로 축약한 **소스** 컬럼 타입에 매칭됩니다:

| 소스 | 키 | 예 |
|---|---|---|
| MySQL | 소문자 베이스 타입, `(` 또는 공백에서 자름 | `int unsigned` → `int`; `decimal(10,2) unsigned` → `decimal`; `varchar(255)` → `varchar`; `longtext` |
| PostgreSQL | typmod를 뺀 `format_type` 이름. 여러 단어 이름은 유지되고(따옴표로 감쌈), `[]`도 유지되며, 사용자 정의 타입의 대소문자도 유지됩니다 | `numeric(10,2)` → `numeric`; `character varying(40)` → `"character varying"`; `integer[]`; `interval day to second(3)` → `interval` |

키는 정확히 입력해도 되고, typmod를 붙여도 되며(`type:varchar(255)`는 `type:varchar`를 뜻함),
대소문자를 달리 써도 됩니다(`type:TEXT`는 `text`를 찾음). 별칭은 없습니다: PostgreSQL에서
`type:varchar`는 `character varying`에 매칭되지 않지만, 오류가 `type:"character varying"`을
제안합니다. MySQL의 `type:integer`도 마찬가지로 `int`를 제안합니다.

**예시.** MySQL 소스에서 개인 데이터와 대형 객체에 쓰는 전형적인 집합:

```
# 개인 데이터: 이 값들은 절대 복사하지 않음
exclude *.*.ssn
exclude shop.*.password_hash
exclude shop.customers.date_of_birth

# Aurora DSQL의 값당 한도를 넘을 수 있는 대형 객체
exclude type:longtext,longblob
include shop.pages.body        # 페이지 본문은 작으므로 유지

# 이 마이그레이션에 필요 없는 테이블
exclude table shop.audit_*
exclude table shop."tmp import"
```

PostgreSQL 소스에서의 같은 예:

```
exclude public.users.national_id
exclude type:bytea,jsonb
include public.settings.preferences
```

아무것에도 매칭되지 않는 줄은 오류이므로, 이 스키마에 해당하지 않는 줄은 지우세요. 예를 들어 `ssn`
컬럼을 가진 테이블이 없으면 `exclude *.*.ssn`은 오류입니다.

---

## 13.4 제외할 수 없는 것과 경고

### 거부되는 컬럼

모든 경로를 하나의 적격성 규칙이 결정하며, 그 사유는 항상 표시됩니다. 거부된 컬럼은 미리보기에
`(not excluded)`로 나열되고 절대 저장되지 않습니다. 그래도 Save할 수 있으며, 그 컬럼은 그냥
이관됩니다.

| 컬럼 | 표시 | 이유 |
|---|---|---|
| 소스 기본 키 컬럼 | primary key | 도구가 행을 읽고, 중복을 제거하고, 복제하는 데 필요합니다. |
| Schema Conversion에서 고른 타깃 기본 키 컬럼: 복합 키의 선두 컬럼, 편집한 DDL의 이름 붙은 `CONSTRAINT ... PRIMARY KEY`의 컬럼, 또는 CDC 재키(re-key) 컬럼(`message.key.columns`) | target key | 로더는 모든 행의 모든 타깃 키 컬럼을 읽으며, CDC는 각 행을 그 키로 쓰고 삭제하므로 이 컬럼이 필요합니다. |
| 선택된 테이블의 보존된 외래 키가 참조하는 컬럼(외래 키를 보존하는 동안만) | referenced by a foreign key | 모든 자식 행이 고아가 되고, 외래 키를 추가할 수 없게 됩니다. |
| Aurora DSQL이 계산하는 컬럼: 보존된 `GENERATED ... STORED` 컬럼, 또는 기본 키 밖의 identity 컬럼 | computed | Aurora DSQL이 계산하며, 도구는 절대 쓰지 않습니다. DEFAULT를 제거해도 identity 컬럼을 NULL로 만들 수 없습니다. |

### 더 이상 적용되지 않는 저장된 제외 항목

저장된 제외 항목은 이전 세션이나 복원에서 올 수 있으므로, 매번 다시 판정됩니다:

- **그 집합으로 아직 데이터가 이동하지 않았다면,** 규칙이 거부하는 항목이나 소스에 더 이상 없는
  테이블·컬럼을 가리키는 항목은 **Continue / Start Full Load**(페이지 헤더의 Run 포함), **Deploy CDC
  infrastructure**, 기존 스택으로의 **Attach**를 막습니다: "N saved column exclusions no longer
  apply: ... Remove them in Exclude tables & columns (under the table list) first."
- **데이터가 이동한 뒤라면,** 그 집합이 이미 로드되거나 캡처된 것의 일부이므로 아무것도 막지
  않습니다. 대신 각 항목을 보고합니다. 키 항목은 "is still migrated"로 보고됩니다(키 항목은 모든
  경로에서 집합에서 빠지므로, Validation이 그 컬럼을 비교합니다). 계산 컬럼 항목은 "is already
  omitted", 외래 키가 참조하는 컬럼은 "stays excluded, but: ..."로 보고됩니다.

### 경고 (허용되지만 읽어 두세요)

이 경고들은 Save를 막지 않습니다. Start 다이얼로그는 처음 두 가지만, DROP NOT NULL과 DROP DEFAULT의
개수와 구문으로 반복해 보여 줍니다. Change & reload는 재적재하는 테이블에 대해 모두 보여 줍니다.

| 제외 컬럼이... | 일어나는 일 |
|---|---|
| Aurora DSQL에서 NOT NULL이면 | NOT NULL이 제거됩니다. **이는 단방향입니다:** Aurora DSQL은 NOT NULL을 제자리에서 다시 추가할 수 없으며, 나중에 그 테이블을 Drop & reload해야만 복원됩니다. 컬럼은 컷오버 후에도 nullable로 남습니다. |
| DEFAULT가 있으면 | 마이그레이션 동안 DEFAULT가 제거되고 Cut over에서 복원됩니다. 복원된 DEFAULT는 새 행에만 적용됩니다(§13.9). |
| 보존된 외래 키의 자식 컬럼이면 | 부모와의 관계가 이관되지 않습니다(NULL은 외래 키 검사를 통과합니다). 그 키가 둘 이상의 컬럼에 걸친 `MATCH FULL`이면 대신 일부만 NULL인 키가 거부됩니다: 키의 모든 컬럼을 제외하지 않는 한 이관된 행에서 그 키의 검증이 실패하고(Cut over가 그 키에서 Finish를 보류합니다), 나중에 이 컬럼만 NULL로 두는 쓰기는 거부됩니다(23503). 키의 모든 컬럼을 제외하거나, 이 컬럼을 유지하거나, 테이블의 대상 스크립트에서 `MATCH FULL`(또는 그 키)을 제거하세요. |
| UNIQUE 제약이나 인덱스에 들어 있으면 | 이관된 행은 모두 NULL이므로, 그 행들에 대해 유일성이 강제되지 않습니다 — 다만 그 인덱스가 NULL을 같은 값으로 보는 `NULLS NOT DISTINCT`(PostgreSQL 15+)이면, 인덱스의 다른 컬럼 값을 공유하는 이관 행은 첫 행 이후 거부되며(23505), Drop & reload에서도 마찬가지입니다(행을 적재하기 전에 인덱스를 만들기 때문). 이 컬럼을 유지하거나, 테이블의 대상 스크립트에서 그 인덱스의 `NULLS NOT DISTINCT`를 제거하세요. |
| 유지되는 생성 컬럼이 사용하면 | 생성 컬럼이 NOT NULL이고 이 컬럼이 NULL일 때 그 식이 NULL을 내면, 모든 행이 거부됩니다(23502). 생성 컬럼이 nullable이면 NULL로부터 계산되어 소스와 달라집니다. |
| 뷰가 언급하면(텍스트 매칭) | 뷰는 NULL을 읽습니다. |

NULL을 거부하는, 직접 작성한 `CHECK` 제약은 미리 감지되지 **않습니다**. 그런 테이블은 로드 시 조용히
넘어가지 않고 오류로 실패합니다(23514).

**테이블**을 제외하는 것에도 결과가 따르며, 미리보기가 이를 나열합니다:

- 로드되지 않고 Aurora DSQL에 그대로 남습니다(drop되지 않음);
- 선택된 자식 테이블에서 그 테이블을 가리키는 외래 키는, 외래 키를 적용할 때(Full load only는 Data
  Migration 단계에서, CDC는 Cut over에서) 이 테이블이 Aurora DSQL에 가진 행을 기준으로 검사됩니다:
  참조하는 행이 모두 있으면 적용되고, 하나라도 없으면 고아 검사로 건너뛰며, 테이블이 Aurora DSQL에
  없으면 적용되지 않습니다;
- 그 테이블을 읽는 뷰는 행을 반환하지 않습니다;
- 그 테이블에 대한 컬럼 규칙은 효과가 없습니다.

### Aurora DSQL 테이블에 없는 소스 컬럼 (42703)

Schema Conversion에서 타깃 DDL의 컬럼을 지웠지만 제외하지는 않았다고 해 봅시다. 그러면 그 테이블의
모든 Full Load 배치가 42703으로 실패하고, CDC는 그 테이블에 대한 모든 변경을 DLQ로 보내게 됩니다.
그래서 **Start Full Load**, **Deploy CDC infrastructure**, **Start CDC**가 "shop.t.c is not in your
Aurora DSQL table; ... Add it back to the target DDL in Schema Conversion. Columns are never dropped
from a migration automatically."와 함께 막힙니다. 컬럼을 다시 추가하거나, 제외하세요(팝업이 그 줄을
제안합니다). 확실히 실패하는 경우만 보고합니다: 적용된 DDL과 라이브 타깃 테이블 양쪽 모두에 그
컬럼이 없어야 합니다.

### 사전 점검

사전 점검은 제외 설정을 반영합니다:

- `TARGET_COLUMNS_LOADABLE`은 DEFAULT 없는 NOT NULL 제외 컬럼을 로드 가능으로 취급합니다. 로드(또는
  Start CDC)가 먼저 nullable로 만들기 때문입니다.
- PostgreSQL에서 `COLUMN_REPLICABLE`은 제외 컬럼을 건너뜁니다. CDC가 그 컬럼을 절대 캡처하지 않기
  때문입니다.
- 점검 실행 후 제외 설정을 바꾸면, 차단하는 대신 중립적인 안내가 그 사실을 알려 줍니다("Exclusions
  changed since these checks ran at HH:MM UTC; size results describe the earlier set."). 점검 후에
  추가된 테이블은 여전히 시작을 차단합니다.

---

## 13.5 초대형 LOB 후보와 Check sizes

Aurora DSQL은 단일 값의 크기를 제한합니다(대부분의 타입에서 1 MiB,
[4장 §4.5](04-cdc-and-dsql-constraints.md#45-1-mib-값당-한도-그리고-dlq) 참고). 로드 시 이보다 큰
값을 가진 행은 격리됩니다: Aurora DSQL에 들어가지 않으며, 격리된 행이 1,000개를 넘으면 그 테이블이
실패합니다. CDC도 그런 행을 적용할 수 없습니다(그 행이 어떻게 되는지는 §4.5에 설명되어 있습니다).
대신 그 컬럼을 제외하면 행은 유지되고 그 컬럼은 NULL이 됩니다.

팝업의 접힌 **Oversized-LOB candidates (N)** 섹션은 범위 안의 컬럼 중 **타입**상 한도를 넘는 값을
담을 수 있는 컬럼을 나열합니다:

- **MySQL:** `mediumtext`, `longtext`, `mediumblob`, `longblob`, `json`;
- **PostgreSQL:** `text`, `bytea`, `json`, `jsonb`, 그리고 길이 없는 `character varying`(또는 단독
  `bpchar`). CHECK가 고정된 리터럴 집합으로 제한하는 컬럼은 나열하지 않습니다.

Evaluation이 `OVERSIZED_LOB`로 표시하는 컬럼과 같은 컬럼들입니다. 각 행은 소스 타입과 타깃 타입, 그
타입이 허용하는 크기(예: "Type allows up to 4 GiB."), 한도, 그리고 편집기에
`exclude schema.table.column`을 추가하는 **+ add rule** 버튼을 보여 줍니다. 적격성 규칙이 거부하는
컬럼은 대신 그 사유를 보여 줍니다. `include`가 유지하는 컬럼은 그 include 줄로 가는 버튼(예: **Go to
L5**)을 보여 줍니다.

**팝업은 기본적으로 아무것도 측정하지 않습니다**(사전 점검은 자체 크기 점검을 실행하며, 사전 점검이
측정한 컬럼은 그 결과가 여기에도 표시됩니다). 타입은 상한일 뿐이라는 점에 유의하세요: `longtext`나
`json` 컬럼이 실제로는 몇 KB를 넘는 값을 한 번도 담지 않을 수도 있습니다. 확실히 알려면 **Check
sizes**를 클릭하세요:

- 현재 페이지의 후보(최대 25개)를 소스에서 컬럼당 범위가 제한된 쿼리 하나로 측정합니다. 각 쿼리는
  한도를 넘는 첫 값에서 멈추고 최대 15초 동안 실행됩니다. 최악의 경우를 포함한 비용이 클릭 전에
  명시됩니다;
- 한 번에 한 패스만 실행되며, **Cancel**은 현재 컬럼이 끝난 뒤 멈춥니다;
- 각 컬럼은 세 결과 중 하나를 받습니다: **Over the limit found**, **No value over the limit**, 또는
  **Not determined**(읽기 실패, 15초 초과, 또는 취소). "Not determined"는 절대 통과로 세지 않습니다;
- **+ add rules for the N over the limit**은 한도를 넘는 것으로 확인된 모든 컬럼에 대해 규칙을
  추가합니다.

각 컬럼은 **적용된 타깃 타입**의 한도로 판정합니다. 예를 들어 Schema Conversion에서 컬럼을
`varchar`로 바꾼 뒤에는 그 타입의 한도를 씁니다. MySQL에서 `utf8mb4`가 아닌 텍스트 컬럼은 자체
charset으로 측정되며, 결과에 그 사실이 표시됩니다. Aurora DSQL은 UTF-8로 저장하므로 더 커질 수 있기
때문입니다. PostgreSQL에서 상속(`INHERITS`) 부모는 사전 점검에서도 Check sizes에서도 Full Load가
읽는 것처럼 자신의 행만으로 측정됩니다(`FROM ONLY`,
[§6.1](06-limitations.md#61-aurora-dsql-기능-한계-스키마가-이에-맞아야-함)): 자식 테이블의 값은 그
자식의 것으로만 셉니다.

이 섹션은 강조 표시하지 않는 것도 알려 줍니다: MySQL 공간(spatial) 컬럼, PostgreSQL 배열, `xml`,
`tsvector`, 기하 타입, 그리고 멀티바이트 텍스트를 담은 큰 `varchar(n)`이나 `char(n)` 컬럼입니다. 2
MiB를 넘는 행은 점검하지 않습니다. 이들도 규칙으로 얼마든지 제외할 수 있습니다.

---

## 13.6 Aurora DSQL에서 일어나는 일

### 오버레이: 첫 행 전에 DEFAULT 없는 nullable로

테이블의 첫 행을 쓰기 전에, 로드가 각 제외 컬럼을 DEFAULT 없는 nullable로 만듭니다. 컬럼마다 먼저
NOT NULL을, 그다음 DEFAULT를 제거합니다:

```sql
-- shop.users.ssn: NOT NULL, DEFAULT 없음
ALTER TABLE "shop"."users" ALTER COLUMN "ssn" DROP NOT NULL
-- shop.users.status: NOT NULL DEFAULT 'active'
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP NOT NULL
ALTER TABLE "shop"."users" ALTER COLUMN "status" DROP DEFAULT
```

- **DEFAULT를 먼저 기록합니다.** DEFAULT는 제거되기 전에 오버레이 원장(아래 참고)에 기록되므로, Cut
  over가 이를 복원할 수 있습니다. 라이브 DEFAULT가 기록과 다르면 도구는 어떤 ALTER보다도 먼저
  거부하므로, 기록하지 않은 DEFAULT를 잃는 일은 없습니다.

오버레이는 **모든 Full Load 경로**에서 실행됩니다에서 실행되며, 모든 Drop & reload 재생성과 도구가
재생성했지만 끝내지 못한 테이블의 재시도도 포함됩니다. 활동 로그에는 `overlay planned`(제거할
DEFAULT를 정확히 명시), `overlay applied`, `column excluded`, 그리고 테이블의 로드 줄에
`loaded as NULL: ...`이 표시됩니다.

### Start Full Load 다이얼로그가 변경을 먼저 보여 줍니다

제외 설정이 관련되면 Start Full Load 다이얼로그는 다음을 보여 줍니다:

- "N columns in M tables will load as NULL"(**show columns** 포함);
- "Aurora DSQL changes before loading: x x DROP NOT NULL (one-way), y x DROP DEFAULT (restored at
  Cut over)", 그리고 **show statements** 아래에 실행 순서대로의 정확한 구문. Append와 Drop 사이를
  전환하면 다시 그려집니다;
- drop 후 재적재해야 하는 테이블(다음 절);
- 로드되지 않는, 규칙이 선택 해제한 테이블의 수.

제외 설정이 관련되면 페이지 헤더의 **Run**도 항상 이 다이얼로그를 거칩니다. 실행이 다이얼로그가 보여
준 것과 다른 것을 바꾸게 되면, 실행은 거부되고 다이얼로그를 다시 열라고 요청합니다.

### 이미 행이 있는 테이블: Drop & reload

append는 생략한 컬럼을 절대 바꾸지 않으므로, append로는 저장된 값을 NULL로 바꿀 수 없습니다. 그래서
도구는 NULL과 저장된 값을 절대 섞지 않습니다. Start Full Load 다이얼로그는 다음 테이블에 **Drop &
reload를 강제**합니다:

- 데이터가 있지만, 그 행이 모두 현재 제외 집합으로 로드된 것은 아닌 테이블;
- 이전에 제외했던 컬럼을 다시 포함하는, 데이터가 있는 테이블(기존 행의 그 컬럼은 NULL);
- 도구가 재생성했지만 끝내지 못한 테이블.

강제된 테이블은 Append 선택지 없이 사유와 함께 빨간 카드에 나열되며, 확인 버튼은 파괴적 동작 색으로
"Drop N table(s), recreate and load"라고 표시됩니다. Cancel, Esc, 배경 클릭, 탭 유실은 강제 집합을
버리므로, 이후의 실행이 drop을 물려받는 일은 없습니다. 엔진도 같은 판정을 적용합니다: 다른 경로로
그런 테이블에 append하려 하면, 그 테이블은 대신 "needs Drop & reload: ..."로 실패합니다.

예외:

- 타깃을 읽을 수 없으면 그 테이블은 경고와 함께 미정으로 남으며, 절대 강제되지 않습니다;
- 같은 집합에서의 재개나 재시도는 절대 강제되지 않습니다.

CDC가 스트리밍하는 동안에는 Full Load가 전혀 실행되지 않습니다
([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)): 대화 상자를 확인할 수 없으므로 아무것도
강제하지 않습니다.

### Start CDC도 같은 오버레이를 적용합니다

CDC only, Retry CDC, 그리고 Stop 후의 재 Start는 **어떤 커넥터도 만들거나 바꾸기 전에** 같은
오버레이를 적용합니다. Full Load와 같은 구문·순서·점검을 쓰므로, CDC가 insert하는 모든 행은 제외
컬럼에 NULL을 갖습니다.

- 이미 오버레이를 적용한 Full Load 이후라면, 이는 검증된 no-op입니다.
- 컷오버 복원 후에 실행하면 복원된 DEFAULT를 다시 제거하므로, **DEFAULT 복원은 마지막 Start CDC
  이후에 하세요**.
- 실패하면 Start는 "Start CDC blocked: ..."로 실패하며, 어떤 커넥터도 건드리지 않습니다. 클릭 후
  세션의 타깃이 바뀌었으면 job도 거부합니다.
- CDC only의 Start 다이얼로그는 변경을 나열합니다: "Aurora DSQL changes when CDC starts: N x DROP
  NOT NULL (one-way), M x DROP DEFAULT (restored at Cut over)", 구문은 **show statements** 아래.

CDC는 제외 컬럼을 절대 쓰지 않으므로, **CDC 시작 전에 이미 Aurora DSQL에 있던 행은 제외 컬럼의 값을
유지합니다**. CDC only의 Start 다이얼로그가 그런 테이블을 이름으로 알려 주고, 테이블마다
`overlay append` WARNING이 기록되며, Validation이 이를 보고합니다(§13.9).

### 오버레이 원장

제거된 DEFAULT는 타깃에서 다시 읽을 수 없으므로, 도구는 ALTER 전에 이를 NOT NULL 제거 여부, 복원
상태와 함께 **오버레이 원장(overlay ledger)**에 기록합니다. 원장의 키는 세션이 아니라
**타깃**(클러스터 엔드포인트 + 데이터베이스)입니다. 따라서 복원된 세션이나 Start over 후의 새 세션도
이전 세션이 바꾼 것을 여전히 봅니다. **읽을 수 없는 원장은 절대 "기록 없음"으로 취급하지 않습니다:**
Start 다이얼로그가 Start를 비활성화하고 **Retry**를 제공하며, 실행은 무엇이든 drop하거나 쓰기 전에
모든 테이블을 실패시키고, Start CDC는 차단됩니다.

---

## 13.7 나중에 제외 설정 바꾸기

### 집합이 잠기는 이유

어떤 제외 집합으로 데이터가 이동한 뒤에 그 집합을 제자리에서 바꾸면, 일부 행은 NULL이고 나머지 행은
값을 가진 상태가 됩니다. CDC 스택도 집합의 자체 사본을 가지고 있습니다: 스택은 캡처 목록과 함께
배포되었고, 멈춘 스택도 제외 설정과 커밋된 오프셋을 유지하므로 **Stop CDC로는 풀리지 않습니다**.
그래서 도구는 집합을 잠그고, 현재 상황에 실제로 맞는 조치만 제공합니다:

| 상황 | 제외 설정을 바꾸는 방법 |
|---|---|
| 아직 아무것도 실행하지 않음(Full Load 없음, CDC 스택 없음) | 잠기지 않음: 규칙을 편집하고 Save합니다. |
| CDC 인프라를 삭제하는 중 | 삭제가 끝나기를 기다립니다. 팝업이 엔진별 예상 시간(MySQL은 약 15-25분, PostgreSQL은 약 2-5분)을 보여 줍니다. |
| CDC 인프라를 생성하는 중 | 생성이 끝나기를 기다린 뒤, 해당하는 "CDC 스택이 있음" 행을 따릅니다. |
| CDC 스택이 있음(배포됨, 스트리밍 중 또는 멈춤), 그리고 Full Load가 실행됨 | 1. 스트리밍 중이면 Stop CDC한 뒤, CDC 단계에서 **Delete CDC infrastructure**. 2. 영향받은 테이블에 **Change & reload**를 사용(CDC only라면 먼저 Full load + CDC로 전환). 3. 다시 배포하고 Start CDC(MySQL은 아래 "삭제 후 다시 CDC 시작" 참고). Full load + CDC에서 이 마이그레이션에서 CDC를 한 번도 시작하지 않았다면(Start CDC를 제출한 적이 없음), 테이블 목록에서 테이블을 체크하거나 해제할 수는 있습니다(아래 "Full Load 후 테이블 집합 바꾸기 (Full load + CDC)" 참고). |
| CDC 스택이 있음, Full Load는 실행되지 않음, CDC only | 스트리밍 중이면 Stop CDC, Delete CDC infrastructure, 규칙 변경(CDC가 한 번이라도 스트리밍했다면 컬럼 규칙만 가능: 테이블 집합을 바꾸려면 Start over가 필요), 다시 배포하고 Start CDC. 스택을 삭제하면 스트림 위치도 삭제됩니다(아래 참고). |
| CDC 스택이 있음, 아직 Full Load가 실행되지 않음(Full load + CDC) | 스트리밍 중이면 Stop CDC, Delete CDC infrastructure, 규칙 변경, 다시 배포한 뒤 Start Full Load. 재적재할 것은 없습니다. CDC가 이미 스트리밍했다면 Start Full Load 다이얼로그에서 drop 후 재생성을 선택하세요. |
| CDC 스택이 없음, 그리고 Full Load가 실행됨(Full load only 또는 Full load + CDC) | **Change & reload**(다음 절). Full load only: 테이블 집합도 바꿀 수 있습니다. Full load + CDC: CDC를 시작하기 전까지 테이블을 빼거나 추가할 수 있습니다(아래 두 절 참고). |
| CDC 스택이 없음, 그리고 유형을 CDC only로 전환하기 전에 Full Load가 실행됨 | 유형을 Full load + CDC로 전환하고, 거기서 Change & reload를 사용한 뒤, CDC로 계속합니다. |
| Full Load가 실행되었지만 그 job 기록을 더 이상 사용할 수 없음 | CDC 스택이 있으면 먼저 삭제한 뒤 **Start over**. |

추가 설명:

- **스택이 있는 Full load only.** 유형이 Full load only인데 스택이 있다면, 스택을 삭제하려면 Full
  load + CDC로 전환하세요. Full load only에는 CDC 단계가 없기 때문입니다.
- **테이블 집합.** CDC only에서는 Full Load 후 고정됩니다. CDC를 시작하기 전에 집합을 바꾸려면 Full
  load + CDC로 전환하세요. Full load + CDC에서는 CDC를 시작하기 전까지 테이블을 빼거나 추가할 수
  있습니다(아래 "Full Load 후 테이블 집합 바꾸기 (Full load + CDC)" 참고). 그 이후에는 Start over를
  사용하세요. Full load only는 아래 "Full Load 후 테이블 집합 바꾸기 (Full load only)"를 참고하세요.
- **삭제 후 다시 CDC 시작.** MySQL에서는 시작 지점이 Automatic인 Start CDC가 Full Load 워터마크에서
  다시 시드합니다(그 로드 이후의 binlog가 아직 있어야 하며, purge되었다면 Start CDC가 경고합니다).
  Change & reload는 그 워터마크를 유지하므로, 그 로드 이후 캡처 대상 테이블의 정의가 바뀌었다면(예:
  소스에 추가된 컬럼) Start CDC가 거부하기도 합니다: 그때는 더 새 워터마크를 위해 "Drop & reload"로
  모든 테이블에 Re-run Full Load를 실행하거나, Manual을 선택하고 Fetch current position을 사용하세요
  ([6장](06-limitations.md) 참고). PostgreSQL에서는 스택을 삭제하면 복제 슬롯과 publication이
  drop됩니다. Delete CDC infrastructure 다이얼로그가 그 둘의 이름을 알려 줍니다: drop하지 못해도
  teardown은 그대로 끝나고 그 로그가 소스에서 실행할 문을 알려 줍니다. 그 경우 teardown은 도구가
  CDC용으로 만든 소스 자격증명 시크릿(비밀번호 인증일 때)을 삭제 예약하지 않고 남겨 둡니다: 그
  시크릿에 슬롯을 drop하는 데 필요한 자격증명이 있기 때문이며, 다이얼로그도 알려 주듯 슬롯이 없어진
  뒤 직접 삭제하세요. drop하는 슬롯이 Full Load의 슬롯이면 Start CDC는 더 이상 그 슬롯에서 재개할 수
  없으므로, Start CDC 다이얼로그에서 **"Re-snapshot every table instead"**를 선택하세요. 이 방법은
  그것이 풀어 줄 Start가 자체 검사를 통과할 때만 제시되며, 그렇지 않으면 다이얼로그가 그 Start의
  거부를 보여 줍니다(예컨대 상속 자식 테이블이 있는 캡처 대상 테이블.
  [§4.3](04-cdc-and-dsql-constraints.md) 참고). 그 뒤에는 그곳에서 통하는 경로를 알려 줍니다: 상속
  때문에 거부되었다면 대신 "Drop & reload"로 Re-run Full Load를 실행하면 그 뒤의 Start CDC는
  스냅샷하지 않습니다(CDC only에서는 먼저 마이그레이션 유형을 Full load + CDC로 바꿈). 그 이름의
  슬롯을 쓸 수 없어 거부되었다면 그 슬롯을 삭제하거나 다른 CDC 스택 이름을 쓴 뒤 "Drop & reload"로
  Re-run Full Load를 실행하고 Start CDC 하세요(CDC only에서는 Start CDC를 다시 실행).
- **CDC only 스택을 삭제하는 대가.** MySQL에서는 시작 지점이 Automatic인 다음 Start CDC가 소스의
  현재 위치에서 시작합니다. Aurora DSQL이 마지막으로 적용한 변경 이후 캡처 대상 테이블에 일어난
  변경은 복제되지 **않습니다**. 이를 다시 복사하려면 Full load + CDC로 전환해 새 스택의 첫 Start CDC
  전에 그 Full Load를 실행하고(그 스택이 스트리밍한 뒤에는 새 Full Load 시작 지점에 Delete와
  재배포가 한 번 더 필요합니다), 행이 있는 테이블은 drop 후 재생성을 선택하세요. PostgreSQL에서는
  다음 Start CDC가 캡처 대상 테이블을 다시 스냅샷하지만, 그 사이 소스에서 삭제된 행은 Aurora DSQL에
  남습니다. 두 경우 모두, 이미 Aurora DSQL에 있던 행은 새로 제외한 컬럼의 값을 유지합니다.

### Change & reload

Full Load가 포함된 마이그레이션 유형에서 그 실행이 기록되어 있고 CDC 스택이 없으면, 읽기 전용 팝업이
**Change & reload**를 제공합니다. 이 버튼은 **Change exclusions & reload**를 엽니다:

- **컬럼 규칙은 편집할 수 있습니다.** 테이블 줄은 테이블 집합을 아직 바꿀 수 있는 경우에만 선택을
  바꿉니다(다음 두 절 참고): 선택에서 빠진 테이블은 Aurora DSQL에 그대로 남고(drop되지 않으며 CDC도
  캡처하지 않음), 선택된 테이블 중 아직 로드되지 않은 테이블은 Retry unfinished tables가
  로드하며(Full load + CDC에서는 로드되면 CDC가 캡처), 직전 Full Load가 이미 로드한 테이블은 그대로
  유지되지만 컬럼 규칙이 그 테이블의 컬럼을 새로 제외하면 제외 컬럼이 바뀌는 다른 테이블과
  마찬가지로 **Drop & reload** 대상으로 표시됩니다. CDC가 아직 캡처하지 않는 테이블의 컬럼 규칙은
  여기서 아무것도 drop하지 않습니다: Full Load 단계에서의 그 테이블의 로드가 그 규칙을 적용합니다.
  CDC를 시작한 뒤에는 선택을 바꾸게 되는 모든 줄이 오류입니다. CDC 스택이 있으면 팝업은 읽기
  전용이므로, 대신 테이블 목록을 사용하세요.
- **무엇이든 drop하기 전에 무슨 일이 일어날지 정확히 보여 줍니다:** drop 후 재적재할 각 테이블과 그
  컬럼 diff(`+ c (NULL)`, `- c`)와 행 추정치("~N rows re-read (estimate)"), 그리고 그 테이블들이
  받을 경고와 ALTER. "Their rows on Aurora DSQL are permanently deleted; each table is recreated
  from your Schema Conversion DDL, made nullable where needed, and reloaded. Other tables are not
  touched."
- **확인 버튼이 동작을 이름으로 밝힙니다:** 파괴적 동작 색의 **Drop & reload** *table* 또는 **Drop &
  reload N tables**. 미리보기에 나온 테이블에만 작용합니다. Start 다이얼로그는 뒤따르지 않으며,
  재적재가 바로 시작됩니다.
- **텍스트만 바뀐 경우에는 아무것도 drop하지 않습니다.** 어떤 테이블의 제외 컬럼도 바뀌지 않으면(예:
  주석만 추가), 버튼이 **Save**로 표시되며 규칙만 저장합니다(Full load only와 Full load + CDC에서는
  테이블 줄이 바꾸는 테이블 선택도 함께 저장합니다).

Change & reload는 CDC only에서는 제공되지 않으며(팝업이 대신 **Change migration type**을 제공), Full
Load의 job 기록이 사라진 경우에도 제공되지 않습니다(Start over 사용). 기록된 Full Load가 없으면
아무것도 잠기지 않으므로 규칙을 직접 편집합니다. 제공되는 경우에도 Full Load가 실행 중일 때, 외래
키를 적용하는 중일 때, Cut over가 DEFAULT를 복원하는 중일 때, 그리고 앱 재시작 후 Connect 단계에서
연결을 다시 검증하기 전까지는 사유와 함께 비활성화됩니다.

### Full Load 후 테이블 집합 바꾸기 (Full load only)

Full load only에서는 Full Load가 테이블 집합을 고정하지 않습니다. Full Load, 외래 키 적용, 컷오버
DEFAULT 복원이 실행 중이 아니고 CDC 스택이 없는 동안에는, 테이블 목록에서 테이블을 체크하거나
해제하거나, Change & reload에서 테이블 줄(`exclude table ...`)을 사용하세요. Start over가 필요
없습니다.

| 동작 | 결과 |
|---|---|
| **테이블 체크 해제** | Aurora DSQL에 그대로 남습니다(drop되지 않음). 이 마이그레이션은 더 이상 그 테이블을 로드하거나 검증하지 않습니다. 테이블 규칙으로 선택을 해제하면 그 테이블의 제외 컬럼 항목은 유지되지 않습니다. |
| **아직 로드되지 않은 테이블 체크** | **Retry unfinished tables**가 로드합니다. 로드된 테이블은 모두 유지하고 원래 워터마크를 재사용합니다. Full Load 단계는 그 테이블을 "Not loaded yet — selected after the last Full Load"로 표시하며, 로드될 때까지 단계는 Failed로 표시됩니다. 새 테이블은 한 번도 점검되지 않았으므로, 도구가 요청하면 먼저 사전 점검을 다시 실행하세요: 그때까지 미완료 테이블이 새 테이블뿐이면 Retry unfinished tables는 비활성화되고, 다른 테이블도 함께 재시도하면 새 테이블은 거부됩니다. 그 테이블의 컬럼 규칙은 이 첫 로드에서 적용됩니다: Change & reload는 직전 Full Load에 없던 테이블을 drop 후 재적재하지 않습니다. |
| **직전 Full Load가 이미 로드한 테이블 체크** | 그대로 유지되며 다시 로드되지 않습니다 (Full Load 단계의 Reload specific tables로 재적재할 수 있습니다). 규칙으로 선택을 해제했던 테이블을 다시 체크해도 제외 컬럼은 돌아오지 않습니다(그 항목은 유지되지 않았습니다): Exclude tables & columns 행이 경고하며, Change & reload로 그 테이블에 규칙을 다시 적용할 수 있습니다. |
| **외래 키** | **Apply foreign keys** 패스가 끝난 뒤 테이블의 체크를 해제해도 그 결과는 유지됩니다: 키는 그대로 남습니다. 테이블을 체크한 뒤(또는 패스를 중간에 멈춘 뒤 체크를 해제하면) 카드가 다시 적용하라고 요청합니다: 이미 있는 키는 그대로 유지되고(아무 일도 하지 않음), 빠진 키는 각각 바로 추가됩니다. |
| **Validation** | 새로 선택한 테이블이 로드되면 Validation을 다시 실행하세요: 결과가 Full Load 이후 선택한 모든 테이블을 다룰 때까지(또는 Objects to validate에서 그 칩을 끌 때까지) Cut over는 보류됩니다. 그런 테이블이 아직 로드되지 않은 동안에는 Cut over가 Data Migration 단계로 돌아가도록 안내합니다. |
| **CDC 유형으로 전환** | 이 마이그레이션에서 CDC를 이미 시작한 경우가 아니라면 테이블의 체크를 해제하는 것만으로는 유형이 잠기지 않으며(이미 시작했다면, 집합을 되돌리거나 Re-run Full Load를 실행할 때까지 집합을 바꾸는 모든 변경이 유형을 잠급니다. 그 재실행 뒤에 유형을 바꾸고, 스트리밍한 CDC 스택이 아직 있으면 Start CDC 전에 CDC 단계에서 Delete CDC infrastructure 후 다시 배포하세요: Full load only에는 CDC 단계가 없으며, 스트리밍한 적 있는 스택은 멈춘 곳에서만 이어 가며, 더 새 시작 지점에서는 결코 시작하지 않습니다), CDC 유형은 로드된 테이블 중 아직 선택된 테이블을 캡처합니다. Full Load 이후 체크한 테이블이 있으면, **Re-run Full Load**가 현재 집합에 대한 새 워터마크를 잡거나 그 테이블의 체크를 다시 해제할 때까지 유형 선택기는 잠깁니다: Full load only에서만 로드한 테이블은 워터마크에 포함되지 않지만, Full load + CDC에서 보호된 로드로 추가한 테이블은 포함됩니다(다음 절 참고). 그 재실행은 **Drop & reload**로 하세요: CDC는 그 새 워터마크에서 시작하는데, *Append*는 앞선 적재가 쓴 행을 그 뒤 소스의 UPDATE와 DELETE 없이 그대로 둡니다. |

### Full Load 후 테이블 집합 바꾸기 (Full load + CDC)

Full load + CDC에서는 이 마이그레이션에서 CDC를 한 번도 시작하지 않은 동안, 즉 Start CDC를 제출한
적이 없고, 커넥터가 없으며, CDC 인프라가 한 번도 스트리밍하지 않은 동안에는 Full Load 후에도 테이블
집합을 바꿀 수 있습니다. 테이블 목록에서 테이블을 체크하거나 해제하거나, Change & reload에서 테이블
줄을 사용하세요. 복원된 세션에서도 똑같이 동작합니다. Start over가 필요 없습니다.

| 동작 | 결과 |
|---|---|
| **테이블 체크 해제** | Aurora DSQL에 그대로 남습니다(drop되지 않음). CDC는 그 테이블을 캡처하지 않고, 이 마이그레이션은 더 이상 그 테이블을 로드하거나 검증하지 않습니다. 컷오버 DEFAULT 복원은 그 테이블을 복원하지 않고 **Needs your decision** 아래에 나열합니다. 테이블 규칙으로 선택을 해제하면 그 테이블의 제외 컬럼 항목은 유지되지 않습니다. |
| **Full Load가 나누어 읽은 MySQL 테이블 체크 해제**(*Reader shards per large table*을 2 이상으로 명시, [§3.8](03-full-load.md#38-병렬-적재)) | 불가능합니다: 그 복사본은 여러 스냅샷의 행을 담고 있어, CDC가 Full Load의 시작 지점부터 재생해야 일관됩니다. 체크박스가 잠기고(잠금 아이콘에 이유가 표시됨), Unselect all도 그 테이블은 선택된 채로 두며, 그 테이블의 선택을 해제하는 테이블 규칙은 거부되고, Full load only와 더 나중의 시작 지점에서의 Start CDC도 거부됩니다. 체크를 해제하려면 CDC를 시작하기 전에, Settings에서 Reader shards per large table을 0(자동) 또는 1(꺼짐)로 설정하고, 그 테이블을 Drop & reload로 다시 적재한 뒤(끝나지 않았으면 Retry unfinished tables, 그 밖에는 Reload specific tables: 리더 하나로 읽을 때는 Append / Drop & reload 선택과 관계없이 삭제 후 다시 적재합니다), 체크를 해제하세요. CDC가 시작되면 테이블 집합은 고정되므로(바꾸려면 Start over), CDC의 재생 뒤 CHECKSUM 모드의 Validation이 그 테이블의 일치를 확인하면 Full load only와 더 나중의 시작 지점에 대한 거부는 끝나지만, 체크 해제는 허용되지 않습니다. |
| **Full Load가 로드하지 않은 테이블 체크** | 먼저 Full Load의 시작 지점 아래에서 로드되고, 그 뒤에야 CDC가 캡처합니다(아래). |
| **CDC 인프라가 그 테이블 없이 배포된 뒤 다시 체크** | CDC를 시작할 때 캡처됩니다: 그 테이블의 Kafka 토픽은 인프라의 기본 파티션 수를 받으며, 이 수는 인프라를 배포할 때의 테이블들을 기준으로 정해졌습니다. |
| **exclude table 규칙이 선택을 해제한 테이블** | Change & reload에서 그 규칙을 삭제해야만 돌아오며, 이때 그 테이블의 컬럼 규칙이 적용됩니다(테이블 목록에서 체크하는 것은 거부됩니다: 그 컬럼 규칙이 적용되지 않기 때문입니다). 잠금 아이콘과 그 사유가 표시됩니다. |
| **Unselect all** | 허용됩니다. 다만 Deploy CDC infrastructure와 Start CDC에는 선택된 테이블이 하나 이상 필요합니다. |
| **CDC 파이프라인 하나가 캡처하는 수보다 많은 테이블**(일반적인 이름이면 약 80–160개, [6장](06-limitations.md) 참고) | 적재할 수 있습니다: Full Load 단계가 적재 전에 경고하고, 여기서 충분한 테이블의 선택을 해제할 때까지 Deploy CDC infrastructure와 Start CDC는 그 캡처를 거부합니다(Full Load가 나누어 읽은 테이블은 제외, 위 참고: 그런 테이블이 캡처되는 동안 그 거부는 테이블을 명시하고 Full load only를 제시하지 않습니다). |

- **Full Load가 로드하지 않은 테이블을 체크하면**, 그 테이블은 먼저 Full Load의 시작 지점 아래에서
  로드되고, 그 뒤에야 CDC가 캡처합니다:
  - 도구가 요청하면 사전 점검을 다시 실행하세요(그 테이블은 한 번도 점검되지 않았습니다). 로드될
    때까지 Full Load 단계는 그 테이블을 "Not loaded under the Full Load's start point yet"로
    표시하고 Failed로 표시되며, Start CDC, Validation, Cut over는 기다립니다.
  - **Retry unfinished tables**가 원래 워터마크 아래에서 그 테이블을 로드하며, 로드된 테이블은 모두
    유지합니다. 그 로드는 먼저 CDC가 Full Load 이후 그 테이블을 공백 없이 캡처할 수 있는지
    점검합니다. 점검이 실패하면 테이블은 로드되지 않으며, 그 행에 이유와 해야 할 일이 표시됩니다.
  - **비어 있는** Aurora DSQL 테이블에만 로드합니다. 테이블에 이미 행이 있으면 다이얼로그가 그
    테이블을 drop 후 재적재합니다: 그 행들이 CDC가 그 테이블에 대해 캡처하는 변경(MySQL에서는 Full
    Load의 시작 지점 이후의 변경, PostgreSQL에서는 그 테이블이 CDC의 publication에 들어간 이후의
    변경)보다 오래된 것인지 도구가 알 수 없고, append는 이미 있는 행을 모두 그대로 두기 때문입니다.
  - **MySQL:** Full Load는 자신이 로드하지 않는 이관 가능한 테이블의 시작 지점 정의를 기록하고,
    로드는 그 정의를 테이블의 현재 정의와 비교합니다. 이전 버전의 Full Load나, 로드 후에 변환된
    테이블에는 그런 기록이 없습니다: **Drop & reload**로 **Re-run all tables**를 사용하거나(모든
    테이블에 새 시작 지점을 잡으므로, append는 이미 로드된 행을 그대로 둡니다) 체크를 해제하세요.
    시작 지점 이후 정의가 바뀐 경우에도 Drop & reload로 Re-run all tables가 필요합니다.
  - **PostgreSQL:** 도구는 소스에서 그 테이블(상속 자식 테이블은 제외)을 CDC의 publication에
    추가하고, 이미 열려 있던 트랜잭션이 끝난 뒤 테이블을 복사합니다. 그 시점에 테이블과 그 모든
    파티션에는 소스에서 사용할 수 있는 `REPLICA IDENTITY`가 있어야 합니다: `DEFERRABLE`이 아닌
    유효한 기본 키가 있는 `DEFAULT`, 유효한 고유 인덱스가 있는 `USING INDEX`, 또는 `FULL`(재키잉된
    테이블은 이미 `REPLICA IDENTITY FULL`이어야 합니다. PostgreSQL 18에서는 identity에 생성 컬럼이
    들어가면 안 되므로, 생성 컬럼이 있는 재키잉 테이블은 거부되고 그 행이 **Keep source PK**로
    되돌리라고 알려 줍니다. [6장](06-limitations.md) 참고). 그렇지 않으면 publication에 들어간 뒤 그
    테이블의 모든 UPDATE와 DELETE가 소스에서 실패하므로, 그 로드는 테이블을 추가하기 전에
    거부하며(identity를 읽을 수 없을 때도 거부합니다), 그 행은 찾은 원인에 맞는 해결 방법을 알려
    줍니다. 소스 사용자는 그 테이블과 publication의 소유자여야 합니다. **Delete CDC
    infrastructure**(슬롯과 publication을 drop함) 후에는 테이블을 추가하려면 Drop & reload로 Re-run
    all tables가 필요합니다.
  - CDC는 **격리된 행이 없는** 로드 후에만 그 테이블을 캡처합니다. **Change & reload는 CDC가 아직
    캡처하지 않는 테이블을 drop 후 재적재하지 않습니다**(Full Load 후에 선택했거나, 격리된 행과 함께
    로드된 테이블): Save가 그 테이블의 컬럼 규칙을 저장하고, Full Load 단계에서의 그 테이블의 로드가
    규칙을 적용합니다.
  - **대가:** MySQL에서는 CDC가 Full Load의 시작 지점 이후 그 테이블에 일어난 모든 변경을 더 새로운
    복사본 위에 재생하고, PostgreSQL에서는 그 테이블이 CDC의 publication에 들어간 이후(복사 직전)의
    모든 변경을 그 복사본 위에 스트리밍합니다. 어느 쪽이든 결과는 수렴합니다. 이미 복사된 행과 고유
    인덱스에서 잠깐 충돌하는 변경은 데드레터가 될 수 있으며, 그러면 Cut over는 CHECKSUM 검증을
    기다립니다. MySQL에서는 Full Load 이후의 모든 변경을 binary log가 아직 보유하고 있어야 합니다.
    그 테이블을 로드하기 전에 CDC 인프라를 배포했다면, 그 테이블의 Kafka 토픽은 인프라의 기본 파티션
    수를 받습니다.

- **CDC 인프라가 배포되어 있으면 팝업은 읽기 전용입니다:** 테이블 목록을 사용하세요.
- **체크를 해제한 테이블에서만 생긴 실패는 더 이상 단계를 붙잡지 않습니다.** 단계는 Done으로
  표시되고, Continue to CDC가 열리며, PostgreSQL에서는 CDC가 여전히 로드 때 만든 슬롯에서
  재개합니다. Retry unfinished tables는 체크를 해제한 테이블을 다시 로드하지 않습니다.
- **기다려야 할 때는 기다림으로 안내하며, Start over를 요구하지 않습니다:** Full Load, 외래 키 적용,
  컷오버 DEFAULT 복원이 실행되는 동안, CDC 인프라를 생성하거나 삭제하는 동안, 실패한 생성이 계정에
  남아 있는 동안(CDC 단계에서 삭제하세요), 그리고 페이지를 연 뒤 CDC 상태를 읽는 몇 초 동안입니다.
  CDC 상태를 읽을 수 없으면 CDC 단계가 그 이유를 보여 줍니다: AWS 접근을 고친 뒤 **Re-check CDC
  state**를 사용하세요.
- **현재 집합으로 Re-run Full Load를 실행하면** 선택된 모든 테이블에 대한 새 워터마크를 잡습니다.
  PostgreSQL에서는 기존 publication을 재사용하고, 거기에 없는 선택된 테이블을 추가합니다(위와 같이
  사용할 수 있는 `REPLICA IDENTITY`가 있을 때만: 그렇지 않으면 실행은 소스에서 아무것도 바꾸기 전에
  멈추고 어느 테이블인지와 그 원인에 맞는 해결 방법을 알려 줍니다). 그 재실행은 **Drop & reload**로
  하세요: CDC는 그 워터마크에서 시작하는데, append는 이미 로드된 행을 그대로 둡니다.
- **외래 키.** 선택된 테이블은 체크를 해제한 테이블을 가리키는 외래 키를 유지합니다: Cut over에서
  orphan 점검이, 부모 행이 복제되지 않은 키를 보고합니다.
- **CDC를 한 번 시작하면, 그 뒤 멈췄더라도 집합은 다시 고정됩니다.** CDC only: CDC를 시작하기 전에
  집합을 바꾸려면 Full load + CDC로 전환하세요. 선택된 테이블이 Full Load의 시작 지점 아래에서
  로드되지 않은 동안 Start CDC는 거부됩니다.

### 격리 또는 데드레터에서

제외할 컬럼은 데이터가 이동한 뒤에야 발견하는 경우가 많습니다. 두 복구 버튼이 같은 팝업으로
이어집니다:

- **Full Load 격리: "Exclude column & reload".** 표시된 컬럼마다 한 줄씩 이미 삽입되었지만
  **저장되지 않은** 상태로 Change & reload를
  엽니다(`exclude s.t.c  # from the Full Load quarantine`). 따라서 재적재에는 여전히 의도적인 두
  번의 클릭이 필요합니다: 그 버튼, 그리고 확인. (Full load + CDC에서 CDC가 아직 캡처하지 않는
  테이블이면 확인은 일반 Save입니다: Retry unfinished tables 아래에 표시되는 Full Load 단계에서의 그
  테이블의 로드가 규칙을 적용합니다. §13.7 참고.) `include`가 그 컬럼을 유지하고 있으면 팝업이 그
  줄을 가리킵니다(예: **Go to L5**). 사유가 그 테이블의 표시된 컬럼 중 하나를 가리키지
  않으면(`maximum column size exceeded`처럼 타입을 밝히지 않거나, 어느 컬럼에도 없는 타입을 밝히면),
  LOB 섹션이 그 테이블로 필터된 채 열리므로 **+ add rule**로 컬럼을 고를 수 있습니다. CDC 스택이
  여전히 제외 설정을 쥐고 있으면(또는 이 세션에서 Change & reload가 제공되지 않으면), 읽기 전용
  팝업이 열리고 그 줄들은 보류 중으로 기록됩니다. Change & reload가 제공되지만 아직 실행할 수
  없으면(예: 재시작 후 다시 연결하기 전), 버튼이 사유와 함께 비활성화됩니다.
- **CDC 데드레터: "Exclude column...".** 이 버튼은 모든 상태에서 활성화됩니다.
  `exclude s.t.c  # from the CDC dead-letter queue`를 보류 중인 줄로 기록한 뒤, 잠금이 허용하는
  모드로 팝업을 엽니다. 잠금의 단계를 따르세요(위의 표 참고): Stop CDC 후 CDC 인프라 삭제, Full
  Load가 실행되었다면 Change & reload 사용(그렇지 않으면 규칙을 직접 변경), 그다음 배포하고 Start
  CDC.

**보류 중인 줄**은 편집기를 기다리는 규칙입니다. 읽기 전용 팝업은 각 줄을
`Pending: "exclude ..." (from ...) will be added when the rules can be edited again.`으로 보여
줍니다. 이 줄들은 세션과 함께 유지됩니다. 다음에 팝업이 편집 가능한 상태로 열리면 편집기에 나타나며,
Save할 때만 효과가 생깁니다. 보류 중인 줄은 그 자체로는 제외 대상을 절대 바꾸지 않습니다.

---

## 13.8 CDC 캡처 목록

CDC는 캡처 단계에서 컬럼을 제외합니다: 제외 컬럼은 변경 스트림에서 완전히 빠집니다. 도구는 이 목록을
cdc-stack의 `ColumnExcludeList` 파라미터로 보냅니다. 이 목록은 CDC가 캡처하는 테이블만 다루며, CDC가
스트리밍해서는 안 되는, Aurora DSQL이 계산하는 생성 컬럼도 포함합니다.

도구는 각 항목이 정확히 한 컬럼에만 매칭되도록 씁니다. 이어 붙였을 때 같은 `db.table.column`
문자열이 되는 두 이름(이름 부분에 `.`가 들어간 경우 가능)은 캡처 시점에 구분할 수 없습니다. 둘 다
제외된 경우가 아니면 Deploy, Start CDC, CDC 단계가 이를 경고합니다.

**4,096바이트 한도.** `ColumnExcludeList`는 CloudFormation 파라미터 하나이며, 파라미터 값은
4,096바이트로 제한됩니다.

- **너무 긴 경우.** 팝업에서 Save가 비활성화됩니다("Narrow the rules: exclude fewer columns in the
  tables CDC captures"). Deploy CDC infrastructure와 Start CDC가 막히고, Full load + CDC에서는 Start
  Full Load도 어떤 로드보다 먼저 막힙니다. 메시지는 바이트 수와 현재 상태에 적용되는 조치를 알려
  줍니다.
- **생성 컬럼만으로 한도를 넘는 경우.** 이때는 제외를 줄여도 소용이 없습니다. Deploy CDC
  infrastructure와 Start CDC가 "capture fewer tables in this CDC pipeline, or use Full load only"와
  함께 거부합니다. 팝업에서는 Save를 막지 않는 경고이며, Start Full Load는 막히지 않습니다.
- **스택이 이미 가진 값은 절대 거부하지 않습니다.**

**규칙을 저장한 뒤 소스가 추가하거나 이름을 바꾼 컬럼.** 목록은 규칙을 저장할 때 정해진 구체적인
컬럼 이름을 담습니다. 그래서 와일드카드나 `type:` 규칙에 맞는 새 컬럼, 또는 소스가 이름을 바꾼 제외
컬럼은 캡처되며, 그 컬럼을 담은 그 테이블의 모든 insert와 update가 데드레터로 갑니다(SQLSTATE
42703). **Fix target schema…**는 그 컬럼을 추가하지 않습니다: 규칙이 막는 데이터를 복사하게 되기
때문입니다. 계속 빼 두려면: Stop CDC, **Delete CDC infrastructure**, 그런 다음 규칙이 그 컬럼을
보도록 Step 1(Evaluation)을 다시 실행하세요. 이름이 바뀐 컬럼이라면 그 컬럼을 제외하던 규칙이 새
이름에 맞도록 바꾸세요: 이전 컬럼 이름을 적은 규칙은 이제 `No column "…"`을 보고하고 이름이 비슷하면
고치는 방법을 제안하지만, 다른 테이블에 여전히 맞는 와일드카드는 아무것도 보고하지 않아 그 컬럼을
통과시킵니다. Full load + CDC에서는 먼저 Aurora DSQL에서 이전 컬럼도 직접
삭제하세요(`ALTER TABLE … DROP COLUMN …`, 대화 상자가 문을 보여 줌): 그 컬럼에는 NULL만, 또는 규칙이
막는 값만 들어 있으며, Change & reload는 새로 고친 정의에 더는 없는 컬럼을 지우기를 거부합니다. 그런
다음 [집합이 잠기는 이유](#집합이-잠기는-이유)에 설명된 대로 규칙을 저장(Full load + CDC: **Change &
reload**, Aurora DSQL에 그 컬럼을 NULL로 만듦)하고, 다시 배포한 뒤 Start CDC(MySQL은 "삭제 후 다시
CDC 시작" 참고). 대신 새 컬럼을 복제하려면, 같은 단계에서 규칙이 그 컬럼에 맞지 않도록 바꾸세요.

**보이는 것.**

- 팝업: **CDC capture list (B of 4,096 bytes)**. Start CDC와 같은 코드로 계산합니다.
- **Deploy CDC infrastructure** 확인 창: "Excluded: N tables, M columns. They are fixed once
  deployed: changing them later needs Delete CDC infrastructure.", 정확한 목록은 **show** 아래.
- CDC 단계: 읽기 전용 요약 "Excluded: N tables, M columns"와, 정확한 값을 담은 펼칠 수 있는 **What
  CDC receives (B of 4,096 bytes; as deployed | Start CDC sends)**. 배포된 값과 세션의 값이 다르면
  단계가 둘 다 보여 줍니다.

**컬럼을 제외하는 스택에 attach하기.** 새로 시작해 아직 실행하지 않은 세션이, `ColumnExcludeList`가
컬럼을 제외하는 시작 가능한 cdc-stack에 attach하면, 도구는 그 목록이 정확히 다시 읽힐 때만
채택합니다. 채택된 컬럼은 `# adopted from <stack>` 표시가 붙은 규칙 줄로 나타납니다. 한 번이라도
스트리밍한 스택과 세션의 집합이 다르면, Start CDC, Retry CDC, 그리고 Full load + CDC에서는 Start
Full Load가 거부하며 두 집합을 모두 명시합니다.

---

## 13.9 Validation과 Cut over

### Validation이 제외 컬럼이 NULL임을 증명합니다

테이블의 행을 세는 타깃 패스가 각 제외 컬럼에 값이 있는 행도 셉니다. 추가 스캔은 필요 없습니다.
타깃만 읽으므로 MySQL 소스와 PostgreSQL 소스에서 똑같이 동작하며, 아무것도 제외하지 않으면 SQL은
바뀌지 않습니다. 제외 컬럼은 값을 비교하지 않지만(도구가 쓰지 않았으므로), 그 NULL 증명은 다음에
표시됩니다:

- 테이블별 **Excluded** 칩(`NULL` 또는 `N non-NULL`);
- readiness 알림("Excluded columns are NULL on Aurora DSQL");
- 실패 사유, AI facts, 텍스트 리포트.

제외 컬럼에 값이 있다는 것의 의미는 마이그레이션 유형에 따라 다릅니다:

| 마이그레이션 | 제외 컬럼에 있는 값 |
|---|---|
| Full load only, Full load + CDC, 또는 Full Load가 실행된 CDC only | 그 테이블은 **불일치(not matched)**이며, 컷오버가 차단됩니다: "Excluded columns hold values: reload those tables with Drop & reload". 이 값들은 보통 테이블에 이미 있던 데이터 위로 행이 append되었다는 뜻입니다. append는 저장된 값을 유지하므로 Full Load를 다시 실행해도 지워지지 않습니다. 조치는 상태에 따라 다릅니다: 스트리밍 중이면 먼저 Stop CDC, CDC only라면 유형 전환, job 기록이 사라졌으면 Start over. |
| 이미 행이 있던 타깃으로의 CDC only | 경고만: "Excluded columns hold values from before CDC started". |

NULL 점검 자체가 실패하면 판정 불가(not determined)로 보고됩니다. 테이블의 행 수와 판정은 영향을
받지 않습니다.

### Cut over가 제거된 DEFAULT를 복원합니다

컷오버 런북에 **Excluded-column DEFAULTs restored: n of m** 블록이 추가됩니다. 이 블록은 정확한
구문, **Restore DEFAULTs** 버튼, 그리고 각 컬럼의 실시간 상태(pending, done, diverged, gone,
unknown)를 보여 줍니다:

```sql
ALTER TABLE "shop"."users" ALTER COLUMN "status" SET DEFAULT ('active'::text)
```

- **자동 복원.** DEFAULT는 컬럼이 여전히 제외되어 있고, 기록된 텍스트가 단일 식이며, Schema
  Conversion DDL이 기록 이후 바뀌지 않았을(또는 여전히 같은 DEFAULT를 선언할) 때만 자동으로
  복원됩니다. 그 밖의 복원 대기 컬럼은 사유와 구문과 함께 **Needs your decision** 아래에 나열됩니다.
  기록된 텍스트가 단일 식이면 자체 **Restore** 버튼이 붙고, 그렇지 않으면 구문을 직접 실행하세요.
- **절대 덮어쓰지 않습니다.** 그 사이 누군가 다른 값으로 설정한 DEFAULT는 그대로 둡니다.
- **새 행에만.** 복원된 DEFAULT는 새 행에 적용되며, 이관된 행은 NULL로 남습니다.
- **NOT NULL은 제거된 채로 남습니다.** Aurora DSQL은 NOT NULL을 제자리에서 다시 추가할 수 없으므로,
  블록은 어떤 제외 컬럼이 nullable로 남는지 나열합니다.
- **CDC 중에는 먼저 drain 확인을 체크하세요.** 이는 "I have frozen source writes, both CDC
  connectors are RUNNING, and CDC has drained to zero lag" 체크박스입니다. 외래 키 블록도 같은
  체크박스를 쓰며, 두 블록 중 하나라도 표시되면 나타납니다. 도구는 지연(lag)을 볼 수 없으므로 그것이
  사실일 때만 체크하세요. 커넥터가 아직 스냅샷하거나 재생하는 중에 DEFAULT를 복원하면 그 행들이
  DEFAULT를 받습니다. 스택이나 커넥터가 아직 변경 중이면 복원은 거부됩니다. 실행 중인 Full Load나
  Start CDC는 복원을 막고, 그 반대도 마찬가지입니다. 모든 Start CDC는 이 체크박스를 해제합니다.
- **마지막 Start CDC 이후에 복원하세요.** Start CDC는 복원된 DEFAULT를 다시 제거합니다(§13.6).
- **복원할 DEFAULT가 남은 채로 컷오버하는 것은 막지 않습니다.** "I've cut over to DSQL" 위의 경고에
  자체 **Restore DEFAULTs** 버튼이 있으며, 컷오버를 확인하면
  `cut over with N DEFAULTs not restored`가 로그에 남습니다.
- **더 이상 선택되지 않은 테이블.** 테이블이 더 이상 마이그레이션에 선택되어 있지 않은 컬럼(Full
  load only, 그리고 CDC를 시작하기 전의 Full load + CDC는 로드된 테이블의 선택을 해제할 수 있음)은
  자동으로 복원되지 않으며, **Needs your decision** 아래에 나열됩니다.

원장을 읽을 수 없으면 블록이 그 사실을 알려 주고("The record of dropped DEFAULTs could not be read
...") **Check again**을 제공합니다.

---

## 13.10 한계, 그리고 라이브로 실행하지 않은 것

**계획에 반영할 한계.**

- **NOT NULL은 단방향으로 제거됩니다.** 나중에 그 테이블을 Drop & reload해 Schema Conversion DDL로
  재생성해야만 복원됩니다.
- 이름에 `:`가 들어간 테이블은 저장된 컬럼 제외를 가질 수 없습니다: 테이블 전체를 제외하거나 컬럼을
  유지하세요. 줄바꿈이 들어간 이름은 규칙으로 쓸 수 없습니다.
- 라이브 CDC가 스트리밍 중인 테이블에 오버레이 ALTER나 컷오버 `SET DEFAULT`를 실행하면, CDC 싱크가
  그 테이블의 다음 배치를 한 행씩(더 느리게) 씁니다. 싱크 연결마다 한 번입니다. 잃는 행은 없고,
  DLQ로 가는 것도 없습니다.
- 도구는 CDC 지연을 볼 수 없습니다. 복원 전에 소스 쓰기가 동결되었고 CDC가 drain되었음을 (컷오버
  체크박스로) 직접 확인합니다.
- Check sizes는 타입 기반이며 opt-in입니다. 한도를 넘을 수 있는 모든 타입을 강조 표시하지는 않으며,
  2 MiB를 넘는 행은 점검하지 않습니다(§13.5).

**라이브 검증.** MySQL 소스(Aurora MySQL 8.0.42)에서 Aurora DSQL로 검증했으며, 워커 2개로 12만 행을
처리한 실행도 포함합니다.

**라이브로 실행하지 않은 것**(단위 테스트로만 다룸): PostgreSQL Full Load 오버레이와 PostgreSQL CDC
부분, 대부분의 캡처 목록 이스케이프(이름 안의 `.`, 쉼표, 앞뒤 공백, 대소문자 쌍둥이 형태), 그리고
Fargate 배포가 쓰는 S3 기반 원장(라이브 실행은 로컬 SQLite 저장소를 사용했습니다).

---

**이전:** [부록: 성능 테스트 결과](12-performance-test-results.md)
