# 9. Query Converter와 AI DBA

_언어: [English](../en/09-query-validation.md) | **한국어** | [日本語](../ja/09-query-validation.md)_

> **이전:** [8. 테스트 및 검증](08-testing-and-verification.md)

스키마와 데이터를 옮기는 것만으로는 끝이 아닙니다 — 애플리케이션의 **쿼리**도 Aurora DSQL에서
돌아가야 하고, *잘* 돌아가야 합니다. **Query Converter**(사이드바의 선택적 도구)는 MySQL 쿼리 하나를
Aurora DSQL로 변환하고, 타깃에서 읽기 전용으로 테스트하며, AI 보조를 켜면 **AI DBA**가 DSQL에 맞게
효율적으로 재작성하고 그 개선을 증명하는 곳입니다.

이 장은 그 흐름을 다룹니다. 소스에는 절대 쓰지 않고, 타깃에 DML을 실행하지도 않습니다.

---

## 9.1 쿼리 변환

MySQL 문을 붙여넣으면 스키마 변환과 동일한 규칙 기반(deterministic-first) 엔진(`sqlglot`)으로 Aurora
DSQL(PostgreSQL)로 변환하고 분류합니다:

- **AUTO** — 규칙 기반으로 변환 완료; 바로 테스트 가능.
- **MANUAL** — 변환은 됐으나 검토 필요(DSQL에서 주의점이 있는 관용구).

또한 MySQL 관용구를 **재작성**하고 DSQL에서 문제가 되는 것을 **표시**합니다 — 예: `ON DUPLICATE KEY UPDATE`
→ `INSERT ... ON CONFLICT DO UPDATE`(MANUAL — 충돌 타깃을 직접 확인), `JSON_UNQUOTE(JSON_EXTRACT(...))`
→ `JSON_EXTRACT_PATH_TEXT`, MySQL `HAVING`-별칭 참조 인라인화, 그리고 `SELECT ... FOR UPDATE`(DSQL의
낙관적 동시성 제어에서 다르게 동작). 이런 발견은 모두 **MANUAL**로 분류됩니다. 애플리케이션 코드 전반의
안티패턴 스캔(`AUTO_INCREMENT`/트리거 의존, 미지원 함수)은 [2장](02-evaluation-and-schema-conversion.md) 참조.

원본과 변환된 SQL을 나란히 보여줘 무엇이 바뀌었는지 정확히 확인할 수 있습니다.

---

## 9.2 타깃에서 테스트 (읽기 전용)

변환된 **SELECT**는 **Test on target**으로 검증된 DSQL 타깃에서 `EXPLAIN`으로 계획을 세웁니다 — 쿼리를
**실행하지 않고** 계획만 세우므로 행을 읽지 않습니다. **EXPLAIN ANALYZE** 토글을 켜면 실제로 (읽기
전용) 쿼리를 실행해 실제 시간·행 수와 Aurora DSQL의 문장별 **DPU 비용 추정**을 캡처합니다.

무엇을 테스트할 수 있고 없는지:

- **SELECT** → `EXPLAIN`(계획만) 또는 `EXPLAIN ANALYZE`(읽기 전용 실행).
- **DDL** → 트랜잭션 안에서 dry run 후 **롤백**(커밋하지 않음).
- **DML**(INSERT/UPDATE/DELETE) → 타깃에 **절대 실행하지 않음**.

판정에는 DSQL이 문을 받아들였는지, 아니라면 정확한 에러(SQLSTATE 포함), 캡처된 쿼리 플랜, 그리고
ANALYZE일 때 DPU 비용이 표시됩니다.

**어느 스키마로 테스트되는가.** 스키마 없이 쓴 테이블명(`FROM orders`)은 세션 `search_path`로 스키마에
해석됩니다; 도구는 연결된 소스 DB의 동일 이름 스키마를 기본값으로 쓰고, **Test against schema** 피커로
바꿀 수 있습니다. `relation "…" does not exist`(SQLSTATE **42P01**)는 그 테이블이 테스트 대상 스키마에
없다는 뜻일 뿐 — 올바른 스키마를 골라 다시 테스트하세요.

> **DSQL 플랜 읽기:** Aurora DSQL은 *분산* PostgreSQL 호환 엔진이라 플랜이 조금 다르게 읽힙니다 —
> §9.4 참조.

---

## 9.3 AI DBA로 쿼리 튜닝

**AI 보조가 켜져 있으면**(Connect 화면; 기본 꺼짐) **Tune with AI DBA** 버튼이 나타납니다 — 단,
변환된 SELECT가 **Test on target을 통과한 뒤에만** 보입니다. AI가 추측이 아니라 근거 기반으로 조언하려면
이 쿼리의 실제 실행 플랜이 필요하므로, 먼저 테스트하는 것이 필수입니다. 테스트를 **EXPLAIN
ANALYZE**로 돌려 **DPU 기준값**을 캡처하면, 버튼에 현재 비용(예: *now ≈ 0.03 DPU*)이 표시되고, 이후
AI가 재작성으로 얼마나 절약되는지 증명할 수 있습니다.

버튼을 누르면 앱 전역의 상시 AI 패널이 열리며 범위가 이 쿼리로 설정되고, **이 쿼리의 실제 EXPLAIN
플랜과 DPU** + Aurora DSQL 실행 모델에 근거해 AI가:

- 재작성 쿼리를 코드 블록으로 제안하고,
- **무엇을 바꿨고 왜 DSQL에서 더 저렴한지**(어떤 스캔 유형(scan type)이나 필터 계층(filter layer)이
  개선됐고, 왜 storage에서 compute로 넘어가는 바이트가 줄어드는지) 설명하며,
- 쿼리의 **결과는 동일하게 유지**합니다 — 빠르게 만들려고 의미를 바꾸지 말라고 지시받습니다.

DSQL에 맞지 않는 일반 PostgreSQL 튜닝 조언(`VACUUM`/`REINDEX`, fillfactor, 플래너 GUC, "`cost=`
숫자를 낮춰라" 같은)은 명시적으로 배제됩니다.

### 증명: 재작성 재테스트

각 재작성 제안 아래에는 **Test rewrite on target** 액션이 있습니다. 도구가 답변에서 정확한 SELECT를
추출해 타깃에서 `EXPLAIN ANALYZE`로 읽기 전용 재실행하고, 측정한 **개선 전/후 DPU**를 같은 채팅에
되먹입니다 — 그래서 **AI가 실제 개선폭을 보고**합니다(개선이 없으면 솔직히 말합니다). 증거는 모델의
설명이 아니라 실측 DPU입니다.

> **권고 전용.** 자동 적용되지 않습니다. 재작성을 편집기에 직접 복사해 Convert / Test를 다시
> 돌리는 것이 사람의 검토(human-review) 게이트 역할을 합니다. AI 보조는 선택적으로 켜는 기능(opt-in)이며
> 컨트롤 플레인에만 있고 데이터 경로에는 절대 관여하지 않습니다.

### AI DBA로 쿼리 검토/수정

Tune(*비용* 관점)과 별개로 **Review with AI DBA** 버튼이 있습니다 — 타깃이 변환된 문을 *거부*하면
**Fix with AI DBA**로 바뀝니다. 이는 *정확성* 액션으로, 변환이 맞는지, 거부됐다면 왜 거부됐는지(정확한
에러 + SQLSTATE 기반)와 어떻게 고칠지를 AI에게 묻습니다. Tune과 달리 **통과한 테스트가 필요 없어서**,
변환이 이상해 보이거나 Test on target이 실패한 즉시 쓸 수 있습니다. Tune과 마찬가지로 권고 전용입니다.

---

## 9.4 DSQL 쿼리 튜닝이 PostgreSQL과 다른 이유

Aurora DSQL은 와이어 프로토콜 수준에서는 PostgreSQL과 호환되지만 *분산* 엔진으로 쿼리를 실행하므로,
쿼리를 효율적으로 만드는 방법이 몇 가지 달라집니다. AI DBA는 이 사실들에 근거하며, 직접 플랜을 읽을
때도 알아 두면 유용합니다.

- **기본 키가 곧 테이블입니다.** 모든 테이블은 기본 키로 정렬된 B-tree이며 별도 heap이 없습니다. 술어에
  쓸 인덱스가 없는 테이블은 **Full Scan**(“Seq Scan”이 아님)으로 읽힙니다. 기본 키에 대한 범위/동등
  필터는 물리적으로 순차 읽기라 본질적으로 저렴하므로, **기본 키 선택이 PostgreSQL보다 훨씬 중요합니다.**
- **compute와 storage가 분리돼 있습니다.** storage에서 compute로 넘어오는 모든 행이 지연 시간과 **DPU**
  (Distributed Processing Unit — DSQL의 비용 단위, `EXPLAIN ANALYZE VERBOSE`에 표시; PostgreSQL의
  `cost=` 숫자는 목표가 아님)를 유발합니다. 옮겨지는 바이트를 줄이도록 **필터를 아래로 미는 것**이 핵심
  지렛대입니다.
- **필터 3단계, 좋은 것부터:** (1) *Index Condition* — 인덱스 키 컬럼에 대한 동등/범위 술어; (2)
  *Storage Filter* — 비-키 컬럼을 인덱스 `INCLUDE`에 넣어 storage가 전송 전에 걸러 냄; (3) *Query
  Processor Filter* — 상단 `Filter:` 줄로 나타나며, 걸러지지 않은 데이터가 이미 네트워크를 건넌 상태
  (최악). 술어를 3 → 2 → 1 방향으로 미세요.
- **스캔 유형, 저렴한 순으로 마지막이 최선:** **Full Scan**(PK나 인덱스 추가) → **Index Scan**(`Storage
  Lookup` 노드는 커버링 인덱스가 불완전하다는 뜻 — 빠진 컬럼을 `INCLUDE`에 추가) → **Index Only Scan**
  (이상적).

AI DBA가 제안할 DSQL 적합 재작성: `SELECT *` 대신 필요한 컬럼만 투영; 선행 와일드카드 `LIKE '%x%'`
회피(인덱스 사용 불가); 옵티마이저가 조인 너머로는 추론하지 못하는 **중복 조인 술어(redundant join
predicate)** 추가; `ORDER BY … LIMIT`에는 **CTE 지연 구체화(late materialization)**; 인덱스를 커버링으로
만드는 `INCLUDE` 컬럼; 핫 파티션을 만드는 단조 증가 키(`AUTO_INCREMENT`, 타임스탬프)보다 무작위 분포
키(UUID) 선호.

---

## 9.5 AI DBA — 앱 전역 어시스턴트

위의 **Tune** / **Review** 액션이 여는 어시스턴트는 **모든** 단계에 있는 바로 그것 — **AI DBA** 패널
(헤더 **AI DBA** 버튼)입니다. Connect에서 켜는 그 Bedrock 전용 AI 보조(**기본 꺼짐**)를 상시
세션-기반 우측 패널로 노출한 것으로, 대화와 스코프가 단계 이동·브라우저 새로고침·앱 재시작을 견딥니다.

챗봇 이상인 이유:

- **이 마이그레이션에 대해 무엇이든 질문.** 일반 챗 모드가 일반 문서가 아니라 실제 실행 데이터를 근거로
  답합니다.
- **read-only 진단 툴.** 답하기 위해 모델이 마이그레이션의 실제 상태를 끌어오는 툴을 호출합니다 — 행 값도
  자격증명도 절대 보지 않습니다. **Full Load** 실패(어떤 테이블이 왜 실패했는지) 진단, **CDC 미스트리밍/
  배포 실패** 원인 설명과 **DLQ 트리아지**(테이블·SQLSTATE별 데드레터 샘플), **사전점검 판정**(무엇이 CDC를
  막고 어떻게 고치는지), **Validation** 불일치 설명, **타깃 DSQL** 카탈로그 조회(테이블·스키마·행 수)를 할 수
  있습니다.
- **단계 교차 컨텍스트.** 주요 동작이 활동 피드로 패널에 미러링되고, **라이브 Full Load 진행 카드가 고정된
  채 다른 화면에서도 계속 갱신**되며, 최근 동작이 각 답변의 근거가 됩니다.
- **"What's next?" 브리핑.** 헤더 액션이 툴 기반으로 다음에 할 일과 현재 최상위 리스크를 알려줍니다.
- **스코프 딥링크.** 각 단계에 "이거 물어보기" 원클릭 진입이 있습니다 — Full Load의 실패 테이블별/격리별
  도움, CDC의 드리프트+DLQ 트리아지, Validation의 "이 불일치 설명", Cut over의 **GO / HOLD "지금 전환해도
  안전한가"** 판단.

**안전(모든 AI 보조와 동일 모델):** 기본 꺼짐; Bedrock 전용(API 키 입력 없음); 툴은 엄격히 read-only이며
**스키마/상태/DDL/플랜 메타데이터만 보고 Full Load·CDC 행 데이터나 자격증명은 절대 보지 않습니다**; 제안한
어떤 것도 사용자의 명시적 동작 없이는 적용되지 않습니다. 권한 모델과 켜는 법은
[§2.1](02-evaluation-and-schema-conversion.md) 참조.

---

## 9.6 다음으로

- **데이터 경로 튜닝(병렬수):** [7장 — 성능과 튜닝](07-performance-and-tuning.md).
- **결론과 컷오버:** [10장 — 결론](10-conclusion.md).
- **자주 묻는 질문:** [11장 — 고객 FAQ](11-customer-faq.md).

---

**다음:** [10. 결론 →](10-conclusion.md)
