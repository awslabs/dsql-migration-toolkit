# 5. Validation 동작 방식

_언어: [English](../en/05-validation.md) | **한국어** | [日本語](../ja/05-validation.md)_

> **이전:** [4. CDC와 DSQL 제약](04-cdc-and-dsql-constraints.md)
>
> **제외한 컬럼**은 값을 비교하지 않고 NULL임을 증명합니다. [13.9 Validation과 Cut over](13-excluding-tables-and-columns.md#139-validation과-cut-over)를 보세요.

**Validation**은 타깃이 소스와 일치함을 증명하는 단계 — 안전한 전환의 근거입니다. **정확한**
`COUNT(*)`와 체크섬을 실행하는 유일한 단계로(Full Load는 의도적으로 스캔 없는 추정을 사용), 근사가
아니라 권위 있는 판정을 줍니다.

Full Load 후(그리고 CDC를 쓴다면 스트림이 따라잡은 후) **Validation** 단계에서 실행합니다. 마이그레이션된
DSQL 타깃을 **워터마크 시점의** 소스와 비교하고, **MySQL** 소스에서는 라이브 소스가 그 사이 변했으면
드리프트를 보고합니다(**PostgreSQL** 소스는 드리프트 판정을 받지 않습니다 — 5.2 참고).

---

## 5.1 비교 대상

Validation은 **테이블 단위**로, 선택할 수 있는 점증적 엄밀도로 동작합니다:

| 모드 | 검사 | 비용 |
|---|---|---|
| **행 수**(기본) | 테이블별 소스 vs 타깃 정확한 `COUNT(*)`. | 저렴. |
| **체크섬** | 순서 무관 테이블별 체크섬(양쪽 엔진에서 동일 방식 계산) 추가 — 행 수만으로는 놓치는 값 차이 포착. | 더 무거움(모든 행 읽기). |
| **대조(전체 PK 셋)** | 양쪽의 **모든 기본 키**를 PK 순서로 스트리밍해 정렬 병합, **타깃에 없는** PK와 **타깃에만 있는** PK(CDC가 아직 적용 안 한 삭제)를 정확히 보고. | 가장 무거움; 전환 전 "불일치 레코드 없음" 증명. |

모두에 무거운 모드를 돌릴 필요는 없습니다: 옵션으로 **행 수가 이미 불일치하는** 테이블에만 비싼
체크섬/대조를 계산하게 할 수 있어, 행 수가 일치하는 테이블은 "심층 미검사"로 보고됩니다(거짓 일치가
아님).

> **CDC 마이그레이션:** 스트림이 변경을 dead-letter 처리한 테이블은 어떤 모드를 고르든 **CHECKSUM**으로도
> 비교됩니다 — 행 수로는 누락된 UPDATE를 볼 수 없기 때문입니다(5.5 참고).

> **대조는 단일 컬럼 정수 PK에 적용됩니다.** 두 엔진에서 정렬이 잘 정의되기 때문입니다. 복합·비정수
> PK는 대조 패스에서 제외됩니다(행 수/체크섬은 적용). **매우 큰** 테이블이면서 복합/누락 PK인 경우,
> 정확한 `COUNT(*)`/체크섬은 단일 무한정 쿼리로 폴백해 DSQL의 ~300초 트랜잭션 한도에 걸릴 수
> 있습니다(테이블별 오류로 표시). 단일 컬럼 PK 테이블은 keyset 페이징이라 영향 없습니다.

> **체크섬은 MySQL 소스의 부동소수점 및 JSON 컬럼을 제외합니다.** MySQL `FLOAT`/`DOUBLE`과 `JSON`은
> 두 엔진에서 똑같이 렌더링되는 정확한 텍스트 형태가 없어 체크섬에서 빠지고, 리포트에 **"value
> 미비교(not value-compared)"** 로 표시됩니다. **PostgreSQL** 소스에서는 하나의 식이 양쪽을 렌더링하므로
> `real`/`double precision`도 **체크섬에 포함**되고, `jsonb`(정규화된 형태가 있음)도 포함되며 — 순수
> `json`만 제외됩니다 — 따라서 비키 컬럼의 jsonb 값 드리프트는 여전히 포착됩니다. 따라서 **비키(non-key)**
> 컬럼에 국한된 값 차이는 어떤 모드로도 감지되지 않습니다(in-place 수정은 행 수를 안 바꾸고, 대조는
> PK *존재*만 비교). 키 컬럼과 그 밖의 모든 타입은 여전히 완전히 비교됩니다. 컬럼 수는 체크섬 제한이
> **아닙니다** — 매우 넓은 테이블은 컬럼을 그룹으로 나눠 해시(중첩 MD5)해 DSQL의 함수 100-인자 한도
> 안에 머무릅니다.

---

## 5.2 라이브 소스 드리프트

실제 소스는 마이그레이션 중에도 계속 변하므로 "행 수가 일치하나?"에는 기준점이 필요합니다. Validation은
**워터마크**를 사용합니다:

- **MySQL 소스에서는** **현재** 소스 위치를 워터마크의 위치와 비교합니다: 워터마크와 라이브 소스가
  **모두** GTID를 노출할 때만 GTID로, 그렇지 않으면 binlog **file:position**으로 비교합니다(GTID를 켤 수
  없을 때의 경로 — 예: RDS MySQL 8.0; 좌표는 MySQL 8.2+/8.4에서 `SHOW BINARY LOG STATUS`, 그 이하에서는
  `SHOW MASTER STATUS`로 읽습니다). 소스가 전진했으면 리포트가 **`drifted = true`**로 표시합니다.
- **PostgreSQL 소스에서는** 워터마크가 binlog/GTID 좌표가 아니라 **WAL LSN**(그리고 논리 복제 슬롯과
  퍼블리케이션)을 기록하며, Validation은 현재 LSN 기반 드리프트를 **재계산하지 않습니다**: binlog/GTID
  프로브를 건너뛰고 `drifted`를 **"could not be determined"** 로 보고합니다. 드리프트 플래그에 의존하는
  대신 CDC 중 타깃이 **수렴**하는지 지켜보세요(다음 항목).
- 어느 쪽이든 리포트는 워터마크의 **스냅샷 타임스탬프**를 "기준 시점"으로 담습니다 — 그래서 행 수
  차이가 마이그레이션 버그가 아니라 *스냅샷 이후 신규 소스 활동* 때문임이 올바르게 귀속됩니다.
- CDC 중에는 보통 타깃이 소스로 **수렴**하는 것을 지켜본 뒤(경량 `scripts/compare_rows.py --watch N`이
  *N*초마다 재확인 — 이 용도로 유용) 따라잡으면 전체 Validation을 실행합니다.

---

## 5.3 판정 모델

판정은 **구조적으로 건전**합니다 — 증거가 뒷받침할 때만 일치로 보고합니다:

- **테이블**은 행 수가 같고 **그리고**(체크섬 모드) 체크섬이 같고 **그리고**(대조 시) PK 셋이 일관될
  때만 `matched`.
- **전체 리포트**는 **모든** 테이블이 일치하고 고아 행이 없을 때만 `is_match`.
- 한 테이블 비교 실패는 그 테이블의 에러 항목으로 **격리**됩니다 — 실행을 중단시키거나 나머지를 조용히
  통과시키지 않습니다.

선택적 **고아 검사**는 매칭되는 부모가 없는 타깃 자식 행 수를 셉니다. 이는 강제되는 외래 키의 **적용 전
게이트**입니다 — 강제되는 `ADD CONSTRAINT`는 자식 행에 부모가 없으면 실패하므로, FK 적용(CDC
마이그레이션은 cut over 시점) 전에 그런 고아 행을 미리 찾아냅니다 — 그리고 FK를 떼어 내고 참조 무결성을
애플리케이션에서 강제하기로 했다면 그때의 안전망이기도 합니다.

불일치 테이블에 대해, 선택적 한정 **행 차이 샘플**이 몇 개의 차이 나는 PK(및 체크섬 토큰)를 보여
줍니다 — **행 값은 절대 아님** — 데이터 노출 없이 조사할 수 있게 합니다.

리포트는 **export 가능**하므로 전환 결정에 첨부할 수 있습니다.

---

## 5.4 전형적 결과 읽기

| 테이블 | source | target | 판정 |
|---|---|---|---|
| `orders` | 3906 | 3906 | **MATCH** |
| `order_items` | 3785 | 3785 | **MATCH** |
| `documents` | 2309 | 2306 | **조사 필요** |

- 모든 테이블 **MATCH**(고아 없음) → 전환 안전(CDC 마이그레이션이면 5.5의 dead-letter 확인이 해소된
  뒤).
- 작은 **타깃 부족**은 보통 (a) **드리프트** — 워터마크 이후 소스가 전진(드리프트 플래그 확인), 또는
  (b) **의도된** — 격리(quarantine)된 행(예: DSQL 1 MiB 초과 값)입니다. 이제 어느 쪽인지 직접 판별할
  필요가 없습니다: 부족분이 마이그레이션이 영구 드롭한 행 수와 **정확히** 일치하면 테이블이 그렇게
  알려 줍니다 — *"Fully explained: N rows were permanently dropped during the migration …
  this deficit is expected, not new data loss"*. 부족분이 드롭 건수보다 **크면**
  *"Partly explained: … but N more are missing and are NOT accounted for"*로 표시되며, 조사해야 할
  대상은 바로 그 N행입니다. 테이블은 여전히 **MISMATCH**로 보고됩니다 — 의도된 동작입니다: 그 행들은
  실제로 없으므로, 이 설명은 갭을 **해명**하는 것이지 면제하는 것이 아닙니다. 소스 값을 고쳐 해당
  테이블을 리로드해 갭을 닫거나, 컷오버 전에 알려진 갭으로 수락하십시오.
  (앱을 재시작하면 테이블별 드롭 건수는 사라집니다 — 영속되지 않습니다 — 그래서 부족분을 추측하지 않고
  미해명으로 보고합니다. 그 경우에는 **Full Load 에러 로그**와 교차 확인하세요. CDC dead letter는
  다릅니다: 도구가 테이블별 건수를 재시작 후에도 유지하며, Cut over가 이를 확인합니다(5.5).)
- CDC 중 **타깃 잉여**(타깃에만 있는 PK)는 보통 스트림이 아직 적용 안 한 소스 **삭제**입니다 — 수렴 후
  재확인하세요.

> **경험칙:** 차이가 드리프트, 의도된 quarantine(초대형 값), 또는 아직 수렴 안 된 CDC로만 설명된다면
> 마이그레이션은 건전합니다. 행 수가 일치하고 따라잡은 테이블에서 **설명되지 않는** 누락/잉여 PK가
> 바로 Validation이 드러내려는 대상입니다.

> **보고서 자체의 복구 카드.** 불일치가 **설명되지 않으면** 보고서는 순서가 있는 클릭 경로와 함께
> **How to recover**를 보여 줍니다. CDC 스택이 **스트리밍한 적 있는** — Start 뒤에 멈췄거나, 커넥터가
> 그대로 있거나, CloudFormation이 Start를 롤백한 스택 — 또는 CDC 상태를 아직 읽지 못한(앱 재시작 후 Data
> Migration 단계가 읽기 전. 카드가 그렇다고 말하며, 읽은 뒤에는 그 스택 상태에 맞는 단계를 보여 줍니다)
> **Full load + CDC**나 **CDC only** 마이그레이션에서는
> 그 백필이 **새 시작 지점**을 가져가므로, 단계가 새 CDC 토픽을 먼저 둡니다: CDC가 실행 중이면 Stop CDC →
> **Delete CDC infrastructure** 후 다시 배포 → **Re-run Full Load**(일부가 실패했다면 **Re-run all
> tables**)를 **Drop & reload**로 → **Start CDC**(새 Full Load의 시작 지점에서 시작) → **Re-run
> validation**. CDC only에서는 다시 배포한 다음 단계에서 마이그레이션 유형을 Full Load 단계가 있는 Full
> load + CDC로 바꿉니다. 그런 스택에서는 인프라를 다시 배포하기 전까지 Full Load 단계가 새 시작 지점을
> 거부합니다([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)): 새 시작 지점을 가져간 Full Load 뒤의
> 재개는 **빈틈 없지 않으며**, 그래서 새 토픽이 먼저 옵니다. 스트리밍한 적 없는 스택에서는 순서가 더
> 짧습니다(CDC only에서는 먼저 마이그레이션 유형을 Full load + CDC로 바꿈 → **Re-run Full Load**(일부가
> 실패했다면 **Re-run all tables**)를 **Drop & reload**로 → **Start CDC**(새 Full Load의 시작 지점에서
> 시작) → **Re-run validation**). 위 단계에서 Stop CDC와 재배포를 뺀 것이며, 그 재배포를 마친 뒤에도 카드가
> 이 단계를 보여 줍니다: CDC가 새 시작 지점에서 시작하고 append는 그보다 오래된 행을 그대로 두므로, 여기서도
> 재실행은 **Drop & reload**입니다. **Full load only**에서는 카드가 *Re-run Full Load to
> backfill the gap*이며, 재실행 다음 **Re-run validation**이고 CDC 단계는 없습니다. **한 테이블**만 다시 적재하는 것은 다릅니다: 특정 테이블의
> **Drop & reload**는 **원래** 시작 지점을 유지하므로, 5.5~5.7의 테이블별 해결 경로는 전혀 막히지 않습니다.
> 타깃에 소스에 없는 행이 있으면 카드는 *The target holds rows the source does not — a delete never
> landed*라고 말합니다: 재실행은 행을 채우기만 하므로, 카드는 그 테이블별 다시 적재(CDC를 멈춘 채 **Reload
> specific tables**, **Drop & reload** 선택. CDC only에서는 마이그레이션 유형을 Full load + CDC로 바꾼 뒤)를
> 알려 줍니다. CDC only이고 이 세션에 Full Load 기록이 없으면 다시 적재할 테이블이 없으므로, 대신 새 Full Load
> 시작 지점을 알려 줍니다: CDC가 실행 중이면 Stop CDC 후 **Delete CDC infrastructure** 하고 다시
> 배포한 다음(스트리밍한 적 있거나 CDC 상태를 아직 읽지 못한 스택이면), 마이그레이션 유형을 Full load +
> CDC로 바꿔 그 Full Load를 **Drop & reload**로 실행하고 **Start CDC** 합니다.

Validation이 깨끗한 **MATCH**(또는 모든 차이가 설명됨)를 보고하고 — CDC 마이그레이션이면 5.5의
dead-letter 확인까지 해소되면 — 마지막 워크플로 단계인 **Cut over**가 UI에 나타나며 애플리케이션을 소스 데이터베이스(RDS/Aurora MySQL 또는 PostgreSQL)에서
DSQL로 전환하는 런북을 제공합니다. Full load only에서는 Cut over에 보고서가 Full Load 이후 선택한 모든
테이블을 다루는 것도 필요합니다: 그런 테이블 중 보고서에 행이 없는 테이블이 있으면, 그 테이블을 로드하고
Validation을 다시 실행할 때까지(또는 **Objects to validate**에서 그 칩을 끌 때까지) Cut over가 보류됩니다.
Full load + CDC(및 CDC only)에서도 Full Load 이후 선택해 그 시작 지점 아래에서 로드한 테이블에 대해 마찬가지입니다: 보고서에 그 테이블의 행이 없으면 Validation을 다시 실행할 때까지 Cut over가 보류됩니다.
Full load + CDC에서 Full Load 후(CDC를 시작하기 전) 체크를 해제한 테이블은 검증하지 않습니다: Validation은 아직 선택된 테이블을 비교합니다. 마지막 보고서가 그 변경 이전의 것이라면 Validation을 다시 실행하세요.
전환 순서(CDC drain vs Full Load freeze)와 롤백 앵커는 [결론](10-conclusion.md)의 권장 end-to-end 흐름을
참고하세요.

> **명령줄에서 검증 (선택).** 이 내장 Validation 외에도, 읽기 전용 CLI 스크립트 2개로 셸에서
> 스팟 체크할 수 있습니다(종료 코드로 스크립트 게이팅 가능): `scripts/compare_rows.py`(테이블별 행
> 수 / PK 범위 확인, CDC 중 타깃 수렴을 지켜보는 `--watch N`)와 `scripts/cdc_consistency_check.py`
> (전체 PK 대조 — 타깃에 **누락**되거나 **잉여**인 정확한 PK를 짚어 주는 전환 전 제로-데이터-손실
> 증명). 자세한 내용은 [`scripts/README.md`](../../../scripts/README.md) 참고. 권위 있는 go/no-go는
> 여전히 이 내장 Validation입니다(체크섬·드리프트 귀속까지 수행).

---

## 5.5 CDC dead letter와 컷오버

**dead letter**는 Aurora DSQL이 거부해서(예: 소스 스키마 변경 후) CDC 스트림이 따로 빼 둔 변경입니다.
**재시도되지 않습니다.** 같은 행의 이후 전체 행 UPDATE가 오면 복구되지만, 다시 건드리지 않는 행은
누락되거나 오래된 값으로 남고, 유실된 DELETE는 타깃에 잉여 행을 남깁니다. 누락된 UPDATE는 행 수를
바꾸지 않으므로 기본 행 수 모드로는 볼 수 없습니다.

그래서 CDC 마이그레이션(**Full Load + CDC** 또는 **CDC only**)에서는 도구가 dead letter의 **테이블별
건수**(최신 시각, 드리프트 종류 포함)를 유지하고, Validation과 Cut over가 이를 사용합니다:

- **Validation은 그 테이블들을 자동으로 CHECKSUM으로 비교합니다.** 어떤 모드를 고르든, 실행은 값 비교 없이
  일치한 dead-letter 테이블을 모두 다시 비교합니다(실행 전에 Options가 어느 테이블인지 보여 줍니다).
  나중에 dead letter가 도착하면 리포트는 *"Tables match — cut over waits for a CHECKSUM re-check of N
  table(s)"* 로 표시하고 **Re-check N table(s) with CHECKSUM**을 제공합니다. dead-letter 이력을 확인할
  수 없으면(아래) 리포트는 대신 *"Tables match — cut over waits for one CHECKSUM run of every table"* 로
  표시하고, 실행 전에 Options도 이를 알려 줍니다. Fast sweep을 끈 CHECKSUM 모드 실행 한 번이면 해소됩니다.
  REPLICA IDENTITY가 FULL 미만으로 내려간 테이블([§5.6](#56-재키잉된-postgresql-테이블과-컷오버))과 CDC 실행
  중 소스 스키마가 바뀐 테이블([§5.7](#57-소스-스키마-변경과-컷오버))도 같은 방식으로 CHECKSUM으로 비교합니다.
- **Cut over는 다음이 모두 충족될 때만 열립니다:**
  1. 각 dead-letter 테이블이 최신 dead-letter 레코드보다 **1분 넘게 뒤에 시작한** CHECKSUM 비교에서
     일치했다;
  2. 도구가 **최신 Validation이 끝난 뒤 시작한**, CloudWatch Logs의 CDC dead-letter 로그에 대한
     **성공적이고 완전한 읽기**를 수행했다;
  3. 그 읽기의 이력이 **Full Load 워터마크**(CDC only이면 CDC 시작)까지 거슬러 올라간다.
- **보류된 변경은 아직 dead letter가 아닙니다.** 싱크가 고유 인덱스 때문에 보류한 변경
  ([§4.4](04-cdc-and-dsql-constraints.md#44-싱크가-데이터-경로에서-dsql-제약을-처리하는-방식))은 대기가 끝날
  때에야 dead-letter 처리되므로, 그 사이의 읽기는 그것을 셀 수 없습니다: 최종 검증 전에 소스의 마지막 쓰기 후
  최소 5분을 기다리세요([§10.3](10-conclusion.md#103-cut-over-애플리케이션을-dsql로-전환) 3단계). 그 테이블의
  Stream lag이 대기를 보여 줍니다.
- **재시작.** 테이블별 건수, 최신 시각, 드리프트 종류는 앱 재시작 후에도 남지만, Data Migration 단계의
  레코드 목록은 남지 않습니다. 재시작 후 Cut over는 로그를 자동으로 다시 읽습니다 — Validation 또는
  Cut over 단계가 그려질 때마다 범위가 제한된 읽기 한 번, 최대 1분에 한 번입니다.

**fail closed**로 동작합니다. 다음 경우 Cut over는 런북 대신 빨간 알림을 표시합니다:

- 로그를 읽을 수 없음 — 대개 `logs:FilterLogEvents` 권한 누락(배포된 태스크 역할은 이름이
  `dsql-cdc-*` 또는 `mysql-dsql-cdc-*`인 CDC 스택을 포함; 로컬 AWS 프로파일에도 같은 권한 필요);
- CDC 인프라(그와 함께 로그 그룹)가 삭제됨;
- 읽기가 페이지 한도에서 멈춤.

이력을 확인할 수 없는 경우 — 이미 실행 중일 때 연결(attach)한 파이프라인, 또는 이전 버전 도구가 저장한
세션 — 에는 대신 노란색(주의) 알림을 표시합니다. 모든 테이블에 대한 전체 CHECKSUM 실행 한 번이면 저절로
해소됩니다.

**Check again**은 멈춘 지점부터 읽기를 이어 갑니다. 한 번에 다 읽을 수 없는 큰 로그 그룹도 누를 때마다 더
읽습니다. **Continue without this check**는 여러분의 결정을 활동 로그에 기록하며, 재확인할 테이블이 남아
있지 않을 때만 제공됩니다. 그때까지 본 레코드로 범위가 한정되어 더 새로운 레코드가 오면 다시 열립니다 —
단, 로그를 읽을 수 없는 동안은 예외입니다. 그때는 도구가 새 레코드를 볼 수 없으므로, 애플리케이션을 전환하기
전에 다른 방법으로 스트림을 확인하세요. 새 Validation 실행이 이를 지우며, Validation 불일치를 덮어쓰는 일은
없습니다. CHECKSUM이 끝나지 않는 dead-letter 테이블(복합 기본 키이거나 기본 키가 없는 매우 큰 테이블, 5.1
참고)은 재확인으로 해소할 수 없는 유일한 경우입니다. **Objects to validate**에서 끄고 다시 실행한 뒤 Cut
over에서 승인하세요.

CHECKSUM은 값 비교할 수 없는 컬럼을 명시합니다(5.1). dead-letter 테이블에 대해서는 Cut over가 이를
차단하지 않는 경고로 다시 보여 줍니다.

일치하지 않는 테이블의 **해결 순서**:

- **Full Load + CDC:** 소스 스키마가 바뀌었으면 Step 1(Evaluation) 재실행 → Stop CDC → **Drop &
  reload**로 테이블 리로드 → Start CDC → 재확인.
- **CDC only:** Aurora DSQL에서 행을 직접 수정(dead-letter queue 카드에 각 레코드의 기본 키와 Kafka
  오프셋이 나오며, 키 값은 정수나 UUID일 때만 나옴) → 재확인. 카드가 키 값을 숨기는 테이블에는
  **Start over**를 사용하세요. 소스 `TRUNCATE`의 dead letter는 어떤 행도 가리키지 않습니다: Stop CDC → 소스에
  더 이상 없는 행을 트랜잭션당 최대 3,000행씩 삭제 → Start CDC → 재확인
  ([§4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)).

**Full Load only**는 영향이 없습니다. 단, 같은 세션에서 앞서 CDC를 실행했다면 그때 기록된 dead letter는
여전히 CHECKSUM 일치가 필요합니다.

---

## 5.6 재키잉된 PostgreSQL 테이블과 컷오버

이 절은 **PostgreSQL** 소스에만, 그리고 CDC가 **재키잉**하는 테이블 — 소스 기본 키 밖의 컬럼을 더한 복합
타깃 키(Schema Conversion의 Composite key, 또는 편집한 키) — 에만 해당합니다. 그런 테이블의 `DELETE`는 소스의
`REPLICA IDENTITY`가 테이블과 모든 파티션에서 **FULL**인 동안에만 그 컬럼을 싣습니다. CDC 실행 중 FULL
미만으로 내려가면(`ALTER TABLE … REPLICA IDENTITY DEFAULT`, 새 파티션, `USING INDEX`) delete가 오류도 dead
letter도 없이 Aurora DSQL에 행을 남길 수 있습니다(언제 그런지는
[§4.6](04-cdc-and-dsql-constraints.md#재키잉된-postgresql-테이블-replica-identity-재확인)에 있습니다). CDC
모니터가 이를 다시 읽고 내려갈 때마다 기록하며
([§4.6](04-cdc-and-dsql-constraints.md#재키잉된-postgresql-테이블-replica-identity-재확인)), Validation과
Cut over가 그 기록을 사용합니다:

- **Validation은 그 테이블들을 자동으로 CHECKSUM으로 비교하며**, 실행 전에 Options에 이름을 보여 줍니다.
  테이블이 아직 FULL 미만이면 리포트는 *"Tables match — cut over is held: REPLICA IDENTITY on N re-keyed
  table(s) is below FULL"* 로 표시합니다: 변경이 계속 유실될 수 있으므로 어떤 CHECKSUM도 이를 풀지 못합니다.
  다시 FULL이 되면 수정 전에 시작한 일치로는 부족합니다: 리포트는 *"cut over waits for a CHECKSUM
  re-check"* 로 표시하고 **Re-check N table(s) with CHECKSUM**을 제공합니다.
- **Cut over는** 그런 테이블이 모두 다시 FULL이고 **그리고** 도구가 FULL을 읽은 시각보다 **1분 넘게 뒤에
  시작한** CHECKSUM 비교에서 일치했을 때**만 열립니다**. 또한 Cut over는 열리기 전에 최신 Validation 이후
  REPLICA IDENTITY를 **한 번 새로 읽어**(몇 초) 그 사이의 변화를 잡아냅니다.
- 리포트에서 빠진 테이블, 또는 수락한 갭 안의 테이블은 FULL 미만이거나 재확인이 필요한 다른 재키잉
  테이블이 없을 때 **Continue without this check**로 승인할 수 있습니다(활동 로그에 기록되며, identity가
  다시 내려가면 다시 열림). 수락한 갭 안의 테이블은 소스 행을 바로잡고 **Drop & reload**로 리로드하기
  전에는 CHECKSUM으로 해소할 수 없습니다. 아직 FULL 미만인 테이블은 결코 면제할 수 없습니다.
- **소스를 읽을 수 없고** — 앱 재시작 후 비밀번호가 없음, 읽기 실패, 테이블을 찾을 수 없음 — 아직 FULL
  미만으로 기록된 테이블이 없으면 Cut over는 보류하지 않습니다: 런북 위에 경고를 표시하고, 활동 로그의 승인
  줄에 identity를 재확인하지 못했다고 기록합니다. 애플리케이션을 전환하기 전에 소스에서 이 테이블들과
  파티션이 FULL인지 확인하거나, Connect 단계에서 소스를 다시 연결하세요(도구가 스스로 다시 읽으며, 읽기
  실패 후에는 **Check again**을 누르세요). 아직 FULL 미만으로 기록된 테이블이 있으면 도구가 FULL을 읽을
  때까지 Cut over는 보류되며, 보류 알림이 다시 읽기 실패를 알려 줍니다.
- **재시작.** 내려간 기록은 앱 재시작 후에도 남고 Start over로만 지워집니다. **MySQL** 소스와 **Full load
  only**는 영향이 없습니다.

수정 후에도 다른 테이블의 **해결 순서**: Full Load + CDC — CDC가 따라잡게 둔 뒤 Stop CDC, **Drop & reload**로
리로드, Start CDC, 재확인; CDC only — Aurora DSQL에서 잉여 행을 직접 삭제한 뒤 재확인.

---

## 5.7 소스 스키마 변경과 컷오버

이 절은 CDC 마이그레이션(**Full Load + CDC** 또는 **CDC only**)에 MySQL·PostgreSQL 모두 해당합니다. 어떤 소스
DDL은 거부되는 행 하나 없이 CDC가 쓰는 값을 바꿉니다 — nullable 컬럼 삭제, 타입 확장, 테이블 이름 변경 등
([§4.2](04-cdc-and-dsql-constraints.md#데드레터를-남기지-않는-변경-소스-스키마-재확인)). CDC 모니터가 소스에서
캡처 중인 테이블의 정의를 다시 읽고, 정의가 더 이상 Step 1(Evaluation) 인벤토리와 일치하지 않는 테이블을
기록하며([§4.6](04-cdc-and-dsql-constraints.md#소스-스키마-재확인)), Validation과 Cut over가 그 기록을
사용합니다:

- **Validation은 그 테이블들을 자동으로 CHECKSUM으로 비교하며**, 실행 전에 Options에 이름을 보여 줍니다.
  테이블의 소스 정의가 아직 Step 1 인벤토리와 다르면, 리포트가 무엇을 보여 주든 Cut over는 보류됩니다. 컬럼
  추가·삭제나 테이블 없음은 대개 테이블을 아예 비교할 수 없다는 뜻입니다(*"N table(s) could not be compared"*;
  Step 1을 다시 실행하세요). 그 밖의 경우 리포트는 *"Tables match — cut over is held: the source schema
  changed on N table(s)"* 로 표시합니다. 어떤 CHECKSUM도 이를 풀지 못합니다: 비교가 각 컬럼을 Step 1 인벤토리
  기준으로 렌더링하므로 잘못 일치할 수 있기 때문입니다(`INT`로 넓어진 `TINYINT(1)`은 여전히 true/false로
  비교됨).
- **두 단계로 해소됩니다.** 먼저 소스가 다시 Step 1 인벤토리와 일치해야 합니다: Step 1(Evaluation)을 다시
  실행해 새 정의를 담거나(추가되거나 이름이 바뀐 컬럼은 먼저 **Fix target schema…**로 Aurora DSQL에서 추가하거나
  이름을 바꾸세요: Validation은 Step 1 인벤토리에 없는 컬럼을 비교할 수 없습니다), 소스의 변경을 되돌리세요 — 도구가 스스로 다시
  읽으며, **Check again**을 눌러도 됩니다. 그런 다음 그 시각보다 **1분 넘게
  뒤에 시작한** CHECKSUM 비교에서 테이블이 일치해야 합니다. 리포트는 **Re-check N table(s) with CHECKSUM**을
  제공합니다. 캡처 중인 테이블의 정의를 바꾸는 Step 1 재실행도 같이 다룹니다: 그 전에 CDC가 적용한 값이 다를
  수 있으므로, 그 테이블도 재실행 후에 시작한 CHECKSUM이 필요합니다.
- **Cut over는** 열리기 전에 최신 Validation 또는 Step 1 실행 이후 소스 정의를 **한 번 새로 읽어**(몇 초) 그
  사이의 변경을 잡아냅니다.
- 리포트에서 빠진 테이블, Step 1 인벤토리에 더 이상 없는 테이블(소스에서 삭제하거나 이름을 바꾼 뒤 Step 1을
  다시 실행), 또는 수락한 갭 안의 테이블은, 열려 있거나 재확인이 필요한 다른 테이블이 없을 때 **Continue
  without this check**로 승인할 수 있습니다(활동 로그에 기록되며, 테이블이 다시 바뀌면 다시 열림). 소스가
  아직 다른 테이블은 결코 면제할 수 없습니다.
- **소스를 확인할 수 없고** — 앱 재시작 후 비밀번호가 없음, Step 1 인벤토리가 없음(Step 1이 실행 중이거나
  마지막 실행이 실패), 읽기 실패 — 바뀐 것으로 기록된 테이블이 없으면 Cut over는 보류하지 않습니다: 런북 위에
  경고를 표시하고, 활동 로그의 승인 줄에 소스 스키마를 재확인하지 못했다고 기록합니다. 애플리케이션을
  전환하기 전에 CDC 실행 중 마이그레이션 대상 테이블에 DDL이 실행되지 않았는지 확인하거나, Connect 단계에서
  소스를 다시 연결하세요(읽기 실패 후에는 **Check again**). 아직 바뀐 것으로 기록된 테이블이 있으면 Cut over는
  보류됩니다.
- **재시작.** 각 변경의 기록은 앱 재시작 후에도 남고 Start over로만 지워집니다. **Full load only**는 영향이
  없습니다. 단, 같은 세션에서 앞서 CDC를 실행했다면 그때 기록된 변경은 여전히 적용됩니다.

그 CHECKSUM에서 다른 테이블의 **해결 순서**: Full Load + CDC — CDC가 따라잡게 둔 뒤 Stop CDC, **Drop &
reload**로 리로드, Start CDC, 재확인; CDC only — Aurora DSQL에서 행을 직접 바로잡은 뒤 재확인.

---

**다음:** [6. 한계 →](06-limitations.md)
