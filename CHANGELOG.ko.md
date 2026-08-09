# 변경 이력 (Changelog)

_언어: [English](CHANGELOG.md) | **한국어** | [日本語](CHANGELOG.ja.md)_

이 프로젝트의 주요 변경 사항을 기록합니다. [유의적 버전(semver)](https://semver.org/)을
따르며, 버그 수정은 패치 릴리스로 올립니다.

## v0.1.290

### 변경

- **CDC 인프라 배포에서 잘못된 VpcId를 넣었을 때, 리전으로 오도하지 않고 VPC ID를 가리키는 메시지를 줍니다.**
  VpcId는 툴이 추론할 수 없는 유일한 값이라 오타가 흔한 실수이며, 이미 프리플라이트 네트워크 진단(VpcId에서
  subnet을 해석)이 과금되는 CloudFormation 생성 전에 일찍 잡아냅니다. 그러나 존재하지 않는 VPC와 실재하지만
  subnet이 없는 VPC가 똑같이 "No subnets found in VPC 'X'. Ensure the VPC is in the same region…" 문구를 냈고,
  이는 VPC가 존재한다고 전제해 단순히 id를 잘못 친 사용자를 엉뚱한 방향으로 보냈습니다. 이제
  `diagnose_cdc_network`이 둘을 구분합니다: VpcId가 subnet을 하나도 못 찾으면 VPC 존재 여부를
  확인(`describe_vpcs`)해 "VPC 'X' was not found in this account and region — check the VPC ID (a typo is the
  usual cause)…" 또는 "VPC 'X' exists but has no subnets. Add subnets (in >=2 AZs…)" 중 하나를 반환합니다.
  진짜로 불확실한 조회(`describe_vpcs`의 권한 오류/스로틀)는 "존재함"으로 처리해, 무관한 API 실패가 "VPC 없음"
  으로 위장하지 않게 합니다 — 그 경우 real-VPC 메시지로 흘러가고 차단형 submit 경로가 API 오류 자체를 표시합니다.
  (프리플라이트 게이트, 배포 중 실패 표면화, `ROLLBACK_COMPLETE` delete-후-재배포 복구는 이미 갖춰져 있었고,
  이번엔 가장 흔한 잘못된-VpcId 메시지만 정교화합니다.)

### 변경

- **CDC 인프라 배포 예상 시간을 실측에 맞춰 "~15-20분"에서 "~10-15분"으로 정정했습니다.** 반복된 실제 배포에서
  전체 `infra` 생성(MSK Serverless 프로비저닝이 지배적)이 ~10분에 끝났는데, 배포 확인 다이얼로그·prerequisite/
  오버랩 안내·프로비저닝 배너·재배포 프롬프트가 모두 ~15-20분으로 표기해 실제보다 대기가 길게 읽혔습니다. 이제
  운영자 향 문구를 모두 ~10-15분으로 바꾸고, 스테이지별 진행 ETA 모델
  (`_CDC_STAGE_ETA_SECONDS["infra"]["stack_create"]`)을 18분에서 9분으로 낮춰 스테이지 힌트와 총 ETA가
  다이얼로그와 일치하게 했습니다. 여전히 대략치(AWS 프로비저닝은 계정·리전·시간대에 따라 변동)라, 느린 실행은
  힌트를 초과할 뿐 잘못 표시되지 않습니다. 인프라 삭제(teardown) 예상치(~15-25분, 자체 실측 근거가 있는 별개
  경로)는 변경하지 않았습니다.

### 수정

- **Full Load이 이미 특정 제외 셋으로 데이터를 적재한 뒤에는 oversized-LOB 제외를 더 이상 편집할 수 없습니다
  — `full_load_only` → `cdc_only` 경로의 조용한 split-brain을 차단합니다.** 이 제외는 Full Load과 CDC가
  공유하는 마이그레이션 전역 단일 선택입니다. Full Load 화면에서는 적재 후 올바르게 잠깁니다
  (`selection_lock_reason`의 `has_job or status is DONE`). 그러나 `full_load_only` 마이그레이션이 어떤 컬럼을
  제외한 채 완료되고(그 컬럼은 `NULL`로 적재됨) 운영자가 복제를 추가하려고 migration type을 `cdc_only`로
  전환하면 — full-load-only 완료 후 툴이 직접 권하는 경로 — 카드가 CDC 단계의 자리로 옮겨가는데, 그쪽 잠금
  (`lob_exclusion_lock`)은 CDC 스트리밍/인프라 배포 상태만 확인하고 "Full Load이 이미 이 셋으로 커밋함"
  조건이 **없었습니다**. 그래서 배포 전 창에서 운영자가 그 컬럼을 **제외 해제**할 수 있었고, 그러면 CDC가
  스냅샷 이후 변경 행에 대해 그 컬럼을 채우는 동안 이미 적재된 행은 `NULL`로 남거나(조용한 부분 데이터),
  반대로 적재된 컬럼에 **제외를 추가**하면 CDC가 그 갱신을 버려 타깃이 stale해졌습니다. 이제
  `lob_exclusion_lock`이 `full_load_committed` 신호(`FULL_LOAD` 스텝이 `DONE`이거나 job 존재 — 둘 다 type
  전환에도 유지)를 받아, 적재가 커밋됐으면 **양방향** 모두 카드를 잠그고 유일한 올바른 재스코프로 *Start over*를
  안내합니다(CDC 스택 삭제로는 해제되지 않음 — 적재가 실제로 이 셋으로 실행됨). 아무것도 적재하지 않은 순수
  `cdc_only` 실행은 영향 없고(배포 전까지 편집 가능), 단일 `full_load_and_cdc` 실행도 원래 영향 없었습니다(카드가
  Full Load 화면에 남아 잠김). 커밋된-적재 잠금에 대한 회귀 테스트를 추가했습니다.

### 변경

- **배포 템플릿의 기본 `ContainerImageUri`를 현재 릴리스로 올립니다
  (`public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.287`).** 기본값이 게시된 이미지보다 여러 릴리스 뒤처져
  있어, override 없이 새로 `aws cloudformation deploy`하면 오래된 앱을 pull했습니다. 이제 현재 게시된 ECR
  Public 태그를 가리킵니다.

## v0.1.286

### 추가

- **활동 로그가 이제 Full Load 데이터 경로뿐 아니라 모든 단계에 걸쳐 여정의 핵심 결정과 판정을 기록합니다.**
  다운로드한 `migration_activity.log`가 무엇을 담는지 감사한 결과, 마이그레이션을 *증명하고 종결하는* 단계가
  대부분 빠져 있었습니다 — 로그는 적재는 기록했지만 결과가 검증됐는지, 운영자가 사인오프했는지는 남기지
  않았습니다. 네 가지 갭을 메웁니다:
  - **Validation 판정(4단계).** 검증 실행이 이제 `[validation] validation started` 이벤트(모드 + 테이블 수)와
    `[validation] validation completed` 판정 — `MATCH`/`MISMATCH`, 모드(ROW_COUNT vs CHECKSUM), 일치/불일치
    테이블 수(reconcile 시 errored/missing/extra 포함), 실패 테이블, cut-over 준비 여부 — 을 기록합니다. 깨끗한
    일치는 `SUCCESS`, 불일치는 `FAILURE`로 기록해 no-go가 분명히 드러납니다. 이전엔 identity-sync만 로깅되고
    판정 자체는 UI에만 있었습니다.
  - **Cut-over 확인(5단계).** "I've cut over" 클릭이 이제 운영자가 사인오프한 release 상태를 지목하는
    `[validation] cut over acknowledged` 이벤트를 기록합니다 — 깨끗한 일치, 또는 명시적으로 ACCEPT한 갭(영구
    드롭된 행을 알고서 없이 마이그레이션). 마이그레이션의 결론이 이전엔 기록되지 않았습니다.
  - **Schema apply 실행 요약(2단계).** 기존 객체별 라인에 더해, 이제 `[schema_conversion] schema apply started`와
    `schema apply completed`에 롤업 — "N of M object(s) applied (C created, S skipped), F failed" — 을 기록합니다
    (Full Load의 run started/completed 브래킷과 동일).
  - **Assessment 시작 + migration type.** Evaluation이 이제 `STARTED` "run assessment" 이벤트를 기록하고(이전엔
    성공/실패만), migration-type 선택(Full Load only / CDC only / both)이 선택 시점에 `[full_load] migration type
    selected`로 기록됩니다 — 실제 변경 시에만이라 refresh로 재기록되지 않습니다.

  모든 이벤트는 값이 없습니다(카운트·모드·테이블 이름만, 행 값은 없음 — Property 7).

### 추가

- **Full Load 워터마크(무손실 CDC 핸드오프의 정합성 지점)가 이제 활동 로그에 기록됩니다.** 워터마크는
  스냅샷이 반영하는 정확한 소스 위치 — 이후 CDC 캐치업이 재개하는 좌표 — 를 고정하지만, 그동안 메모리상
  job 레코드에만 저장돼서 다운로드한 `migration_activity.log`(변경 티켓에 첨부하는 산출물)에는 마이그레이션이
  어느 소스 시점을 캡처했는지 기록이 없었습니다. 이제 Full Load가 스냅샷 직후 `[full_load] watermark
  captured` 이벤트를 `INFO`로 기록합니다: GTID가 있으면 GTID(없으면 `binlog_file:position`, 그것도
  없으면 바이너리 로깅 off/제한 시 "no coordinate available"), 스냅샷 UTC 타임스탬프, 카운트된 테이블 수,
  그리고 그 카운트가 근사치 `information_schema` 추정인지 여부. 재실행도 재개한 원본 워터마크를 기록하므로
  재실행에 걸쳐 감사 추적이 완전합니다. 이벤트는 로그 위치와 타임스탬프만 담습니다 — 행 값은 없음(Property 7).

### 추가

- **제외된 대형 LOB 컬럼이 이제 활동 로그에 기록됩니다 — 의도적 데이터 누락은 마이그레이션 감사 추적에
  남아야 합니다.** "Oversized LOB columns" 카드에서 컬럼을 체크하면 그 컬럼 데이터가 Full Load에서
  빠지지만(타깃에는 `NULL`로 도착), 활동 로그에는 아무것도 기록되지 않아 다운로드한
  `migration_activity.log`만 봐서는 어떤 컬럼이 의도적으로 제외됐는지 알 수 없었습니다 — 로그가 변경
  티켓에 첨부하는 산출물인 만큼 거버넌스 공백입니다. 행 단위 quarantine은 이미 로깅되고 있었고, 컬럼
  단위 제외도 이제 로깅됩니다. Full Load 실행(및 재실행은 재실행한 테이블로 범위 한정)이 이제 제외된
  컬럼마다 `[full_load] column excluded` 이벤트를 `INFO`로(예상된, 사용자가 선택한 누락 — 결함 아님)
  기록하며, `table.column`을 지목하고 그 컬럼이 타깃에서 `NULL`로 남음을 명시합니다. 해당 테이블의
  `load table` 라인에도 반영됩니다 — 예: `15 rows newly loaded (1 column excluded: content)`. 이벤트는
  컬럼 이름만 담습니다(행 값 없음 — Property 7).

### 변경

- **DSQL 타깃측 Full Load 실패도 이제 원시 드라이버 메시지만이 아니라 "다음에 무엇을 할지"를 설명합니다(감사 발견 U6).**
  테이블별 실패 경로는 소스측 힌트(연결 끊김, too-many-connections)만 덧붙여, *타깃* 에러 — 낙관적 동시성 재시도
  예산 소진(`40001`), 테이블당 구조적 한계(`54000`, 예: 인덱스 24개 초과), 제약/데이터 거부(`23xxx`/`22xxx`) —
  는 에러 로그·활동 로그·인라인 테이블 메시지에 그대로 원시 드라이버 문구로 노출됐습니다. 신규 `target_error_hint`가
  SQLSTATE를 기준으로(없으면 이미 값이 제거된 메시지 텍스트로 폴백) 소스 경로와 동일한 "무슨 일이 있었고 / 어떻게
  대응할지" 안내를 붙입니다 — 예: "OCC 재시도 소진 → 병렬도/배치 크기를 낮춰 재실행; 로드는 멱등적". 어떤 행 값도
  노출하지 않습니다(Property 7).
- **`KEEP_INTEGER` 기본키 권고가 이제 cut-over 후 DSQL이 키를 자동 생성하지 않음을 경고합니다(감사 발견 U5).**
  `AUTO_INCREMENT` 컬럼의 정수 키를 유지하면 깨끗하게 변환되지만, Aurora DSQL은 그 위에 identity/default를 두지
  않습니다 — 따라서 DB가 키를 생성해 주리라 믿던 애플리케이션은 cut-over 후 insert에서 실패하거나 충돌합니다. 이제
  Schema Conversion 메시지가 이를 분명히 알리고, DSQL이 키를 채우길 원하는 경우 "Server-generated (IDENTITY)"
  전략을 안내합니다(기존엔 처리량 노트만 제공).

## v0.1.282

### 수정

- **소스 `CHECK` 제약이 더 이상 조용히 드롭되지 않고 Evaluation에서 표면화됩니다(감사 발견).** introspector가
  CHECK 제약을 반영하지 않아, DSQL 관련 특징이 CHECK뿐인 테이블이 `AUTO`/"호환성 문제 없음"으로 분류되면서
  제약이 타깃에서 사라졌습니다. 이제 CHECK 제약을 반영하고(`TableDef.check_constraints`, SQLAlchemy
  `get_check_constraints` 경유), 신규 `CHECK_CONSTRAINT_DROPPED` 평가 룰이 해당 테이블을 `MANUAL`로
  플래그하며 제약 이름을 지목하고 타깃에 수동 재생성(표현식이 DSQL 호환일 때)이나 애플리케이션 계층 시행을
  권고합니다. 변환기는 임의의 CHECK 표현식을 자동 변환하지 않습니다(MySQL 표현식이 DSQL과 다른 함수/연산자를
  쓸 수 있어 무조건 복사는 잘못된 DDL 위험) — 목표는 완전성: 소스 제약을 표면화 없이 드롭하지 않는 것(Property 8).

## v0.1.281

### 수정

- **스키마 변환 정확성 수정(감사 C11, U1, U2, U3).**
  - *C11 — `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` 타임존.* MySQL `TIMESTAMP`는 DSQL `timestamptz`로
    매핑되는데, `CURRENT_TIMESTAMP` 기본값이 `now() AT TIME ZONE 'UTC'`(naive)로 감싸져 timestamptz
    컬럼에서 세션 TimeZone으로 재해석되어 defaulted insert가 offset만큼 이동했습니다. naive-UTC 래퍼는
    이제 DATETIME → plain `timestamp` 타깃에만 적용; timestamptz는 plain instant 유지.
  - *U1 — `TINYINT(1) UNSIGNED` 오표기.* Evaluation이 "boolean 매핑"으로 표시(발생하지 않는 0/1 로드
    실패 경고)했으나 변환기는 `smallint`로 매핑. assessor가 이제 unsigned 형을 제외해 변환기와 일치.
  - *U2 — `TIME` 범위.* MySQL `TIME`은 duration(−838:59:59..838:59:59), DSQL `time`은 time-of-day.
    범위 초과 값이 사전 신호 없이 로드 중 행 단위로 실패했습니다. Schema Conversion이 이제 사전 경고
    (duration 저장 시 interval/text로 remap), ENUM/BIT/YEAR 패턴과 일치.
  - *U3 — COMPOSITE_KEY에서 24-index cap off-by-one.* 인덱스 한계 경고가 COMPOSITE_KEY가 추가하는
    UNIQUE 인덱스를 무시해, 소스 인덱스 23개를 composite key로 변환하면 25개(> 24)인데도 사전 체크를
    통과하고 로드 후 추가 CREATE INDEX ASYNC가 실패했습니다. 이제 변환-추가 인덱스를 계산에 포함.

## v0.1.280

### 수정

- **CDC offset-seeder가 binlog 파일명 롤오버에서 전진한 커넥터를 되감지 않습니다(감사 C9).** 이미
  워터마크 이상으로 전진한 커넥터의 재-seed를 건너뛰는 no-clobber 가드가 binlog 파일명을 사전식으로
  비교했습니다. MySQL binlog suffix는 zero-pad이지만 롤오버 시 자릿수가 늘어(`mysql-bin.999999` →
  `mysql-bin.1000000`), 여기서 문자열 비교가 뒤집혀(`'1000000' < '999999'`) 전진한 커넥터를 뒤처진
  것으로 오판해 재배포 시 되감아 이미 스트리밍한 변경을 재생했습니다. 이제 파싱된 숫자 binlog 시퀀스로
  비교합니다(파싱 불가 suffix만 사전식 fallback). offset-seeder Lambda zip을 재빌드하고 `PLUGIN_VERSION`
  을 `v23`으로 bump했습니다(`PLUGIN_VERSION` bump은 CDC 인프라 Delete + 재배포가 있어야 반영됨).

### 비고

- 감사 C10(composite-key re-keying이 leading 컬럼 변경 시 행 중복 가능)은 코드 변경 불필요: composite-key
  전략이 이미 원본 PK에 UNIQUE 인덱스를 발행하므로, 변경 UPDATE는 조용한 중복이 아니라 unique-violation /
  DLQ 이벤트로 표면화되고, immutability 요건은 opt-in 시점에 이미 경고됨.

## v0.1.279

### 수정

- **Full Load 배치 byte-cap이 크기 편중 배치에서도 지켜집니다(감사 C7/P1).** 단일 쓰기 트랜잭션이 DSQL의
  10 MiB 한계를 넘지 않도록 배치를 분할합니다. 기존 분할기는 각 배치의 **첫 행만** 샘플링해, 그 외삽값이
  예산 이하이면 나머지 행의 per-row 바이트 체크를 건너뛰었습니다 — 그래서 첫 행이 작고(빈/NULL 텍스트)
  이후 행이 큰(BLOB/JSON) 배치가 cap을 크게 초과했습니다. 하류의 재귀 split로 복구되긴 했으나 광고된 cap이
  조용히 미집행됐습니다. 이제 분할기는 실제 러닝 바이트 합계를 유지(모든 행을 한 번씩 추정해 누적)하고
  다음 행이 예산을 넘길 시점에 즉시 flush합니다. 단일 초과 행은 여전히 자체 배치를 이룹니다. 메모리는
  배치 하나 단위로 bounded 유지.

## v0.1.278

### 수정

- **검증 건전성: "match"인데 실제로는 다른 데이터를 통과시킬 수 있던 여러 경로를 닫거나 명시(감사 C1~C6, U4).**
  - *checksum 토큰 충돌(C4).* 각 컬럼 값을 `|` 구분자로 join하기 전에 이스케이프(`~`→`~~`, `|`→`~|`)하여,
    `|`를 포함한 값이 컬럼 경계를 넘어 구분자를 밀어내지 못하게 했습니다(`CONCAT_WS('|','a|','b')`와
    `CONCAT_WS('|','a','|b')`가 둘 다 `a||b`가 되던 문제). NULL 센티넬은 이제 위조 불가능한 `~N`
    (이스케이프된 실제 값이 절대 만들 수 없음)로, 기존 literal-`<NULL>` vs SQL NULL 충돌을 닫았습니다.
    이스케이프는 양 엔진에서 바이트 동일하고 backslash-free입니다(예전에 backslash 방식이 MySQL/PG 간
    갈렸음).
  - *DATETIME 타임존(C3).* plain `timestamp`(DATETIME) checksum 항을 이제 `AT TIME ZONE 'UTC'` 없이
    직접 렌더합니다 — `timestamp without time zone`에서 그건 no-op가 아니라(세션 TimeZone을 거쳐 변환되어
    wall-clock을 이동) 시프트를 유발했습니다. `timestamptz`는 계속 사용합니다. DSQL 연결에 `TimeZone=UTC`도
    고정해 비교가 세션 기본값에 의존하지 않게 했습니다.
  - *정직한 라벨/명시(C5, C1/C2, C6, U4).* readiness 체크가 ROW_COUNT 모드에선 "Row counts match"
    (값 미비교), CHECKSUM 모드에서만 "Data identical"로 표기되며 후자는 FLOAT/DOUBLE·JSON이 값 비교되지
    않음을 명시합니다. PK reconcile 체크는 "No missing or extra records"로 개명(값 동등이 아니라 키 집합
    검증), composite/비정수 PK footnote는 ROW_COUNT 모드에서 "row count only"로 표기(기존 "count/checksum"
    과대 표현 교정).

## v0.1.277

### 수정

- **`FLOAT UNSIGNED` / `FLOAT(M,D) UNSIGNED` 컬럼이 더 이상 테이블 전체 변환을 중단시키지 않습니다.**
  sqlglot의 MySQL dialect가 `float unsigned`를 독립 타입으로 파싱하지 못해, 이를 포함한 테이블이
  "auto-convert 불가" 플레이스홀더(CREATE TABLE 없음)로 떨어졌고 — 그런데 Evaluation은 여전히
  AUTO/호환으로 평가해, 운영자가 진단할 수 없는 모순이었습니다(감사 B1). 근사 수치형의 unsigned는
  표현 불가하고 저장 의미도 없어, 이제 제거하고 `real`로 매핑합니다 — `DOUBLE UNSIGNED`와 동일한 처리.
  정수 `UNSIGNED`(범위 확장)와 `DECIMAL UNSIGNED`는 건드리지 않습니다.
- **MySQL prefix 인덱스(`KEY (col(N))`)가 조용히 full-column 인덱스가 되어 로드 후 실패하는 대신 표면화됩니다.**
  Aurora DSQL엔 prefix 인덱스 등가물이 없어 변환기가 컬럼 전체를 인덱싱하는데, 값이 DSQL의 ~255바이트
  인덱스 키 한계를 넘는 가변 길이 컬럼은 테이블과 데이터가 이미 로드된 **후에** `CREATE INDEX ASYNC`가
  실패하며 사전 신호가 없었습니다(감사 B2). 이제 소스에서 prefix 길이를 반영하고
  (`IndexDef.prefix_lengths`), Schema Conversion이 인덱스와 컬럼을 지목하는 경고를 표시해, 운영자가 값이
  맞는지 확인하거나 — 경계 있는 substring 표현식 인덱스로 교체 — 할 수 있게 합니다. 인덱스 DDL은 여전히
  발행됩니다(경고는 권고).

## v0.1.276

### 수정

- **Full Load가 torn read를 재조정할 수단이 없는 테이블을 더 이상 reader 샤딩하지 않습니다 — 프로덕션
  경로의 cross-shard torn-read 데이터 손실 창을 닫음.** reader 샤딩은 큰 테이블의 읽기를 K개의 disjoint
  PK 범위로 나눠 동시 스트리밍하는데, 각 샤드가 자신만의 독립 타이밍 `START TRANSACTION WITH CONSISTENT
  SNAPSHOT`을 엽니다. 로드 중 소스에 쓰기가 일어나면 다중 행 소스 트랜잭션이 샤드 간에 찢어질 수 있습니다
  (한 행은 샤드 A의 스냅샷에, 그 형제 행은 아직 샤드 B에 없음). 이는 CDC 스트림이 스냅샷 이후 쓰기를
  재조정할 때만 안전합니다. 깨끗한 **replace**(plain INSERT, CDC 없음)나 **비-CDC append**는 재조정할
  것이 없으므로 단일 reader로 읽어야 합니다(스냅샷 하나 = 한 시점 컷). 단일프로세스 경로는 replace만
  가드했고 비-CDC append 샤딩은 여전히 허용했으며, 더 심각하게 **멀티프로세스 경로(테이블 병렬 > 1일 때의
  프로덕션 기본)**는 순전히 "단일 정수 PK 있음"만으로 샤딩을 결정해 replace/CDC 상태와
  `full_load_reader_shards` off-switch/상한을 모두 무시하고 replace와 비-CDC append를 그대로 샤딩했습니다
  (감사 D1 + C12). 이제 두 경로 모두 **CDC-coexisting일 때만** 샤딩하고, 멀티프로세스 플래너는 샤드 수를
  워커 풀 예산이 아니라 (소스 연결 상한으로 클램프된) `full_load_reader_shards`에서 도출합니다. 샤딩하지
  않는 로드는 동작 변화 없음.

## v0.1.275

### 수정

- **실패한 identity 시퀀스 동기화가 더 이상 성공으로 표시되지 않습니다.** 컷오버 전에 도구는 각
  `GENERATED BY DEFAULT AS IDENTITY` 시퀀스를 마이그레이션된 행 뒤로 전진시켜(`RESTART WITH max+1`),
  애플리케이션의 첫 insert가 중복 키에 걸리지 않게 합니다. 이 `ALTER`는 DSQL의 낙관적 동시성 하의
  DDL이라 40001/concurrent-DDL을 던질 수 있고 IAM 토큰도 세션 중 만료될 수 있는데, 실패가
  `except: pass`로 삼켜져 "이 테이블엔 서버 생성 키 없음"과 구별 불가능해졌고, 컷오버 런북이 초록색
  *"Done — no server-generated key needed advancing."* 로 칠했습니다. 그러면 운영자가 뒤처진 시퀀스
  위로 앱을 재지정(repoint)해, 동기화가 막으려던 바로 그 중복 키 장애를 맞을 수 있었습니다(감사 D2).
  `sync_identity_sequences`는 이제 테이블별로 구분되는 결과를 반환합니다 — `int`(전진됨) /
  `None`(할 일 없음) / `str`(`RESTART` **실패**, 값이 없는 이유 포함) — 신규 `partition_identity_sync`
  헬퍼가 분류합니다. 모든 호출부(컷오버 버튼, 검증 자동 동기화, Full Load / accept-quarantine 로드 후
  동기화)가 이제 실패를 표면화합니다: 런북이 성공 줄 대신 **error** notice("Identity sequence sync
  failed — do not cut over yet", 재시도 / 수동 `RESTART WITH` 안내)를 표시하고, activity log에
  `FAILURE`로 기록합니다. identity-sync 설정 예외 메시지도 값이 없는 sanitizer를 거칩니다(Property 7,
  v0.1.274와 일관).

## v0.1.274

### 수정

- **Full Load 에러 로깅이 더 이상 실패한 행의 컬럼 값을 디스크에 기록하지 않습니다 (Property 7).**
  드라이버(psycopg) 에러의 `str()`은 서버 `DETAIL:` / `Failing row contains (...)` 줄을 보존하는데,
  이 줄은 문제가 된 행의 컬럼 값(중복 이메일, 토큰 등)을 담고 있습니다. 이 raw 텍스트가 quarantine
  레코드, 테이블별 실패 메시지, 디스크 NDJSON activity log, 그리고 (미러링된) CloudWatch에 그대로
  저장되고 있었습니다. 이제 모든 로드 실패 지점(quarantine 레코드, 배치 결과의 `first_error`, 테이블별
  실패 핸들러)에서 단일 라인 sanitizer(`safe_error_message`)를 거칩니다 — 실행 가능한 주 메시지
  (`duplicate key value violates unique constraint "…"`)와 SQLSTATE는 유지하고, 값이 담긴 `DETAIL`
  줄은 제거합니다. 기존 `" ".join(str(exc).split())` 방식은 도움이 안 됐습니다 — DETAIL 줄을 한 줄로
  접을 뿐 값은 그대로 남았습니다. DEBUG 전용 activity-log 스택트레이스도 같은 방식으로 수정 — 이제
  (값이 없는) 스택 프레임 + `Type: 첫줄` 꼬리만 유지하고 `format_exception`의 값 포함 메시지 줄은 뺍니다.

## v0.1.273

### 변경

- **오버사이즈 LOB 제외가 CDC 인프라 배포 후 잠길 때, 이제 조용히 얼지 않고 잠긴 이유를 설명합니다.**
  이전에는 cdc-stack이 존재하면(Stop CDC로 스택과 MSK committed offset이 유지된 경우 포함) 제외
  체크박스가 아무 메시지 없이 비활성화됐는데, CDC를 중지한 사용자에게는 이유 없이 얼어붙은 박스가
  버그처럼 보였습니다. 잠금 자체는 올바르며 유지됩니다: 이미 스트리밍한 파이프라인에서 제외 컬럼을 바꾸면
  (resume offset이 Stop 후에도 살아남으므로) 이미 마이그레이션된 행과 이후 처리되는 행이 어긋나므로,
  제외는 인프라 수명 동안 고정됩니다. 이제 박스가 그 이유와 유일한 안전 조치 — CDC 인프라를 삭제하고 새
  구성으로 재배포 — 를 명시하며, 이는 테이블 피커가 동일한 스택 phase에서 잠기는 방식과 일치합니다. 이
  안내는 neutral 톤으로 표시됩니다(배포된 파이프라인은 정상 상태이지 경고가 아님). CDC 시작점과 테이블
  선택 동작은 변경되지 않았습니다.

## v0.1.272

### 추가

- **오버사이즈 LOB 컬럼 제외가 이제 마이그레이션 전체에 적용됩니다 — CDC뿐 아니라 Full Load도
  이 선택을 따릅니다.** 값이 Aurora DSQL의 값당 ~1 MiB 한계를 넘을 수 있는 MySQL
  `mediumtext`/`longtext`/`mediumblob`/`longblob` 컬럼을 빼는 opt-in "오버사이즈 LOB 컬럼"
  컨트롤은 그동안 CDC 캡처(Debezium `column.exclude.list`)에만 반영됐습니다. 이제는 단일한
  마이그레이션 전역 선택입니다: 체크한 컬럼은 Full Load INSERT 컬럼 목록과 CDC 캡처 **양쪽에서**
  빠지므로, gapless 핸드오프를 사이에 두고 두 데이터 경로가 어긋나는 일이 없습니다(한쪽에서만 빠지면
  은밀한 부분 데이터가 남습니다). Full Load가 포함된 마이그레이션에서는 이 카드가 이제 Full Load
  화면의 **테이블 선택 직후·사전 점검 직전**에 표시되어, 컬럼을 실을 로드 전에 미리 제외할 수 있습니다.
  CDC 전용은 카드를 CDC 하위 흐름에 그대로 둡니다(`column.exclude.list` 미리보기 포함). 제외는
  스트리밍 전에 유효 테이블에서 컬럼을 떨어뜨리는 방식으로 적용됩니다 — 익스포터의 `SELECT` 목록과
  임포터의 `INSERT` 목록이 모두 여기서 파생되므로 — 소스에서 그 컬럼을 읽지도, 타깃에 쓰지도 않습니다.
  타깃 스키마는 여전히 그 컬럼을 유지하며(적용된 DDL로 재생성), 기본값/NULL을 취합니다. **기본 키 컬럼은
  절대 제외되지 않으며**, **로드 가능성 사전 점검이 제외 후 컬럼 집합을 평가**합니다: 타깃에서 `NOT NULL`
  이고 기본값이 없는 컬럼을 제외하면 로드 도중 매 배치가 실패하는 대신 사전 게이트가 올바르게 실패합니다
  (더는 채울 수 없으므로). 카드는 **로드가 커밋되기 전까지 편집 가능**합니다(테이블 피커와 동일한 시점에만
  잠깁니다 — Full Load 시작, CDC 스트리밍, 또는 CDC infra 배포 시 — read-only 점검이 돌았다는 이유만으로
  잠기지 않습니다). 점검 통과 후 제외를 변경하면 Run 버튼이 "사전 점검을 다시 실행하라"며 새로 제외된 컬럼을
  지목해 막으므로, stale 통과 상태로 점검되지 않은 컬럼 집합에 로드가 시작되는 일이 없습니다(테이블 피커와
  같은 비대칭 가드 — 제외 해제는 절대 막지 않음). 기본 동작은 그대로입니다 — 체크하지 않으면 아무것도
  제외되지 않고, 오버사이즈 단일 값은 여전히 행 단위로 격리(quarantine)됩니다.

### 수정

- **"Start Full Load" 확인 다이얼로그가 이제 첫 클릭에 열립니다.** 이전에는 다이얼로그 엘리먼트 생성과
  열기가 한 번의 클라이언트 업데이트로 합쳐져, Quasar `QDialog`가 열림 애니메이션에 필요한 `false→true`
  전이를 보지 못해 첫 클릭이 조용히 무시됐고, 두 번째 클릭(엘리먼트가 이미 등록된 상태)에만 떴습니다. 이제
  열기를 한 tick 뒤로 미뤄 첫 클릭에 다이얼로그가 나타납니다.
- **"Change migration type" 점프 링크가 이제 다른 내비게이션 버튼(Back / Next / Continue)과 동일한
  외곽선 테두리를 갖습니다** — 이전에는 flat 스타일이라 테두리가 없었습니다.
- **"Start Full Load" 직전 타깃 probe가 훨씬 빨라졌습니다(DSQL 연결 수 감소).** 선택한 각 테이블의 실제
  기본 키를 테이블마다 별도 연결로 읽어서, N개 테이블이면 N+1개 연결을 열었습니다 — DSQL 연결은 매번 IAM
  토큰 생성 + (리전 간) TLS 핸드셰이크(개당 ~1초)라, 확인 다이얼로그가 뜨는 데 수 초가 걸린 원인이었습니다.
  이제 모든 테이블의 기본 키를 **단일 연결**로 읽어(신규 `target_primary_keys`), 테이블 수와 무관하게 probe를
  2개 연결로 줄였습니다. 서울 리전 7개 테이블 기준 실측: PK 읽기가 ~9.5초 → ~2.8초로 감소.

## v0.1.271

### 변경

- **컷오버 전 identity 시퀀스 동기화가 이제 렌더 시 자동 실행이 아니라, Cut-over 런북의 명시적 "Sync
  identity sequences" 버튼입니다.** v0.1.270은 이 동기화를 Cut-over 화면이 렌더되는 side-effect로
  실행했습니다 — 페이지를 보기만 해도 클릭 없이 타깃에 `ALTER TABLE … RESTART WITH`가 나갔는데, 이는 좋지
  않은 설계(읽기/조회가 쓰기를 유발)이고 "컷오버는 오퍼레이터의 명시적 행위"라는 이 단계의 원칙과도 맞지
  않습니다. 이제 런북은 repoint 스텝 직전에 **"Sync identity sequences"** 버튼을 표시하며, 오퍼레이터가
  최종 drain/reload 후 repoint 전에 클릭합니다. 모든 identity 키를 현재 타깃 `MAX(pk)` 위로 전진시키고
  (멱등, 여러 번 클릭해도 안전), 클릭이 UI를 막지 않도록 백그라운드로 실행되며, 결과(어떤 시퀀스가 전진했는지,
  또는 "전진할 것이 없었음")를 표시합니다. 이로써 v0.1.270이 노린 안전망 — 런북에 도달해 버튼 한 번 누르면
  마지막 Validation 이후 CDC가 전달한 행까지 커버, 재검증을 기억할 필요 없음 — 은 유지하면서 렌더 시 타깃
  쓰기는 제거했습니다. (v0.1.270은 배포되지 않았으며, 이 버전이 이를 대체합니다.)

## v0.1.270

### 수정

- **Cut-over 런북이 열릴 때 identity 시퀀스를 마지막으로 한 번 더 재동기화하여, 깨끗한 컷오버가 "먼저
  재검증하기"를 기억하는지에 더 이상 의존하지 않습니다.** Validation은 이미 `GENERATED BY DEFAULT AS
  IDENTITY` 시퀀스를 현재 `MAX(pk)` 위로 재동기화하지만(v0.1.266), 이는 최종 CDC drain 이후 사용자가
  검증을 실행할 때만입니다. 그 마지막 검증 *이후에* CDC가 전달한 행이 있으면 시퀀스가 뒤처진 채 남아, 컷오버
  후 앱의 첫 INSERT가 충돌(중복키, SQLSTATE 23505)합니다 — count/checksum은 맞아 컷오버 후에야 드러나는
  조용한 실패입니다. 이제 Cut-over 단계는 런북이 열리는 시점(release가 clean 또는 accepted)에 best-effort
  identity 시퀀스 재동기화를 실행합니다 — 사용자가 앱을 repoint하기 직전, 그리고 타깃 `MAX(pk)`가 최종
  확정되는, 툴이 통제하는 마지막 지점입니다. verdict당 1회 실행(재검증 시 재무장)되며, 검증된 테이블에 대해
  백그라운드로 현재 `MAX(pk)` 기준으로 돕니다. 타깃 카탈로그의 `is_identity`로 identity가 아닌 테이블은
  건너뜁니다. 이제 런북에 도달하는 것만으로 충분하며, Validation을 다시 실행할 필요가 없습니다. ("I've cut
  over" 확인 시점에 동기화하는 방안 대신 선택 — 그 시점은 앱이 이미 라이브라 첫 충돌을 막기엔 너무 늦습니다.)

## v0.1.269

### 수정

- **격차(quarantine) 수용 시에도 identity 시퀀스를 동기화합니다 — v0.1.268이 "로드 후 수용" 흐름에 남긴
  구멍을 막습니다.** v0.1.268은 로드가 *이미 수용된 상태로 실행*될 때 시퀀스를 동기화했지만, 실제 워크숍
  흐름은 그 반대입니다: 먼저 로드가 실행되고(아직 아무것도 수용 안 함 → `_finalize_run`이 incomplete로 보고
  동기화를 건너뜀), 그 다음 사용자가 **"Accept quarantined rows & continue"** 를 클릭하는데, 이 버튼은
  단계를 완료 처리하고 CDC를 풀어줄 뿐 동기화를 전혀 트리거하지 않았습니다. 그 결과 `GENERATED BY DEFAULT
  AS IDENTITY` 키가 `nextval` = 1에 머문 채 이미 적재된 id들이 존재해, 컷오버 후 앱의 첫 INSERT가
  충돌(중복키, SQLSTATE 23505)했습니다 — 0.1.268에서도 실제 이벤트에서 재현됨. 이제 수용 액션이 마이그레이션
  범위 전체에 대해 백그라운드 identity 시퀀스 동기화를 제출하며, 현재 타깃 `MAX(pk)` 기준으로 맞춥니다(격리된
  행은 영구 드롭이라 최종값). 타깃 카탈로그의 `is_identity`로 identity가 아닌 테이블은 건너뜁니다. 로드 시점
  동기화(클린/로드시 수용)와 검증 재동기화(v0.1.266)와 함께, 이제 로드를 완료하는 모든 경로가 시퀀스를
  전진시킵니다.

## v0.1.268

### 수정

- **격리(quarantine)를 수용한 Full Load 이후에도 identity 시퀀스를 동기화합니다(완전히 클린한 로드에서만
  하던 것을 개선).** 로드 후 identity 시퀀스 동기화(=`GENERATED BY DEFAULT AS IDENTITY` 키를 적재된
  `MAX(pk)` 위로 전진)는 완전 클린 완료 경로에서만 실행됐습니다. 행이 하나라도 격리되면 — 영구 드롭, 예: DSQL
  값당 ~1 MiB 한계 초과 — 실행이 "격리 수용" 경로로 완료되는데, 이 경로가 **동기화 없이** 반환해 시퀀스가
  시작값(`nextval` = 1)에 머물렀습니다. 그 결과 컷오버 후 앱의 첫 자동 INSERT가 이미 적재된 id와 충돌해
  중복키(SQLSTATE 23505)로 실패했습니다. 격리된 행은 영구 드롭되어 다시 채워지지 않으므로 현재 `MAX(pk)`가
  최종값이고 그 기준으로 동기화하는 것이 안전합니다 — 이제 동기화 게이트를 "완전 클린"이 아니라
  `real_failed == 0`(격리 수용 포함) 기준으로 완화했습니다. 진짜 부분 실패(`real_failed > 0`)는 여전히
  동기화를 건너뜁니다(이후 재시도가 행을 더 채울 수 있으므로). 이는 v0.1.266의 "검증 시 재동기화"를 보완합니다:
  이전에는 격차를 수용한 로드가 검증을 건너뛰고 바로 컷오버로 가면 그 안전망을 잃었지만, 이제는 로드 시점에
  동기화가 실행됩니다.

## v0.1.267

### 수정

- **`bigint unsigned` AUTO_INCREMENT 키를 서버 생성 IDENTITY로 변환할 때, 범위가 축소된다는 경고를
  이제 표시합니다(조용히 처리하지 않음).** Aurora DSQL identity 컬럼은 `bigint`여야 하는데, `bigint
  unsigned`는 전체 `0..2^64-1` 범위를 보존하려고 `numeric(20,0)`로 매핑됩니다 — 따라서 이를 identity로
  만들면 `bigint`의 `0..2^63-1`로 좁혀집니다. 새로 생성되는 id는 영향이 없지만, *기존* 소스 값이 2^63-1
  (9223372036854775807)을 초과하면 더 이상 들어가지 않아 해당 행이 Full Load에서 numeric 범위 초과
  오류(SQLSTATE 22003)로 실패합니다 — 로드 시점까지 드러나지 않는 실질적 실패입니다. 이제 변환은 정확한
  임계값과 안전한 대안(소스 PK 유지, 또는 UUID 키 사용)을 명시하는 별도의 LOSS 경고를 내며, 처리량 조언과
  섞이지 않고 Schema Conversion의 "gaps" 섹션에 표시됩니다. 무손실 확장(`int`/`bigint`/`int unsigned`
  → `bigint`)은 변경 없이 이 경고를 내지 않으며, 소스 PK를 유지하면 `bigint unsigned`는 전체 범위를 가진
  `numeric(20,0)`로 남습니다.

## v0.1.266

### 수정

- **Identity(AUTO_INCREMENT) 시퀀스를 검증(Validation) 시 다시 동기화하여, Full Load + CDC
  마이그레이션에서 컷오버 후 발생할 수 있던 중복키 문제를 막습니다.** `GENERATED BY DEFAULT AS IDENTITY`
  키의 경우 Full Load와 CDC 모두 *명시적* id를 삽입하는데, 명시적 id는 identity 시퀀스를 전진시키지
  않습니다. Full Load는 로드 완료 시점에 시퀀스를 이미 동기화하지만, 이후 CDC가 계속 삽입하므로 컷오버
  시점에는 시퀀스가 실제 `MAX(pk)`보다 다시 뒤처지고, 컷오버 후 앱의 첫 자동 INSERT가 중복키(SQLSTATE
  23505)로 실패할 수 있었습니다. 이는 최악의 실패 형태였습니다 — 행 수·체크섬은 일치해서 Validation은
  깨끗하게 통과하고, 소스가 동결된 컷오버 이후에야 드러났습니다. 이제 전체 검증 실행 시 모든 identity
  테이블에 대해 멱등한 `ALTER TABLE … ALTER COLUMN … RESTART WITH max(pk)+1`를 다시 실행합니다(타깃
  카탈로그의 `is_identity`로 판별하므로 identity가 아닌 테이블은 건드리지 않음). 검증은 CDC drain 이후
  컷오버 직전에 사용자가 거치는 단계이며, 그때가 타깃 `MAX(pk)`가 최종 확정되는 시점입니다. 시퀀스가
  전진되면 결과 화면에 "Identity sequences advanced for cut-over" 안내와 새 `RESTART WITH` 값을
  표시하고, CDC 컷오버 런북의 마지막 "재검증" 단계도 이 동작을 명시합니다. 비교 자체는 읽기 전용을
  유지하며, 시퀀스 전진은 리포트를 실패시키지 않는 별도의 (보고되는) 타깃 쓰기입니다.

## v0.1.265

### 추가

- **Schema Conversion 기본키 피커가 AUTO_INCREMENT 테이블에 "Server-generated (IDENTITY)" 타일을
  제공합니다.** 변환기에는 이미 identity 전략(`BIGINT ... GENERATED BY DEFAULT AS IDENTITY (CACHE
  65536)`)이 end-to-end로 구현돼 있었고 — Full Load 이후 identity 시퀀스 동기화까지 포함 — 다만 타깃
  DDL을 수동 편집해야만 도달할 수 있었습니다. 단일 컬럼 AUTO_INCREMENT 키의 경우, 피커에 세 번째 타일이
  나타나 한 번의 클릭으로 이 전략을 적용합니다. 키에 gap·느슨한 순서가 생긴다는(각 DSQL 노드가 자체 값
  블록을 사용) 안내를 함께 표시하며, Full Load는 여전히 소스 id를 그대로 유지합니다. 이는 AUTO_INCREMENT의
  가장 충실한 이식이며 데이터 경로를 그대로 보존합니다(`BY DEFAULT`라 로더가 소스의 id를 삽입 가능). 타일은
  적용 가능한 경우(단일 컬럼 AUTO_INCREMENT 키)에만 노출되고, 선택은 복합키 선택과 동일한 편집 DDL 필드에
  저장되어 별도 상태 없이 재개(resume) 안전합니다.
- UUID는 피커 타일로 **제공하지 않습니다**: UUID 전략은 키 컬럼을 `uuid`로 재타입하는데, 이 툴의 Full
  Load는 소스의 정수 id로 그 컬럼을 채울 수 없어(int→uuid 삽입 실패) 여기서 노출하면 데이터 마이그레이션을
  깨진 로드로 유도하게 됩니다. 스키마 전용/그린필드 용도로 수동 DDL 편집을 통해서는 여전히 사용할 수 있습니다.

## v0.1.264

### 변경

- **Validation "Validating" 카드에서 "Migration type" 행을 더 이상 표시하지 않습니다.** 검증은 소스와
  타깃을 비교하는 순수 동작이며 — 엔진(`validator.validate`)은 마이그레이션 타입을 인자로 받지 않고, 데이터가
  어떤 경로(Full Load / CDC)로 들어왔든 동일하게 동작합니다 — 타입이 이 화면의 동작을 바꾼 적이 없습니다.
  게다가 세션은 마지막에 선택한 타입만 기록하므로, Full Load → CDC only 순서로 진행한 경우 "CDC only"만
  표시되어 실제로 무엇이 마이그레이션되었는지 잘못 전달했습니다. 마이그레이션 타입은 이미 Data Migration
  단계와 상단 배너에서 전달되므로 여기서는 제거했습니다. (cut-over 런북과 드리프트 판정은 CDC 사용 여부로
  계속 분기하며, 그 로직은 변경되지 않았습니다.)

## v0.1.263

### 변경

- **Validation 결과 화면의 섹션 사이에 작게 표시되던, 불필요한 문구 두 가지를 제거했습니다.** (1) "For a
  definitive zero-loss verdict, quiesce the source first" 경고를 격차 복구 카드에서 제거했습니다 —
  스냅샷 이후 소스가 변경되었는지, 그리고 소스 쓰기 중단 / CDC 드레인 후 재검증하라는 안내는 전용 "Source
  changes since the comparison" 섹션이 이미 다루고 있어, 복구 카드에도 넣는 것은 섹션 간 중복이었습니다.
  (2) 옵션 블록 아래의 "Changing options applies on the next run — use Re-run (top right) to apply
  them." 문구를 제거했습니다 — 옵션 블록은 명백히 실행 전 설정 영역이고 Re-run 버튼은 우측 상단에 항상
  보입니다. (3) 검증 결과 아래의 "Completed in Xs" 실행 시간 문구를 제거했습니다. 실행 중일 때 표시되는
  회색의 "Options apply to the next run." 안내는 유지됩니다 — 실행 중 토글이 비활성인 이유를 설명하기
  때문입니다.

## v0.1.262

### 수정

- **Validation 표별 결과 표의 Result 열이 항상 대시(—)로 표시되던 문제를 고쳐, 이제 각 행에 match /
  mismatch / ERROR 뱃지가 나타납니다.** Result 열은 실패 행을 위로 올리기 위해 정수 키(`result_sort`)로
  정렬하는데, 색상 뱃지 슬롯이 그 열의 필드 값 — 즉 정수 — 을 `{text, color}` 뱃지 페이로드인 것처럼
  읽고 있었습니다. 정수에는 `.text`가 없으므로 모든 행이 "—" 자리표시자로 흘러버렸습니다. 이제 Result에는
  행에서 직접 뱃지 페이로드를 읽는 전용 셀 슬롯이 생겼고, 열은 계속 `result_sort`로 정렬합니다(실패·오류
  행이 여전히 맨 위로 정렬됨). 행 수(row-count)와 체크섬 열은 변경되지 않았습니다.

## v0.1.261

### 변경

- **Cut-over readiness 카드가 같은 설명을 세 번 반복하지 않습니다.** 격차가 완전히 설명된 경우, 카드
  상단 리드인이 이미 "아래 Heads-up 항목은 모두 마이그레이션이 보고한 드롭 행"이라고 밝히는데,
  "Data identical"과 "No mismatched records" 체크가 각각 "…is exactly the N rows dropped during
  the migration — already reported, not new data loss" 꼬리를 다시 붙였습니다. 이제 리드인이 커버할
  때(완전 설명)는 체크별 꼬리를 생략하고 각 체크에 숫자 사실만 남깁니다. 부분 설명 런은 리드인이
  없으므로 체크가 꼬리를 유지해 원인을 직접 담습니다. 섹션 간 반복(판정, 테이블별 등)은 그대로 —
  그 중복은 각 섹션의 독립 판독성을 위한 것입니다.

## v0.1.260

### 변경

- **"소스를 먼저 정지하라" 경고가 영구 격차에서 clean match를 약속하지 않습니다.** 이 경고(라이브
  소스 드리프트 시 표시)는 두 복구 분기에 공통으로 걸리지만, 마무리 문구 — "re-validate — a clean
  match then truly means no data was lost" — 는 설명 안 된, 적재 가능한 격차에서만 참입니다. 완전히
  설명된 격차에서는 그 행들이 영구적 Aurora DSQL 한도를 초과해 무엇을 하든 계속 빠지므로, 얼리고
  재검증해도 clean match에 도달할 수 없습니다 — 이 약속은 같은 카드의 "이 행들은 그대로 저장 불가 —
  값을 줄이거나 격차 인정"과 모순됐습니다. 이제 마무리 문구가 분기별로 나뉩니다: 완전 설명 →
  "재검증해 다른 드리프트 행이 없는지 확인 — 설명된 격차는 값을 줄이거나 인정할 때까지 남음";
  미설명 → 기존 clean-match 문구(그쪽에선 올바른 목표). 드리프트 게이트 자체는 그대로입니다.

## v0.1.259

### 변경

- **복구 섹션의 제목과 아이콘이 이제 acceptable 격차의 내용과 일치합니다.** v0.1.258이 섹션 본문을
  격차의 완전 설명 여부로 분기했지만, 제목은 렌치 아이콘의 "How to recover"로 남아 있었습니다.
  완전히 설명된 격차에서는 그 행들을 아예 저장할 수 없어 인정이 정당한 최종 선택인데, "recover"·렌치가
  결정을 수리처럼 보이게 해 판정 배너 및 섹션 본문("shrink the value or accept the gap")과
  충돌했습니다. 이제 완전 설명 케이스는 Cut over 단계와 동일한 제목·아이콘 "Acknowledge the known
  gap" / fact_check를 사용해 두 화면이 같은 어휘로 말합니다. 진짜 설명 안 된(적재 가능) 격차는, 한
  테이블은 설명되고 다른 테이블은 실제 손실인 mixed 케이스를 포함해, "How to recover"와 렌치를
  유지합니다.

## v0.1.258

### 수정

- **"How to recover" 섹션이 acceptable(완전히 설명된) 격차를 잘못된 해법으로 유도했습니다.** 모든
  누락 행이 마이그레이션이 이미 보고한 드롭 행 — 영구적 Aurora DSQL 한도(예: ~1 MiB 값 한도) 초과 —
  일 때 Full Load를 다시 돌려도 또 격리되는데, recovery 섹션은 "Re-run Full Load + CDC to backfill
  the gap. Full Load는 누락 행만 채운다" + Stop CDC → reload → 재개 런북을 렌더했습니다. 이 행들에
  대해선 사실이 아니고 바로 위 판정 배너와도 모순됐습니다. 이제 섹션이 격차가 완전히 설명됐는지에
  따라 분기합니다: 영구 격리 격차에는 "이 행들은 그대로 저장할 수 없다"고 알리고 두 가지 실제 경로 —
  소스 값을 한도 아래로 줄인 뒤(예: 큰 객체를 Amazon S3로 이전) reload, 또는 격차 인정 후 진행 — 를
  제시하며 reload 런북은 생략합니다. 진짜 설명 안 된(적재 가능) 격차는, 한 테이블은 설명되고 다른
  테이블은 실제 손실인 mixed 케이스를 포함해, 기존 reload 런북을 그대로 받습니다.
- **판정 배너 본문도 "reload로 완전 일치"라는 함의를 담고 있었습니다.** 이제 "reload만으로는 안 됨 —
  DSQL이 원래 값을 여전히 저장할 수 없기 때문"을 앞세운 뒤 소스 값 축소 또는 격차 인정을 안내해,
  recovery 섹션 및 Cut over 단계와 정합합니다.

## v0.1.257

### 변경

- **Validation 복구 섹션의 "소스를 먼저 정지하라" 안내가 실제 드리프트가 있을 때만 표시됩니다.**
  "How to recover"는 no-go 섹션으로, 남은 문제는 설명되지 않은 실제 불일치이고 해법은 순서화된
  Full Load 재적재 단계입니다. 소스를 멈추라는 조언은 라이브 드리프트로 인해 불일치 일부가 in-flight일
  수 있을 때(재적재가 움직이는 대상을 쫓을 수 있을 때)만 이 화면에 적합합니다. 이 notice는 드리프트가
  없어도 `info`로 무조건 렌더되어, 구체적 격차를 고치는 화면에 일반적인 컷오버 곁다리를 붙였습니다.
  그 폴백을 제거합니다. 드리프트 없는 경우의 정지 안내는 소스 변경 섹션과 Cut over 단계가 이미
  다루므로, 이는 조언이 아니라 중복을 없애는 것입니다.

## v0.1.256

### 변경

- **Validation 판정이 더 이상 acceptable 격차를 "blocked"라고 부르지 않습니다.** 모든 차이가
  마이그레이션이 이미 보고한 드롭 행과 정확히 일치할 때 툴은 이 상태를 *acceptable*(격차 인정 후
  cut-over 진행 가능)로 분류하는데, 노란 판정 헤더는 "Cut-over blocked only by rows dropped
  during the migration"으로 표시됐습니다. "blocked"는 빨강급 "완전 정지" 단어라 툴 자신의 게이트와
  모순되고 실제 빨강 "Not ready" 판정보다 더 심각하게 읽혔으며, 같은 상태를 "Every difference is
  explained"로 부르는 Cut over 단계와도 어긋났습니다. 이제 헤더가 결정을 명시합니다 — "Every
  difference is explained — accept the gap or fix the source and reload" — Cut over 단계와
  맞춰 두 화면이 같은 상황으로 읽힙니다. 초록 "Ready for cut-over"와 빨강 "Not ready for cut-over"
  헤더는 그대로입니다.
- **Cut-over readiness 패널이 판정과 연결하는 리드인 한 줄로 시작합니다.** v0.1.255부터 이 패널이
  맨 아래(증거 뒤)에 렌더되어 "Heads-up" 행이 새로운 약한 신호처럼 읽힐 수 있었습니다. 모든 차이가
  알려진 드롭 행일 때, 이제 패널이 "Same conclusion as the verdict above — nothing unexplained.
  Each 'Heads-up' item below is the same rows the migration already reported dropping, not a
  new problem."로 시작합니다. (설명 안 된 차이가 없을 때만 표시)

## v0.1.255

### 변경

- **Validation 섹션 순서가 요약이 아니라 근거를 먼저 보여주도록 바뀌었습니다.** no-go일 때
  "How to recover"와 "Cut-over readiness" 체크리스트가 그 판정의 근거인 비교 결과보다 *위*에 떠서,
  해법과 요약이 "왜"보다 먼저 나왔습니다. 판정(상단 배너)과 그 복구 안내는 헤드라인 답으로 함께
  유지하되, 근거에서 요약된 readiness 체크리스트는 이제 그 뒤로 이동합니다: 판정 → 복구 방법 →
  주의가 필요한 테이블 → 테이블별 결과 → orphan 레코드 → 소스 변경 → **cut-over readiness** →
  내보내기. 위에서 아래로 읽으면 "cut over 가능한가?", "무엇을 해야 하나?", "왜?", 그리고 마지막
  집계 순서가 됩니다.

## v0.1.254

### 수정

- **Full Load 후 Validation/Cut over 단계에서 migration-type 배너가 "CDC only"로 표시됐습니다.**
  배너가 "one journey" 헤더를 따라 모든 단계에 표시됐지만, 세션의 migration type은 하나뿐입니다 —
  그래서 안내된 경로(Full load only 실행 후 "CDC only"로 전환해 적재된 대상에 스트리밍)를 따르면
  이후 단계에서 "Migration type: CDC only" + "no Full Load in this session" 설명이 떠, 방금 한 일과
  모순되는데도 그 화면에서는 바로잡을 방법이 없었습니다. 이제 배너는 선택기가 있는 Data Migration
  단계에서만 표시되며, 그곳에서는 라벨이 항상 현재 상태와 일치합니다(Full Load 중에는 Full load
  only, 전환 후에는 CDC only). 단계별 진행 스테퍼는 모든 단계에서 계속 연속성을 제공합니다.

## v0.1.253

### 수정

- **CDC 삭제 중 seeder ENI 대기 구간의 로그가 여전히 약 18분간 비어 있었습니다.** v0.1.251에서
  보고 기능을 넣었지만 인터페이스 개수가 *변할 때만* 기록했기 때문에, 실제 관측된 삭제에서는
  "MskCluster DELETE_COMPLETE"(02:46) → 아무것도 없음 → "Seeder network interfaces
  released."(03:05) 순으로 나타났습니다. 사용자는 상황을 끝난 뒤에야 알게 되어, 이 기능이 없애려던
  "멈춘 것처럼 보이는" 증상이 그대로 남았습니다. 이제 개수가 그대로여도 약 2분마다 다시 보고하며
  경과 시간을 함께 표시합니다("Waiting for AWS to reclaim 2 seeder network interfaces —
  6 min so far"). 그래서 회수가 오래 걸려도 진행 중임이 드러납니다. 그 간격 안의 빠른 폴링은
  여전히 한 줄만 남기므로 스택 이벤트가 묻히지 않습니다.

## v0.1.252

### 수정

- **CDC 인프라를 삭제해도 파이프라인이 계속 스트리밍 중인 것처럼 보였습니다.** "Delete CDC
  infrastructure"를 누르면 삭제가 시작되지만 CloudFormation이 커넥터를 즉시 제거하지는 않으므로,
  약 20분의 삭제 동안 탐색은 두 커넥터를 계속 RUNNING으로 보고했습니다. 그래서 카드 본문은
  "Deleting infrastructure"인데 그 옆에 초록색 **"Streaming"** 뱃지가 붙어 있었고, Live status
  패널과 그 안의 dead-letter queue, 테이블별 마이그레이션 상태 표까지 모두 화면에 남아 해체 중인
  파이프라인의 복제 수치를 보여줬습니다. 이제 사용자가 요청한 제거 작업이, 앞으로 1~2분만 참인
  커넥터 상태보다 우선합니다: 뱃지는 "Deleting…"(MSK를 남기는 Stop CDC는 "Stopping…")으로 바뀌고,
  세 모니터링 화면은 제거가 끝날 때까지 숨겨집니다. 세 화면은 하나의 표시 조건을 공유해 서로
  어긋날 수 없으며, Start CDC 진행 중에는 그대로 표시됩니다 — 그 구간이 가장 필요한 시점입니다.

## v0.1.251

### 추가

- **CDC 인프라 삭제 시 seeder 네트워크 인터페이스 회수 상황을 deploy log에 표시합니다.** 삭제에서
  가장 오래 걸리는 구간은 MSK 클러스터가 사라지기 전에 AWS가 VPC 내 offset-seeder Lambda의 ENI를
  해제하는 시간(약 15-20분)인데, 이 동안 CloudFormation은 아무 이벤트도 내지 않아 로그가 멈춘 것처럼
  보였습니다. 이제 삭제 대기 중 그 ENI를 읽기 전용으로 폴링하며 변경 시에만 로깅합니다 — 남아 있는
  동안 "Waiting for AWS to reclaim N seeder network interface(s)…", 모두 해제되면
  "Seeder network interfaces released." — 그래서 조용한 구간에도 무엇을 기다리는지 보입니다. 이를
  위해 앱 스택의 CDC deploy role에 `ec2:DescribeNetworkInterfaces`(읽기 전용)를 추가했으며, CDC
  인프라 재배포는 필요 없습니다.

## v0.1.250

### 수정

- **CDC 인프라 삭제 시 계속 초과하는 정확한 ETA를 표시했습니다.** 단계 진행 카드가 "est. ~5 min
  remaining"으로 표시했지만, 삭제는 MSK 클러스터가 사라지기 전에 AWS가 VPC 내 seeder Lambda의
  ENI를 회수하는 데 걸리는 시간에 좌우됩니다 — 예측 불가능하며 실측상 추정치를 크게 초과합니다 —
  그래서 4배 짧은 카운트다운이 멈춘 UI처럼 보였습니다. 이제 삭제는 단계별 ETA 힌트 없이 정직한 상한
  ("can take up to ~20 min")을 표시합니다. 타이밍이 안정적인 다른 작업은 카운트다운을 유지합니다.

## v0.1.249

### 변경

- **테이블별 Consistency 컬럼: 이를 채우는 Refresh 버튼과 연결했습니다.** Consistency 판정은
  source/target 행 수와 high-water PK로 계산되며, 이 값은 오직 명시적 "Refresh source/target
  counts" 액션(의도적으로 자동 폴하지 않는, 소스를 스캔하는 COUNT(*))에서만 옵니다. 버튼을 누르기
  전까지 각 행의 Consistency는 "refresh to check"로 표시되는데, 이는 사용자가 스스로 연결해야 하는
  버튼을 가리켰습니다. 이제 표 위 info 안내가 첫 조회 전에는 그 연결을 명시하고("Consistency
  컬럼은 'Refresh source/target counts'를 누를 때까지 'refresh to check'로 표시됩니다") 조회 후에는
  그 안내를 뺍니다. 컬럼과 버튼을 유지할 가치가 있는지 검토했습니다: 이 컬럼은 Validation의 정밀
  검증과는 구별되는 실시간 컷오버 신호이고, 그것을 채우는 것이 이 버튼이므로 — 둘은 하나의 기능이라
  둘 다 유지하되, 그 관계를 제거하는 대신 명시적으로 드러냈습니다.

## v0.1.248

### 수정

- **CDC 단계 시각 요소: "Live status" 섹션과 펼침 패널들에 테두리가 없었습니다.** CDC 시작 전
  "Live status"는 빈 차트와 "커넥터가 감지되면 표시됩니다"라는 맨 회색 문구 위에 테두리 없는
  제목으로 렌더되어, 주변의 테두리 카드들과 어울리지 않는 빈 공간이었습니다. 이제 CDC가 시작될
  때까지 섹션 전체를 숨기며(테이블별 표와 동일), 표시된 뒤 아직 준비 중인 상태의 안내는 제대로 된
  테두리 info notice로 나옵니다. 또한 펼침 패널들(Connector configuration, cdc-stack 파라미터
  파일, Delete CDC infrastructure, Infrastructure inputs, 배포 로그 등)에 테두리가 없어 카드
  섹션 옆에서 스타일 없는 제목처럼 보였는데, 이제 design system의 단일 `EXPANSION_PANEL_CLASSES`
  테두리 토큰을 공유합니다.

## v0.1.247

### 수정

- **CDC 인프라 생성 중에도 migration type을 전환할 수 있었습니다.** 생성 작업은 커넥터를 만들지
  않고 아무것도 스트리밍하지 않는다는 이유로 제외되어 있었지만, 그것이 곧 "선택이 자유롭다"는
  뜻은 아닙니다: 이는 과금되는 Amazon MSK 클러스터를 프로비저닝하는 약 15-20분짜리 작업이며,
  진행 상황 화면과 "Delete CDC infrastructure" 제어가 모두 CDC 하위 단계에 있습니다 — "Full load
  only"로 전환하면 그 하위 단계가 아예 사라져, 계정에서 클러스터가 계속 생성되는데도 진행 상황도
  완료 신호도 삭제 수단도 화면에 없게 됩니다. 툴 내부도 일관되지 않았습니다: oversized-LOB 제외는
  이미 인프라 생성 중에 잠깁니다. 이제 그 작업이 실행되는 동안 타입이 잠기며 비용과 제어 위치를
  함께 알리고, 작업이 끝나면 다시 열립니다(유휴 인프라는 여전히 사용자가 감수하는 trade-off입니다).
- **CDC 단계의 "Per-table migration status" 표가 CDC 시작 전에 나타났습니다.** CDC 하위 단계가
  표시되는 즉시 — 인프라 생성 내내 포함 — 함께 나타났는데, 그 시점에는 커넥터가 없어 모든 CDC
  열(Stream lag, Quarantined, Inserts/Updates/Deletes, Consistency)이 비어 있으므로 "CDC가
  동작 중인데 아무것도 복제하지 않는다"로 읽혔습니다. 이제 CDC가 실제로 시작된 뒤에 나타나며(위쪽
  라이브 상태 패널과 동일한 기준), 커넥터가 약 10-20분의 준비를 마친 뒤가 아니라 Start CDC를 누른
  시점부터 표시됩니다. 정보 손실은 없습니다: Full Load의 자체 테이블별 표는 Full Load 단계에
  그대로 있습니다.

## v0.1.246

### 수정

- **"Accept quarantined rows & continue" 클릭 후에도 빨간 "Migration failed" 배너가 남았습니다.**
  격리 행 수락은 사용자가 바로 그 오류를 해결하는 행위입니다 — Full Load가 DONE이 되고 해당 단계는
  "Full Load complete — with an accepted gap"을 표시합니다 — 그런데 원본
  `FullLoadIncompleteError`가 그 위에 그대로 붙어 있었습니다. 한 화면이 세 가지 판정(실패 /
  격차를 수락한 완료 / DONE)을 동시에 보여주고, 사용자가 이미 처리한 일을 다시 문제로 표시했으며,
  방금 따른 "'Accept quarantined rows & continue'를 선택하세요"라는 지시까지 포함하고 있었습니다.
  이제 격차를 수락하면 배너가 사라지며, 마이그레이션 타입을 전환해도 계속 숨겨집니다(해결된 오류는
  이전 타입의 참고 정보도 아닙니다). 정보 손실은 없습니다: 수락 안내가 이미 삭제된 행 수, 해당
  테이블, Validation이 여전히 격차를 보고한다는 사실을 담고 있고, 오류 로그에는 삭제된 모든 행이
  기본 키로 남아 있습니다. 수락 전에는 배너가 그대로입니다 — 그때는 실제 실패이므로 분명히
  표시해야 합니다.

## v0.1.245

### 추가

- **Full Load 완료 후 "CDC를 이어서?" 안내에 이동 링크를 추가했습니다.** 안내는 "위의 migration
  type을 변경하세요"라고 알렸지만, Full Load가 끝난 시점에 그 선택기는 긴 페이지의 맨 위에 있어
  대개 화면 밖입니다 — 안내가 요구하는 유일한 행동을 사용자가 직접 찾아가야 했습니다. 이제
  "Change migration type" 링크가 선택기로 바로 스크롤하고 잠시 테두리를 표시하므로, 도착했을 때
  어떤 컨트롤인지 분명합니다. 문구도 방향을 알려주는 대신 링크를 가리키도록 바꿨습니다. 순수하게
  이동만 합니다: 타입은 여전히 타일을 직접 클릭해서 변경합니다.

## v0.1.244

### 수정

- **"Generate DDL for selected"가 낡은 스냅샷으로 타깃 존재 여부를 판정했습니다.** 각 diff의
  "'x' already exists on the target — SKIP 또는 REPLACE(파괴적)를 선택하세요" 경고가 캐시된 타깃
  인벤토리에서 답을 얻었는데, 이 캐시는 SQL을 발행하지 않으며 Evaluation의 브라우즈나 수동
  "Refresh target" 버튼으로만 채워집니다. 그래서 타깃을 모두 비운 뒤에도 이미 사라진 객체에 대해
  경고가 나타나 사용자를 아무 이유 없이 파괴적 선택으로 몰았습니다. 반대 경우가 더 나빴습니다 —
  스냅샷 이후 생성된 객체에는 경고가 전혀 없어 예상치 못한 SKIP이 발생했습니다. 이제 Generate가
  타깃 카탈로그를 먼저 다시 읽습니다 — 읽기 전용, UI 스레드 외부, 조용히(토스트나 이중 렌더 없이)
  — 따라서 사용자가 갱신 단계를 기억할 필요 없이 모든 판정이 실제 타깃을 반영합니다. 재조회가
  실패해도 생성은 막지 않습니다: DDL diff는 그대로 생성되며, 실제 적용 경로는 라이브 브라우즈로
  존재 여부를 독립적으로 다시 확인하므로 낡은 판정이 실제 DDL을 잘못 처리한 적은 없습니다.

## v0.1.243

### 수정

- **CDC 단계의 테이블별 "Quarantined" 열이 여전히 Full Load 격리 행을 집계했습니다.**
  v0.1.241이 dead-letter 카드를, v0.1.242가 Full Load 로그를 필터했지만 이 열이 누락되어 있었고,
  그 결과 한 화면이 같은 세션에 대해 모순된 두 숫자를 보여줬습니다: 카드는 "0 quarantined"인데 바로
  위 열은 3이었습니다. 이제 카드와 같은 필터를 공유하므로 두 값이 항상 일치합니다. Full Load
  활동 로그의 테이블별 실패 원인도 함께 필터했습니다 — 그대로 두면 dead-letter 행의 원인이
  FULL_LOAD 로그 줄에서 특정 테이블의 원인으로 기록될 수 있었습니다. 공유 오류 로그를 직접 읽는
  나머지 지점도 모두 점검했습니다: 필터되지 않은 두 곳은 필터 자체에 바로 공급되는 읽기이고,
  격리 카운터는 Full Load 기록자만 생성하는 메시지 접두사를 사용하므로 이미 안전했습니다.

## v0.1.242

### 수정

- **Full Load 오류 로그가 CDC dead-letter 행을 Full Load 실패로 집계했습니다.** v0.1.241의
  거울상으로, 방향만 반대입니다: Full Load가 실행된 세션에서는 CDC가 그 job id 아래에 기록하므로,
  필터 없이 읽으면 Full Load 격리 3건과 dead-letter 2건이 합쳐져 "Download Full Load error log
  (5 errors)"가 되고 파일에 CDC 행이 섞였습니다. 컷오버 시점에는 실제로 3행을 잃었는데 "Full
  Load가 5행을 잃었다"로 읽힙니다. 테이블별 실패 원인에도 같은 결함이 있었습니다 — 테이블별로
  마지막 메시지만 보관하므로, dead-letter 행이 Full Load가 정상 적재한 테이블의 "원인"을 제공할 수
  있었습니다. 이제 Full Load 표시(건수, 테이블별 행과 원인, 격리 목록, 다운로드의 라벨 *및* 파일
  내용)가 자신의 레코드만 보고하므로 두 화면이 오류 로그를 정확히 분할합니다: Full Load + CDC가
  전체와 일치하며, 누락도 중복 집계도 없습니다.

## v0.1.241

### 수정

- **CDC dead-letter queue 카드가 Full Load 격리 행을 DLQ 레코드로 집계했습니다.** DLQ 패널의
  오류 로그 키는 Full Load가 실행된 세션에서는 그 job id이므로 두 출처가 하나의 키를 공유했고,
  배치 로더가 격리한 행이 "Dead-letter queue (poison records)"에 나타났습니다 — 스트림에 들어간
  적도 없고 커넥터가 존재하기 몇 시간 전에 격리된 행에 대해 "DLQ로 격리했고 파이프라인은 계속
  동작한다"고 설명하면서요. Full Load에는 DLQ가 없습니다. 특히 해로운 경우는 방금 대용량 LOB
  컬럼을 제외한 사용자입니다: 0이 아닌 격리 건수를 보고 제외가 실패했다고 판단하지만, CDC 격리가
  0인 것이 바로 제외가 작동했다는 증거입니다. 이제 모든 DLQ 표시 — 건수 뱃지, 테이블별 칩, 레코드
  표, 다운로드의 라벨 *및* 파일 내용 — 이 CDC 출처 레코드만 보고하며, 단일 필터를 거치므로 건수와
  그 아래 행이 어긋날 수 없습니다. Full Load 격리 행을 숨기지는 않습니다: 대상에 도달하지 못한
  행은 컷오버 판단에 여전히 중요하므로, 패널이 Full Load 섹션을 가리키는 중립적인 한 줄로
  상호 참조합니다.

## v0.1.240

### 수정

- **Stop CDC 또는 인프라 삭제 후에도 Data Migration 뱃지가 "CDC: IN_PROGRESS"에 머물렀습니다.**
  커넥터가 감지되면 CDC 워크플로 단계를 NOT_STARTED → IN_PROGRESS로 승격했지만 되돌리는 경로가
  없었기 때문에, 커넥터가 사라진 뒤에도 뱃지는 CDC가 실행 중이라고 표시했습니다 — 워크플로는
  영속되므로 세션을 복원할 때마다 그 낡은 값이 다시 나타났습니다. 이제 단계 상태가 커넥터의 실제
  존재 여부를 양방향으로 따라갑니다: 제거 시 NOT_STARTED로 내려가며, 이것이 정확한 정지 상태입니다
  (스트리밍 중인 것이 없고 Start CDC가 다시 제공됩니다). CDC에는 여전히 종료 상태 DONE이 없습니다 —
  명시적인 Stop/Delete로만 끝나는 연속 복제이기 때문입니다. 다른 경로가 기록한 FAILED는 일상적인
  탐색 과정에서 덮어쓰지 않고 그대로 둡니다.

## v0.1.239

### 수정

- **CDC 인프라 삭제가 약 24분 걸렸고, 그 대부분은 MSK 클러스터를 삭제하지 *않는* 데 쓰였습니다.**
  두 IAM 역할이 클러스터 수준 MSK 권한의 범위를 `Fn::GetAtt: [MskCluster, Arn]`로 지정했습니다.
  CloudFormation은 이를 의존성으로 읽어 역순으로 삭제하므로, 클러스터는 자신을 참조하는 모든
  역할을 기다려야 했습니다 — offset-seeder의 역할도 포함되는데, 이 seeder는 VPC 내 Lambda라
  AWS가 ENI를 회수하는 데 약 15-20분이 걸립니다. 실측 결과 클러스터 자체 삭제(93초)가 시작되기도
  전에 seeder에서 18분 30초를 대기했고, 그동안 UI는 계속 "Deleting infrastructure"를 표시했습니다.
  삭제 시점에 그 정책들은 전혀 필요하지 않습니다 — 커넥터는 이미 사라졌고 IAM은 호출 시점에만
  평가됩니다 — 따라서 이제 역할이 클러스터 ARN을 이름으로 구성합니다(`Fn::Sub`, AWS가 붙이는 UUID
  접미사는 와일드카드 처리). 권한은 동일하지만 클러스터와 seeder가 병렬로 삭제됩니다. 생성 순서는
  그대로입니다 — 커넥터는 여전히 클러스터에 `DependsOn` 합니다.
- **삭제가 끝나는 순간 CDC 카드가 곧바로 배포 폼을 다시 내밀었습니다.** 과금되는 MSK 클러스터가
  제거되기를 20분 기다린 사용자가 원하는 답은 "제거되었습니다"이며, 방금 없앤 것을 다시 만들려는
  듯한 20줄짜리 BYO-VPC 폼이 아닙니다. 이제 카드가 삭제 완료(및 과금 중단, 마이그레이션 데이터는
  영향 없음)를 먼저 알리고, 재구축은 비용을 명시한 명시적 선택(약 15-20분, 과금)으로 제공합니다.
  최초 배포는 게이트하지 않으며(그 경우 폼이 곧 다음 단계입니다), 두 번째 삭제 때는 이전 답을
  재사용하지 않고 다시 묻습니다.

## v0.1.238

### 수정

- **oversized-LOB 제외 체크박스가 이미 확정된 뒤에도 클릭 가능한 상태로 남아 있었습니다.**
  잠금이 아예 없었습니다 — 체크가 먹고 상태도 실제로 바뀌지만, `column.exclude.list`는 인프라
  생성 시 cdc-stack의 `ColumnExcludeList` 파라미터로 구워지고 Start CDC 때 소스 커넥터로
  전달되므로, 그 이후의 변경은 조용히 버려지면서도 체크박스는 반영된 것처럼 보였습니다. 이제
  체크박스를 실제로 비활성화합니다 — 회색 처리와 클릭 차단을 함께 하므로 겉모습과 동작이
  일치합니다. 일시적인 두 경우에는 사용자가 아직 선택 중일 수 있으므로 이유와 해결 방법을 함께
  표시합니다: 인프라 생성 중에는 스택과 함께 제출됨, CDC가 시작되면 커넥터로 전달됨(변경하려면
  Stop CDC). 인프라가 배포된 상태에서는 문구 없이 잠깁니다 — CDC 진행의 정상 상태이고, 회색으로
  비활성화된 체크박스만으로도 선택이 닫혔음이 전달되기 때문입니다.
- **앱 재시작 후 CDC 파이프라인 카드가 비어 보이고 새 배포 폼을 제시했습니다.** 카드가
  "아직 조회하지 않음"과 "스택이 없음"을 같은 상태로 취급했습니다. 실제 CDC 상태를 읽는 읽기 전용
  AWS 프로브는 target 리전이 필요하고 없으면 조용히 반환하며, 복원된 세션은 재확인 전까지 이전
  연결을 신뢰하지 않습니다 — 그래서 실제 파이프라인이 스트리밍 중인데도 상태가 미지가 되고,
  카드는 중복 MSK 클러스터(과금)를 만들도록 유도했습니다. 이제 두 상태를 구분해, 상태가 정말
  확인되지 않은 경우에는 그 사실과 "실행 중인 파이프라인은 영향 없음"을 밝히고 복구 방법(target
  연결 재확인)을 안내합니다.

## v0.1.237

### 수정

- **CDC 커넥터가 생성되는 중에도 마이그레이션 타입을 전환할 수 있었습니다.** 전환하면 잠시 뒤
  커넥터가 나타나며 곧바로 잠겼습니다. 기존 잠금 조건 두 개가 모두 커넥터 생성 중을 놓칩니다 —
  커넥터가 아직 만들어지지 않아 발견된-커넥터 검사가 비어 있고 스택 단계도 아직 `running`이
  아니며, CDC only 계획에서는 `full_load` 스텝도 `IN_PROGRESS`가 아닙니다. 시작 지점과 테이블
  집합은 Start CDC를 누른 순간 확정되므로, 이제 그 시점에 선택이 잠깁니다 — 테이블 선택기가
  이미 사용하던 `cdc_streaming_started` 신호를 그대로 씁니다. 인프라 생성 중에는 의도적으로
  잠기지 않습니다: `create_stack`은 MSK·네트워킹·플러그인만 만들고 커넥터를 만들지 않으므로,
  15~20분 동안 아무것도 확정되지 않고 스트리밍도 없어 계획을 바꿀 수 있어야 합니다. 함께 고친
  사항: 잠금 설명이 job manager 없이 따로 재계산되어, 새 조건으로 잠길 때 타일은 비활성인데
  이유가 표시되지 않았습니다. 이제 비활성 상태와 설명이 한 번의 계산에서 나와 서로 어긋날 수
  없습니다.

## v0.1.236

### 수정

- **Start / Re-run Full Load 버튼이 두 번 눌러야 반응하는 것처럼 보였습니다.** CDC를 포함하는
  계획에서는 Data Migration 화면 렌더 약 0.05초 후 계정 전체 CDC 탐색이 실행되어 워커 스레드에서
  AWS를 조회하고, 끝나면 화면 전체를 무조건 다시 그렸습니다. 이때 모든 위젯이 재생성되므로,
  렌더와 재렌더 사이에 누른 클릭은 이미 사라진 요소로 전달되어 조용히 유실됐습니다. 이제 탐색
  결과가 실제로 바뀐 경우에만 다시 그립니다 — 재방문 시에는 보통 같은 스택과 커넥터를 찾으므로
  재렌더가 아무 의미가 없었습니다. 실제로 바뀐 경우에는 여전히 다시 그리므로 중복 MSK 방지
  안내는 제때 나타납니다. Full load only는 원래 해당되지 않았습니다(탐색이 실행되지 않음).
- **복원된 CDC only 세션에서 CDC를 실행한 적 없는데 "CDC: DONE"으로 표시됐습니다.** 배지의
  상태값은 모든 마이그레이션 타입이 공유하는 단일 `full_load` 워크플로 스텝에서 왔고, 워크플로는
  통째로 저장·복원되므로, 이전에 Full Load를 완료한 세션이 "CDC" 라벨에 그 Full Load의 DONE을
  달고 돌아왔습니다. 한 단계의 이름에 다른 단계의 값을 붙이는 것은 v0.1.231에서 라벨로 대체하려
  했던 모호한 "DONE"보다 더 나쁩니다. 이제 CDC only는 별도로 관리되는 `cdc` 스텝을 읽어
  NOT_STARTED와 IN_PROGRESS 사이를 오갑니다 — 연속 복제는 완료 개념이 없고 명시적 Stop/Delete로만
  끝나므로 이것이 CDC의 실제 동작입니다. 표시만 바뀌며 `full_load` 스텝은 여전히 Validation
  게이트입니다.
- **CDC 단계에 CDC 인프라 카드가 두 번 표시됐습니다.** v0.1.235에서 그곳에 준비 섹션 호출을
  추가했지만, 해당 단계의 라이프사이클 카드가 스택이 없을 때 이미 같은 BYO-VPC 배포 폼(또는
  adopt 선택)을 렌더하므로 동일한 폼이 두 번 나왔습니다. 준비 섹션은 *추가* 진입점이며 —
  15~20분 걸리는 MSK 생성을 Full Load와 겹치게 하려고 Prerequisites에 제공됩니다 — 겹칠 Full
  Load가 없는 CDC only에서는 계속 억제됩니다.

## v0.1.235

### 변경

- **CDC only에서는 CDC 인프라 카드가 Prerequisites 대신 CDC 단계에 표시됩니다.**
  Prerequisites에 둔 이유는 15~20분 걸리는 MSK 생성을 Full Load와 겹치게 하기 위함인데,
  Full Load가 없는 CDC only에는 해당하지 않습니다. 그대로 두면 하나의 연속된 작업이 두 섹션으로
  쪼개져, 사용자가 Prerequisites에서 인프라를 만든 뒤 Start CDC를 다른 섹션에서 찾아야 했습니다.
  이제 시작 지점 결정보다 앞에 배치됩니다 — 스택 없이는 이후 단계를 진행할 수 없기 때문입니다.
  Full load + CDC는 그대로입니다: Prerequisites 배치와 그 이유인 오버랩을 유지합니다. 카드는
  마이그레이션 타입별로 정확히 한 곳에만 표시되며, 두 곳에 동시 표시되지 않습니다 — 과금이
  발생하는 배포 폼이 두 번 보이면 안 되기 때문입니다.

## v0.1.234

### 수정

- **CDC 인프라를 조작한 직후 CDC 섹션이 접혀 다음 행동이 숨었습니다.** 인프라 카드는
  Prerequisites 하단에 있어 cdc-stack 배포·삭제가 거기서 일어나는데, CDC 섹션은 활성
  서브스텝이 `cdc`일 때만 열리고 이를 옮기는 로직이 없었습니다. 두 경우가 여기 해당했습니다 —
  **CDC only에서 인프라가 준비된 상태**: "CDC infrastructure is ready"는 Prerequisites 안에
  표시되는데 Start CDC는 접힌 CDC 섹션 안에 있었습니다. **삭제를 제출한 직후**: 작업 중인데도
  화면이 Prerequisites로 되돌아갔습니다. 기존에 CDC 실행 중인 경우를 처리하는 고정 로직이
  있었지만 커넥터 존재를 조건으로 해서, 커넥터가 아직 없는 이 두 경우를 놓쳤습니다. 이제 인프라
  생성·삭제가 진행 중일 때, 그리고 CDC only에서 인프라가 준비된 때도 CDC 섹션에 고정합니다.
  Full load + CDC는 준비 상태만으로 전환하지 않습니다 — 완료된 Full Load는 결과를 화면에
  남기고 "Continue to CDC"로 진행하는 설계이므로, 여기서 전환하면 스냅샷의 행 수와 워터마크가
  시야에서 사라집니다.
- **준비 완료 안내가 CDC only 사용자에게 "Full Load 이후" 스트리밍을 시작하라고 했습니다.**
  그 계획에는 Full Load가 없어 충족되지 않은 사전 조건처럼 읽혔습니다. 이제 CDC 단계의
  Start CDC를 가리킵니다.

## v0.1.233

### 추가

- **Full Load 사전 검사가, 소스가 채울 수 없는 target의 NOT NULL 컬럼을 로드 중이 아니라
  로드 전에 잡습니다.** Full Load는 INSERT 컬럼 목록을 소스 테이블에서 만들므로, target에만
  있는 컬럼(예: Schema Conversion에서 target DDL을 편집하며 추가한 컬럼)은 INSERT에 포함되지
  않습니다. 그 컬럼이 nullable이거나 DEFAULT가 있거나 identity면 문제없습니다(라이브 클러스터로
  확인: 로드가 NULL/기본값으로 채웁니다). 하지만 DEFAULT 없는 `NOT NULL` 컬럼이면 치명적입니다 —
  넣을 값이 없어 target에 일부 데이터가 적재된 뒤 not-null 위반으로 실패합니다. 새 검사
  `TARGET_COLUMNS_LOADABLE`(필수)는 target의 값-필수 컬럼에서 소스 컬럼을 뺀 나머지에만
  실패합니다 — 소스에도 있는 컬럼은 INSERT가 채우므로 걸리지 않습니다. target 테이블이
  없거나 읽을 수 없으면 `TARGET_SCHEMA_READY`에 맡기고 통과시켜, 같은 원인을 이중으로 보고하지
  않습니다.

## v0.1.232

### 추가

- **설정한 모델을 계정에서 사용할 수 없으면 AI assist가 Claude Sonnet 4.6으로 대체합니다.**
  Bedrock의 `global.` 추론 프로필이 리전에서 `ACTIVE`라는 것은 그 계정이 호출할 수 있다는
  뜻이 아닙니다. 모델 접근 권한은 계정별로 부여되므로, 새 계정에서는
  `global.anthropic.claude-sonnet-5`가 활성 상태로 보이면서도 model-not-enabled 오류가
  납니다. 이 경우 "Verify AI access"가 막다른 길이 되어 모델 ID를 직접 고쳐 넣는 수밖에
  없었습니다. 이제 사전 검사가 대체 모델로 재시도하고, 실제로 응답한 모델이 무엇인지와 원래
  선택한 모델을 되살리는 방법을 함께 알려줍니다. 의도적으로 좁게 적용합니다 — 재시도는
  model-not-enabled 실패에만 발동합니다. IAM 권한 부족, 스로틀링, 연결 오류는 호출자나
  네트워크의 문제이므로 다른 모델로 재시도해도 지연만 늘고 실제 원인이 묻힙니다. 대체 모델로
  통과한 경우는 초록색 성공이 아니라 경고로 표시합니다 — 정상 통과처럼 보이면 사용자가 고른
  모델이 동작한다고 오해하게 됩니다.

## v0.1.231

### 수정

- **마이그레이션 타입을 전환하면 "Success" 헤더 옆에 빨간 "Migration failed" 배너가 남았습니다.**
  행이 격리된 Full Load 이후 CDC only를 선택하면 판정이 세 개나 동시에 표시됐습니다 — 헤더는
  `Success`, 상태는 `DONE`, 배너는 여전히 `Migration failed`. 사용자는 "성공했는가"에 대한
  답을 얻을 수 없었습니다. 배너가 마지막 오류 메시지만 보고 렌더되었고, 타입을 전환해도 그
  메시지를 지우는 경로가 없었기 때문입니다. 다만 이 메시지는 불필요한 잡음이 아닙니다 —
  target에서 실제로 누락된 행을 알려주며, CDC는 이후 변경만 스트리밍하고 Full Load의 공백을
  메우지 않으므로 CDC를 시작하려는 사람이 반드시 알아야 하는 사실입니다. 그래서 다른
  마이그레이션 타입에서 기록된 오류는 지우지 않고 error에서 warning으로 낮추어, 이전 작업에서
  이어진 상황임을 밝히고 해결 방법(Full Load 재실행)을 함께 안내합니다. 출처를 알 수 없는
  오류(이전 세션에서 복원된 경우)는 조용히 완화하지 않고 error로 유지합니다.

### 변경

- **Data Migration 상태 배지가 어느 단계의 상태인지 함께 표시합니다.** 예를 들어 그냥 `DONE`
  대신 `Full Load: DONE`으로 표시합니다. 모든 마이그레이션 타입이 하나의 내부 단계를 공유하므로,
  Full Load를 끝낸 뒤 CDC only로 전환하면 CDC가 완료된 것처럼 읽혔습니다. Full load + CDC의
  경우 실제 진행에 따라 표시되며, 파이프라인이 실제로 스트리밍할 때만 `CDC`가 됩니다.

## v0.1.230

### 수정

- **v0.1.229를 기존 스택에 적용할 수 없었습니다.** ALB에 이름을 지정하면(Cognito 로그인 수정)
  ALB가 교체되는데, CloudFormation의 교체는 "새로 만들기 → 참조 재지정 → 옛것 삭제" 순서로
  진행됩니다. 타깃 그룹에는 이름이 없어 변경 사항이 없었으므로 **재사용**되었고, 새 리스너가
  옛 ALB에 아직 붙어 있는 그룹을 붙이려 했습니다. ELBv2는 타깃 그룹을 하나의 로드 밸런서에만
  연결할 수 있으므로
  `The following target groups cannot be associated with more than one load balancer`
  (`ServiceLimitExceeded`)로 실패하고 롤백됐습니다 — 신규 배포는 되지만 기존 스택은 모두 옛
  릴리스에 갇히는 상태였습니다. 이제 타깃 그룹에 `${AWS::StackName}-tg` 이름을 지정합니다.
  이 속성도 create-only이므로 같은 업데이트에서 타깃 그룹이 함께 교체되고, 새 리스너는 어떤
  로드 밸런서도 갖고 있지 않은 그룹을 붙입니다. 실제 스택을 업그레이드해 검증했습니다 —
  변경 세트에 `TargetGroup`이 나타나고(이전에는 아예 없었습니다), 3초 만에 실패하던 리스너가
  통과해 스택이 `UPDATE_COMPLETE`에 도달했습니다. 일회성 대응이 아니라 구조적 수정입니다 —
  앞으로 ALB가 교체되는 어떤 변경(예: 역시 create-only인 `AlbScheme` 변경)도 같은 벽에
  부딪혔을 것입니다. 다만 타깃 그룹에 고정 이름이 붙으므로, 이후 그룹의 create-only 속성
  (`AppPort`, `VpcId`, `TargetType`)을 바꾸면 교체되는 대신
  `DuplicateTargetGroupName`으로 실패합니다 — 이름을 지정한 ALB에서 이미 받아들인 것과 같은
  트레이드오프입니다.

## v0.1.229

### 수정

- **이미 선택한 프라이머리 키를 가진 빈 대상 테이블을 "키를 적용하려고" 다시 만들었습니다.**
  Schema Conversion에서 복합 키를 선택하고 Apply all to target을 실행한 뒤 첫 Full Load를
  시작하면, 방금 그 키로 만들어진 테이블에 대해 확인 대화 상자가
  `1 empty table will be recreated to apply the chosen primary key`라고 안내했습니다.
  모순인 데다 불필요한 DROP+CREATE가 실행됩니다(DSQL은 트랜잭션당 DDL이 1문장이므로 테이블마다
  별도 왕복입니다). 대화 상자의 안내와 엔진의 승격 판정이 모두 적용된 DDL과 **소스** 키만
  비교하고 대상의 실제 키는 읽지 않았습니다 — 바로 아래 append 경로는 이미 읽고 있었는데도
  그랬습니다. 이제 둘 다 대상의 실제 프라이머리 키를 확인해 이미 일치하면 재생성을
  건너뜁니다. 키는 대화 상자가 열리기 전에 이미 실행되는 프로브가 한 번에 함께 읽으므로 화면을
  그리는 경로에는 대상 조회가 추가되지 않습니다. 의도적으로 비대칭입니다 — 확실히 일치할 때만
  건너뛰고, 키를 읽을 수 없으면 "안전"이 아니라 "알 수 없음"으로 보아 재생성합니다.

- **Cognito 로그인이 아예 되지 않았습니다 — ALB와 앱 클라이언트가 콜백 URL의 대소문자를
  서로 다르게 봤습니다.** `EnableCognitoAuth=true`로 배포하면 로그인이 항상 실패하고
  hosted UI가 `Client is not enabled for OAuth2.0 flows.` 메시지와 함께 되돌려보냈습니다.
  `AllowedOAuthFlowsUserPoolClient`는 처음부터 끝까지 `true`였는데도 그랬습니다. ALB에
  이름을 지정하지 않으면 CloudFormation이 대소문자 섞인 이름
  (`mysql--LoadB-u9DQdeKlckt9`)을 만들고 `DNSName`이 그 대소문자를 그대로 물려받습니다.
  앱 클라이언트의 `CallbackURLs`는 `GetAtt DNSName`으로 만들어지니 역시 대소문자가
  섞입니다. 그런데 ALB는 OAuth `redirect_uri`의 호스트를 소문자로 변환해 보내고, Cognito는
  두 문자열을 정확히 비교합니다. `/oauth2/authorize`는 이 불일치를 허용하기 때문에 로그인
  화면은 정상 표시되고 제출만 실패했으며, 최초 로그인에서는 "비밀번호는 변경됐는데 에러가
  뜨는" 것처럼 보였습니다 — 리다이렉트가 거부된 시점에 비밀번호 변경은 이미 적용된
  뒤였기 때문입니다. 이제 ALB 이름을 `${AWS::StackName}-alb`로 지정하므로 DNS 이름이
  (소문자인) 스택 이름을 따라가고 두 문자열이 일치합니다.

### 추가

- **스택이 첫 Cognito 로그인 사용자를 생성합니다.** 사용자 풀은 `AllowAdminCreateUserOnly`를
  설정하므로, 사용자가 없으면 `EnableCognitoAuth=true` 배포가 성공하고도 아무도 로그인할 수
  없는 앱을 돌려줬습니다. 새 필수 파라미터 `CognitoAdminEmail`이 그 사용자를 만들고 Cognito가
  임시 비밀번호를 이메일로 보냅니다. 값이 없으면 템플릿 `Rules` 단정문이 배포 전에 거부합니다.
  사용자를 더 추가할 수 있도록 풀 ID도 출력하며(`CognitoUserPoolId`),
  `CognitoHostedUiDomain`은 접두사만이 아니라 전체 로그인 URL로 바뀌었습니다.

### 변경

- **배포 가이드에 스택 이름 제약을 명시했습니다.** ALB 이름이 스택 이름에서 파생되므로 스택
  이름은 소문자로 28자 이내여야 합니다. 더 길면 약 2분간 롤백한 뒤에야
  `The load balancer name '<스택이름>-alb' cannot be longer than '32' characters`로
  실패하고, 대문자가 섞이면 위와 같이 Cognito 로그인이 깨집니다.

## v0.1.228

### 수정

- **재구성된 소스 DDL이 문자열 DEFAULT를 인용부호 없이 렌더해 유효하지 않은 SQL이 되던 문제.**
  `information_schema.COLUMN_DEFAULT`는 값을 인용부호 없이 저장하는데 그것을 그대로 출력해서, MySQL이
  `DEFAULT 'pending'`으로 표시하는 컬럼이 `DEFAULT pending`으로 나왔습니다. 이 패널에는 **Copy Source
  DDL** 버튼이 있으므로 복사해 준 텍스트를 실행할 수 없었습니다 — MySQL은 인용 없는 `pending`을 컬럼 참조로
  해석합니다. 최초 공개 릴리스부터 있던 문제입니다.

  이제 인용 여부를 `default_is_expression`으로 결정합니다 — 컨버터가 쓰는 것과 같은 신호입니다. MySQL의
  `DEFAULT_GENERATED` 플래그에서 오며, 리터럴 문자열 `'CURRENT_TIMESTAMP'`와 같은 이름의 함수 호출을
  구분할 수 있는 유일한 근거입니다(값의 모양으로는 구분 불가). 값에 포함된 인용부호도 이스케이프합니다.
  라이브 소스로 검증했습니다: 재구성된 `ecommerce.orders` DDL이 이제 MySQL에서 실행되고 컬럼이 동일하게
  라운드트립됩니다.

## v0.1.227

### 수정

- **Schema Conversion 화면이 Evaluation에서 이미 지적한 것들에 대해 침묵하던 문제.** 워크숍에서
  `AUTO_INCREMENT`로 제기됐고, 수집되는 모든 필드와 모든 assessor 규칙을 변환 결과와 대조한 결과 **9건**의
  동일한 형태를 발견했습니다 — Evaluation에서는 문제를 알려주고, 변환 화면에서는 깨끗해 보이는 상태입니다.

  재구성된 **소스 DDL**이 이제 타겟이 재현할 수 없는 것들을 표시합니다: `AUTO_INCREMENT`,
  `ON UPDATE CURRENT_TIMESTAMP`, `COLLATE`, `FULLTEXT`/`SPATIAL` 인덱스 종류, 그리고 생성 컬럼과 네이티브
  파티셔닝 마커(둘 다 boolean만 수집되므로 구문을 만들어내지 않고 표시만 합니다). 일반 테이블은 이전과 동일하게
  렌더됩니다.

  **변환 경고**가 아무 알림도 없던 6개 규칙을 커버합니다:

  - **FULLTEXT / SPATIAL 인덱스** → 같은 컬럼에 일반 `CREATE INDEX ASYNC`로 나가, 일반 인덱스와 DDL이
    동일합니다. 인덱스는 생성되지만 `MATCH … AGAINST`는 이를 쓸 수 없습니다. Evaluation은 UNSUPPORTED /
    SIGNIFICANT로 평가합니다.
  - **네이티브 파티셔닝** → 올바르게 제거되지만(DSQL은 기본 키로 분산), 파티션 범위 SQL과
    `DROP`/`TRUNCATE PARTITION` 아카이빙은 이전되지 않습니다.
  - **255 컬럼 한도**, **24 인덱스 한도**(기본 키 포함) → 하드 한도이므로 *apply에서 거부*됩니다. 인덱스는
    테이블 생성 성공 후 실패해 타겟이 부분 인덱싱 상태로 남습니다.
  - **대용량 LOB/TEXT** → DDL은 문제없고, ~1 MiB 한도가 마이그레이션 중 **행 단위로** 작용해 초과 값은
    영구히 드롭됩니다.
  - **생성 컬럼** → 일반 컬럼이 됩니다. Full Load가 소스가 계산한 값을 복사하므로 타겟은 처음엔 정확하고,
    값을 공급하지 않는 첫 쓰기부터 어긋납니다.
  - **대소문자 무시 collation** → DSQL은 대소문자를 구분하므로 equality, `LIKE`, `ORDER BY`, `UNIQUE`
    동작이 바뀌는데 행 수와 체크섬은 계속 일치합니다. `_ci`만 보고하고 `_cs`/`_bin`은 이미 타겟과 동일합니다.
  - **`ON UPDATE CURRENT_TIMESTAMP`** → `DEFAULT`는 유지되지만 `UPDATE` 시 값을 갱신하는 것이 없어
    `updated_at`이 삽입 시점에 고정됩니다.

  생성되는 DDL은 하나도 바뀌지 않았습니다 — 알림입니다. 한도와 타입 집합은 assessor에서 가져오고, 두 엔진을
  같은 테이블에 돌려 규칙이 발동했는데 변환 알림이 없으면 실패하는 테스트를 추가했으므로 향후 규칙이 같은
  공백으로 회귀할 수 없습니다.

## v0.1.226

### 수정

- **Schema Conversion 트리가 Evaluation 보고서에서 이미 명명한 객체를 다른 이름으로 표시하던 문제.**
  Evaluation은 **Stored procedures**와 **Functions**로 나열하는데(`KIND_LABELS`), 오브젝트 브라우저는
  둘을 **Routines (n)** 노드 하나로 묶었습니다 — 같은 객체가 다음 화면에서 다른 이름으로 나타나고, 한 화면의
  목록을 다른 화면과 대조할 방법이 없었습니다. 워크숍에서 발견됐습니다.

  이제 트리가 이를 분리하고, 제목을 `assessor.KIND_LABELS`에서 가져옵니다 — Evaluation 목록, UI 차트 축,
  HTML 내보내기가 이미 공유하는 그 매핑입니다. 여기서 레이블을 하드코딩한 것이 두 화면이 어긋난 원인이었습니다.
  introspector는 처음부터 `PROCEDURE`와 `FUNCTION`을 구분했으므로, 트리는 이미 가진 정보를 버리고 있었던
  것입니다. MySQL이 둘 중 어느 것으로도 보고하지 않는 루틴은 명명된 종류로 단정하지 않고 계속 **Routines**로
  묶입니다.

  노드 ID는 그대로입니다(종류와 무관하게 `routine:<name>`): 선택 해석 시 파싱되고 체크/생성 집합이 렌더 간에
  이를 통해 유지되므로, 종류별로 키를 바꾸면 복원된 선택과 DDL 생성 범위가 조용히 무효화됩니다.

## v0.1.225

### 수정

- **"Generate DDL for selected"를 누르면 오브젝트 브라우저가 초기 상태로 모두 접히던 문제.** 스키마를 펼쳐
  테이블을 찾아 체크한 뒤 생성하면 트리가 닫혀 버려서, 방금 작업하던 행들이 사라졌습니다. 트리는 매 렌더마다
  (Generate, apply, 진행 폴링) 다시 만들어지고 NiceGUI는 열림/닫힘 상태를 클라이언트 측에 두므로, Python에서
  복원하지 않은 것은 접힌 상태로 돌아갑니다. 체크된 집합은 이미 렌더 간에 유지되고 있었고, 펼침 상태가 빠진
  나머지 절반이었습니다. 이제 소스·타겟 두 브라우저 패널 모두 이를 기록하고 복원합니다. "Clear"는 생성된 DDL,
  편집, AI 제안은 계속 버리지만 — 어디까지 탐색했는지는 분석 결과가 아니므로 유지합니다.

## v0.1.224

### 수정

- **캐시된 identity 기본 키가 Full Load 이후 시퀀스를 전진시키지 않아, cut-over 후 애플리케이션의 첫
  INSERT가 중복 키로 실패하던 문제.** 컨버터의 identity 전략은 `GENERATED BY DEFAULT AS IDENTITY`를
  emit하고, `BY DEFAULT`는 바로 Full Load가 소스의 원래 키 값을 쓸 수 있게 해 주는 부분입니다. 그런데
  **명시적으로 지정된 값은 시퀀스를 전진시키지 않습니다.** 그래서 적재 후에도 시퀀스는 시작점에 남아 있고
  그 값들은 이미 점유된 상태였습니다.

  실제 `ap-northeast-2` 클러스터에서 재현했습니다: id 1~3을 적재한 뒤 id 없는 INSERT를 실행하면
  `duplicate key value violates unique constraint`가 발생합니다.

  이것은 마이그레이션 실패 중 가장 나쁜 형태였습니다. 행 수와 체크섬이 **일치**하므로 Validation은 깨끗하게
  통과하고, cut-over 후 — 소스를 이미 정지시켜 롤백이 간단하지 않은 시점에 — 비로소 드러납니다.

  이제 Full Load가 적재한 행보다 앞으로 각 타겟 identity 시퀀스를 전진시키고
  (`ALTER TABLE … ALTER COLUMN … RESTART WITH max(pk)+1`, 이 역시 라이브 검증) 활동 로그에 기록합니다.
  **완전히 끝난** 적재 후에만 실행됩니다 — `MAX(pk)`가 시퀀스가 넘어서야 할 값이므로 부분 적재에서 동기화하면
  아직 들어올 행과 충돌이 남을 수 있습니다. 적재를 완료시키는 재시도는 동기화를 수행합니다. 일반 정수 키
  (`KEEP_INTEGER` 기본값)를 쓰는 테이블은 시퀀스가 없어 건드리지 않고, 빈 테이블도 마찬가지입니다 —
  NULL인 `MAX(pk)`에서 재시작하면 시퀀스를 오히려 뒤로 옮기게 됩니다. 이 단계가 실패해도 적재는 실패로
  처리하지 않지만, 미보정 시퀀스는 cut-over 후 장애이므로 정확한 수동 명령과 함께 FAILURE로 기록합니다.

## v0.1.223

### 변경

- **CloudFormation 템플릿의 기본 앱 이미지를 `0.1.222`로 갱신**했습니다(기존 `0.1.209`, 13개 릴리스
  뒤처짐). 이것은 새로 `git clone`한 사용자가 배포할 때 쓰이는 기본값이므로, 방치하면 새 배포가 조용히 옛
  앱을 올립니다 — 0.1.218~0.1.222의 Start CDC 재시작 수정과 validation/cut-over 수정이 빠진 상태로.

## v0.1.222

### 수정

- **여러 스택을 대상으로 한 Start over teardown이 첫 스택만 표시하고, 나머지가 삭제 중인데 조용해지던 문제.**
  v0.1.214에서 Start over가 발견된 **모든** cdc-stack을 삭제하도록 했지만, durable teardown 마커는 슬롯이
  하나이고 첫 job만 그것을 차지합니다 — 그래서 배너는 스택 1만 추적하고 그것이 끝나는 순간 사라졌으며,
  나머지는 여전히 삭제 중이고 MSK / NAT 과금이 계속되는데 화면에는 아무것도 없었습니다. 이제 배너는 실행된
  모든 teardown의 큐를 따라갑니다: 추적 중인 스택이 끝나면 다음 미완료 스택으로 넘어가고, 여러 개가 대기
  중이면 어느 것인지 표시합니다(*"Deleting 'cdc-b' (2 of 3; the rest follow)"* — 그리고 마지막 스택에서는
  *"(3 of 3, the last one)"*, 운영자가 기다릴지 판단하는 지점이므로).

- **완료된 teardown이 새로고침 후 아무 흔적도 남기지 않던 문제.** 완료는 `ui.notify` 토스트로만 알렸고,
  이는 `ui.timer`에 매달려 페이지와 함께 사라집니다 — 15~45분이 걸리고 자리를 떠나도록 설계된 작업인데
  *"완료됨"*과 *"애초에 실행 안 됨"*을 구분할 방법이 없었습니다. 이제 배너가 결과를 durable하게 표시하고
  (**"CDC infrastructure deleted — MSK / NAT billing has stopped"**, 모든 스택 이름 포함) 운영자가
  **✕** 버튼으로 닫을 때까지 유지합니다. 자동으로 숨지 않으며, 진행 중인 teardown이 있으면 이전 완료
  알림보다 우선합니다.

## v0.1.221

### 수정

- **남은 차이가 DSQL이 저장할 수 없는 행뿐일 때 cut-over에 도달할 수 없었던 문제 — 게이트 문구는 가능하다고
  약속하면서.** 메시지는 *"Cut over only when Validation reports a clean MATCH (**or every
  difference is explained**)"*라고 했지만, 게이트는 단순 일치인 `ready_for_cutover`를 검사했습니다.
  "explained" 경로는 어디에도 없었습니다. 그래서 유일한 발견이 영구 격리된 행(DSQL의 값당 ~1 MiB 한도
  초과)인 마이그레이션은 워크플로를 끝낼 수 없었고, 재적재로도 고칠 수 없었습니다 — DSQL이 그 값을 아예
  저장할 수 없기 때문입니다. 이 단계는 운영자의 결정이 아니라 설계상 도달 불가였습니다.

  약속된 경로를 명시적 사인오프로 구현했습니다: 모든 차이가 마이그레이션이 이미 보고한 드롭 행과 정확히
  일치하면, 해당 단계가 **"Accept the N-row gap and continue to cut-over"**를 제시하며 테이블,
  행 수, 그리고 재적재로는 바뀌지 않는다는 사실을 함께 알립니다. 수락하면 런북이 해제되고, 대안(소스 값을
  고쳐 Validation 재실행으로 완전 일치)도 함께 제시됩니다.

  **자동 해제하지 않습니다** — 그 행들은 실제로 대상에 없고 이는 운영자의 판단입니다 — 그리고 수락 후에도
  런북은 완전 일치처럼 읽히지 않도록 **"Cutting over with an accepted gap"**으로 시작합니다. 가장
  중요한 순간에 사인오프가 조용히 잊히지 않도록.

  사인오프는 부족분이 **전부** 설명될 때만 제시됩니다. 설명되지 않은 불일치, 비교조차 못 한 테이블, 또는
  설명된 테이블 하나가 진짜 잘못된 테이블과 함께 있는 경우 모두 cut-over를 막고, 수락이 이후의 더 나쁜
  실행으로 전이되지 않습니다.

## v0.1.220

### 수정

- **Validation이 마이그레이션에서 이미 드롭한 행을 설명되지 않은 실패로 보고하며, 같은 화면에서 스스로
  모순되던 문제.** 한 테이블이 격리된 행 수만큼 정확히 부족한 상황에서 패널은 **Not ready for cut-over —
  "1 of 8 table(s) did not pass. Review the failing checks"**와 그 행들을 세는 빨간 **Failed** 검사
  2개를 표시했습니다 — 바로 아래 테이블별 항목은 *"Fully explained: 3 rows were permanently dropped …
  this deficit is expected, not new data loss."*라고 적혀 있었는데도요. 검토자는 이미 발견되고 보고되고
  Full Load 단계에서 명시적으로 수락된 결함을 조사하도록 내몰렸습니다.

  테이블별 모델은 이미 이를 알고 있었지만(`deficit_explained_by_quarantine`) 집계하는 곳이 없어 요약과
  준비 상태 검사가 그것을 보지 못했습니다. 이제 봅니다:

  - **판정**이 실제로 남은 것을 말합니다 — *"Cut-over blocked only by rows dropped during the
    migration … Nothing unexplained"* — 그리고 실제 선택지 두 개를 제시합니다: 소스 값을 고쳐 재적재,
    또는 갭을 의도적으로 수락.
  - **준비 상태 검사**가 원인을 문구에 담고 **Failed**에서 **Heads-up**으로 내려갑니다.

  의도적으로 **통과로 보고하지 않습니다**: 그 행들은 실제로 대상에 없으므로, cut-over는 툴이 통과시켜 줄
  것이 아니라 운영자가 내려야 할 결정으로 남습니다.

  완화는 부족분이 **전부** 설명될 때만 적용됩니다. 1행을 드롭했는데 3행이 부족한 테이블은 강한 실패로
  유지되고, 설명된 테이블 하나가 진짜 불일치 테이블과 함께 있는 실행도 빨갛게 남습니다 — 알려진 손실 뒤에
  실제 손실을 숨기는 것은 이 귀속 로직이 절대 만들어서는 안 되는 결과입니다.

## v0.1.219

### 수정

- **Validation 진행 중 패널에서 Cancel 버튼의 툴팁이 레이블로 렌더링되던 문제.** 문장 전체 —
  *"Stop the comparison. Tables not yet started are skipped; the ones already running finish
  first. Read-only, so nothing is left half-changed."* — 가 버튼 캡션이 되어 버튼이 패널 전체로
  늘어나고, 실제 동작을 나타내는 문구("Cancel validation")는 사라졌습니다.

  원인: NiceGUI의 `Element.tooltip()`은 툴팁을 생성한 뒤 체이닝을 위해 **`self`**(소유 엘리먼트)를
  반환합니다 — 툴팁을 돌려주지 않습니다. 따라서 그 반환값을 받아 `set_text()`로 실행 중/중지 중 문구를
  교체하려던 코드가 실제로는 **버튼의** 텍스트를 바꾸고 있었습니다. 이제 버튼 컨텍스트 안에서
  `ui.tooltip()`으로 핸들을 얻습니다(이쪽은 툴팁 엘리먼트를 반환). 텍스트는 여전히 제자리에서
  교체되므로 폴링이 hover 중인 툴팁을 파괴하지 않습니다.

  이 문제가 테스트에서 보이지 않았던 이유는 NiceGUI 더블이 `Element.tooltip()`을 툴팁 객체를 반환하는
  것으로 모델링했기 때문입니다 — 실제 API와 정반대입니다. 그래서 잘못된 호출이 테스트에서는 올바르게
  보이면서 화면에서는 깨져 있었습니다. 이제 더블이 실제 반환값을 반영하고, 회귀 테스트가 툴팁 문구가
  버튼 텍스트로 절대 나타나지 않음을 검증합니다. 버그를 되살려 확인: 테스트 3개가 실패합니다.

## v0.1.218

### 수정

- **Full Load 작업 레코드가 사라진 상태에서 Stop 후 Start CDC가 비활성 — 실제로는 완벽히 이어갈 수 있는데도.**
  버튼의 준비 조건이 시작 지점(Full Load 워터마크 또는 수동 입력 좌표)을 요구했는데, 워터마크는 Full Load
  **작업 레코드**에서 읽습니다. 그래서 앱 재시작(작업 레코드 소멸) 후나 CDC 전용 세션에서는 워터마크가 없어
  Start CDC가 *"Set the CDC start point above first"*와 함께 비활성화됐습니다.

  실제로 잃은 것은 없었습니다. CDC 중지는 커넥터 2개만 삭제합니다: 소스 커넥터의 오프셋 토픽은 인스턴스별
  UUID 토픽이 아니라 고정 이름(`<stack>-debezium-source-offsets`)으로 핀되어 있어 Stop을 넘어 살아남고,
  다음 Start에서 seeder가 그 오프셋을 읽어 워터마크 이상이면 재시딩을 *건너뜁니다*. 중지된 바로 그 지점부터
  스트리밍이 재개됩니다. 즉 게이트가 백엔드는 이미 지원하는 재시작을 막고 있었고, 운영자를 binlog 좌표
  수동 입력이나 Full Load 전체 재실행으로 몰아넣고 있었습니다 — 커넥터가 여전히 갖고 있던 위치를 되찾기 위해.

  이제 스택에 커밋된 재개 오프셋이 있으면 Start CDC가 함께 해제됩니다. 신호는 `MskBootstrapServers`가 빈
  값이면서 `DeploySink=true`인 조합이고, 이는 모호하지 않습니다: 인프라 생성은 `DeploySink=false`로 고정하고,
  `true`로 바꾸는 것은 Start CDC뿐이며, Stop은 *부트스트랩만* 덮어씁니다(나머지는 `UsePreviousValue`로
  전달) — 따라서 그 조합은 "시작했다가 중지함"으로만 도달합니다. 한 번도 스트리밍하지 않은 스택은 여전히
  시작 지점을 요구하며, 이것이 최초 시작이 소스의 현재 binlog부터 시작해 Full Load 구간 전체를 조용히
  잃는 것을 막습니다.

- **재시작을 재시작으로 표현합니다.** 패널이 최초 시작 문구("…begins streaming")를 보여주고, 시작 지점 카드는
  **Action needed** 배지와 *"Automatic — needs a Full Load watermark (unavailable)"*를 제시했습니다 —
  그 아래 버튼은 활성이고 정상 동작하는데도. 재개 시에는 선택할 시작 지점이 없으므로(위치는 그 카드가 설정할
  수 없는 오프셋 토픽에 있음) 이제 재개 사실을 표시합니다: *"Resuming from the last streamed position"*.

- **Stop CDC 다이얼로그가 스트림 위치 보존을 명시합니다.** 기존에는 MSK와 플러그인이 유지되어 "so you can
  restart with Start CDC"라고만 했는데, 이는 **인프라**를 말하고 **위치**는 말하지 않아 운영자가 추측하게
  됩니다. 그 합리적 추측(커넥터를 삭제하면 위치도 사라진다)은 틀렸고, 그대로 행동하면 재적재 비용이 듭니다.
  이제 Start CDC가 중지된 바로 그 지점부터 이어지며 갭도, 재적용도, Full Load나 시작 지점 재입력도 없다는
  것을 명시하고, 중지/재시작을 자유롭게 반복할 수 있다고 안내합니다.

## v0.1.217

### 변경

- **Activity log 탭의 Download 버튼을 다시 정상 크기의 주요 액션으로 되돌렸습니다.** 이전 릴리스에서
  공용 폼 행을 통과시켰는데, 그러면 버튼이 우측 **컨트롤 슬롯**에 들어갑니다 — 숫자 입력 크기에 맞춰져
  있고, 입력 **칼럼**이 정렬되도록 우측 정렬된 슬롯이라 버튼 하나에는 의미가 없습니다. 결과적으로 작은
  버튼이 오른쪽 끝에 떨어져 있고 설명이 그 아래로 감겼습니다. 이 탭은 필드 모음이 아니라 액션이므로,
  이제 설명이 있는 섹션 아래에 액션이 놓이는 형태이고, 맨 동사가 아니라 대상을 명시합니다
  ("Download activity log").

- **Settings 헤더의 주의 문구를 회색 미세 텍스트에서 info 알림으로 승격했습니다.** 이 문구는 필요합니다 —
  실제 실수를 막습니다: 값을 조정하고 자리를 떠난 운영자는 그 값이 유지된다고 가정하지만, 재시작(본인이
  시작하지 않은 Fargate 태스크 교체 포함)이 일어나면 조용히 배포 시점 기본값으로 되돌아가, 다음 실행이
  이유 없이 다르게 동작합니다. 제목 아래 캡션 상태로는 상투적 문구로 읽혀 건너뛰어졌습니다. 이제 결과를
  앞세우고("These settings are not permanent") 영속적인 대안(배포에서 `DSQL_MIGRATOR_*` 환경
  변수 설정)을 명시합니다.

- **Start CDC가 정상 경로에서 같은 비중의 알림 2개를 쌓지 않습니다.** "Ready to start CDC" 아래에
  "This table set is now fixed" 헤더의 전체 너비 파란 박스가 또 있었습니다 — 첫 시작이라는 평범한
  상태에 알림이 2개였고, 운영자가 실제로 찾는 한 줄(**어떤** 테이블이 스트리밍되는지)이 MSK 파티션
  회계에 관한 문단 속에 묻혔습니다. 이제 테이블 목록은 체크 글리프 옆의 일반 텍스트입니다 — 검증 가능한
  사실이므로 **계속 보이게** 두었고 — 불변성 근거와 범위 변경 방법은 info 툴팁으로 옮겼습니다(많아야
  한 번 필요한 배경 지식). 재시작 경고(amber)는 그대로입니다: 반복적인 start/stop이 회수되지 않는 MSK
  용량을 실제로 소모하므로 그것은 진짜 경고입니다.

## v0.1.216

### 변경

- **Settings 다이얼로그가 탭 전환 시 더 이상 크기가 변하지 않습니다.** 패널 컨테이너가 min/max 높이
  범위를 써서 각 탭의 자연 높이를 따랐고(Full Load는 노브 3개, Validation은 1개), 카드가 전환마다
  커지고 작아졌습니다. 중앙 정렬 다이얼로그는 중심을 기준으로 배치되므로 **탭 스트립 자체가 포인터
  아래에서 움직였고**, 탭을 눌러 넘길 때마다 패널 전체가 튀었습니다. 이제 가장 큰 패널에 맞춘 고정
  높이라 스트립이 고정되고 내용만 바뀝니다(뷰포트 상한과 내부 스크롤은 유지 — 작은 화면에서 다이얼로그가
  화면 밖으로 밀리지 않습니다).

- **탭 순서를 마이그레이션 여정에 맞췄습니다: Full Load → CDC → Validation.** CDC는 Full Load와
  한 쌍이고(둘 다 데이터 이동 처리량), Validation은 사후 검증입니다. CDC가 마지막이었던 건 그 노브가
  나중에 추가됐기 때문일 뿐입니다. 순서는 config 레지스트리의 속성이고 탭 스트립이 그것을 따릅니다.

- **"Sink compute (MCU)"에 한 줄로 담기지 않는 도움말을 info 툴팁으로 추가했습니다:** 언제 올릴지
  (소스는 따라가는데 싱크가 밀릴 때 — **소스** MCU를 올려도 소용없음), 비용(단계마다 커넥터가 도는 동안
  계속 과금, 8이 MSK Connect API 상한), 언제 적용되는지(다음 Start CDC. 리사이즈만을 위해 재실행하는
  것은 안전 — 커넥터 용량은 in-place 업데이트라 복제 gap도, MSK 파티션 쿼터 비용도 없음). 보이는
  레이블·설명·허용값은 그대로이므로 hover 전용으로 되돌아간 것이 아니라 깊이를 더한 것입니다.

- **Activity log 탭이 다른 탭들과 통일되었습니다.** 문단 하나에 버튼이 아래 붙은 형태여서 다른 종류의
  화면처럼 보였습니다. 이제 동일한 폼 행(레이블 + 설명 + 컨트롤)이고, 행이 이미 무엇인지 말하므로 버튼은
  "Download"로 줄였습니다. 툴팁에는 ECS에서 이 파일이 임시 태스크 스토리지에 있다는 점과, durable한
  CloudWatch 사본은 Diagnostics → Mirror to stdout이라는 안내를 담았습니다.

## v0.1.215

### 변경

- **Settings에 튜닝 카테고리별 탭 — Full Load, Validation, CDC — 을 만들었습니다.** 기존에는
  "Performance" 탭 하나에 전부 들어 있었습니다. "Performance"는 운영자가 생각하는 범주가 아닙니다 —
  Full Load를 바꾸려고, 또는 CDC 싱크를 바꾸려고 옵니다. 통합 패널은 다른 그룹을 지나쳐 읽게 만들었고,
  각 그룹의 적용 시점 문구("다음 실행" vs "다음 Start CDC")가 목록 중간에 놓여 바로 뒤 필드에 대한
  설명처럼 읽혔습니다. 이제 각 탭이 자기 적용 시점으로 시작하고, Full Load의 커넥션 곱셈 경고는 해당
  탭에만 표시합니다(Validation·CDC의 단일 필드 옆에서는 의미가 없습니다). 탭 목록은 config 레지스트리에서
  파생되므로, 새 그룹에 노브를 추가하면 탭이 자동으로 생깁니다.

- **모든 설정 컨트롤이 AWS 스타일(Cloudscape) 폼 필드가 되었습니다: 보이는 레이블, 설명, 허용값을
  명시한 constraint 텍스트.** 이전에는 각 노브를 한 줄에 유지하려고 설명을 **hover 전용** info
  글리프 뒤에 숨겼는데, 그래서 폼을 한눈에 읽을 수 없었고(무엇을 하는 값인지 알려면 다섯 필드를 차례로
  hover해야 했습니다) hover가 없는 터치 환경에서는 접근조차 불가능했습니다. 컨트롤은 우측 정렬해 입력
  칼럼이 패널 아래로 정렬되고, constraint는 monospace로 렌더해 허용값이 데이터로 읽힙니다. 인라인
  스타일링이 아니라 `ui/design.py`의 `form_field`(단일 소스)로 추가했습니다.

- **Diagnostics 탭도 동일한 폼 행을 사용합니다.** floating 레이블 select 옆에 맨 switch가 놓여
  하나의 폼이 아니라 무관한 위젯 둘로 읽혔습니다. 이제 둘 다 레이블과 설명을 갖습니다(stdout 미러가
  CloudWatch에 도달하는 경로라는 점도 포함 — 로그 파일 자체는 임시 태스크 스토리지에 있습니다).

- **모달 헤더가 더 이상 "changes apply to the next run"이라고 하지 않습니다** — Full Load /
  Validation 노브에만 맞는 말입니다. 이제 다이얼로그 전체에 참인 것만 표시합니다: 배포 시점 파라미터가
  아니며, 재시작하면 값이 초기화됩니다.

## v0.1.214

### 수정

- **계정에 cdc-stack이 2개 이상일 때 Start over가 "Delete all CDC infrastructure"를 제안하고
  아무것도 삭제하지 않던 문제.** 제안은 발견된 모든 스택을 세었지만, teardown은 이름을 *하나만*
  해석하고 발견된 스택이 정확히 1개일 때만 채택했습니다 — 여러 개면 이 세션의 자기 스택 이름으로
  폴백했는데, 그 분기는 애초에 프로브가 그 이름을 **찾지 못했을 때만** 도달합니다. 그래서 삭제는
  대상을 못 찾고 성공으로 보고했고, 운영자는 이를 가리키는 것이 툴에 아무것도 없는 상태로 MSK / NAT
  비용을 계속 지불했습니다. 이제 제안·다이얼로그 목록·teardown이 하나의 리졸버를 공유하며 **모든**
  스택을 처리합니다. 공유되는 source 자격증명 시크릿은 여전히 정확히 한 번만 정리합니다(스택 외부에서
  생성되므로, 스택마다 삭제를 다시 예약하면 추가 스택마다 실패합니다).

- **Start over teardown 타일이 삭제할 cdc-stack 이름을 표시합니다.** "Delete all CDC
  infrastructure"는 *무엇을* 삭제하는지 말하지 않았습니다. 스택에 소유자 태그가 없어서 계정에 다른
  창이 사용 중인 파이프라인이 있을 수 있고 툴은 이를 구분할 수 없으므로, 운영자가 안전하게 답할 수
  있는 유일한 근거가 이름입니다. 타일 위 notice에 이름이 있었지만 거기서는 삭제 대상이 아니라 질문의
  배경으로 읽힙니다. 이제 두 파괴적 타일 모두 이름을(2개 이상이면 개수까지) 담고, 문구 전체가 복수형으로
  전환됩니다.

- **Start CDC 안내가 이미 락된 상태에서 테이블을 고르라고 하던 문제.** "Pick all your tables
  before you start … Choosing everything you need up front keeps this smooth"는 카드 phase가
  `infra`일 때만 렌더되고, 이는 프로브된 `cdc_stack_phase`가 `infra`여야 하며 — 바로
  `selection_lock_reason`이 테이블 picker를 동결하는 조건입니다(이 버튼에 도달 가능한 모든 마이그레이션
  타입에서). 체크박스가 비활성인데 안내가 그것을 가리키고 있었습니다.

  이제 사실과 **실제로 통하는** 해결책을 상황별로 표시합니다: Full Load 이후에는 **Start over**만
  범위를 바꿉니다(Full Load 락 절이 우선하며 cdc-stack 삭제로 풀리지 *않습니다* — 이 수정의 첫 초안은
  운영자를 ~45분 teardown으로 보내고도 picker가 그대로 락인 상태로 남겼을 것입니다). CDC only
  세션에서는 인프라 삭제 후 재배포가 실제로 동작합니다. Full Load 이후 문구에는 집합이 고정된 **이유**도
  담았습니다 — 스냅샷과 일치해야 gapless 핸드오프가 성립합니다.

## v0.1.213

### 추가

- **CDC 싱크의 컴퓨트를 UI에서 조정 — Settings → Performance → CDC → "Sink compute (MCU)".**
  매뉴얼은 오래전부터 싱크가 따라오지 못하면 (소스가 아니라) `SinkMcuCount`를 올리라고
  안내해 왔습니다. 싱크가 파이프라인의 CPU 바운드 절반이고, 단일 태스크 Debezium 소스는
  CPU에 여유가 있기 때문입니다. 그런데 앱은 그 파라미터를 **한 번도 보내지 않았습니다** —
  `grep SinkMcuCount src/` 결과가 0건이라 모든 배포가 조용히 템플릿 기본값을 쓰고,
  `submit_update`가 `UsePreviousValue`로 그대로 이어받았습니다. 매뉴얼의 조언을 실행할
  유일한 방법이 CloudFormation 콘솔에서 스택 파라미터를 직접 수정하는 것이었고, 이는
  "핵심 기능은 모두 브라우저에서 도달 가능"이라는 원칙과 충돌했습니다. 이제 세 경로 모두에서
  `SinkMcuCount`를 전달합니다(인프라 생성, Start CDC, 그리고 읽기 전용 파라미터 미리보기 —
  미리보기가 실제 배포와 다른 값을 광고하지 않도록).

  제공값은 1 / 2 / 4 / 8뿐이며 숫자 입력이 아닌 드롭다운으로 렌더합니다. 이는 MSK Connect
  API의 `mcuCount` 유효값(워커당 최대 8)이므로, 숫자 필드라면 3도 받아들이고 CloudFormation이
  과금이 시작된 Start CDC 수 분 뒤에 거부합니다. 저장 전에 이 집합으로 정확히 검증합니다.

  툴의 기본값은 의도적으로 템플릿과 동일(4)합니다. 값이 다르면, 이 파라미터를 보내기 전에
  배포된 스택에서는 실제 설정 변경으로 읽혀 다음 Start CDC 때 RUNNING 커넥터 둘을 불필요하게
  재생성하고, 회수되지 않는 MSK 파티션 쿼터를 소모합니다.

### 변경

- **Settings → Performance 폼을 섹션으로 분리하고 각 섹션이 자신의 적용 시점을 명시합니다.**
  이전에는 전부 "applies to the next run"으로 표시했는데, 이는 Full Load / Validation 노브에만
  맞습니다(로더와 검증기가 실행마다 `load_config()`를 호출). CDC 노브는 CloudFormation
  파라미터이므로 다시 읽는 주체가 없고, 이미 스트리밍 중인 싱크는 Start CDC가 커넥터를
  업데이트할 때까지 현재 용량을 유지합니다. 그래서 CDC 섹션은 "applies to the next Start CDC"로
  표시하고, 확인 토스트도 각 노브의 실제 시점을 반복합니다. 그룹핑을 config 레지스트리로
  옮기면서 잠재 렌더 버그도 제거했습니다: 기존 루프는 튜플을 순회하며 그룹이 바뀔 때마다
  헤더를 냈으므로, 한 그룹의 노브가 연속되지 않으면 두 헤더로 쪼개졌습니다.

- **매뉴얼 §7(성능 및 튜닝)에 싱크 MCU 변경의 적용 시점을 문서화**했습니다. 싱크 리사이즈만을
  위해 Start CDC를 다시 실행하는 것이 안전하다는 점도 포함: 커넥터 `Capacity`는 in-place
  업데이트이므로 싱크가 재생성이 아니라 리사이즈됩니다 — 테이블 집합 변경과 달리 파티션 쿼터
  비용도, 복제 gap도 없습니다.

## v0.1.212

### Fixed

- **gapless 시작이 불가능한 워터마크에도 "Automatic — gapless from Full Load"를 제시했습니다.** 이
  옵션이 "재개 좌표가 하나라도 있는가"로 판정됐는데, 핸드오프는 MSK `connect-offsets`에 binlog
  **file:position**으로 키가 잡힌 레코드를 시드하는 방식입니다 — in-VPC seeder는 그것이 없는 워터마크를
  거부하고, `build_watermark_params`가 전부 빈 값을 반환해 템플릿이 seeder를 건너뛰며 커넥터가 소스의
  **현재** binlog부터 시작합니다. 따라서 GTID만 있으면 "gapless (recommended)" + *Ready*로 표시되면서
  Full Load 중의 모든 변경이 조용히 유실되고, Validation이나 컷오버 후에야 드러났습니다.

  이는 이론적 상황이 아닙니다: 두 좌표는 독립적으로 degrade하는 별개 쿼리에서 나옵니다 —
  `SHOW MASTER STATUS`는 `REPLICATION CLIENT` 권한이 필요한데(RDS/Aurora에서 흔히 제한됨)
  `@@GLOBAL.gtid_executed`는 일반 전역 읽기입니다. 이제 Automatic은 `can_seed_offset()`으로 판정하고,
  GTID만 있는 경우는 별도 문구를 씁니다 — "needs a Full Load watermark"라고 하지 않고(워터마크는 있음)
  빠진 binlog 위치와 그 결과, 그리고 해결책(`REPLICATION CLIENT` 부여 후 Full Load 재실행)을 명시합니다.
- **CDC 스텝은 여전히 다른 테이블을 스트리밍하는 파이프라인에 Attach를 제안했습니다.** v0.1.211은
  플랜 단계 배너를 보호했는데, 이 패널은 별개 렌더 경로여서 검사가 전혀 없었습니다. 이제 같은 범위
  검사로 Attach를 보류합니다.

### Changed

- **attach가 안전하지 않을 때는 배포를 진행 경로로 제시합니다.** 배포 폼이 "Deploy a separate CDC
  pipeline instead"라는 이름으로 경고 삼각형 뒤에 접혀 있었습니다 — 그래서 후보가 불일치일 때 운영자는
  누르면 안 되는 파란 Attach 버튼을 크게 보고, 올바른 액션은 위험한 것처럼 보이면서 **숨겨져** 있었습니다.
  attach 가능한 후보가 없으면 이제 펼쳐진 상태로 "Deploy a CDC pipeline for this table set" 제목과
  경고 글리프 없이 표시됩니다. attach가 **유효할** 때는 접힌 채 경고를 유지합니다 — 두 번째 MSK
  클러스터는 비싸고 의도된 경우가 드물기 때문입니다.

### Tests

- UI의 gapless 주장이 seeder가 실제로 배포되는지와 일치하는지를 워터마크 4가지 형태 전부에 대해
  검증하는 불변식 테스트를 포함합니다. 변이 4개 검출, 처음 하나는 통과했습니다 —
  `can_seed_offset()`을 `has_coordinates()`로 되돌리는 변이 — 다른 모든 테스트가 플래그를 미리 계산해
  넘기므로 배선이 미검증이었습니다.

## v0.1.211

### Fixed

- **다른 테이블 집합을 스트리밍하는 CDC 파이프라인에도 "Attach"를 제안했습니다.** attach는 세션을 살아
  있는 파이프라인에 연결하고, 그 파이프라인이 스트리밍 중이므로 Data Migration을 `DONE`으로 승격시켜
  Validation을 엽니다. 라이브 계정에서 확인: 어떤 스택이 `ecommerce_demo.*` 11개 테이블을 복제하는 중인데
  세션은 방금 `ecommerce.*` 8개 테이블을 적재한 상태였습니다. attach했다면 마이그레이션이 완료됐다고
  보고하고 컷오버로 진행하게 하면서, **이 세션이 적재한 모든 테이블에는 CDC가 전혀 붙지 않아** 워터마크
  이후의 모든 소스 변경을 조용히 잃게 됩니다.

  이제 후보 파이프라인이 이 세션이 적재한 테이블을 복제하지 않으면 attach를 제공하지 않고, 어떤 테이블이
  커버되지 않는지 정확히 명시하는 알림과 두 가지 진행 방법(이 테이블 집합용 CDC 배포, 또는 선택을 그
  파이프라인에 맞추기), 그리고 유휴 인프라가 계속 과금 중이라는 안내를 표시합니다. 의도적으로 비대칭입니다:
  파이프라인이 선택보다 **더 넓은** 경우는 불일치가 아닙니다 — 다른 테이블 집합을 병행 서비스할 수 있고,
  이 세션이 소유한 것은 하나도 커버되지 않은 채 남지 않습니다. 그리고 테이블 집합을 읽을 수 없는 후보는
  attach 가능하게 유지합니다 — 미조사 스택을 차단하면 두 번째 값비싼 MSK 클러스터를 배포하도록 유도하게
  되는데, 그것이 바로 이 배너가 막으려는 것입니다.

### Changed

- **Start over가 실행 중인 CDC 파이프라인을 이 세션의 것이라고 암시하지 않습니다.** 이제 스택 이름을
  명시하고, 이전 세션이 남긴 것이거나 같은 계정을 보는 다른 창에서 사용 중일 수 있음을 분명히 말합니다 —
  스택에 소유자 태그가 없어 도구가 판별할 수 없습니다. "Leave CDC untouched"는 다른 것이 그 파이프라인을
  쓰고 있을 때의 올바른 선택으로 설명되며, 미루기처럼 읽히지 않습니다.

## v0.1.210

### Fixed

- **Start over가 다른 이름으로 배포된 cdc-stack의 삭제를 제안하지 않았습니다.** CDC가 전혀 없다고
  보고하다가, CDC 단계로 이동하면 `mysql-dsql-cdc-stack-0729-new`에 *연결*할지 물었습니다 — 계정에
  실제로 존재하는 스택에 대해 두 프롬프트가 서로 모순된 것입니다. Start over의 두 신호
  (`cdc_stack_phase`, `cdc_connector_names`)는 **이** 세션이 target하는 이름으로 한정되므로, 이전
  세션이 배포했거나 커스텀 접미사를 가진 스택은 보이지 않았습니다 — 그리고 침묵한 쪽이 바로 MSK / NAT
  과금을 멈출 수 있었던 프롬프트입니다. 이제 Start over도 발견된 스택을 함께 확인하며, 삭제 대상은
  제안한 **바로 그** 스택으로 해석됩니다(제안만 발견 목록에 맞추면, 삭제를 제안한 뒤 존재하지 않는 이름을
  target해 조용히 아무것도 하지 않고 인프라 과금을 남기게 됩니다). 발견된 스택이 여러 개면 제안은 하되
  선택하지 않습니다 — 각각이 별개 파이프라인일 수 있으므로, 무엇을 삭제할지는 CDC 단계에서 운영자가
  결정합니다.

### Deployment

- `0.1.209`를 ECR Public에 발행하고 템플릿의 `ContainerImageUri` 기본값을 그것으로 갱신했습니다. 기본값이
  `0.1.188`까지 밀려 있었고(21개 릴리스 뒤처짐) 이는 새로 `git clone`한 사용자가 배포하는 이미지이므로,
  이를 강제하는 가드 테스트가 다시 통과합니다. 서울 Fargate 스택도 같은 빌드로 업데이트했습니다
  (change set: TaskDefinition + Service만, 파라미터 24개 전부 유지).

## v0.1.209

### Fixed

- **드롭된 행마다 "Reload" 버튼이 있었지만 Reload는 테이블 전체에 작용합니다.** 3행이 드롭되면 카드
  3개가 생기고 각각의 Reload가 완전히 같은 동작을 하면서도 해당 행만 처리하는 것처럼 보였습니다. 이제
  드롭된 행들이 **테이블당 카드 하나로 그룹화**되고 Reload도 하나입니다.

### Changed

- **한 테이블의 드롭된 행을 카드 하나씩이 아니라 압축해서 표시합니다.** 카드마다 테이블명과 같은 사유를
  반복해서, 3행 드롭이 거의 동일한 박스 3개로 화면을 채웠습니다. 이제 테이블당 카드 하나가 테이블명과
  사유를 한 번만 말하고, 개수를 표시하고("3 rows dropped"), PK를 monospace 칩으로 나열합니다 — 개수가
  늘어도 읽을 수 있는 형태입니다. 칩 12개를 넘으면 "+N more"로 잘리며, 개수 배지는 항상 실제 총합을
  보고하고 전체 목록은 다운로드 가능한 에러 로그에 있습니다. 한 테이블 안에서 사유가 실제로 다르면 모두
  유지합니다 — 중복 제거가 두 번째 원인을 숨겨선 안 되므로 — PK를 파싱할 수 없는 메시지도 사라지지 않고
  사유를 남깁니다.

### Tests

- 변이 6개 검출 — 그룹화 제거, 서로 다른 두 사유를 하나로 뭉개기, 칩 제한 제거, 개수 배지가 실제 총합
  대신 잘린 수를 보고하는 경우 포함.

## v0.1.208

### Fixed

- **여러 행이 드롭됐는데 목록에는 한 행만 나왔습니다.** 개수는 "3 rows permanently dropped"인데 아래
  목록엔 정확히 하나만 표시됐습니다 — 패널이 `latest_messages()`로 만들어졌고 이 함수는 **테이블당**
  메시지 하나만 남기므로(마지막 것이 이김), N행을 드롭한 테이블이 1행만 나열되어 같은 화면의 두 숫자가
  어긋났습니다. 이제 드롭된 모든 행이 각자의 PK와 함께 표시됩니다 — PK가 각 항목의 실행 가능한
  부분(소스에서 찾을 때 쓰는 값)이기 때문입니다. 행별 레코드가 없는 호출자(구버전 호출부, 또는 in-memory
  로그가 사라진 복원 세션)는 아무것도 없는 대신 테이블 단위 표시를 유지합니다.

### Changed

- **갭 수락 시 거의 동일한 초록 박스 두 개가 나오지 않습니다.** 확인 알림이 바로 위 완전성 배너가 이미
  말하는 내용(개수, 테이블, 다음 단계 해제, Validation이 갭 보고)을 되풀이했습니다. 이제 배너에 없는
  사실 하나만 담은 한 줄로 표시됩니다 — 소스 값을 고쳐 테이블을 리로드하면 갭이 닫힌다는 점 — 클릭
  확인용 체크 표시와 함께.
- **에러 로그 다운로드를 수락 버튼 아래에서 옮겼습니다.** "Accept quarantined rows & continue" 바로
  아래에 있어 그 결정의 부차적 선택지처럼 읽혔는데, 실제로는 같은 행별 정보를 가져가는 수단일 뿐입니다.
  이제 그것이 직렬화하는 상세 옆에 위치합니다.
- **워터마크의 테이블별 카운트가 주변 패널과 어울립니다.** Quasar 기본 확장 헤더(회색 전체 폭 바 + 큰
  선행 글리프)가 평평한 패널을 가로지르는 무거운 띠였고, 카운트 행은 위쪽 좌표와 다른 정렬을 썼습니다.
  이제 둘 다 라벨-monospace 값의 같은 형태를 쓰고, 헤더도 필드 라벨과 같은 크기입니다.
- **"Workloads to migrate" 아래 상시 문구를 제거했습니다.** 세 가지 주장이 각각 독자에게 더 도움이 되는
  곳에서 이미 전달됩니다: picker 캡션이 선택 출처를 말하고(바로 위에 배지로 나열됨), Export watermark
  패널이 약속이 아니라 실제 좌표를 보여 주고, 확인 다이얼로그가 커밋 시점에 소스는 읽기 전용임을 명시합니다.

## v0.1.207

### Changed

- **갭 수락 액션이 판정 아래로 이동했습니다.** 격리 패널 안에 렌더링됐는데 이 패널은 완전성 배너보다
  *먼저* 나옵니다 — 그래서 운영자가 판단 대상인 결론을 읽기 전에 결정을 요구받았습니다. 이제 순서는
  행별 상세 → 판정 → 판정이 설명한 액션 → 다운로드입니다.
- **"Data errors" 헤딩과 개수를 제거했습니다.** 에러가 있으면 이미 위에 테이블·PK·이유와 함께 전부
  나열되므로 개수를 되풀이하는 헤딩은 같은 사실의 네 번째 반복이었고, 없으면 "No data errors recorded."
  위에 섹션 헤더를 찍어 부재를 주장하는 블록이 됐습니다. 이 섹션의 고유 가치는 다운로드이므로 버튼만
  남겼습니다.
- **다운로드 버튼 이름을 파일 형식이 아니라 읽는 사람 기준으로 바꿨습니다.** "Download error log
  (NDJSON)"는 아무도 묻지 않은 형식을 앞세우고, Full Load와 CDC 양쪽에 다운로드가 있는데도 어느 쪽
  에러인지 말하지 않았습니다. 이제 "Download Full Load error log (3 errors)" /
  "Download CDC error log (N errors)"로 표시하고, 형식과 줄 단위 내용은 툴팁으로 옮겼습니다.

### Fixed

- **"Accept quarantined rows & continue" 버튼이 아무 반응 없어 보였습니다.** 클릭은 **동작했습니다** —
  단계를 완료로 표시하고 다음 단계를 열고 activity log에도 기록합니다 — 그런데 렌더 경로가 수락 플래그를
  아무도 읽지 않아서(다음 적재 실행 때만 소비됨) 패널과 버튼이 똑같이 다시 그려졌습니다. 이제 버튼이
  초록색 "Gap accepted — Full Load marked complete" 알림으로 **교체**되고, 이제 무엇이 가능해졌는지도
  함께 알려 줍니다 — 두 번째로 눌러도 역시 보이지 않을 클릭을 유도하는 컨트롤을 남기지 않습니다.
- **수락한 갭이 여전히 문제로 보고됐습니다.** 앰버 "Full Load finished with issues" 배너가 초록 확인
  알림 바로 아래에 남아, 운영자가 방금 명시적으로 해결한 바로 그것을 다시 문제로 표시했습니다. 이제
  "Full Load complete — with an accepted gap"으로 표시하며 드롭된 행을 명시하고 Validation을
  가리킵니다 — 단, 모든 행이 적재됐다고는 결코 말하지 않습니다(사실이 아니므로). **진짜 실패**가 있는
  실행은 플래그가 설정돼도 경고를 유지하므로, 갭 수락이 재시도 가능한 작업을 덮을 수 없습니다.

### Changed

- **Snapshot row counts가 워터마크 패널과 어울리도록 개선했습니다.** 패널 *아래에* 전체 폭 확장으로
  매달려 테두리 있는 `ui.table`을 감싸고 자체 정렬 헤더까지 갖고 있었습니다 — 화면의 다른 어디에서도
  쓰지 않는 스타일의 두 번째 시각적 컨테이너였습니다. 테이블당 값 하나이므로, 이제 패널 안의 라벨 붙은
  행으로 위쪽 좌표들과 같은 형태로 표시되고, 우측 정렬 monospace + 천단위 구분으로 숫자가 자리를 맞춰
  한눈에 비교됩니다. 목록이 길 수 있으므로 기본 접힘은 유지합니다.

### Tests

- 변이 6개 검출. 처음에 하나가 통과했습니다 — 렌더 호출에서
  `quarantine_accepted=migration_state.accept_quarantined_rows`를 삭제하는 변이 — 다른 모든 테스트가
  플래그를 직접 넘기므로 **배선**이 미검증이었습니다. 이번 세션에서 state→render 배선 공백이 통과한 것이
  세 번째라, 두 렌더 호출을 모두 덮는 구조 단언을 넣었습니다.

## v0.1.206

### Changed

- **같은 드롭을 여덟 곳에서 알리던 것을 정리했습니다.** 3행 격리가 요약 칩, 행의 Status 배지,
  Attempts 셀, 섹션 헤더, 행별 카드, 완전성 배너, data-error 개수, 그리고 빨간 "Load failed" 박스까지
  여덟 곳에서 보고됐습니다. 이제 각 박스가 하나의 역할만 맡습니다:
  - **Attempts** 셀은 반복하지 않습니다 — 같은 행의 Status 배지가 이미 "3 dropped" 칩(hover 설명 포함)을
    갖고 있어 한 테이블 행에서 같은 사실을 두 번 말했습니다(그 외 에러는 여전히 `1 · 3 errors`);
  - 격리 섹션의 **개수 헤더**를 제거했습니다. 이 섹션은 다른 어디에도 없는 행별 상세(어느 행, 왜,
    Reload)를 보여 주고, 판정은 배너가 담당합니다;
  - 격리가 **유일한** 불완전성일 때 빨간 **"Load failed"** 박스를 숨깁니다. 배너 내용을 예외 클래스명과
    함께 빨간색으로 되풀이하며 앰버의 "나머지는 적재됨" 프레이밍과 모순됐고, 절대 적재될 수 없는 행뿐인
    실행에 "failed"는 과장입니다. 진짜 실패는 원문 그대로 계속 표시됩니다.
- **Export watermark가 통계 테이블 아래로 이동하고 컴팩트해졌습니다.** 구분선과 테이블별 진행 사이에
  있어서 진행 테이블(그리고 완료된 실행에서는 완전성 판정과 격리 상세)을 정적 참조 데이터 아래로
  밀어냈습니다. 한 번 읽는 출처 정보이므로 이제 라이브 상세 뒤에 오며, 여전히 refreshable 영역
  **밖에서** 렌더링되어 ~1.5초 폴이 행 개수 확장을 접을 수 없습니다. 고정 좌표 4개는 4개 행을 위해
  "Field"/"Value" 헤더까지 있는 정렬 가능한 2열 `ui.table`이었는데, 이제 하나의 테두리 패널 안에
  라벨이 붙은 monospace 줄로 표시되고, 식별용 요약은 헤더 줄에, 사용 불가한 좌표는 누락된 값처럼
  보이지 않도록 흐리게 처리합니다. 테이블별 스냅샷 개수는 진짜 표 형태이므로 접힌 테이블로 유지합니다.

### Tests

- 변이 4개 검출. 처음에 하나가 통과했습니다: 렌더 순서 단언이 `src.index("_live_detail()")`를 썼는데
  이는 `def` 줄에 먼저 매칭되어 두 호출을 뒤바꿔도 통과했습니다. 이제 AST로 **호출** 줄 번호를 비교하며,
  실제로 뒤바꿔 검증했습니다.

## v0.1.205

### Fixed

- **격리 헤더가 행이 아니라 테이블 수를 셌습니다.** 바로 아래 배너가 "3 rows permanently dropped"라고
  하는데 헤더는 "Quarantined rows (1)"로 표시돼, 한 화면의 두 박스가 같은 숫자를 다르게 말했습니다.
  헤더가 센 목록은 *테이블*당 한 항목(해당 테이블의 최신 메시지)입니다. 이제 행 수와 테이블 수를 구분해
  보고합니다: *"3 rows permanently dropped across 1 table — the rest of each table loaded"*.

### Changed

- **드롭된 행이 한 줄 로그가 아니라 라벨이 붙은 세 가지 사실로 읽힙니다.** 이전에는 원시 로그 텍스트
  (`quarantined row pk[id=3]: datatype limit greater than 1048576 bytes not supported for bytea`)로
  표시되고, 테이블명은 위쪽 배지에, PK는 문장 중간에 묻혀 있었습니다. 이제 앰버 카드로 표시됩니다:
  테이블명을 눈에 띄는 텍스트로, **PK를 독립된 monospace 칩으로**(소스에서 찾을 때 쓰는 실행 가능한
  핸들이므로), "dropped" 배지, 그리고 그 아래에 중복되는 `quarantined row pk[...]` 접두를 뗀 기술적
  이유. 파싱할 수 없는 메시지는 변형하지 않고 그대로 보여 줍니다.
- **Attempts 열이 그 숫자의 의미를 말해 줍니다.** `1 · 3 err`는 재시도 횟수처럼 읽히고, 그것이 *대상이
  영구히 갖지 못할 행 수*라는 힌트가 전혀 없었습니다. 이제 영구 격리 행은 `1 · 3 rows dropped`,
  그 외에는 `1 · 3 errors`로 표시합니다.
- **"Accept quarantined rows & continue" 옆의 중복 문구를 제거했습니다.** 완전성 배너의 조치 안내와
  같은 내용("소스 값을 고쳐 해당 테이블을 Reload하거나 갭을 수락")이어서 한 화면에 같은 조언이 두 번
  나왔습니다. 배너가 판정과 조치를 함께 담고 있으므로 배너 쪽을 유지합니다.

### Tests

- 변이 5개 검출 — 테이블 수 헤더 복원, 난해한 `err` 표기 복원, 비정형 메시지에서 잘못된 PK 추출 포함.

## v0.1.204

### Fixed

- **복원된 세션에서 "Accept quarantined rows & continue" 버튼이 사라졌습니다 — 완전한 막힘.**
  Full Load는 `FullLoadIncompleteError`로 끝나고 그 메시지는 운영자에게 바로 그 버튼을 쓰라고
  안내하는데, 앱 재시작 후에는 버튼이 렌더링되지 않았습니다. quarantine-only 판정이 **in-memory**인
  `ErrorLogStore`의 행을 세었기 때문에, 재시작하면 카운트가 0이 되고 판정이 `False`가 됐습니다. 다른
  복구 경로도 없습니다 — 영구 거부된 값은 재시도해도 절대 적재되지 않으므로 — 남는 것은 "Start over"
  뿐이었습니다. 이제 카운트를 **job의 chunk**에서 가져오며(job 저장소는 영속적), 구버전이 기록한 job도
  동작하도록 에러 로그 스캔으로 폴백합니다. 미완료 테이블이 있으면 override를 보류하는 가드는 그대로입니다.

### Added

- **실패가 이제 영속적인 activity log에 진단 정보를 담습니다.** activity log는 세션보다 오래 남는
  기록인데, 실패 항목 세 종류가 그것만으로는 트러블슈팅이 불가능했습니다:
  - **격리된 각 행**이 in-memory 에러 로그에만 기록되어, 재시작 후에는 *어느* 행이 손실됐는지 알 수
    없었습니다 — 개수만 남았습니다. 이제 드롭된 모든 행이 PK, 거부 이유, 그리고 나머지 행은 정상
    적재됐다는 사실을 기록합니다. 공유 헬퍼로 세 적재 경로(in-process, 샤드 워커, 단일 테이블 워커)
    전부에 배선했습니다 — 샤드 테이블은 큰 테이블이고, 수작업 확인 가능성이 가장 낮은 경우입니다.
  - **"1 of 8 table(s) did not fully load"** 는 개수일 뿐 진단이 아니었습니다. 이제 실행 요약이 영향받은
    테이블과 이유를 명시하며, 중복 제거 후 8개로 제한하고("+N more" 표기) 대규모 실행이 로그를 넘치게
    하지 않도록 했습니다.
  - **"connector X failed"** 는 원인을 말하지 않았습니다. 이제 다른 커넥터들의 상태(파이프라인의 어느
    쪽이 깨졌는지), DLQ 깊이, 테이블별 에러 수, 그리고 스택 트레이스를 볼 CloudWatch 로그 그룹
    포인터를 담습니다. 폴이 아직 진단 정보를 수집하지 못했을 때는 우아하게 축약되며, 실제로 읽지 않은
    DLQ 깊이를 보고하지 않습니다.

### Tests

- 변이 6개 검출. 처음에 하나가 통과했고 이제 덮었습니다: 커넥터 전환이 `detail=None`을 넘기는 변이 —
  다른 모든 테스트가 상세 생성 함수를 직접 호출했으므로 **배선**이 미검증이었습니다. 앞서 복원 세션
  테이블 선택 버그를 배포한 것과 같은 종류의 공백입니다.

## v0.1.203

### Added

- **Full Load 테이블에서 행이 드롭된 테이블을 표시합니다.** 격리가 발생한 테이블도 `DONE`으로 끝나므로
  Status 배지가 정상 테이블과 **동일**했습니다 — 유일한 신호는 테이블 전체 아래의 노란 패널 하나였고,
  그것은 *어느 행*인지 말해 주지 않으며 스크롤하면 사라집니다(해당 행이 다른 페이지에 있을 수도 있습니다).
  두 가지 표시를 추가했고, 둘 다 앰버색이며 hover 시 스스로 설명합니다:
  - 행의 `Done` 배지 옆 외곽선 **"N dropped"** 배지 — 툴팁이 무슨 일이 있었는지, 나머지 행은 정상
    적재됐다는 것(실패가 아니라 `DONE`), 갭을 닫는 방법(소스 값 수정 후 해당 테이블 Reload)을 설명합니다;
  - 테이블 위 상태 요약의 **"Dropped: N rows"** 칩 — 스크롤하기 전에 `Done: 8`과 같은 시야에서 손실이
    보이도록. 보고된 실행이 완벽해 보였던 바로 그 요약입니다.

  둘 다 실제로 드롭이 있을 때만 렌더링되며, 정상 실행은 변화가 없습니다.

### Tests

- 요약 칩(존재/부재/복수형), 행별 툴팁, 그리고 Quasar 슬롯 템플릿의 계약 검사를 덮었습니다 — 템플릿이
  읽는 모든 `props.row.*` 키는 행 매핑이 제공해야 합니다. 마지막 항목이 중요한 이유: 슬롯의 키가 틀리면
  **런타임에 빈 화면으로 렌더링되는데 스위트는 통과**합니다. 키 이름을 바꾸는 변이가 이제 잡힙니다.
  변이 4개 검출.

## v0.1.202

### Added

- **Validation이 타깃 부족분을 마이그레이션이 드롭한 행에 귀속시킵니다.** 행이 격리된 테이블(DSQL이
  저장할 수 없는 값)은 타깃이 부족하므로 Validation은 그냥 `MISMATCH` / "investigate"로 보고했고,
  매뉴얼은 운영자에게 *"부족분을 Full Load 에러 로그 / CDC DLQ와 교차 확인하라"*고 안내했습니다 —
  도구가 이미 갖고 있던 정보인데도 말입니다. Validation에는 격리에 대한 지식이 **전혀** 없었습니다.
  이제:
  - 부족분이 드롭된 행 수와 **정확히** 일치하면 *"Fully explained: N rows were permanently dropped
    during the migration … this deficit is expected, not new data loss"*로 표시됩니다;
  - 부족분이 **더 크면** *"Partly explained: … but N more are missing and are NOT accounted for"*로
    표시되어 조사가 필요한 부분을 정확히 지목합니다. 정확히 일치할 것을 요구하는 것이 안전장치입니다:
    4행 부족한데 1행을 드롭했다면 3행이 미해명이며, 그것을 "예상됨"으로 부르는 것이 바로 진짜 손실이
    이 검사를 통과하는 경로입니다.
  - 판정은 의도적으로 여전히 **실패**입니다. 그 행들은 실제로 없으므로, 이 귀속은 갭을 해명하는 것이지
    면제하는 것이 아닙니다 — 격리가 테이블을 `matched`로 뒤집어 손실된 데이터 위에서 컷오버를 열어
    줄 수는 없습니다.
  - 앱 재시작 후에는 테이블별 건수가 사라지므로(영속되지 않음), 부족분을 추측하지 않고 미해명으로
    보고합니다.

  건수는 Full Load 작업의 chunk(`quarantined_rows_by_table`)에서 흘러와 완성된 리포트에 한 번 부착되며,
  소스-타깃 비교는 두 데이터베이스만의 순수 함수로 유지됩니다.

### Docs

- 매뉴얼 §4.5에 Full Load가 격리한 행이 이후 변경될 때 CDC 동작을 문서화했습니다: `DELETE`는 0행에
  매칭되어 조용히 적용되고(정상 — 의도한 최종 상태가 이미 성립하며, 에러로 취급하면 멱등성이 깨짐),
  값을 1 MiB 아래로 줄이는 `UPDATE`는 싱크의 upsert로 **갭을 치유**하며, 여전히 초과하는 `UPDATE`는
  DLQ로 재격리됩니다. 두 가지 귀결도 명시: 갭은 스스로 알려 주지 않으며(Validation이 보고), 0행 delete는
  정상 재생과 구분할 수 없다(멱등성을 위한 의도적 트레이드오프).
- 매뉴얼 §5의 수작업 교차 확인 안내를 새 귀속 설명으로 교체(en/ko/ja).

## v0.1.201

### Fixed

- **행을 영구 드롭한 Full Load가 여전히 "모든 소스 행을 적재했다"고 보고했습니다.** 실제 실행에서
  보고됨: "Quarantined rows (1) — these rows were permanently dropped" 노란 박스 바로 아래에
  "Full Load complete — All 8 tables loaded every source row" 초록 박스가 있었고, 해당 테이블은
  `12 / 15`로 표시됐습니다. 원인 두 가지를 모두 수정했습니다:
  - 테이블별 `complete` 검사가 적재량 대 소스 추정치만 비교해 드롭을 보지 못했고, 추정치의 20%
    샘플링 허용오차(`information_schema` 값이 샘플 기반이라 양방향으로 어긋나므로 존재)가 15행
    테이블의 3행 부족을 조용히 흡수했습니다. 격리된 행은 추정 노이즈가 아니라 **확정된** 손실이므로,
    이제 기준값 비교보다 **먼저** 검사에 실패시킵니다 — 추정치가 아예 없을 때도 잡힙니다.
  - 그 건수가 판정까지 도달하지 못했습니다: `ChunkState`/`FullLoadTableRow`에 격리 필드가 없어
    실행 단위 요약이 **구조적으로** 볼 수 없었습니다. 엔진은 이미 드롭을 에러 로그에 기록하고 불완전
    적재로 취급하고 있었고, 이제 chunk에도 건수를 기록해 완전성 요약이 읽습니다.
- **드롭된 행이 예상된 추정치 편차로 보고될 수 있었습니다.** 기준값이 근사치일 때 개수 차이는 (정당하게)
  "counts differ from the pre-load estimate … This is expected" 정보 알림으로 표시됩니다. 격리된
  행은 결코 여기 속할 수 없으므로(샘플 추정치로는 적재 실패한 행을 설명할 수 없음) 이제 항상
  "Full Load finished with issues"로 표시되며, 테이블명과 건수가 함께 명시되고 별도의 row-count
  mismatch로 중복 보고되지 않습니다.
- **해결 안내가 해당되지 않는 컨트롤을 가리키지 않습니다.** 격리가 발생한 테이블은 `DONE`으로 끝나므로
  재시도 대상이 아닌데, 배너는 실패가 없어도 "Retry the failed tables"라고 안내했습니다. 이제 소스 값을
  고쳐 해당 테이블을 Reload하라고(또는 갭을 수락하라고) 안내하고, 실제로 실패한 테이블이 있을 때만
  재시도를 언급합니다.

## v0.1.200

### Fixed

- **Prerequisites의 가드 메시지가 우측 정렬로 표시됐습니다.** 사전검사 이후 테이블을 추가하면
  "Re-run the prerequisite checks — … was added to the selection after the checks ran…"가 오른쪽
  끝에 붙어 나타났습니다. 이 행은 원래 primary "Continue" 버튼만 담기 때문에 `justify-end`이고
  (디자인 시스템상 primary 액션은 오른쪽), 그 버튼을 **대체하는** 가드 문장이 정렬을 그대로 물려받은
  것입니다. 이제 버튼이 있을 때만 우측 정렬하고 메시지는 좌측 정렬해, 설명하는 내용 옆에서 산문으로
  읽히게 했습니다. Data Migration·Schema Conversion·Validation의 다른 `justify-end` 행도 확인했고
  모두 버튼만 담고 있어 같은 결함은 없었습니다.

## v0.1.199

### Fixed

- **복원된 세션에서는 여전히 모든 대상 테이블이 체크됐습니다.** v0.1.198은 기본값을
  `generated_node_ids`에 맞췄는데, 이 필드는 "Generate DDL for selected"를 눌러야만 설정됩니다 — 그래서
  그것 없이 Apply만 한 세션(또는 이후 Clear를 누른 세션)은 이 필드가 빈 상태로 복원되어 곧바로 "대상의
  모든 테이블" 폴백으로 떨어지며 전부 다시 체크했습니다. "Start over"가 해결해 보인 것은 새 세션이
  generated ids를 다시 채우기 때문일 뿐입니다. 이제 기본값은 Schema Conversion의 apply가 쓰는 것과 같은
  방식(`_selected_apply_names`)으로 2단계 범위를 결정합니다: 커밋된 generated ids가 있으면 그것,
  **없으면 ticked ids** — 둘 다 영속되므로 재시작을 견딥니다. 대상 존재 폴백은 둘 다 없을 때만 쓰입니다.

### Tests

- 빠져 있던 배선 검증을 추가했습니다: 호출 지점에서 `ticked_node_ids`를 제거하는 변이가 모든 테스트를
  통과했는데, 이게 바로 이 버그가 배포된 경로입니다 — 순수 헬퍼는 맞고 테스트도 있었지만 UI는 여전히
  과다 체크했습니다. 새 테스트는 화면의 `default_migration_selection(...)` 호출을 파싱해 ticked 범위를
  빠뜨리면 실패합니다. 변이 3개 검출(ticked 폴백 제거, generated보다 ticked 우선, 호출 지점 배선 해제).

## v0.1.198

### Fixed

- **"Tables to migrate"가 Schema Conversion에서 선택한 항목이 아니라 모든 테이블을 체크했습니다.**
  기본값이 "대상에 이미 존재하는 모든 테이블"이어서, 이전 실행에서 남은 테이블을 가진 대상은 그것들을
  조용히 전부 다시 선택하고 의도적인 2단계 선택을 버렸습니다 — 실제 세션에서 3개를 골랐는데 11개가
  체크되어 있다고 보고되었습니다. 게다가 요청보다 **더 많이** 마이그레이션하는 방향의 기본값이었는데,
  장시간 실행되는 적재에서는 잘못된 방향입니다.

  이제 이 세션의 Schema Conversion 선택이 기본값이 되고, 이 세션에서 생성한 것이 없을 때만
  (재연결이거나 스키마가 외부에서 적용된 경우) 대상 존재 집합으로 폴백합니다 — 그 경우 2단계 선택을
  알 수 없고, 빈 기본값은 아무 설명 없이 0개 체크된 화면을 남기기 때문입니다. 4개 호출 지점이
  `default_migration_selection()` 헬퍼 하나를 공유하므로 서로 어긋날 수 없습니다.
- **선택기 캡션이 실제 체크된 것이 아니라 기본값을 설명했습니다.** 항상 "Pre-selected: N table(s)
  already on the target"로 표시됐는데, 기본값이 Schema Conversion을 따르게 되면서 사실이 아니게
  되었습니다. 이제 실제 사전 체크 개수를 전체 대비로 보여주고 출처를 명시합니다("selected in Schema
  Conversion" vs "already on the target"). 두 집합이 다른지로 판정하므로, 재연결한 사용자에게 이
  세션에서 하지 않은 2단계 선택이라고 말하지 않습니다.

## v0.1.197

### Fixed

- **스키마 적용 중 앱을 재시작하면 해당 단계가 영원히 스피닝했습니다.** 실제 세션에서 보고됨:
  "Applying converted DDL to the target..." 진행 중에 UI를 재시작했고, 다시 연결한 뒤 스피너가
  멈추지 않고 Apply 컨트롤이 계속 잠겨 있었습니다. 적용은 in-process로 실행되고 그 job id는 의도적으로
  영속되지 않으므로, 재시작은 작업을 죽이면서 **핸들까지 잃어버립니다** — 단계는 스냅샷에서 여전히
  `IN_PROGRESS`로 복원되어(스피너를 그림) 있는데, 상태를 마무리해줄 폴 타이머는 job id가 없으면 즉시
  반환했습니다. 그래서 무엇도 이걸 해제할 수 없었습니다.

  이제 살아있는 적용 핸들이 없는 재연결은 단계를 `FAILED`로 조정하고(`DONE`이 아님: 완료를 증명하는
  리포트가 없고, 실제로 끝나지 않았으므로) 무슨 일이 있었는지 설명합니다 — 재시작 전에 생성된 객체는
  이미 대상에 있고, "Skip if exists"로 재실행하면 기존 객체를 건드리지 않고 나머지를 마칩니다. job id를
  여전히 들고 있는 **진짜 진행 중인** 적용은 그대로 둡니다. 4단계(Validation)에는 이미 있던 조정이
  2단계에는 없었습니다.

### Changed

- **일괄 Apply가 이제 위쪽 Generated DDL 목록에 대한 액션으로 읽힙니다.** 이 카드는 목록 아래에 있고
  제목이 문자 그대로 "Apply to target" — 각 행의 개별 버튼과 같은 세 단어 — 이어서 별개 기능처럼
  보였고, 안내문이 "…in the Generated DDL list above"로 두 번이나 위를 가리켜야 했습니다. 이제 카드
  제목은 "Apply generated DDL to target"이고, 본문이 실시간 개수로 범위를 명시하며("Applies the 7
  objects from the Generated DDL list above"), 버튼도 무엇을 적용하는지 이름을 밝힙니다
  ("Apply all 7 generated objects to target" — 범위가 모호했던 "Apply all to target (7)" 대체).
  범위가 객체 1개일 때는 개별 적용 안내문을 생략합니다 — 버튼이 이미 하는 일을 지시할 뿐이므로.

### Docs

- `CLAUDE.md`: UI가 **표시하는** 버전은 `pyproject.toml`이 아니라 설치된 패키지 메타데이터
  (`importlib.metadata`)에서 온다는 점을 기록했습니다 — editable 설치는 코드 편집은 즉시 반영하지만
  버전은 갱신하지 않으므로, 버전 범프 후 재시작 전에 **`uv sync`**(`uv lock`만으로는 부족)가 필요합니다.
  이 문제로 로컬 UI가 6개 릴리스 뒤처져 있었습니다.

## v0.1.196

### Fixed

- **대상 PK 조회가 실제 Aurora DSQL에서 모든 테이블의 모든 컬럼을 반환했습니다.**
  v0.1.192에서 추가한 `target_primary_key_columns()`가 `pg_index.indkey` 전체를 unnest했지만,
  실제 키는 앞쪽 `indnkeyatts`개뿐이고 나머지는 인덱스의 비-키 stored/included 컬럼입니다. DSQL에서는
  이것이 예외 상황이 아닙니다: 모든 primary index가 테이블의 남은 컬럼을 payload로 싣고 있어서, 11개
  테이블 스키마에서 `indnatts`가 최대 14인데 `indnkeyatts`는 전부 1이었고, 이 함수는
  `information_schema.key_column_usage`와 **11개 중 11개** 테이블에서 불일치했습니다.

  결과는 v0.1.192의 의도와 정반대였습니다: 전체 컬럼 목록은 적용된 복합 키와 결코 같아질 수 없으므로,
  데이터가 있는 대상에 키가 변경된 append는 **모두 거부**되고 터무니없는 "실제 PK"가 메시지에 인용될
  상황이었습니다. unnest를 `indnkeyatts`로 제한해 수정했고, 같은 클러스터로 재검증해 11/11 일치,
  없는 테이블은 여전히 `None`을 반환합니다.

  라이브 클러스터(`ap-northeast-2`)에 **읽기 전용**으로 검증: `unnest … WITH ORDINALITY`,
  `JOIN LATERAL`, `pg_index.indisprimary/indkey/indnkeyatts`, `pg_table_is_visible`,
  `pg_attribute`가 모두 DSQL에서 동작하며, `indkey`가 `'2 1'`인 실제 2-키 인덱스가 컬럼을
  **인덱스 순서**로 반환함을 확인했습니다 — 복합 키 전략이 의존하는 바로 그 보장입니다.

### Tests

- `_PkCursor` 더블이 미리 정해둔 PK를 그대로 돌려주는 대신 쿼리의 키 컬럼 제한을 실제로 반영합니다.
  즉 statement에 `indnkeyatts`가 없으면 stored 컬럼까지 반환해 라이브 클러스터의 형태를 재현합니다.
  새 테스트 2개(payload 제외, 복합 키 순서 유지 + payload 제거)는 제한을 없애면 실패합니다. 이전
  fake로는 실제 모든 테이블에서 틀린 함수를 두고도 2394개가 전부 통과했습니다.

## v0.1.195

### Tests

- **Full Load 확인 다이얼로그를 실제로 열어서 검증합니다.** v0.1.194의 알림은 순수 헬퍼와 클로저
  구조 검사로만 덮여 있었습니다 — 다이얼로그가 Start 핸들러 안에서 지연 생성되기 때문입니다. 이제
  실제로 구동합니다: NiceGUI의 `context.client`(Context 인스턴스의 읽기 전용 프로퍼티)와 다이얼로그
  직전의 `run.io_bound` 대상 프로브를 패치하고, 캡처한 Start 핸들러를 await한 뒤 렌더된 텍스트와
  버튼 라벨을 단언합니다 — 테이블 이름과 "데이터 손실 없음" 설명을 담은 알림, 그리고 재생성이 없을
  때의 기존 `"Confirm and start"` 경로. 두 변이(알림 제거, 버튼 라벨 미변경) 모두 소스 텍스트가
  아니라 렌더 결과로 잡힙니다.

## v0.1.194

### Fixed

- **Full Load 확인 다이얼로그가 스키마를 재생성할 테이블을 이제 알려줍니다.** v0.1.193은 적용된
  PK가 소스와 다를 때 빈 대상을 재생성하는데, 그 판단이 확인 다이얼로그**보다 나중에** 엔진 안에서
  일어났습니다. 그래서 다이얼로그는 `"Confirm and start"`만 표시하고 테이블이 드롭·재생성된다는
  사실을 전혀 언급하지 않았습니다. 잃는 것은 없지만(대상이 비어 있고, DDL도 Schema Conversion에서
  이미 승인한 것), Schema Conversion 외부에서 대상 테이블에 손으로 가한 변경은 대체되므로 실행
  전에 알려야 합니다. 이제 다이얼로그가 해당 테이블을 정보 알림으로 나열하고 버튼을
  `"Recreate and load"`로 표시합니다. **데이터가 있는** 테이블은 영향이 없고 기존의
  Append / Drop & reload 선택(파괴적 라벨과 빨간 버튼)을 그대로 따릅니다.

### Added

- `schema_recreate_tables()` — 적재가 PK를 재생성할 빈 대상을 알려주는 순수 헬퍼. 다이얼로그와
  엔진이 같은 집합을 보게 합니다.

### Tests

- 알림 헬퍼(빈 대상의 키 변경, 데이터 보유 테이블 제외, 변환/인벤토리 없을 때 침묵)와, 그 목록이
  파라미터로 다이얼로그에 전달되어 클로저에 잡히는지 확인하는 구조 검사를 덮었습니다. 마지막
  테스트가 존재하는 이유: 이 알림을 처음엔 다이얼로그 클로저 안에서 `conv_state`/`inventory`를
  읽도록 작성했는데 그 스코프에 없는 이름이라, **Start 클릭마다 `NameError`가 발생**하면서도
  스위트는 전부 통과했을 것입니다(다이얼로그를 여는 테스트가 없으므로). 그 결함 복원을 포함해
  4개 변이가 잡혔습니다.

## v0.1.193

### Fixed

- **변경된 primary key를, 대상의 현재 형태에 append하는 대신 스키마를 다시 생성해서 반영합니다.**
  v0.1.192는 "충돌할 것이 없다"는 근거로 빈 대상의 적재를 허용했는데, 행 충돌에 대해서는 맞지만
  핵심을 놓쳤습니다: PK 변경은 **스키마** 변경이고, append로는 기존 테이블에 키를 소급 적용할 수
  없습니다. 따라서 원래의 단일 컬럼 키를 그대로 가진 빈 대상은 모든 행을 받아들이고 성공으로
  보고했습니다 — 핫 파티션을 피하려고 Composite key 전략을 고른 사용자가 옛 방식으로 키가 잡힌
  테이블을 얻고, 그것도 이미 데이터가 채워진 상태여서 파괴적 재적재만이 해결책이 되는 상황입니다.
  잘못된 형태로 데이터를 조용히 적재하는 것은 거부하는 것보다 나쁩니다.

  적용된 DDL이 다른 키를 요구하고 대상이 **비어 있으면** 이제 replace 경로로 승격됩니다: 적용된
  DDL로 대상을 다시 생성하고(파괴되는 것이 없음) plain `INSERT`로 적재하므로, 선택한 키가 구조적으로
  보장됩니다. **데이터가 있는** 대상은 v0.1.192와 동일합니다 — 실제 키를 기준으로 판정하고 불일치
  시 거부합니다(그곳에서의 `DROP`은 사용자가 동의하지 않은 데이터를 파괴하므로). 거부 메시지는 이제
  실제로 사용 가능한 해결책을 제시합니다: 평시에는 `Drop & reload`, sink가 스트리밍 중이면 테이블
  재생성이 불가능하므로 "먼저 CDC를 중지하라"로 안내합니다.
- **샤딩된 적재가 잘못된 컬럼으로 skip 필터를 걸었습니다.** 샤딩은 *소스* PK를 기준으로 결정되므로
  단일 정수 `id`를 가진 테이블은 *대상* 키가 복합 `(leading, id)`여도 샤딩됩니다 — 그런데 샤드
  워커는 `key_columns`를 아예 전달하지 않았습니다. importer가 소스 키로 폴백해, 대상이
  `(leading, id)`로 키가 잡힌 상태에서 `WHERE (id) IN (…)`으로 필터링했고, `id` 단독은 대상에서
  유일하지 않으므로 **다른 행에 매칭되어 기록되지 않은 소스 행을 건너뛸 수** 있었습니다. 샤드 임계값
  (기본 100만 행) 이상의 테이블만 해당되며, 이는 수동으로 검증할 가능성이 가장 낮은 적재입니다.
  이제 샤드 워커가 대상 키를 전달하고, 키가 동일할 때는 소스 키 폴백을 유지합니다.

### Tests

- 빈 대상의 스키마 재생성(옛 키를 가진 경우 포함), CDC 공존 append와 그 구분된 거부 문구, 두 가지
  샤드 워커 키 경로를 덮었습니다. 재생성 승격 제거, 데이터 보유 대상에 적용(파괴적), 라이브 sink
  하에서 적용, 샤드 키 누락을 포함해 6개 변이가 잡혔습니다.

## v0.1.192

### Fixed

- **권장 방식인 Composite key 전략을 적용한 테이블을 빈 대상에 적재할 수 없었습니다.** Schema
  Conversion에서 "Composite key"(도구 자신이 핫 파티션 회피책으로 권하는 방식)를 고르고 적용한
  뒤 첫 Full Load를 돌리면 *"configured with a changed primary key … Load it fresh (Drop &
  reload)"*로 해당 테이블이 실패했습니다. 가드는 append라면 "대상이 아직 원래 키를 갖고 있을
  것"이라고 가정했지만, Schema Conversion이 방금 복합 키를 적용했으므로 대상은 실제로 그 키를
  갖고 있었습니다. 안전한 적재를 위험하다고 오판해 거부한 것입니다.

  게다가 오류가 지시한 해결책에 도달할 수 없었습니다. "Drop & reload" 선택지는 이미 데이터가 있는
  테이블에만 렌더링되고, replace 집합도 바로 그 집합에서 *파생*되므로 — 빈 대상에서는 고를 방법이
  없었습니다. **Full load + CDC**는 더 심각했습니다: 이 경우 append 경로가 강제되므로(DROP은
  라이브 sink와 경쟁하게 됨) 아예 어떤 경로도 없었습니다.

  이제 Full Load는 가정하지 않고 라이브 대상을 기준으로 키를 판정합니다: **빈 대상**은 적용된
  키로 적재하고(충돌할 대상이 없으며, 소스 키로 유일한 행은 그 키를 포함하는 복합 키에서도 유일함),
  **데이터가 있는 대상**은 카탈로그에서 읽은 *실제* PK와 비교해 일치하면 그것을 사용합니다. 대상이
  진짜로 다른 키를 가졌을 때(실제 키를 메시지에 명시), 또는 키를 아예 읽을 수 없을 때는 여전히
  거부합니다("알 수 없음"을 안전으로 취급하지 않습니다). 대상 키가 소스 키와 같은 테이블은 영향이
  없고 대상 조회도 발생하지 않습니다.

### Added

- `target_primary_key_columns()` — 대상 테이블의 실제 PK 컬럼을 키 순서대로 반환하는 읽기 전용
  카탈로그 조회(스키마·테이블명은 바인드 파라미터로 전달). "판정 불가"는 `None`이며, 호출자는 이를
  안전하지 않음으로 취급해야 합니다.

### Tests

- append 키 판정의 모든 분기(빈 대상, Full load + CDC, 일치하는 데이터 보유 대상, 옛 키를 가진
  대상, 읽을 수 없는 키, 그리고 대상을 조회하지 않음을 단언하는 키 동일 경로)와 새 조회의 키 순서·
  비정규화 이름 처리·인젝션 안전성·불명 경로를 덮었습니다. 옛 일괄 거부 복원과 불명 키를 일치로
  취급하는 변이를 포함해 8개 변이가 각각 잡혔습니다.

## v0.1.191

### Fixed

- **Data Migration 테이블 선택기가 너무 일찍 잠기고, 그 해제 방법도 막다른 길이었습니다.**
  전제조건 체크가 실행되는 순간 잠겼는데 — 체크는 *미리보기*일 뿐 확정이 아니므로, 어떤 마이그레이션도
  시작하기 전에 대상 범위가 잠겨버렸습니다. 게다가 잠금 툴팁은 "마이그레이션할 테이블을 바꾸려면
  체크를 다시 실행하세요"라고 안내했지만, 재실행은 같은 집합을 다시 고정하므로 Start over 외에는
  빠져나갈 길이 없었습니다. 이제 선택기는 되돌릴 수 없는 무언가에 실제로 커밋되기 전까지 편집
  가능한 상태로 유지되며, 각 잠금은 자신의 원인과 해제 방법을 설명합니다:
  - 이 집합으로 Full Load가 실행됨 (해제: Start over);
  - CDC가 스트리밍 중이라 소스 커넥터의 테이블 목록이 고정됨 (해제: CDC 중지);
  - CDC 인프라가 배포되었거나 배포 중임 — 각 테이블의 Kafka 토픽 파티션은 토픽 생성 시점에
    확정되므로, 이후에 추가된 테이블은 영원히 단일 파티션으로 스트리밍됩니다 (해제: CDC 인프라 삭제).
    이 잠금은 MSK 생성이 Full Load와 겹치는 ~15-20분 구간을 보호하며, 이전에는 무방비였습니다.
- **체크 이후에 추가된 테이블이 Full Load 전체를 조용히 실패시킬 수 있었습니다.** 전제조건
  리포트는 그것이 커버한 선택보다 오래 살아남습니다(아무것도 지우지 않고, 이제 선택기는 편집
  가능하므로). 이후 추가된 테이블은 대상 스키마 검사를 받은 적이 없으며, 테이블 하나의 실패가 작업
  전체를 실패시킵니다. 이제 Run 버튼은 검사되지 않은 테이블 이름과 함께 차단되고, Prerequisites
  패널에도 일치하는 알림이 표시되며, 체크를 다시 실행할 때까지 유지됩니다. 테이블 제거는 갭으로
  간주하지 않습니다 — 그 경우 리포트가 상위집합이므로 여전히 선택된 모든 것이 이미 검사되었습니다.

### Tests

- 지금까지 커버리지가 전혀 없던 테이블 선택기 잠금에 테스트를 추가했습니다: 모든 커밋 상태에서의
  순수 `selection_lock_reason`(리포트만 있을 때는 편집 가능; 실행 중/완료된 Full Load, 라이브
  CDC, 배포됨/배포 중인 CDC 인프라로 인한 잠금; Full-load-only 실행이 무관한 CDC 스택으로 얼지
  않도록 스코프됨), 원인별 사유를 담은 렌더된 잠금 툴팁, 그리고 비대칭 `prereq_scope_gap`(제거는
  괜찮고 추가는 차단). 모든 테스트를 변이 테스트로 확인했습니다 — 옛 조기-잠금 재도입을 포함한 9개
  변이가 각각 잡혔습니다.

## v0.1.190

### Fixed

- **Edit 모드의 Copy 버튼이 편집 전 DDL을 복사했습니다.** 에디터 헤더가 에디터 생성 시점의 DDL
  문자열을 캡처해서, 수정을 입력한 뒤 "Copy Target DDL"이 원본을 돌려줬습니다 — 긍정 "복사됨"
  토스트와 함께 — 반면 "Apply to target"은 편집본을 보냈습니다. 같은 버튼 줄이 서로 어긋난 것입니다.
  `_render_copy_ddl_button`이 이제 클릭 시점에 읽는 콜러블을 받고, 에디터 헤더가 편집 버퍼를 읽는
  콜러블을 넘깁니다. 브라우저로 확인: 입력 후 복사하면 편집본이 나옵니다.

### Tests

- **코드 리뷰가 변이 테스트로 찾아낸 5개 공백을 메웠습니다** — 테스트가 렌더 결과가 아니라
  `inspect.getsource(...)` 문자열을 검사하고, `_NotesUi`/`_DdlPaneUi` 더블이 `props()`/
  `classes()`/`on_click`을 버려서, 회귀가 나도 스위트가 통과하던 동작들입니다. 각각 변이를 다시
  돌려 잡히는 것을 확인했습니다:
  - 변환 노트 톤 반전(진짜 `LOSS`가 차분한 sky-blue, 선택적 권장이 중립 회색 — 이 시리즈가
    고치려던 심각도 역전);
  - 권장 배지를 `negative`로(빨간 조언);
  - `dialog.open()` 삭제 / 렌더 가드 반전 / 확대 핸들러 raise(확대 기능 전체가 무동작);
  - `current`를 생성 DDL로 되돌림(저장한 편집이 화면에서 사라지는데 Apply는 여전히 편집본 전송);
  - 인라인 `.ddl-pane` 높이 규칙 삭제(비교 패널이 CodeMirror 256px 기본값으로 복귀).
  `_NotesUi`와 `_DdlPaneUi` 더블이 이제 카드-배지 짝, 에디터 클래스, 버튼 클릭, 다이얼로그 열림을
  기록하므로, 소스 텍스트가 아니라 실제 렌더를 검증합니다.

## v0.1.189

### Changed

- **공개된 ECR Public 기본값이 `0.1.188`을 가리킵니다.** 두 리전 ECR(`ap-northeast-2`,
  `us-east-1`)과 ECR Public 모두 `0.1.188`을 갖고 있으며, 신규 배포가 사용하는 익명 pull 경로까지
  확인했습니다.

## v0.1.188

### Fixed

- **Schema Conversion의 노트 카드가 Evaluation 결과와 같은 스타일이 되었습니다.** 각 변환 경고와
  권장사항이 색 지정 없는 `border`를 써서 Tailwind 기본값인 거의 검정으로 렌더됐습니다 — 이 앱의
  카드가 아니라 테두리 친 표 셀처럼 읽혔고, 선택적인 권장사항에 Evaluation이 `UNSUPPORTED` 항목에
  두르는 것보다 더 진한 선을 그렸습니다. 이제 두 화면이 같은 틴트 표면과 대응하는 `*-200` 테두리를
  씁니다 — 실제 갭은 중립 회색, 조언은 차분한 sky 톤(Evaluation이 쓰는 것과 같은 짝), `rounded-md`
  라운딩과 같은 여백까지 동일합니다. 두 화면을 함께 묶는 테스트를 추가해, 한쪽 스타일을 바꾸면 다른
  쪽이 뒤처졌다는 사실이 드러나게 했습니다.

## v0.1.187

### Changed

- **공개된 ECR Public 기본값이 `0.1.186`을 가리킵니다.** 새로 clone한 사용자가 이미지를 빌드하지
  않고도 코드 에디터 기반 DDL 비교와 정렬된 Bedrock 기본값으로 배포됩니다. 두 리전 ECR
  (`ap-northeast-2`, `us-east-1`)과 ECR Public 모두 `0.1.186`을 갖고 있습니다.

## v0.1.186

### Changed

- **확대한 DDL이 화면을 다 덮는 대신, 내용 크기에 맞춘 다이얼로그로 본문 위에 열립니다.** 최대화는
  1440x900 화면을 다 덮으면서 정작 **가장 클 때도 약 1060x800**만 필요한 패널을 보여줬고(실제
  소스로 측정한 최장 줄 144자, 최장 DDL 29줄), 뒤 페이지가 사라져 비교 화면이 놓여 있던 문맥도
  함께 잃었습니다. 이제 다이얼로그 폭은 `min(1100px, 92vw)`이고 높이는 DDL에 따라
  `min(44rem, 74vh)`까지 늘어납니다 — 29줄 객체는 679px, 4줄 객체는 160px, 둘 다 잘리지 않습니다.
  이를 가능하게 하는 것이 래퍼의 `height: auto`입니다 — 없으면 CodeMirror가 256px로 고정되어 길이와
  무관하게 모든 DDL이 같은 높이가 됩니다.

## v0.1.185

### Added

- **각 DDL 패널을 전체 화면으로 확대할 수 있습니다.** 비교 화면은 좌우 분할이라 각 패널이 창의
  절반만 쓰는데, 실제 소스로 측정해보니 **18개 테이블 중 14개가 그 폭에 담기지 않는 줄**을 갖고
  있었고 4개는 패널 높이를 넘었습니다. 둘 다 스크롤은 되지만, 144자짜리 `CHECK` 제약을 절반 폭
  창구로 읽는 일이 바로 운영자가 여기서 검토하는 대신 DDL을 에디터로 복사해 나가게 만드는 이유
  입니다. 각 패널의 복사 버튼 옆 확대 아이콘이 그 DDL을 최대화 다이얼로그로 엽니다 — 전체 폭,
  약 82vh 높이, 같은 방언 하이라이팅, 읽기 전용. 패널을 더 높이는 대신 전체 화면인 이유는 제약이
  **폭**이기 때문입니다. 옵트인이므로 이미 잘 담기는 객체에서는 기본 2패널 화면이 그대로입니다.
  Edit 모드에는 의도적으로 넣지 않았습니다 — 다이얼로그는 읽기 전용이고, 편집 중인 에디터 옆에
  두면 닫으면 버려지는 복사본에 편집을 유도하게 됩니다.

## v0.1.184

### Fixed

- **DDL 에디터가 어느 쪽을 편집하는지 알려줍니다.** **Edit**를 누르면 헤더 밴드가 둘 다 사라져
  아무 이름 없는 코드 박스만 남았습니다 — 소스 패널은 설계상 읽기 전용이므로, source 대 target이
  핵심인 화면에서 "내가 지금 어느 쪽을 고치는가"는 당연한 의문이었습니다. 이제 에디터가 읽기 전용
  비교와 **같은 `Target — Aurora DSQL` 헤더 밴드**를 복사 버튼과 함께 갖고, `Editing` 배지도 그
  밴드 위 제목 옆으로 옮겼습니다. 타깃 헤더만 전체 폭으로 표시합니다 — 소스는 화면에 없어 혼동할
  대상이 아니고, 함께 표시하면 소스도 편집 가능하다는 오해를 줍니다. 헤더는 이제 공유 헬퍼 하나라
  두 모드가 어긋날 수 없습니다.
- **에디터가 비교 패널과 같은 처리를 씁니다.** 기존에는 범용 `SQL`로 하이라이팅되고 줄바꿈이
  켜져 있어서, 옆 패널(`PostgreSQL`, 줄바꿈 없음)과 같은 DDL이 다르게 읽혔습니다. 이제 둘 다 타깃
  방언을 쓰고 한 논리적 줄을 한 줄로 유지합니다.

## v0.1.183

### Changed

- **Schema Conversion의 DDL 비교가 양쪽 모두 실제 코드 에디터가 되었습니다.** 기존에는 두 DDL을
  줄 단위로 정렬해 보여주는 수작업 diff 표였는데, 줄이 길어지면 무너졌습니다 — `break-all`로
  줄바꿈하면서 토큰 중간을 잘라 `ENUM` 목록이 `'cancel` / `led')`로 쪼개졌고, 한 논리적 줄이 여러
  행을 차지하면서 애초에 그 표가 제공하려던 좌우 정렬이 오히려 어긋났습니다. 이제 각 패널은 NiceGUI
  내장 CodeMirror이며, 표에는 없던 것들을 함께 얻습니다 — **방언별 실제 SQL 하이라이팅**(왼쪽
  MySQL, 오른쪽 PostgreSQL이라 백틱 식별자와 이중 따옴표 식별자가 각각 올바르게 렉싱됨), 줄 번호,
  코드 폴딩, 그리고 깨끗한 줄로 복사되는 텍스트 선택. 긴 줄은 한 줄로 유지되고 마크다운 코드
  블록처럼 가로 스크롤됩니다.
  - 포기한 것은 줄 단위 대응입니다. 각 패널이 1행부터 시작하므로 바뀐 줄이 상대 줄 옆에 물리적으로
    놓이지 않습니다. 이 패널은 객체 하나의 DDL을 담고, 아래 변환 노트가 이미 무엇이 바뀌었는지
    (제거된 외래 키, async 인덱스, 재매핑된 타입) 명시하므로, 그 대응은 행 위치로 추측하는 대신
    말로 서술됩니다.
  - 패널은 `readonly`가 아니라 `disable`입니다. NiceGUI의 CodeMirror에는 readonly prop이 없고
    넘겨도 조용히 무시되므로, 비교 화면이 편집 가능한 상태로 남아 있었습니다 — 사용자가 타이핑하면
    다음 렌더에서 사라지는데 **Apply to target**은 편집 전 DDL을 그대로 보냈습니다. 편집은 여전히
    **Edit** 버튼 뒤의 자체 모드에서 하고 Apply는 그 버퍼를 보냅니다. 둘 다 브라우저로 종단
    검증했습니다.
  - 기존 뷰의 diff 엔진(`diff_ddl_lines`, `DiffRow`, `DiffKind`, 셀 렌더러와 `DIFF_*` 디자인
    토큰)은 다른 호출자가 없어 제거했습니다 — 순 139행 감소.

## v0.1.182

### Changed

- **공개된 ECR Public 기본값이 `0.1.181`을 가리킵니다.** 새로 clone한 사용자가 이미지를 빌드하지
  않고도 정렬된 Bedrock 기본값과 Sonnet 5로 배포됩니다. 두 리전 ECR(`ap-northeast-2`,
  `us-east-1`)과 ECR Public 모두 `0.1.181`을 갖고 있습니다.

## v0.1.181

### Fixed

- **기본 설정으로 배포하면 AI Assist가 `AccessDenied`로 실패했습니다.** CloudFormation 템플릿의
  `BedrockModelId` 기본값은 `us.anthropic.claude-sonnet-4-6`인데 앱 자체 기본값은
  `global.anthropic.claude-sonnet-4-6`이었습니다. 태스크 역할의 `bedrock:InvokeModel` 범위는
  템플릿 값에서 **파생**되지만, Connect 폼의 Model ID를 비워 두면 앱은 *자기* 기본값으로
  호출합니다 — 그래서 기본 배포는 정책이 허용하지 않는 프로필을 호출했고, "Verify AI access"는
  두 기본값이 어긋난 문제를 IAM 정책 결함처럼 보이게 하는 권한 오류를 냈습니다. 두 값이 어긋날 수
  없다는 것과 기본값이 `AllowedValues`에 포함된다는 것을 테스트로 고정했습니다.
- **AI Assist를 켠 뒤에는 `BEDROCK_MODEL_ID`가 폼에 반영되지 않았습니다.** prefill이 설정 *전체*를
  깨끗한 `AiAssistConfig()`와 비교했기 때문에, Enable 스위치를 켜기만 해도 값이 달라져 이후 모든
  렌더에서 seed가 조용히 건너뛰어졌습니다 — IAM은 배포 값으로 범위가 잡혀 있는데 앱은 내장 기본값을
  쓰는 상태가 됩니다. 이제 필드별로 검사합니다: 모델 ID는 아직 내장 기본값일 때, 리전은 설정되지
  않았을 때만 seed하며, 사용자가 입력한 값은 절대 덮지 않습니다.

### Changed

- **기본 모델이 Claude Sonnet 5**(`global.anthropic.claude-sonnet-5`)가 되었고 Opus 5도 함께
  제공합니다. 실제로 검증했습니다 — 프로필이 `ACTIVE`이고 `ap-northeast-2`에서 호출이 성공하며,
  템플릿이 파생하는 IAM 범위(`inference-profile/global.anthropic.claude-sonnet-5` +
  `foundation-model/anthropic.claude-sonnet-5`)가 실재하는 모델로 연결됩니다.
- **이제 `global.` 추론 프로필만 제공합니다.** `us.` 계열은 `us-east-1` / `us-east-2` /
  `us-west-2`에서만 동작하고 그 밖에서는 실패하는데, `global.`은 그 세 리전에서도 동작합니다 —
  `us-east-1`, `us-west-2`, `ap-northeast-2`에서 확인했습니다. 선택지가 아니라 함정이었고, 애초에
  템플릿 기본값이 앱과 어긋난 원인도 지역 접두어가 둘이었기 때문입니다. 저장소 어디에도 `us.`
  모델 ID가 남아 있지 않으며, 배포 가이드·매뉴얼(EN/KO/JA)과 README도 함께 갱신했습니다.

## v0.1.180

### Fixed

- **같은 화면에서 한 객체 종류를 두 가지로 부르지 않습니다.** 상단 집계는 `3 Routines`인데 아래
  목록과 차트는 바로 그 객체들을 **Stored procedures**와 **Functions**로 나눠 보여줬습니다 — 존재
  하지 않는 헤딩의 개수를 세게 만든 셈입니다. MySQL은 실제로 둘을
  `information_schema.ROUTINES`로 묶으므로 인벤토리 필드명은 정확합니다. 평가 쪽이 나누는 이유는
  DSQL이 둘을 다르게 다루기 때문입니다(`LANGUAGE SQL` 함수는 살아남을 수 있지만 plpgsql은 불가).
  이제 집계가 목록의 어휘를 씁니다 — `2 Stored procedures · 1 Functions`. 서브타입을 정말 알 수
  없을 때만 `Routines`로 표시하고, 개수가 0인 종류는 타일을 만들지 않습니다.
- **차트 축이 원시 enum 값을 보여줬습니다.** UI 차트와 HTML 내보내기 차트 모두 막대를
  `PROCEDURE` / `FUNCTION`으로 표시하면서 바로 아래 목록 헤딩은 `Stored procedures`였습니다 —
  같은 불일치가 한 줄 아래에 있었던 것입니다. 라벨 맵을 UI에서 `core/assessor.py`
  (`KIND_LABELS`)로 옮겨, 목록 헤딩·두 차트·집계가 모두 하나의 출처를 읽습니다. UI가 복사본이
  아니라 같은 객체를 갖는다는 것도 테스트로 고정했습니다.

## v0.1.179

### Changed

- **공개된 ECR Public 기본값이 `0.1.178`을 가리킵니다.** 이 이미지는 새로 `git clone`한 사용자가
  아무것도 빌드하지 않고 배포하는 대상인데, 11개 릴리스 동안 `0.1.167`에 고정돼 있었습니다 —
  즉 신규 배포에는 `0.1.168` 이후의 Evaluation 개선이 하나도 포함되지 않았습니다. 두 리전 ECR
  (`ap-northeast-2`, `us-east-1`)과 ECR Public 모두 이제 `0.1.178`을 갖고 있습니다.

## v0.1.178

### Changed

- **접힌 객체 행이 대표 배지 하나 대신 카테고리별 라벨 배지를 갖습니다.** 대표 배지는 가장 심각한
  분류만 말하고 나머지에 침묵했기에, `Unsupported`로 표시된 행이 6개 항목을 숨기면서 그중 4개는
  단순 검토, 1개는 선택적 조언인 경우가 있었습니다 — 대부분이 막히지 않았는데 객체 전체가 막힌
  것처럼 보였습니다. 이제 각 행이 `1 Unsupported · 4 Review needed · 1 Recommended`를 색 배지로
  보여줍니다(빨강, 호박, 그리고 조언이 카드 안에서 쓰는 차분한 info-blue). 숫자만 두지 않고 라벨을
  유지하므로 심각도가 색에만 의존하지 않습니다 — 그러지 않으면 모노크롬 스크린샷이나 색맹 독자는
  차트 legend를 찾아야 합니다. 첫 배지는 기존 대표 배지가 보여주던 분류이므로 행은 여전히
  심각한 것부터 읽히고, 이를 대체한 별도의 회색 요약 줄은 사라졌습니다.

### Fixed

- **클러스터 수준 항목이 `concerns` 도입 이전 스타일로 렌더되고 있었습니다.**
  `Database / cluster-level` 행(다중 소스 데이터베이스, 테이블 수 한계)이 **Risk** /
  **Recommendation** 문단을 그대로 보여주는 반면 옆의 모든 테이블은 라벨 카드 스타일을 써서,
  목록의 한 행만 다른 앱처럼 보였습니다. 원인은 스타일이 아니라 데이터였습니다 — inventory 수준
  검사는 `concerns`를 채우는 집계를 거치지 않고 `AssessmentItem`을 직접 만들면서 그 필드를 비워
  두었습니다. 이제 자신의 발견을 concern으로 담으므로, 다른 테이블과 동일한 카테고리 배지·세로
  스파인·Risk/Recommendation 패널을 갖습니다. 텍스트·HTML 내보내기도 같은 목록을 렌더하므로 함께
  개선됩니다. `AUTO` 객체만 concern이 없을 수 있다는 리포트 전체 불변식도 테스트로 고정했습니다.

## v0.1.177

### Changed

- **접힌 객체 행이 배지가 숨기던 항목 구성을 함께 보여줍니다.** 헤더 배지는 *대표* 분류만 말하고
  나머지에 대해서는 침묵합니다. 실제 소스로 측정했을 때 18개 테이블 중 16개가 배지 하나 뒤에
  혼합 구성을 갖고 있었고(대개 진짜 갭 + `AUTO_INCREMENT` 권장사항), `Unsupported`로 표시된 행이
  6개 항목을 숨기면서 그중 4개는 단순 검토, 1개는 선택적 조언인 경우도 있었습니다. 대부분이
  막히지 않았는데도 객체 전체가 막힌 것처럼 보였습니다. 이제 각 행에
  `1 Unsupported · 3 Review needed · 1 Recommended`가 붙습니다 — 바로 위 kind 그룹 헤더가 이미
  쓰는 `N Label · M Label` 표기와 같고, 조언 항목은 카드 안의 배지와 같은 단어인 `Recommended`로
  셉니다. 배지를 그대로 반복하게 되는 경우(대표 분류의 단일 항목, 또는 문제 없는 객체)에는
  표시하지 않습니다.
- **객체 단위 effort 배지가 접힌 행에서 사라졌습니다.** 이 배지는 객체 전체를 설명하는데 행은 이제
  항목 구성을 요약하며, `SIMPLE` 하나와 `SIGNIFICANT` 하나는 평균을 내도 쓸모 있는 숫자가 되지
  않습니다. 각 항목은 펼치면 여전히 자기 견적을 갖고 있고, 스키마 전체 분포는 목록 위 요약에
  그대로 남아 있습니다.

## v0.1.176

### Changed

- **작업량(effort) 배지가 모든 곳에서 같은 중립 외곽선으로 표시됩니다.** 기존에는 요약 행만
  녹/호박/적 램프로 레벨별 색을 입혔고, 객체 행과 항목 카드는 같은 값을 회색으로 그렸습니다 —
  하나의 값에 두 가지 표현이었습니다. 게다가 이 램프는 잘못된 신호입니다. 이 화면에서 램프는
  *호환성*을 뜻하며(차트, 분류 배지, Risk/Recommendation 패널이 모두 사용), 작업량은 심각도가
  아니라 시간의 순서 척도입니다. 색을 입히면 그 의미가 희석되고, 객체 행에서 충돌합니다 —
  호박색 `Review needed` 옆에 호박색 `effort: MEDIUM`, 빨간 `Unsupported` 옆에 빨간
  `effort: SIGNIFICANT`가 놓였습니다. 이제 세 곳이 상수 하나를 공유하므로 색은 호환성 전용으로
  남고, 다시 갈라질 수 없습니다.

## v0.1.175

### Changed

- **작업량(effort) 요약이 리포트 헤더에서 객체 목록 옆으로 이동했습니다.** 기존에는 분류 행 바로
  아래, 즉 분류 기준으로 나뉜 차트 위에 있었습니다 — 차트가 아무것도 말하지 않는 요약이 차트의
  근거가 되는 요약과 나란히 놓여 있던 셈입니다. 더 나쁜 것은 두 행이 똑같이 생겼는데 합계가
  달랐다는 점입니다(`SIMPLE 1 · MEDIUM 3 · SIGNIFICANT 2` = 6인데 객체는 8개). 필수 작업이 없는
  객체 — 전부 `AUTO`이거나, v0.1.174부터는 권장사항만 있는 객체 — 는 작업량 견적이 없어 어느
  버킷에도 들어가지 않기 때문입니다. 모든 객체를 합산하는 분류 행 옆에서 읽히니, 다른 질문이라기
  보다 객체가 누락된 것처럼 보였습니다. 작업량은 목록을 다루는 도구이므로 이제 그 목록과 작업량
  필터 옆에 놓이며, "(*n* of *m* objects need work)"로 스스로 설명합니다. 작업이 필요한 객체가
  없으면 아예 표시되지 않습니다. 헤더에는 분류 개수만 남아 차트와 문구가 정확히 일치합니다.

## v0.1.174

### Changed

- **객체의 발견 항목이 우선순위대로 정렬됩니다 — 실제 갭이 먼저, 조언이 마지막.** 기존의
  심각도 단독 정렬은 둘을 섞어버렸습니다. 조언인 `AUTO_INCREMENT`도 분류가 `MANUAL`이라,
  단지 룰 선언 순서 때문에 진짜 `MANUAL` 갭보다 위에 놓였고, 테이블을 펼친 사용자는 실제로
  처리해야 하는 외래 키보다 선택적인 처리량 조언을 먼저 마주쳤습니다. 이제 갭이 모든 권장사항보다
  앞에 오고, 갭끼리는 심각도 순(`UNSUPPORTED` → `MANUAL`)을 유지합니다. 목록이 위에서 아래로
  "지금 조치할 것"에서 "선택적으로 튜닝할 것"까지 읽힙니다. 부수 효과도 유용합니다 — 행 헤더의
  대표 룰이 갭이 있는 객체에서는 항상 갭이 되므로, 권장사항이 객체의 대표 항목처럼 표시되지
  않습니다. 발견이 조언 하나뿐인 객체는 그대로 그것을 보고합니다. 화면·텍스트 내보내기·HTML
  내보내기가 같은 목록을 렌더하므로 세 곳이 함께 정렬됩니다.

## v0.1.173

### Changed

- **Evaluation이 권장사항과 실제 변환 갭을 구분합니다.** `Classification`은 "일이 얼마나 많은가"에
  답하지만 "실제로 뭔가 잘못됐는가"에는 답하지 않는데, 이 둘이 뭉쳐 있어서 조언이 결함처럼 보였습니다.
  이제 각 발견이 **note kind**를 함께 가집니다 — `LOSS`(옮기지 못했거나 의미가 바뀜) 또는
  `RECOMMENDATION`(변환은 완전하고 정확하며, 무시하면 성능을 잃지만 정확성은 잃지 않음).
  `AUTO_INCREMENT`가 그 권장사항입니다 — 이 키는 깨끗하게 변환되고, UUID/랜덤 키나 캐시된 identity로
  바꾸는 것은 insert 처리량을 얻기 위한 선택입니다.
  - enum은 `ConversionNoteKind`이며 `core/converter.py`에서 `core/models.py`로 옮겨 **두** 평가가
    공유합니다. v0.1.151에 도입될 때 converter 전용이었고, 바로 그래서 Schema Conversion만 이 구분을
    갖고 Evaluation은 `AUTO_INCREMENT` 키를 계속 위험이라고 불렀습니다 — 두 화면이 같은 키에 대해
    20개 릴리스 동안 서로 다르게 말했습니다. enum 하나를 공유하면 이런 종류의 어긋남이 한 번 고쳐지는
    게 아니라 구조적으로 불가능해집니다. `core.converter`가 재수출하므로 기존 import는 그대로 됩니다.
  - **권장사항이 객체의 예상 작업량을 부풀리지 않습니다.** 작업량은 "이걸 마이그레이션하려면 내가 얼마나
    일해야 하는가"에 답하는데, 선택적인 처리량 조언은 마이그레이션이 요구하는 일이 아닙니다. 외래 키
    우회만 필요한 테이블(`SIMPLE`, 2시간 미만)이 `AUTO_INCREMENT` 키도 있다는 이유만으로
    `MEDIUM`(2~6시간)으로 보고됐고, MySQL 테이블은 대부분 그 키를 가지므로 가장 흔한 테이블 형태의
    견적이 부풀려졌습니다. 실제 7개 테이블 스키마로 측정했을 때 2개가 `MEDIUM`에서 `SIMPLE`로
    내려갔습니다. 발견이 *전부* 조언인 객체는 이제 작업량을 아예 갖지 않습니다. 조언을 채택할 때의
    비용은 계속 표시되므로("effort if you take it") 판단 정보는 유지됩니다.
  - 조언 발견은 차분한 info-blue로 렌더됩니다 — `RECOMMENDED` 배지, `Risk` 대신 `Note` 라벨 — 화면,
    텍스트 내보내기(`[RECOMMENDED]`), HTML 내보내기(녹/호박/적 심각도 램프 밖의 info-blue 셀) 모두에
    적용됩니다. 발견의 기본값은 `LOSS`이므로 다른 모든 룰은 그대로이고, 이 변경 이전에 저장된 리포트도
    이전과 똑같이 렌더됩니다.

### Fixed

- **KO 매뉴얼의 깨진 링크 3개 추가 수정.** `ko/11-customer-faq.md`가 여전히 `10-conclusion.md`를
  영어 앵커로 가리키고 있었고, v0.1.166에서 16개를 고칠 때 누락된 것들입니다. 이제 `docs/manual/`의
  모든 장 간 앵커가 정상적으로 연결됩니다.

## v0.1.172

### Fixed

- **Evaluation이 `AUTO_INCREMENT` 키를 결함처럼 표시하지 않습니다.** 기존에는 호박색 **Risk**
  제목 아래에 *"AUTO_INCREMENT column 'id' produces monotonic keys that cause hot partitions
  in Aurora DSQL"*로 나왔지만, 이 키는 깨끗하게 변환되고 정상 동작합니다 — 누락되는 것도 없고
  쿼리 결과가 달라지지도 않습니다. UUID/랜덤 키나 캐시된 identity로 바꾸는 것은 **insert
  처리량**을 얻기 위한 선택입니다(DSQL은 행을 기본 키 순서로 저장하므로 단조 증가 키는 쓰기를
  한 파티션에 집중시킵니다). 이제 문구가 테이블에 대해 사실인 것부터 말하고("converts cleanly
  and works as-is") 변경이 선택 사항임을 명시합니다 — Schema Conversion이 v0.1.151에서 이미
  적용한 수정(`ConversionNoteKind.RECOMMENDATION`)과 같은 방향이며, 당시 이 룰이 누락되어 두
  화면이 같은 키에 대해 서로 다르게 말하고 있었습니다.

## v0.1.171

### Changed

- **호환성 차트가 객체 종류를 개수 많은 순으로 정렬합니다.** 기존의 "문제 비율" 기준 정렬은
  `UNSUPPORTED` 트리거 한 개를 테이블 200개보다 위에 올려서, 짧은 막대가 긴 막대 위에 떠 있는
  모양이 됐습니다 — 우선순위로 읽히기보다 차트가 깨진 것처럼 보였습니다. 이제 막대가 길이순으로
  내려가고(`TABLE`, `PROCEDURE`, …), 각 막대는 여전히 자체 빨간 구간과 "*n*% need attention"
  설명을 달고 있으므로 심각도 신호는 그대로입니다. HTML 내보내기도 같은 집계를 쓰므로 동일하게
  정렬되며, 두 순서가 어긋나지 않도록 테스트로 고정했습니다.
- **Evaluation의 각 항목이 문제와 해결책을 뚜렷이 구분된 두 블록으로 표시합니다.** 기존에는
  위험이 그냥 한 문장이고 해결책은 그 아래 작은 화살표만 붙은 흐린 문장이어서, 둘이 한 문단처럼
  읽히고 해결책을 지나치기 쉬웠습니다. 이제 각각 자체 틴트 패널 위에 글리프와 라벨을 달고 놓입니다
  — 호박색 **Risk**, 녹색 **Recommendation**으로, 앱 전반의 호박색=주의 / 녹색=해결 톤을 따릅니다.
  텍스트·HTML 내보내기는 이미 둘을 구분해 표시하고 있었으므로(`Risk:` / `Fix:`, 그리고 각자의 표
  열), 이번 변경으로 화면이 내보내기 수준을 따라잡았습니다.

## v0.1.170

### Changed

- **Evaluation 차트가 작업량(effort) 대신 호환성(classification) 기준으로 막대를 나눕니다. 내보낸
  리포트도 화면을 따릅니다.** 이 막대는 분류 요약과 Auto-converted / Review needed / Unsupported
  뱃지가 달린 목록 바로 위에 있는데도, 막대 자체는 Simple / Medium / Significant actions로
  나뉘어 있었습니다 — 하나의 그림에 두 가지 어휘가 섞여, "내 스키마 중 실제로 넘어가는 건 얼마나
  되는가?"에 답하려면 둘을 머릿속에서 변환해야 했습니다. 이제 세 가지 분류가 그 순서대로 쌓이므로
  막대가 왼쪽에서 오른쪽으로 "스스로 넘어감"에서 "넘어갈 수 없음"까지 읽히고, 객체 종류별 행은
  가장 많이 막힌 것부터 정렬됩니다. HTML 내보내기도 같은 집계·같은 라벨·같은 색을 쓰며 제목을
  **Compatibility by object kind**로 맞췄고, 막대별 설명은 "*n*% need attention"(자동 변환되지
  않는 전부)으로 바뀌었습니다. 작업량은 그대로 유지되며 자체 요약 뱃지, 필터, 객체별 표시에서
  계속 보고됩니다.
- **Evaluation을 펼쳤을 때 각 항목이 하나의 들여쓰기 세로선 뒤에 테두리 카드로 표시됩니다.** 기존
  에는 항목이 평평한 구분선으로만 나뉘어, 객체 두 개를 펼치면 블록들이 뒤섞여 어떤 항목이 어느
  객체의 것인지 알기 어려웠습니다. 세로선 + 카드는 Schema Conversion의 객체 트리가 쓰는 것과 같은
  포함 관계 표현입니다.
- **HTML 리포트가 각 항목에 별도의 표 행을 부여합니다.** 이전 버전은 위험 목록을 한 셀에, 해결책
  목록을 다른 셀에 넣어서, 여전히 목록의 순서를 세어 짝을 맞춰야 했습니다. 이제 각 항목이 자체
  rule id·분류·작업량을 가진 하나의 행이 되고 객체·종류 셀이 그 그룹을 걸쳐 병합됩니다. 필터는
  그룹 전체를 함께 숨기고, 개수 표시는 그대로 객체 수를 셉니다.

## v0.1.169

### Changed

- **Evaluation이 각 위험과 그에 대응하는 해결책을 항목별로 나눠 보여줍니다.** 하나의 객체가 서로
  독립적인 규칙 여러 개에 걸리는 일은 흔합니다 — 외래 키, `AUTO_INCREMENT` 키, 대소문자 구분 없는
  collation, `ENUM` 컬럼, `ON UPDATE` 타임스탬프는 각각 별개의 판단이고 해결책도 각각 다릅니다.
  기존에는 모든 규칙의 문구가 하나의 **Risk** 문단과 하나의 **Recommendation** 문단으로 합쳐져,
  보고할 내용이 가장 많을 때 오히려 읽을 수 없게 되었고 *n*번째 위험과 *n*번째 해결책을 짝지어
  읽는 일이 사용자에게 떠넘겨졌습니다. 이제 규칙마다 자체 블록으로 표시되며 각자의 rule id, 분류,
  작업량을 함께 보여줍니다 — Evaluation 화면, 텍스트 내보내기, HTML 리포트(두 열이 정렬된 목록이
  됩니다) 모두에 적용됩니다. 항목별 분류 뱃지도 붙어서, 하나가 `UNSUPPORTED`이고 나머지가
  `MANUAL`인 상황이 드러납니다 — 행 헤더는 가장 심각한 분류만 보여주므로 그 사실이 가려져
  있었습니다. 합쳐진 `risk`/`recommendation` 문자열은 하위 호환과 단순 내보내기를 위해 그대로
  채워지며, 이 변경 이전에 저장된 리포트는 그 문자열을 그대로 렌더링합니다.

## v0.1.168

### Changed

- **공개 ECR Public 기본값이 `0.1.167`을 가리킵니다.** 새로 CloudFormation으로 배포할 때 받아가는
  이미지이므로 배포 버전을 따라가야 합니다 — v0.1.166/v0.1.167의 컬럼 `DEFAULT` 보존과 DDL 거부
  수정 3건이 담겨 있으며, 이것들이 없으면 신규 배포는 클러스터가 거부하는 스키마를 만들거나(또는
  적용은 되지만 모든 기본값이 사라진 스키마를) 조용히 생성합니다.

## v0.1.167

### Fixed

- **`ON UPDATE CURRENT_TIMESTAMP`가 생성된 `CREATE TABLE`을 실패시키던 문제를 수정했습니다.**
  v0.1.166에서 컬럼 기본값을 생성하기 시작했지만, SQLAlchemy의 MySQL 리플렉션은 `ON UPDATE` 절을
  기본값 *안에* 접어 넣습니다 — `datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`가
  `"CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"` 하나의 문자열로 반환됩니다 — 그래서 그대로
  통과되어 타깃이 *`syntax error at or near "ON"`* 으로 거부했습니다. 가장 흔한 감사 컬럼에서
  기본값 누락이 변환 실패로 바뀐 셈입니다. 근본 원인을 소스에서 수정했습니다: 컬럼 기본값을
  이제 **`information_schema.COLUMN_DEFAULT`** 에서 읽으며, `ON UPDATE` 부분은 `EXTRA`에 남아
  `auto_update_timestamp`가 이미 읽고 있습니다(그리고 assessor가 이미 MANUAL로 보고합니다 —
  DSQL에는 `ON UPDATE` 절도 트리거도 없습니다).
- **같은 리플렉션 경로가 유발한 결함 2건.** MySQL 8은 *표현식* 기본값을 괄호로 감싸 보고하므로
  `DEFAULT (uuid())`가 `"(uuid())"`로 도착해 리터럴로 오판되었고, `bit(1) DEFAULT b'1'`은
  리플렉션 정규식이 **조용히 버렸습니다**. `information_schema`는 둘 다 정확히 보고합니다.
  표현식/리터럴 판별은 이제 따옴표 추론이 아니라 MySQL의 `DEFAULT_GENERATED` 플래그가 결정합니다 —
  추론으로는 리터럴 문자열 `'CURRENT_TIMESTAMP'`와 함수 호출을 구분할 수 없었습니다.
- **identity 전략에서 `bigint unsigned AUTO_INCREMENT` 키가 실패하던 문제를 수정했습니다.**
  unsigned 정수 키는 범위 보존을 위해 `DECIMAL(20,0)`으로 매핑되는데 DSQL identity 컬럼은
  `BIGINT`여야 합니다 — 그래서 참조 스키마 11개 테이블 중 6개가 *`identity column type must be
  bigint`* 로 거부되었습니다. 이제 좁은 정수 타입과 함께 `DECIMAL`도 확장합니다. DSQL의 identity
  시퀀스는 어차피 BIGINT 범위이므로 생성 가능한 값의 손실은 없습니다.
- **`DATETIME`의 `CURRENT_TIMESTAMP` 기본값을 UTC로 고정합니다.** `DATETIME`은 타임존 없는
  `timestamp`로 매핑되고 로더는 의도적으로 마이그레이션 행을 naive UTC로 정규화합니다. 맨몸
  `CURRENT_TIMESTAMP` 기본값은 세션 `TimeZone`을 따르므로, 컷오버 후 애플리케이션이 쓴 행이
  마이그레이션된 행과 몇 시간씩 어긋날 수 있었습니다.

### Changed

- **지원하는 인벤토리 형식은 하나, 휴리스틱은 없습니다.** 컨버터는 `introspector.enrich_columns`가
  만드는 형식(따옴표 없는 값 + `DEFAULT_GENERATED` 플래그)만 읽습니다. 모든 MySQL 소스는 이
  보강을 무조건 거칩니다. 따옴표 기반 폴백은 추측에 맡기는 대신 제거했습니다. 검증 스크립트도
  이제 보강을 수행합니다 — 그것이 없으면 앱이 결코 쓰지 않는 코드 경로를 시험하게 되고, 실제로
  거기서만 존재하는 실패를 보고했습니다.
- **드문 리터럴/타깃 불일치에는 의도적으로 전용 분기를 두지 않았습니다.** 정수 타깃의 비트 문자열
  기본값, `bytea`의 이진 기본값, MySQL의 `0000-00-00` 제로 날짜 — 실제 스키마에는 하나도 없으며,
  각각 아무도 겪지 않는 케이스를 위해 코드 경로와 테스트를 늘립니다. 일반 규칙으로 흘려보내
  거부가 발생하면 변환 시점에 조용하지 않게 드러납니다.
- **매뉴얼에 기본값 처리를 문서화했습니다**(EN/KO/JA) — 2장의 "변환이 해주는 일" 목록에 기본값이
  빠져 있었고, 4장 제약 표에는 DSQL이 기본값을 *지원한다*는 사실과 `ON UPDATE CURRENT_TIMESTAMP`는
  재현 불가라는 점을 기록했습니다.

### Added

- **`scripts/verify_conversion_on_dsql.py`를 이제 배포에 포함합니다**(`scripts/*` 무시 규칙에
  제외돼 있었습니다). `scripts/README.md`에 고객용 읽기 전용 검증 도구로 문서화했습니다: 자신의
  스키마를 변환해 마이그레이션 *전에* Aurora DSQL이 무엇을 거부하는지 확인합니다. 합성 매트릭스는
  위의 모든 기본값 형태를 포함해 53 케이스로 늘었습니다.

## v0.1.166

### Fixed

- **스키마 변환에서 컬럼 `DEFAULT` 값이 조용히 사라지던 문제를 수정했습니다.** Aurora DSQL은
  컬럼 기본값을 지원합니다(`DEFAULT default_expr`가 문서화된 `CREATE TABLE` 문법에 있고,
  리터럴·표현식·`CURRENT_TIMESTAMP`/`now()`·`gen_random_uuid()`·`NOT NULL DEFAULT`를 실제
  클러스터에서 모두 확인) — 컨버터가 그것을 생성하지 않았을 뿐입니다: `ColumnDef.default`는
  introspector가 채우지만 읽는 코드가 없었습니다. 마이그레이션된 행은 영향이 없지만(로더가 명시적
  값을 씀), 컷오버 후 **애플리케이션**이 쓰는 행은 다릅니다: MySQL은 기본값이 있는 `NOT NULL`
  컬럼을 생략한 `INSERT`를 받아주지만, 타깃은 같은 `INSERT`를 not-null 위반으로 거부합니다.
  참조 스키마에서 기본값을 가진 22개 컬럼이 **전부** `NOT NULL`이었으므로 코너 케이스가 아닙니다.
  이제 기본값이 보존되며, 단순 통과로는 틀리는 세 가지를 변환합니다:
  `tinyint(1) DEFAULT '1'`은 `DEFAULT TRUE`로(boolean에 `DEFAULT 1`은 DSQL에서 오류),
  `AUTO_INCREMENT` 컬럼에는 기본값을 넣지 않고(identity 컬럼의 DEFAULT는 거부됨), generated
  컬럼에도 넣지 않습니다(값이 계산됨). 정말로 번역할 수 없는 기본값(MySQL `UUID()`,
  0/1을 벗어난 `tinyint(1)` 기본값)은 **경고와 함께** 드롭하며, 컷오버 후 어떤 결과가 되는지
  명시합니다 — 기존에는 아무 보고도 없었습니다.
- **Identity 기본 키가 Aurora DSQL이 거부하는 DDL을 생성하던 문제를 수정했습니다.** 두 개의
  별개 결함이며, 둘 다 `IDENTITY_WITH_CACHE` 전략에서 발생하고 테스트가 생성된 DDL *텍스트*를
  검사하기 때문에 보이지 않았습니다:
  - `CACHE 100` — DSQL은 `CACHE`를 명시하도록 요구하며 `1` 또는 `>= 65536`만 받습니다
    (*"CACHE (100) must be greater than or equal to 65536 or equal to 1"*). 이제 65536이며,
    이는 허용되는 최소 캐시값이자 이 전략의 목적에도 맞습니다.
  - `INT` identity 컬럼 — DSQL 시퀀스는 BIGINT 전용이며(*"datatype integer not supported,
    identity column type must be bigint"*), MySQL `int AUTO_INCREMENT`는 가장 흔한 기본 키라
    전형적인 테이블이 깨졌습니다. 좁은 정수 identity 컬럼은 이제 `BIGINT`로 확장됩니다(무손실).
- **Aurora DSQL 정밀도 상한을 넘는 `DECIMAL`이 적용되지 않던 문제를 수정했습니다.** MySQL은
  `DECIMAL(65,30)`까지 허용하지만 DSQL은 정밀도 38, 소수 자릿수 37이 상한입니다. 이제 스펙을
  클램프하며 — 범위가 손실되므로 경고와 함께 — 정밀도가 줄면 과대한 소수 자릿수도 함께
  줄입니다(`scale > precision`은 그 자체로 오류).

### Added

- **`scripts/verify_conversion_on_dsql.py`** — 컨버터의 출력을 실제 Aurora DSQL 클러스터에
  적용해 클러스터가 거부하는 것을 보고합니다. 빠져 있던 바로 그 검증입니다: 단위 테스트는 생성된
  DDL 텍스트를 검사하므로(그래서 스냅샷이 잘못된 `CACHE 100`을 그대로 고정하고 있었습니다)
  E2E는 위 형태들을 하나도 포함하지 않는 손으로 만든 스키마 하나만 돕니다. MySQL 방언의 *롱테일*
  49 케이스(`SET`, `BIT`, 공간 타입, 넓은 `DECIMAL`, 따옴표/빈/음수 리터럴 기본값, generated
  컬럼 등)를 훑고, 선택적으로 실제 소스 스키마의 모든 테이블도 훑습니다 — 세 결함 중 둘이
  비기본 전략에서만 닿았으므로 모든 기본 키 전략에 대해 수행합니다. 소스는 읽기 전용이며,
  타깃에서는 스크래치 스키마에 테이블을 만들고 삭제합니다. 실패 시 non-zero로 종료하므로
  릴리스 게이트로 쓸 수 있습니다.

## v0.1.165

### Fixed

- **공개된 ECR Public 이미지 — 새 배포가 실제로 받아가는 이미지 — 가 130 릴리스나 낡아 있었습니다.**
  `deploy/cloudformation.yaml`은 `ContainerImageUri` 기본값을
  `public.ecr.aws/.../mysql-dsql-migrator:<태그>`로 두어 일반 배포에 이미지 빌드가 필요 없게 하는데,
  그 태그가 앱이 `0.1.164`인 상황에서도 여전히 `0.1.34`(2026-07-02)였습니다. 템플릿으로 배포한
  사람은 7월 2일 빌드를 받았습니다 — Query Converter 이름 변경, Settings 다이얼로그, CDC 보안 그룹
  수정 등 이후 모든 변경이 빠진 상태로. ECR Public 발행은 빌드 스크립트에 `PUBLIC_IMAGE_URI=…`를
  주는 옵트인 추가 단계라 빠뜨리기 쉬웠고, 그 결과를 확인하는 장치가 없었습니다. 이제 `0.1.164`를
  발행하고 기본값이 그것을 가리키며, 기본값이 배포 버전과 같은 major.minor 라인에 있고 패치 20개
  이내로 유지되는지 검사하는 테스트를 추가했습니다 — 실패 메시지에 재발행 명령까지 담았습니다.
  기본값이 `latest`가 아닌 고정 숫자 태그인지도 확인하므로, "같은 템플릿" 재배포가 조용히 다른
  이미지를 쓰는 일이 없습니다.

## v0.1.164

### Changed

- **사이드바 하단이 "Settings" 하나로 정리되어, 클릭하면 탭이 있는 다이얼로그가 열립니다.**
  Performance tuning, Diagnostics, 활동 로그 다운로드는 기존에 인라인 확장 패널 2개와 버튼으로
  사이드바에 놓여 있었습니다 — 9개 입력 필드가 폭 약 16rem의 사이드바에 밀려들어갔고, 한 패널을
  열면 나머지가 밀려났습니다. 세 항목 모두 성격이 같으므로(앱 전역 런타임 설정이며 마이그레이션
  흐름의 일부가 아님) 이제 하나의 톱니 항목 뒤로 모았습니다. 다이얼로그는 이를
  **Performance / Diagnostics / Activity log** 탭으로 묶습니다 — 여기 오는 이유는 세 카테고리를
  모두 읽기 위해서가 아니라 하나를 변경하기 위해서이기 때문입니다. 본문은 한 번만 생성되므로 입력
  중이던 값이 닫고 다시 열어도 유지되며, 다이얼로그는 persistent이고 명시적인 닫기 버튼이 있어
  바깥 클릭으로 사라지지 않습니다. 각 패널은 상한(`max-height` + 스크롤)이 있지만 공통 높이로
  늘리지는 않습니다 — 컨트롤이 두 개뿐인 패널이 화면 한가득 빈 공간을 만들지 않습니다. 여유가
  생긴 만큼, 문구도 "live, app-wide" 주의를 세 번 반복하는 대신 각 섹션이 실제로 무엇에 영향을
  주는지 설명하도록 바꿨습니다.

## v0.1.163

### Changed

- **Query Converter의 SQL 편집창을 우측 하단 모서리를 드래그해 크기를 조절할 수 있습니다.** 긴
  구문을 고정된 박스 안에서 스크롤할 필요가 없습니다. 이를 위해 Quasar의 `autogrow`를 제거했습니다 —
  입력 이벤트마다 textarea의 인라인 높이를 내용 높이로 다시 써서, 타이핑하는 순간 수동 드래그가
  되돌려졌습니다. 두 기능은 공존할 수 없습니다. 편집창은 기존과 같은 기본 높이로 시작하고,
  `max-height`를 두어 아주 큰 붙여넣기가 Convert 버튼을 화면 밖으로 밀어내지 않으며, 타이핑 중에도
  드래그한 크기가 유지됩니다(실제 브라우저에서 검증). 리사이즈 핸들도 필드 모서리로 당겨야 했습니다 —
  Quasar가 필드 내용을 좌우 12px 안쪽으로 밀어넣어, 브라우저가 그리는 핸들이 둥근 테두리 아래에
  깔려 절반만 보였습니다.

## v0.1.162

### Changed

- **"Query validation" 도구의 이름을 "Query Converter"로 변경했습니다.** 기존 이름은
  *Validation* 을 재사용했는데, 이는 4단계의 고유 명칭이며 완전히 다른 작업입니다 — **마이그레이션된
  데이터** 를 정확한 `COUNT(*)`, 체크섬, 테이블별 PK 재조정으로 비교하는 작업입니다. 그래서 선택적
  보조 도구가 워크플로 단계를 다시 하는 것처럼 읽혔습니다. 또한 부차적 동작을 화면 이름으로 삼고
  있었습니다: 변환은 이 화면이 항상 수행하는 유일한 동작이고(타깃 테스트는 검증된 타깃 연결이
  필요하며, AI 리뷰와 AI DBA 튜닝은 그 위에 추가로 선택하는 기능입니다), 이제 제목이 핵심 동작을
  가리킵니다. 2단계의 **Schema Conversion** 과 짝을 이룹니다 — 거기서는 스키마, 여기서는 쿼리입니다.
  캡션은 여전히 *"Convert & test app queries"* 이므로 좁아진 제목이 무언가를 숨기지 않습니다.
  매뉴얼 3개 언어를 모두 갱신했습니다. 챕터 파일명은 여러 곳에서 링크되므로 변경하지 않았고, 코드는
  `query_playground` 모듈명을 유지하되 두 표기가 서로 다른 화면으로 오해되지 않도록 해당 모듈
  독스트링에 명시했습니다.

## v0.1.161

### Fixed

- **백그라운드 작업 실행 중 툴팁이 깜빡여서 읽을 수 없던 문제를 수정했습니다.** Quasar 툴팁은 부착된
  요소의 *자식* 이므로, 영역을 다시 렌더링하면 포인터가 올라가 있던 요소가 파괴됩니다 — Quasar가
  툴팁을 닫고, 다시 호버해야만 열립니다. 1초 미만 간격의 폴링 두 곳이 매 틱마다 조건 없이 전체를
  다시 렌더링했고, 그 결과 툴팁이 초당 2~3회 재생성되었습니다:
  - **Validation의 "Cancel validation"** (0.5초 폴링). 실행 중 실제로 변하는 것은 진행 라벨, 진행률
    바, 취소/중지 상태 세 가지뿐이므로, 이제 패널을 한 번만 만들고 폴링이 그 세 가지를 제자리에서
    갱신하며(`set_text` / `set_enabled` / `set_value`) 자체 타이머를 다시 설정합니다 — Connect
    단계가 Next 버튼을 제어하는 방식과 동일합니다. 작업이 종료 상태가 되면 화면 전체가 결과 화면으로
    바뀌므로 그때는 여전히 다시 렌더링합니다.
  - **Query playground의 "Test on target"** (0.4초 폴링). 프로빙 분기에서는 틱 사이에 바뀌는 것이
    없으므로(스피너와 고정 텍스트), 이제 프로브가 끝날 때까지 기다렸다가 결과를 그려야 하는 시점에
    정확히 한 번만 다시 렌더링합니다.
  `render_notice`가 헤더/본문 라벨을 반환하도록 하여 폴링 영역이 문구를 제자리에서 교체할 수 있게
  했습니다. 정적 알림을 그리는 기존 호출부는 영향을 받지 않습니다.

### Notes

- 더 느린 폴링에는 같은 패턴이 남아 있습니다 — Full Load 진행률(1.5초)과 CDC 모니터링(5초).
  해당 간격에서는 방해가 훨씬 적어 별도 변경으로 남겨두었습니다.

## v0.1.160

### Fixed

- **소스 변경 확인 기능이 RDS MySQL에서 동작하게 되었습니다 — 기존에는 항상 "unavailable"이었습니다.**
  drift를 GTID로만 판정했지만 RDS MySQL 8.0에서는 GTID를 켤 수 없습니다 — 그래서 이 툴의 주
  지원 소스에서 매 실행마다 *"could not be determined (GTID unavailable)"* 만 표시되어, 이 섹션은
  자기가 묻는 질문에 한 번도 답할 수 없었습니다. 워터마크는 이미 binlog `file:position`을 기록하고
  있고(CDC도 그것으로 재개합니다), 필요한 좌표를 수집해두고도 쓰지 않던 상태였습니다. 이제 양쪽에
  GTID가 있으면 GTID로, 없으면 binlog `file:position`으로 판정하며, 어떤 기준을 썼는지 리포트에
  기록합니다. 비교는 순서가 아니라 동일성을 검사합니다 — 그래서 로그 로테이션에서도 올바릅니다
  (position은 새 파일마다 다시 시작하므로 나중 파일이 더 작은 값을 가질 수 있습니다) — 그리고 좌표가
  뒤로 이동한 경우(소스 복원, `RESET MASTER`)도 정상이 아니라 "변경됨"으로 처리합니다.

### Changed

- **이 섹션이 원시 사실을 나열하는 대신 마이그레이션 유형에 맞춰 해석합니다.** 소스가 계속 변하는
  것은 CDC가 살아 있을 때의 정상 상태인데, 패널은 유형과 무관하게 "소스가 스냅샷 이후 전진했다"고만
  말해서 문제처럼 읽혔습니다. 이제 CDC가 있으면 `info`("정상 — CDC가 복제 중이며, 최종 확인 전에
  지연을 0으로 배출하세요"), CDC가 없으면 `warning`으로 해당 행들이 타깃에 **없으며** 지금 컷오버하면
  유실된다고 분명히 말합니다. 변경이 없으면 `success`, 판정 불가면 경고하지 않고 `info`입니다.
- **헤더와 상세 표시를 평이하게 바꿨습니다.** "Drift since snapshot"은 두 단어 모두 전문 용어였습니다 —
  "drift"는 복제 용어이고 "snapshot"은 워터마크의 내부 명칭입니다 — 그래서 섹션 이름을
  **"Source changes since the comparison"** 으로 변경했습니다. 원시 좌표는 접힌 "Technical detail"
  블록으로 이동했고(그 값으로 "얼마나 뒤처졌는지"는 알 수 없습니다 — GTID는 거리가 아닙니다), 실제로
  판정을 만든 좌표를 먼저 보여주며 GTID가 꺼져 있으면 그 *이유* 까지 밝힙니다 — "unavailable" 두 줄을
  맨 위에 두고 실제 근거를 묻어버리던 기존 방식을 바꿨습니다.

## v0.1.159

### Changed

- **Validation의 "Objects to validate" 단축 버튼이 다른 객체 선택 화면과 일치하게 되었습니다.**
  Schema Conversion과 Data Migration은 "Select all"/"Unselect all"을 동일한 방식으로 —
  긍정 동작은 primary + `done_all`, 해제 동작은 grey + `remove_done` — 렌더링하는데,
  Validation의 "Include all"/"Exclude all"에는 색상도 아이콘도 없어서 다른 화면과 나란히 두면
  다른 앱처럼 보였습니다. 이제 공용 규약을 따르며, 세 화면이 다시 어긋나지 않도록 다른 화면의
  소스와 비교해 검증하는 테스트를 추가했습니다. 활성화 조건은 그대로입니다 — 실행 중에는 둘 다
  비활성이고, 각 버튼은 실제로 무언가를 바꿀 수 있을 때만 활성화됩니다.

## v0.1.158

### Changed

- **"Cancel validation"이 무엇을 기다리는지 알려줍니다.** 취소는 협조적(cooperative) 방식이며
  폴링 지점이 두 곳뿐입니다 — 각 테이블 시작 전, 그리고 PK 재조정 중 수천 행마다. 그래서 대형
  테이블에서 이미 실행 중인 `COUNT(*)`나 체크섬은 중단 지점이 없어 끝까지 수행되며(수 분), 동시에
  비교 중인 다른 테이블들도 마찬가지입니다. 화면에는 "Stopping…"만 표시되고 그 옆에는
  "Comparison in progress — safe to leave running" 패널이 그대로 남아 있어서, 실제로는 정상적으로
  종료 중인 취소가 클릭이 무시된 것처럼 보였습니다. 이제 라벨은
  *"Stopping… waiting for the in-flight table comparisons to finish."*로 표시되고, 패널은 종료
  과정을 설명하도록 바뀌며(무엇이 건너뛰어지는지, 실행 중인 쿼리를 왜 중단할 수 없는지, 부분
  리포트는 생성되지 않는다는 점), 버튼은 "Stopping…"으로 이름을 바꾸지 않고 원래 이름을 유지합니다
  — 그렇게 하면 상태 라벨과 중복되고 어떤 동작을 요청했는지 알려주는 표시가 사라지기 때문입니다.
  진행률 바는 취소 중에는 숨깁니다. 테이블 *완료* 수를 추적하므로 "Cancelling" 메시지와 상충하며
  계속 올라가기 때문입니다. 동작 자체는 변경되지 않았습니다 — 새로운 중지 메커니즘이 아니라 정직한
  피드백이며, 검증은 처음부터 끝까지 읽기 전용입니다.

## v0.1.157

### Fixed

- **CDC start을 누르는 순간 — 아직 데이터가 흐르기도 전에 — Data Migration이 "Success"로
  표시되던 문제를 수정했습니다.** 이 스텝(과 상단 스테퍼 헤더·화면 내 상태 칩의 뱃지)은 CDC가
  라이브가 되면 Done으로 승격되며 Validation도 함께 열립니다. 그런데 승격 판단을 CDC *입력*을
  잠그는 것과 같은 신호에 걸어두었습니다 — 그 신호는 의도적으로 Start를 누르는 즉시 발동해(시작
  지점·테이블 셋을 더 이상 편집하지 못하도록) 커넥터가 MSK Connect에서 올라오는 동안(~10–20분,
  아직 어떤 행도 타깃에 도달하지 않음)에도 켜져 있습니다. 그래서 start 도중에 헤더가 Success로
  보였습니다. 이제 승격은 별도의 더 좁은 신호 — 커넥터가 실제로 감지되었거나 cdc-stack phase가
  `running` — 를 사용하므로 "Success"는 데이터가 실제로 흐른다는 의미가 됩니다. 입력 잠금 래치는
  그대로(원래대로 Start 시점에 발동)이고, Full Load가 끝나면 예전처럼 스텝이 Done이 됩니다.

## v0.1.156

### Fixed

- **CDC 템플릿의 아포스트로피 하나 때문에 CDC 배포가 매번 실패하고, 실패한 스택은 수동 정리가
  필요한 상태로 남았습니다.** `ConnectorSecurityGroup`의 인라인 HTTPS egress 규칙 설명에 S3에
  도달하는 경로를 "via the *customer's* own NAT"로 적어두었습니다. EC2는 보안 그룹 **규칙**
  설명에 `a-zA-Z0-9`와 `. _-:/()#,@[]+=&;{}!$*`만 허용하는데 아포스트로피는 여기에 없습니다.
  더구나 이 문자 집합은 같은 템플릿의 `Parameters`나 리소스 설명에 허용되는 자유 텍스트보다
  좁아서, 문장으로는 전혀 이상해 보이지 않았습니다. 그 결과(`mysql-dsql-cdc-stack-0729`에서
  확인) `ConnectorSecurityGroup CREATE_FAILED - Invalid rule description`으로 스택이
  롤백되었고, 롤백마저 `ROLLBACK_FAILED`가 되었습니다 — `CustomPlugin` 두 개가 아직
  `CREATING` 상태였고 MSK Connect는 그 상태의 플러그인 삭제를 거부하기 때문입니다. 즉 문자
  하나가 단순 재시도가 아니라 수동 스택 정리를 요구했습니다. 해당 문구를 수정했고, 템플릿의 모든
  보안 그룹 규칙 설명(인라인 규칙과 독립 `AWS::EC2::SecurityGroup{Ingress,Egress}` 리소스
  모두)을 EC2 허용 문자 집합과 255자 제한으로 검증하는 테스트 2개를 추가해 다음 실수는 배포까지
  도달할 수 없게 했습니다.

## v0.1.155

### Fixed

- **VPC ID를 입력하기 전에는 "Deploy CDC infrastructure"가 더 이상 준비된 것처럼 보이지 않습니다.**
  VpcId는 이 툴이 유추할 수 없는 유일한 배포 입력값입니다 — 서브넷/NAT, 플러그인 S3 버킷, DSQL
  클러스터 ARN, 소스 호스트와 그 자격 증명 시크릿은 모두 배포 시점에 자동으로 해석됩니다 — 그런데
  검증이 제출 경로에만 있었습니다. 그래서 버튼은 활성처럼 보였고, 클릭하면 확인 대화상자가 열려
  VPC 네트워크 진단과 비용 추정까지 수행한 뒤, 마지막 Deploy를 누른 다음에야 *"Enter your VPC ID."*
  토스트가 떴습니다. 이제 필드가 채워지기 전까지 버튼이 비활성이며, 무엇이 빠졌는지 한 줄 힌트로
  알려주고, ID를 입력하면 곧바로 활성화됩니다. 폼을 다시 그리는 대신 버튼 상태를 제자리에서
  갱신하므로, 입력 중인 필드가 커서 아래에서 재생성되지 않고 첫 Deploy 클릭도 삼켜지지 않습니다 —
  ID를 입력한 사용자의 다음 동작이 곧 Deploy 클릭이고, 아직 비활성인 버튼을 누르면 그 클릭은 조용히
  사라지기 때문입니다. 공백만 남긴 필드도 제출 경로 검증과 정확히 동일하게 비어 있는 것으로
  처리하며, 선행 조건 검사 미충족이 여전히 우선하므로 차단 사유는 항상 하나만 표시됩니다.

### Changed

- **사이드바 Connect 항목이 실제 연결 상태를 보여줍니다.** 기존 아이콘은 Connect가 선택된 화면인지만
  나타냈기 때문에, 앱 재시작으로 자격 증명이 사라진 세션(자격 증명은 절대 영구 저장하지 않습니다 —
  Property 7)이 정상 세션과 완전히 똑같이 보였고, 무언가를 실행하기 전에 Connect를 다시 거쳐야 한다는
  단서가 전혀 없었습니다. 이제 아이콘이 연결 상태를 담습니다: 소스와 타깃이 모두 검증되면 초록색 링크와
  "Connected", 복원된 진행 상태가 재검증을 필요로 하면 앰버색 끊어진 링크와 "Reconnect to resume",
  새 세션이 아직 연결하지 않은 상태면 중립적인 회색 링크입니다. 빨강이 아니라 앰버인 것은 의도된
  선택입니다 — 데이터는 그대로이고 자격 증명을 다시 입력하면 해결되므로, 디자인 시스템의 심각도 기준에
  따르면 진행을 막는 오류가 아니라 복구 가능한 경고이며, 같은 상태를 설명하는 기존 앰버색 재연결 배너와
  다이어그램 배지와도 일치합니다. 아이콘은 그 배너와 동일한 신호로 구동되므로 둘이 서로 어긋날 수 없습니다.

## v0.1.154

### Fixed

- **Full load + CDC 실행이 끝난 뒤 "Deploy CDC infrastructure"가 차단되던 문제를 수정했습니다.**
  v0.1.145에서 추가한 CDC 선행 조건 게이트가 CDC 모드 리포트를 요구했지만, 그 리포트는 프로세스
  메모리에만 존재합니다 — 의도적으로 영구 저장하지 않으며, Full Load가 시작될 때 초기화됩니다.
  그래서 정상 흐름(CDC 선행 조건 실행 → 적재 완료 → 배포)에서 방금 실행한 점검을 두고
  *"Run the CDC prerequisite checks first"*가 표시되었습니다. 이제 게이트가 적재 시작 시 기록되는
  durable 신호도 인정합니다: Full Load는 CDC 상위 집합 점검이 통과했을 때만 **시작될 수** 있기
  때문입니다. 리포트가 존재하지만 **실패**한 경우는 여전히 차단되고(라이브 신호), Full-load-only
  통과로는 CDC 게이트가 면제되지 않으며, 한 번도 점검하지 않은 세션도 여전히 차단됩니다. CDC
  라이프사이클 게이트 두 곳(Deploy infrastructure, Start CDC) 모두 적용했습니다.

## v0.1.153

### Fixed

- **"Stop Full Load"가 영구히 멈출 수 있었고, 그러면서 거의 끝난 것처럼 표시했습니다.** 실제로
  관측됨: 화면이 *"Stopping… finishing the current batch."* 상태로 진행 없이 머물렀고 — job은
  `RUNNING`, 워커 프로세스 4개는 CPU 0%로 유휴, 행 수는 변하지 않았습니다. 느린 종료가 아니라
  데드락이었습니다: progress drain이 소비를 멈추자 워커들이 IPC 큐를 채우고 블로킹 `queue.put`
  안에 갇혔고, **그 지점에서는 취소 이벤트를 확인하는 코드에 도달할 수 없습니다** — 따라서 취소가
  관측될 수 없었습니다. 부모는 timeout 없는 `as_completed(futures)`에서 대기했습니다. 세 가지를
  수정해 각 연결을 끊었습니다:
  - 워커의 progress는 `put_nowait`으로 보내고 큐가 가득 차면 버립니다. progress는 텔레메트리이며 —
    카운터는 다음 flush가 다시 누적하는 델타이고, 최종 합계는 워커의 반환값에서 옵니다 — 메시지를
    잃어도 진행률 표시가 약간 오래된 값이 될 뿐입니다. 블로킹은 살아있음(liveness)을 잃었습니다.
  - 정리용 sentinel도 non-blocking으로. `finally` 경로에서 큐가 가득 차면 job을 정리해야 할 그
    teardown 자체가 멈출 수 있었습니다.
  - 부모는 이제 취소 후 제한된 유예 시간 동안 조각 단위로 대기합니다. 워커가 제때 종료하지 않으면
    대기를 중단하고 풀을 정리하며, 미완료 테이블을 재시도 가능으로 표시합니다(적재는 멱등).
- **중지 메시지가 실제보다 과장하지 않습니다.** "Stopping… finishing the current batch"는 도구가
  지킬 수 없는 약속처럼 읽혔습니다. 이제 진행 중인 배치를 기다린다고 말하고, 응답하지 않는 워커는
  유예 시간 후 정리되며 해당 테이블은 재시도 가능해진다고 툴팁에서 설명합니다.

## v0.1.152

### Fixed

- **Full Load 진행 테이블의 "Records per page" 설정이 이제 유지됩니다.** 적재가 진행되는 동안
  테이블별 진행 테이블은 약 1.5초 폴링마다 다시 만들어지는데, 그 재생성 과정에서 *페이지*만
  유지되고 rows-per-page는 10으로 하드코딩되어 있었습니다. 그래서 값을 올려도 바로 다음 폴링에서
  되돌아갔습니다 — 설정이 동작하지 않는 것처럼 보이고, 선택값이 되돌아가면서 테이블이 계속
  새로고침되는 느낌을 주었습니다. 이제 폴링을 견디는 홀더가 `rowsPerPage`도 함께 유지하며,
  Quasar의 "All" 옵션(`0`)도 지원합니다. 테이블이 줄어들 때 페이지는 여전히 클램프되어 빈 페이지에
  남지 않습니다.

## v0.1.151

### Fixed

- **고객이 제공한 서브넷에서의 CDC teardown이 과금되는 MSK 클러스터를 남기지 않습니다.**
  offset-seeder Lambda는 S3로 HTTPS PUT을 보내 CloudFormation에 응답하므로, 커스텀 리소스가
  삭제되는 시점에도 커넥터 보안 그룹이 443을 허용하고 있어야 합니다. 바로 이 이유로 해당 규칙을
  보안 그룹의 **인라인** 규칙으로 만들어 두었는데(인라인 규칙은 Lambda의 ENI가 SG를 참조하는 동안
  삭제될 수 없음), 그것이 스택이 자체 네트워크를 소유한 경우로 한정되어 있었습니다. BYO-subnet
  배포에서는 SG가 *standalone* `ConnectorHttpsEgress` 리소스에 의존했는데, CloudFormation은 이를
  커스텀 리소스와 **병렬로** 삭제합니다. `mysql-dsql-cdc-stack-0727`에서 실제로 관측됨: seeder의
  `Delete`가 실행되기 전에 송신 규칙이 사라져 응답이 3회(각 5분) 타임아웃되고 스택이
  `DELETE_FAILED`로 끝나면서 **ACTIVE 상태의 MSK Serverless 클러스터가 과금**되고 있었습니다.
  이제 인라인 규칙을 **두 네트워크 모드 모두**에서 생성하고, 경쟁 상태를 다시 만들 수 있는 중복
  standalone 리소스는 제거했습니다.

## v0.1.150

### Fixed

- **반쯤 삭제된 CDC 스택을 더는 "Attach" 대상으로 제시하지 않으며, 조용히 사라지지도 않습니다.**
  teardown이 `DELETE_FAILED`로 끝난 뒤, Data Migration 단계는
  **"Attach to &lt;stack&gt; (DELETE_FAILED)"** 버튼을 그럴듯하게 보여주었습니다. 그런 스택에
  attach하는 것은 동작할 수 없고(리소스가 일부 삭제되어 스트리밍이 불가), 그 버튼은 정작 중요한
  사실을 가렸습니다 — 남아 있는 **Amazon MSK / NAT가 계속 과금**되고 있는데 이를 추적하는 세션이
  없다는 점입니다. 이제 발견된 스택을 상태로 분리합니다: 실패/롤백/삭제 중인 스택은 과금 위험을
  명시하고 삭제를 마치라고 안내하는 **error** 알림으로 표시되며 attach 버튼이 **없습니다**.
  정상 스택만 attach 대상입니다.
  - 화면 간 teardown 배너가, *스택*은 여전히 깨져 있는데 *작업*이 끝났다는 이유로 스스로 사라지지
    않습니다. `DELETE_FAILED` 결과나 앱 재시작으로 유실된 작업 기록은 이전에는 마커를 지우고 배너를
    완전히 감췄습니다. 이제 마지막으로 확인된 스택 상태도 함께 보므로, 남아 있는 과금 인프라가
    계속 보이고 조치할 수 있습니다.

- **뷰의 source DDL이 한 줄로 이어지지 않고 포맷되어 표시됩니다.** MySQL의 `SHOW CREATE VIEW`는
  전체 정의를 한 줄로 반환하며 앞에 서버 메타데이터(`ALGORITHM=`, `DEFINER=`, `SQL SECURITY`)가
  붙는데, 이것을 그대로 보여주고 있었습니다 — 읽을 수 없는 텍스트 덩어리였고, 타깃 쪽은
  pretty-print되므로 좌우 diff에서 서로 정렬될 수 없었습니다. 이제 소스를 sqlglot MySQL 방언으로
  다시 렌더하고 서버 메타데이터를 제거합니다(변환과 무관하며, `DEFINER=\`user\`@\`host\`를
  왕복시키면 backtick이 double quote로 바뀌어 유효하지 않은 MySQL이 사용자에게 노출됩니다).
  파싱할 수 없는 정의는 그대로 표시합니다 — 바로 그때가 원본을 봐야 하는 경우입니다.
- **스키마 적용이 실행되는 동안 객체 브라우저가 잠깁니다.** 적용 워커는 시작 시점에 고정된 객체
  목록을 받으므로, 실행 중 다시 선택해도 실제로 기록되는 내용은 바뀌지 않고 화면만 타깃과
  어긋났습니다. 더 나쁜 것은 실행 중 "Generate DDL"이나 "Reset all"을 누르면 진행 중인 적용이
  실행하고 있는 DDL이 교체되거나 폐기된다는 점입니다. 이제 적용이 진행되는 동안 트리, 일괄 선택
  버튼, 필터, 소스 새로고침, Generate, Reset이 모두 설명과 함께 비활성화됩니다.

### Changed

- **객체 브라우저의 좌우 패널이 같은 위치에서 시작합니다.** "Select all" / "Unselect all"을
  **Source (MySQL)** 헤더 행(새로고침 옆)으로 옮기고, primary key 범례를 트리 아래로 옮겼습니다.
  둘 다 소스 트리 위에 있어서 소스 트리를 아래로 밀었고, 타깃 트리는 필터 바로 뒤에서 시작했기
  때문에 좌우 비교가 눈에 띄게 어긋나 보였습니다.

- **"drop & replace" 실패 시, 데이터베이스의 위험한 힌트를 반복하는 대신 해결 방법을 알려줍니다.**
  뷰가 참조하는 테이블을 교체하려 하면 드라이버 원본 오류(`cannot drop table … because other
  objects depend on it … HINT: Use DROP ... CASCADE`)가 그대로 표시되었습니다. 이 힌트는 여기서는
  잘못된 조언입니다 — CASCADE는 이 도구가 재생성하지 못할 수도 있는 뷰를 조용히 삭제합니다. 적용
  과정은 테이블을 재생성하기 전에 **선택된 범위 안의** 뷰를 미리 DROP하므로, 막고 있는 뷰는 단지
  선택되지 않았을 뿐입니다(보통 이전 적용에서 생성된 것). 이제 실패 메시지가 막고 있는 뷰의 이름을
  알려주고, 객체 브라우저에서 그 뷰도 선택해 다시 실행하라고 안내하며(그러면 pre-pass가 먼저 DROP하고
  해당 뷰의 적용 단위가 다시 생성합니다) `DROP ... CASCADE`는 피하라고 명시합니다. 의존성 실패는
  OCC 재시도도 하지 않습니다 — 의존성은 일시적 충돌이 아니라 고정된 상태입니다.

### Changed

- **Schema Conversion이 권고와 실제 변환 손실을 분리해서 보여줍니다.** 기존에는 모든 항목이
  **"Conversion warnings"** 아래 같은 노란색 `MANUAL` 배지로 나열되어, 성능 권고가 결함처럼
  보였습니다: 유지된 `AUTO_INCREMENT` 키는 완벽하게 변환되고 정상 동작하며, UUID/랜덤 키나 캐시된
  identity로 바꾸는 것은 DSQL 파티셔닝을 위한 *성능* 제안일 뿐 고쳐야 할 문제가 아닙니다. 그런데
  그것이 "외래 키 제약이 DDL에서 제거되었습니다"(실제로 무언가를 잃은 항목) 바로 옆에 놓여
  있었습니다. 이제 변환 노트에 `kind`(`LOSS` / `RECOMMENDATION`)가 있고 UI가 두 섹션으로 나눕니다:
  **Conversion warnings**(옮기지 못했거나 의미가 바뀐 것 — MANUAL/UNSUPPORTED 심각도 유지)와
  **Recommendations**(차분한 info 파란색 `RECOMMENDED` 배지 + 변환은 완료되었다는 안내 문구).
  객체 헤더의 개수도 따로 세므로, 권고만 있는 테이블이 "Review needed · 1 warning"으로 읽히지
  않습니다.
  - 노트는 기본값이 `LOSS`(기존 모든 노트의 의미)이므로 AUTO_INCREMENT 키 노트만
    `RECOMMENDATION`을 명시합니다. composite key 노트는 `LOSS`로 유지됩니다 — 애플리케이션이
    키를 잡는 방식이 실제로 바뀌기 때문입니다.
  - AUTO_INCREMENT 문구를 "hot partitions를 유발한다"로 시작하는 대신 무슨 일이 있었는지("정수
    키가 유지되었고 깔끔하게 변환됨")로 시작하도록 다시 썼습니다. 위험을 실패처럼 서술하고
    있었습니다.
- **Primary key 선택 UI가 segmented control에서 AWS 스타일 타일로 바뀌었습니다.** Keep source PK
  와 Composite key는 지속적인 결과를 낳는 설계 결정이므로(composite key는 모든 쿼리·조인·upsert를
  바꾸고, DSQL 키는 생성 후 변경 불가) 각 옵션이 트레이드오프를 설명하는 Cloudscape "Tiles" 카드를
  갖습니다 — AWS가 결과가 따르는 선택에 쓰는 패턴이고, segmented control은 뷰 전환용입니다.
  이 스타일의 단일 소스로 `ui/design.py`에 `radio_tiles`를 추가했습니다.
- **source/target DDL diff가 AWS Console의 코드 서피스 방식으로 바뀌었습니다.** 기존에는 변경된
  모든 행을 진한 빨강/초록으로 채웠는데, MySQL→DSQL 이종 변환은 거의 모든 행을 다시 쓰기 때문에
  패널 전체가 칠해졌습니다. 검토 화면이 아니라 오류 보고서처럼 읽혔고, 진한 배경이 monospace
  텍스트와 경쟁했습니다. 이제 코드 영역은 **중립**(흰 배경, 차분한 헤더)이고 변경은 좁은
  **`+` / `−` 상태 거터**와 거의 보이지 않는 행 음영(`-50` 계열 40% 알파)이 전달합니다. 색이
  유일한 신호가 아니므로 흑백 스크린샷에서도, 색약 사용자에게도 diff가 읽힙니다. 실제로 바뀐 쪽만
  표시하므로 재작성된 행은 두 개의 요란한 블록이 아니라 하나의 before/after 쌍입니다. 관련 토큰은
  단일 소스로 `ui/design.py`(`CODE_*` / `DIFF_*`)로 옮겼습니다.
- **"Recommendations" 설명이 고정 텍스트에서 툴팁으로 바뀌었습니다.** "고칠 문제가 아니라 선택적
  튜닝 제안"이라는 문구는 이제 제목 옆 도움말 글리프에 있습니다 — `RECOMMENDED` 배지와 제목이 이미
  그 의미를 전달하고, 이 블록은 객체마다 반복되기 때문입니다.

## v0.1.149

### Fixed

- **Evaluation 단계가 사용자가 고르지도 않은 마이그레이션 타입을 띄우는 문제를 수정했습니다.**
  Migration plan 단계를 폐지(v0.1.147)하면서 여정 헤더의 마이그레이션 타입 배너가 모든 단계에
  렌더되기 시작했는데, `migration_type`은 full-load-only가 *기본값*이라 항상 값을 돌려줍니다.
  그래서 첫 단계부터 "Migration type: Full load only"와 전체 설명이 떠서, 선택 기회가 세 단계
  뒤에나 오는데도 손대지 않은 기본값을 확정된 결정처럼 보여주었습니다. (폐지된 단계에서는 선택이
  먼저였으므로 발생할 수 없었던 문제입니다.) 이제 세션이 타입을 **명시적으로 선택했는지**를
  추적하고, 배너는 그 시점 이후에만 표시됩니다 — 그 전 단계는 진행 스테퍼만 보여줍니다.
  - 이미 선택된 타일을 다시 누르는 것도 선택으로 인정됩니다. 타입에는 기본값이 있으므로
    "Full load only"를 클릭하는 것이 곧 확정 행위인데, 기존 선택기는 "변경 없음"으로 조기
    반환해서 그 사용자에게는 배너가 아예 뜨지 않았습니다. 하위 단계 초기화는 실제 변경에만
    적용되므로 확정만 하는 경우 화면은 그대로입니다.
  - 이 플래그는 영구 저장되므로, 이미 선택한 세션은 재접속 후에도 배너가 유지되고, 선택하지
    않은 세션에는 없던 선택이 만들어지지 않습니다. 이전 스냅샷은 "미선택"으로 복원되며 이것이
    안전한 방향입니다.

## v0.1.148

### Changed

- **진행 중인 백그라운드 작업을 알리는 notice에 애니메이션 스피너와 "In progress" 배지가
  표시됩니다.** 화면 간 CDC 배너("'<stack>'을 백그라운드에서 삭제 중(~15~45분)…")에는 정적 info
  아이콘만 있어서, 15~45분이 걸리는 teardown이 멈춰 있는 것과 구분되지 않는 무반응 메시지처럼
  보였습니다. `render_notice`에 `busy` 플래그를 추가해 정적 글리프를 톤 색상의 스피너로 바꾸고
  헤더 옆에 **In progress** 배지를 고정합니다. 진행 중인 teardown/stop 배너와 Data Migration의
  Prerequisites 하위 단계에 있는 CDC 인프라 배포 notice가 모두 이를 사용합니다. **실패한**
  teardown은 정적으로 남아 Retry/Dismiss 액션을 유지합니다 — 거기에 스피너를 쓰면 아직 작업이
  진행 중이라는 잘못된 인상을 주기 때문입니다.

## v0.1.147

### Changed

- **워크플로가 5단계가 되었습니다: "Migration plan" 단계를 폐지했습니다.**
  `Connect → Evaluation → Schema Conversion → Data Migration → Validation → Cut over`.
  이 단계는 "CDC를 포함할지?"라는 질문 하나를 **정보가 가장 적은 시점에** 물었습니다: 그 답을
  이후 3개 단계 동안 아무도 사용하지 않았고, (예를 들어 CDC가 절대 복제할 수 없는 캐스케이딩
  외래 키를 감지하는) Evaluation은 아직 실행되지도 않은 상태였습니다. 또한 Data Migration이 이미
  가지고 있는 결정을 중복했습니다: 같은 CDC 선택이 거기에 3-way 마이그레이션 타입 선택기로
  존재했고 잠기지도 않았으므로, 타입을 두 번 결정하는 구조였습니다. 이 단계가 하던 일은 모두
  실제로 조치 가능한 곳으로 옮겼습니다:
  - **마이그레이션 타입**은 Data Migration에서, 호환성 리포트로 상황을 파악한 뒤에 선택합니다.
  - **CDC 인프라 배포**는 Data Migration의 Prerequisites 하위 단계에서 제공됩니다(v0.1.146).
    여기는 여전히 Full Load보다 앞이므로, 약 15~20분의 MSK 생성이 Evaluation보다 먼저 앞당겨지는
    대신 스냅샷과 겹쳐서 진행됩니다.
  - Connect는 이제 곧바로 **Evaluation**으로 넘어가며, 중복된 "Include CDC?" 컨트롤은 삭제되었습니다.
- **마이그레이션 타입 배너가 이제 모든 단계에 표시됩니다.** 폐지된 단계가 이 배너를 숨겨야 했던
  유일한 화면이었기 때문에(2択 "Include CDC?" 컨트롤이 3값 배너와 충돌하는 것처럼 읽혔습니다),
  이제 여정 헤더가 드디어 모든 곳에서 동일합니다.
- **"Start over"가 고아가 된 CDC 인프라를 더 많은 경우에 경고합니다.** 이 경고는 마이그레이션
  타입이 **여전히** CDC 모드를 가리킬 것을 요구했는데, 이는 구멍이었습니다: 타입은 자유롭게 바꿀 수
  있으므로, MSK를 배포한 뒤 Full-load-only로 되돌린 사람은 아무 경고도 받지 못하고 과금되는
  클러스터를 조용히 남길 수 있었습니다. 이제 입력된 인프라 값 — 또는 새 세션이 절대 다시 발견하지
  못하는 기본값이 아닌 스택 이름 — 만으로도 충분합니다.

### Fixed

- **읽을 수 없는 저장된 세션이 더는 페이지를 깨뜨리지 않습니다.** SQLite 세션 저장소가 payload를
  오류 처리 없이 파싱했고, `SessionSnapshot`과 `WorkflowState` 둘 다 `extra="forbid"`이므로 —
  더 새로운 빌드가 쓴(또는 이후 제거된 필드를 담은) 스냅샷은 페이지 빌드에서 예외로 터져 복원된
  진행 상황만 잃는 게 아니라 도구 전체를 쓸 수 없게 만들었습니다. 이제 경고를 남기고 새로
  시작하며, S3 저장소와 동일하게 동작합니다.

### Compatibility

- `WorkflowStep.MIGRATION_PLAN`과 `WorkflowState.migration_plan`은 **back-compat 전용으로
  유지**됩니다(기존 `data_migration` 별칭과 동일). 필드를 제거하면 이를 담고 있는 기존 스냅샷이
  모두 검증에 실패합니다. 참조 세션 저장소의 19개 스냅샷은 전부 그대로 로드됩니다.
- 폐지된 단계에 **파킹된 세션**은 복원 시 Evaluation으로 리디렉션됩니다(19개 중 8개가 해당).
  이전처럼 조용히 Connect 화면으로 떨어지지 않습니다.
- 발표 자료(`docs/tech-talk-*`, `docs/full-load-cdc-slides-*`)는 저장소에서 제거되었습니다.
  이제 `docs/`에는 사용자 매뉴얼과 UI 스크린샷만 남습니다.
- README/배포 문서의 스크린샷이 **정적 PNG**(`docs/demo-ui.png`)로 바뀌었고, 5단계 UI에서 다시
  캡처했습니다. 기존 애니메이션 GIF는 폐지된 6단계 사이드바와 낡은 버전 칩이 프레임에 박혀
  있었습니다.

## v0.1.146

### Added / Changed

- **이제 Data Migration 단계에서 CDC 인프라를 배포할 수 있어, 약 15~20분의 MSK 생성이 Full Load와
  겹쳐서 진행됩니다.** 기존에는 배포 폼이 CDC 하위 단계 안에만 있었는데, Full Load + CDC
  마이그레이션에서 그 지점은 스냅샷이 **끝난 뒤에야** 도달합니다. 그래서 대기가 직렬화되었습니다:
  적재가 끝나고 나서야 약 15~20분의 프로비저닝이 시작됐습니다. 이제 **Prerequisites** 하위 단계
  하단에서 제공되며(여전히 Full Load보다 앞), 배포가 백그라운드로 돌아가니 스냅샷을 지금 시작하라고
  명시적으로 안내합니다. 깊은 CDC 하위 단계의 폼도 그대로 유지됩니다(아무것도 배포하지 않은 채로 CDC에
  도달하는 세션이 여전히 가능합니다).
  - 마이그레이션 타입 타일이 아니라 Prerequisites가 올바른 위치입니다: 점검을 실행하는 행위가 확정된
    테이블 셋을 고정하고 잠그며, 커넥터의 테이블 목록과 토픽 파티션 계획이 모두 그것을 필요로 합니다.
  - 이 섹션은 상황에 맞게 바뀝니다: 미배포 → 폼, 배포 중 → 실시간 진행 + "지금 Full Load를
    시작하세요", 이미 배포됨 → 짧은 "준비됨, 여기서 할 일 없음", 다른 이름으로 발견됨 → 두 번째 MSK
    클러스터 비용을 내지 말고 attach. 계정 전체 탐색이 보고하기 전에는 **아무것도** 렌더하지 않으므로,
    중복 클러스터 가드가 채워지기 전에 신규 배포 폼이 나타나는 일은 없습니다.
- **선행 조건 점검이 이제 실제로 커버한 테이블 셋을 기록합니다.** 리포트가 생기는 순간 선택기가
  잠기므로 그 셋이 곧 마이그레이션 범위인데, 사용자가 선택기를 건드리지 않은 경우에는 기본값으로만
  암시되어 저장된 선택이 비어 있었습니다. 그러면 이를 읽는 모든 곳이 "테이블 없음"으로 해석되었습니다:
  Full Load watermark가 생기기 전에 시작한 CDC 배포는 커넥터 테이블 목록이 비고 토픽 파티션 계획이
  균일하게 처리되었습니다.
- **진행 중인 CDC 인프라 배포가 이제 모든 화면에서 보입니다.** 사용자가 자리를 떠도 되는 유일한 CDC
  작업인데, 화면 간 배너는 stop/delete만 다루고 있었습니다 — 그래서 Data Migration을 벗어나면 아직
  돌아가는지 알 방법이 없어 사용자가 그것을 기다리며 앉아 있을 수 있었습니다. 이제 배너가 진행 중인
  배포도 알리고, Full Load가 막히지 않는다는 점을 다시 강조합니다.
- **Evaluation의 CDC 전용 외래 키 발견 항목을 CDC를 선택하는 지점에서 보여줍니다.** 평가는 자동
  `ON DELETE`/`ON UPDATE` 동작이 걸린 외래 키를 이미 감지합니다: MySQL은 이를 InnoDB 내부에서 자식
  행에 적용하므로 바이너리 로그에 남지 않고, CDC는 볼 수 없으며, DSQL(외래 키 없음)은 다시 수행할 수도
  없습니다 — 자식 행이 대상에 조용히 남겨집니다. 이 항목의 권고는 "CDC를 시작하기 전에"로 시작하는데,
  정작 사용자가 CDC 사용 여부를 알기 **전에** 읽는 Evaluation 리포트에만 나타났습니다. 이제 CDC
  마이그레이션 타입을 선택하면 영향받는 테이블을 바로 알려줍니다.

### Fixed

- **CDC 인프라 배포의 진행 예상 시간이 더는 실제보다 짧게 표시되지 않습니다.** `ensure_bucket`과
  `upload_plugins` 단계는 약 43 MiB의 커넥터 플러그인을 업로드하는데도 예상치가 없어서, 배포 중
  사용자의 유일한 신호인 총 ETA가 콜드 스타트 기준 약 1분가량 짧게 표시되었습니다.

## v0.1.145

### Fixed

- **Full Load만 실행한 뒤 CDC를 추가할 때 CDC 선행 조건 점검을 건너뛰던 문제를 수정했습니다.**
  선행 조건 리포트는 의도적으로 저장하지 않기 때문에, 한 번 적재가 실행된 뒤에는 리포트가 없어도
  실행 가드가 이를 면제해 줍니다 — 재접속한 사용자가 완료된 Full Load를 다시 실행할 수 있게 하는
  장치입니다. 그런데 이 면제가 **실제로 게이트를 통과시킨 모드로 한정되지 않아서**, "Full Load만
  먼저 하고 나중에 CDC 추가" 경로에서 Full Load의 통과가 **CDC** 모드에까지 상속되었습니다:
  Prerequisites가 "완료"로 접히고, **바이너리 로그 형식이 한 번도 검증되지 않은 채로** CDC
  하위 단계가 열렸습니다. `STATEMENT`/`MIXED` 형식이거나 `binlog_row_image=FULL`이 아닌 소스는
  절대 스트리밍할 수 없으므로, 이 문제는 과금되는 커넥터 생성 약 26분 뒤에 진단 불가한 실패로만
  드러났습니다. 이제 실행을 게이트한 모드를 기록(및 영구 저장)하고, 다른 점검이 필요한 전환에는
  그 점검을 요구합니다. 이전 스냅샷에는 이 필드가 없으므로 기존의 관대한 동작이 유지되어 재접속이
  강제로 막히는 일은 없습니다.
- **사이드바의 Data Migration 실행 가드가 화면 내 가드와 일치하게 되었습니다.** 모드를 넘기지
  않고 가드를 호출해 조용히 Full Load로 기본값 처리되었기 때문에, CDC 마이그레이션 타입에서는
  사이드바 Run 버튼이 활성화되어 보이는데 화면 안의 버튼은 (CDC 상위 집합으로 올바르게 게이트되어)
  비활성인 상태였습니다. 이제 두 곳 모두 선택된 마이그레이션 타입에서 모드를 유도합니다.

### Added

- **Deploy CDC infrastructure와 Start CDC에 자체 선행 조건 게이트가 추가되었습니다.** 두 액션은
  지금까지 하위 단계 순서(Prerequisites → Full Load → CDC)에 의존해 점검이 먼저 실행되었음을
  보장받았습니다 — 마이그레이션 타입을 이른 시점에 고르기 때문에만 성립하던 암묵적 보장입니다.
  이제 두 액션 모두 CDC 모드 점검이 실행되고 **`BINLOG_ROW_FORMAT`이 통과**했을 것을 명시적으로
  요구하며, 과금 인프라가 만들어지기 전에 무엇을 고쳐야 하는지(RDS라면 파라미터 그룹 변경 + 재부팅)
  설명합니다. 이 게이트는 무관한 필수 실패(예: 테이블별 대상 스키마 점검)는 의도적으로 무시합니다 —
  그건 Full Load 가드가 이미 보고하므로 한 문제를 두 번 알리지 않습니다.
- **Start CDC가 스냅샷 시점의 바이너리 로그가 이미 삭제된 경우 경고합니다.** watermark는 Full Load
  **시작** 시점에 캡처되므로, 긴 적재 시간 + 약 15~20분의 인프라 생성 + 커넥터 생성이 모두 지난
  뒤에야 Debezium이 그 위치를 읽습니다. 그 사이 소스가 해당 로그를 삭제했다면 무손실(gapless)
  인계는 불가능하고, 올바른 복구 방법은 새 스냅샷뿐입니다. 이제 커넥터 생성 전에 읽기 전용
  `SHOW BINARY LOGS`를 한 번 실행해서 사라진 로그, 아직 남아 있는 가장 오래된 로그, 보존 기간을
  늘리는 명령을 함께 알려줍니다 — 약 26분 뒤 진단 불가한 `CREATE_FAILED`(MySQL 오류 1236)로
  실패하는 대신입니다. 이는 **차단이 아니라 경고**이며(간극을 알고도 시작하는 것은 의도적일 수
  있습니다), 판단할 수 없는 경우에는 조용히 넘어갑니다 — watermark가 없거나, 수동 시작 위치를
  쓰거나, 해당 구문/권한을 쓸 수 없는 경우입니다.

## v0.1.144

### Fixed

- **CDC 인프라 배포가 더는 "CDC 스트리밍 중"으로 위장하지 않습니다.** 약 15~20분이 걸리는
  인프라 배포(`create_stack`: MSK Serverless, 네트워킹, 플러그인, IAM)는 **커넥터를 만들지
  않습니다** — 템플릿이 두 커넥터를 모두 `HasBootstrapServers` 조건에 걸어두고, 인프라 패스는
  그 값을 비워두기 때문입니다. 따라서 배포 중에는 아무것도 스트리밍되지 않습니다. 그런데 "CDC가
  스트리밍 중인가?" 판정이 진행 중인 **모든** CDC 라이프사이클 작업을 인프라 배포까지 포함해
  세고 있었기 때문에, (예: Migration plan 단계에서) 배포를 시작한 뒤 Data Migration을 열면
  파이프라인이 살아 있는 것처럼 동작했습니다:
  - **Data Migration이 `Success`로 승격**되어 **한 행도 적재되지 않은 상태로 Validation이
    열렸습니다.** 이 승격은 되돌아오지 않으므로 잘못된 상태가 **영구 저장**되어 재시작 후에도
    남았습니다.
  - **Start Full Load가 비활성화**되고 "CDC가 스트리밍 중 — 먼저 CDC를 중지하세요"라는 잘못된
    툴팁이 떴으며, 테이블 선택기도 잠겼습니다.
  - **"Drop & reload" 재실행이 조용히 append로 바뀌었습니다**: 라이브 sink가 대상에 쓰는 동안은
    DROP이 억제되므로, 재적재가 오래된 행을 갱신하지 않고 건너뛰었습니다("신규 0건 + 기존 N건").
  - Schema Conversion 단계에서 **스키마 적용이 차단**되었습니다. Data Migration(CDC를 중지할 수
    있는 유일한 곳)은 그 단계를 선행 조건으로 잠겨 있으므로 사용자는 갇히게 됩니다.
  이제 `kind="infra"` 작업은 제외됩니다. **커넥터 수준** 작업(Start / Stop / Delete CDC)이나
  실제로 스트리밍 중인 파이프라인만 인정됩니다. 탐지된 커넥터와 스택 phase `running`은 여전히
  우선하므로, CDC 라이브 안전장치는 모두 그대로입니다.
- **인프라 배포 내내 선행 조건 "Check" 버튼이 비활성화되던 문제를 수정했습니다.** 별개의 두 번째
  경로가 있었습니다: 패널이 CloudFormation의 모든 `*_IN_PROGRESS` 상태를 진행 중인 마이그레이션
  작업으로 취급해서, `CREATE_IN_PROGRESS` 동안 약 15~20분간 읽기 전용 점검을 쓸 수 없었습니다 —
  정작 그때가 점검을 돌려야 할 시점입니다. 첫 배포만 `create_stack`을 쓰므로(Start / Stop CDC는
  `update_stack` → `UPDATE_IN_PROGRESS`), 이제 `CREATE_IN_PROGRESS`는 "인프라 프로비저닝 중,
  아직 스트리밍 없음"으로 인식되어 점검을 그대로 쓸 수 있습니다. `UPDATE_IN_PROGRESS` /
  `DELETE_IN_PROGRESS`는 여전히 진행 중인 작업으로 셉니다.

이 두 수정으로 약 15~20분의 MSK 생성이 Full Load 뒤에 직렬화되지 않고 **겹쳐서** 진행됩니다 —
스냅샷을 적재하는 동안 배포가 백그라운드로 돌아갑니다.

## v0.1.143

### Fixed

- **적재 후 인덱스 생성 실패가 더는 완전히 적재된 테이블을 실패로 만들지 않습니다.** secondary
  index는 모든 행을 쓴 **뒤에** `CREATE INDEX ASYNC`로 생성되는데, 그 오류가 import 밖으로
  전파되어 **데이터가 완전히 적재된** 테이블이 `FAILED`로 표시됐습니다. 그 결과 빠진 것이 없는
  테이블 때문에 **Validation 게이트가 막혔습니다.** 재실행도 도움이 되지 않았습니다 — 주된
  원인(DSQL의 24개 인덱스 제한)이 일시적 오류가 아니어서 매번 같은 지점에서 실패했습니다.
  - 인덱스 실패를 **격리**합니다: 데이터 적재는 성공으로 보고되고(`failures=0`, 모든 행 존재),
    실패는 `BatchedImportResult.index_failures`로 따로 전달됩니다.
  - **하나가 실패해도 나머지는 계속 생성합니다.** 각 DDL을 독립적으로 시도하므로 남은 인덱스가
    만들어집니다(기존에는 첫 실패에서 루프가 중단됐습니다).
  - Full Load 결과에 **info** 톤 블록으로 표시합니다 — *"Indexes not created (N) — the data
    loaded completely"*. 데이터가 빠진 게 아니므로 실패 목록·격리 행과 분리했습니다. 에러 로그에
    어떤 인덱스가 왜 실패했는지, 그리고 재실행이 필요 없다는 점이 담깁니다.
  - 멀티프로세스 적재 경로에도 적용되어 워커 모드에 따라 동작이 달라지지 않습니다.

## v0.1.142

### Added

- **Evaluation이 Aurora DSQL의 테이블당 인덱스 제한을 검사합니다**(`TOO_MANY_INDEXES`).
  DSQL은 테이블당 **인덱스 24개**를 허용하며(MySQL은 64개), **PK 인덱스가 이 한도에
  포함됩니다** — 실제 클러스터로 검증했습니다(PK가 있는 테이블에서 24번째 `CREATE INDEX`가
  실패하고, 그때 `pg_indexes`가 PK를 포함해 24행을 보였습니다). 따라서 이관된 테이블은
  **secondary index를 최대 23개**까지 가질 수 있고, 소스에서 읽은 인덱스 목록을 이 값과
  비교합니다.
  - 계획 단계에서 잡는 이유: 그러지 않으면 최악의 시점에 드러납니다. secondary index는
    **적재 후** `CREATE INDEX ASYNC`로 생성되므로, 한도 초과는 **Full Load가 모든 행을 다 쓴
    뒤에야** 발생합니다 — 수 시간짜리 적재가 실패 테이블로 끝나고, 재실행으로도 해결되지
    않습니다(한도는 일시적 오류가 아님).
  - **MANUAL**로 분류: 사용되지 않거나 중복인 인덱스가 흔하므로 보통 몇 개를 삭제하면
    해결됩니다(표시 내용이 `sys.schema_unused_indexes`를 안내). 메시지에 양쪽 개수, 정확한
    오류 코드(`54000`), 그리고 언제 터질 문제인지가 담깁니다.

- **Evaluation이 CDC로 복제되지 않는 cascade 외래 키를 표시합니다**(`FK_CASCADE_CDC_GAP`).
  MySQL은 `ON DELETE/UPDATE CASCADE`(및 `SET NULL`/`SET DEFAULT`)를 **InnoDB 엔진 내부에서**
  수행하므로, 그 결과로 바뀐 자식 행 변경이 바이너리 로그에 기록되지 않습니다 — cascade 동작이
  트리거를 발동하지 않는 것과 같은 이유입니다. Debezium은 바이너리 로그를 읽으므로 이를 볼 수
  없고, Aurora DSQL은 외래 키가 없어 cascade를 대신 수행할 수도 없습니다. 결과적으로 자식 행이
  **오류도 경고도 없이** 대상에 남습니다. (MySQL 버그 #32506 — 코드 수정 없이 "문서화된 동작"으로
  종결. 이 도구만이 아니라 binlog 기반 CDC 도구 전체의 한계입니다.)
  - 참조 동작을 인트로스펙션 시 수집합니다(`ForeignKeyDef.on_delete`/`on_update`). 소스
    리플렉션이 이미 반환하는 정보를 읽으므로 추가 소스 쿼리는 없습니다.
  - **MANUAL**로 분류(UNSUPPORTED 아님): 테이블 자체는 문제없이 이관되지만 cascade를
    애플리케이션으로 옮겨야 합니다 — DSQL에는 외래 키가 없으므로 어차피 필요한 작업입니다.
    표시 내용에 구체적 동작, CDC가 놓치는 이유, 그리고 임시 안전망(Validation의 orphan 레코드
    검사 + 최종 비교 전 소스 쓰기 정지)이 함께 담깁니다.
  - `RESTRICT`/`NO ACTION`은 표시하지 **않습니다**: 부모 변경을 거부할 뿐이라 기록되지 않는
    자식 쓰기를 만들지 않습니다.

## v0.1.141

### Fixed

- **CDC 라이프사이클 액션이 활동 로그에 "started"만이 아니라 결과까지 기록합니다.** Deploy
  infrastructure / Start CDC / Stop CDC / Delete infrastructure는 각각 수 분~수십 분이
  걸리는데 제출 시점만 기록되어, 성공했는지·실패했는지·얼마나 걸렸는지가 남지 않았습니다.
  그래서 컷오버 시점에 가장 중요한 질문 — *Stop이 정말 성공했는가, 언제?* — 에 감사 로그로
  답할 수 없었습니다. 커넥터 상태 전이가 유일한 대리 지표였는데 그건 UI 폴러가 기록하므로,
  운영자가 다른 화면에 있는 동안 완료된 액션은 아예 기록되지 않았습니다(Start CDC 소요 시간을
  로그에서 복원하려면 이후 폴링 줄로 추측해야 했습니다).
  - 각 라이프사이클 잡 본문을 감싸서 **잡 스레드에서** 결과를 기록합니다(UI 표시 상태와
    무관): 경과시간과 함께 `success`, 경과시간+오류와 함께 `failure`, 협조적 취소는 `info`
    (`run_cdc_*`는 취소 시 정상 반환하므로 예외가 아니라 잡 핸들이 "중단"과 "완료"를 구분).
  - 실패는 계속 재던져지므로 JobManager가 잡을 `FAILED`로 표시하는 동작은 그대로입니다.
  - `core` 배포자는 건드리지 않았습니다 — `core`는 의도적으로 활동 로그에 의존하지 않으므로
    로깅은 UI 계층에 둡니다(Full Load와 동일한 패턴).
  - 알려진 한계: 액션 중간에 프로세스가 죽으면 그 잡은 `started` 줄만 남습니다(재시작 시
    JobManager가 `FAILED`로 정리하지만 활동 이벤트는 남기지 않음).

## v0.1.140

### Fixed

- **실패한 소스 읽기가 재시도 대기 중에 MySQL 연결을 계속 붙잡던 문제 수정.** 소스 행 스트림은
  제너레이터이고 자체 `finally`에서 엔진을 dispose하므로, 버려진 스트림은 close되거나 GC될
  때까지 연결을 유지합니다 — 그리고 예외를 던진 프레임이 그 참조를 잡고 있습니다. 그래서
  v0.1.139의 재시도는 죽은 연결을 열어둔 채 failover 백오프(최대 60초)를 전부 기다린 뒤 다시
  읽기 위해 또 하나를 열었습니다. 16 테이블 × 8 샤드에서는 **방금 승격된 Aurora writer가 가장
  취약한 순간에 소스 연결 수가 두 배**가 되어 `1040 Too many connections` 위험이 있었고, 그
  경우 테이블이 그대로 실패했습니다.
  - `migrate_table`이 로드 실패 시 자신이 만든 행 스트림을 닫으므로, 예외가 빠져나갈 때 연결이
    해제됩니다.
  - 재시도의 백오프 대기를 `except` 블록 **밖으로** 옮겨, 대기 전에 traceback(그리고 실패한
    시도의 프레임과 제너레이터)이 정리됩니다.
  - 실제 코드 경로에서 검증: 대기가 시작되기 **전에** 연결이 dispose됩니다.

### Added

- **소스의 `Too many connections`도 재시도하며, 전용 안내를 제공합니다.** MySQL 1040/1203은
  자기유발적이고 자연히 해소되므로(failover 시 모든 리더가 동시에 재연결하고, 다른 리더가
  끝나면 슬롯이 빕니다) transient로 분류합니다. 다만 안내는 failover와 다릅니다 — 기다리는 것이
  해결책이 아니므로 `FULL_LOAD_TABLE_PARALLELISM`/`FULL_LOAD_READER_SHARDS`와 소스의
  `max_connections`를 짚어줍니다.
- **reader shard 수가 clamp되면 이를 알립니다.** 동시 소스 리더는 32개로 제한되는데
  (`table_parallelism × reader_shards`), 이 상한이 설정값을 줄일 때 이전/이후 값과 이유를
  로그에 남깁니다. 기존에는 조용히 더 적은 리더로 로드해서 설정이 무효인 것처럼 보였습니다.

## v0.1.139

### Added

- **Full Load이 소스 Aurora failover를 견딥니다.** writer 승격(패치, 인스턴스 교체, AZ
  이벤트)은 열린 MySQL 연결을 모두 끊으므로 수 시간짜리 로드는 이를 만나게 되는데, 기존에는
  진행 중인 테이블이 그냥 실패하고 사람이 Re-run을 누를 때까지 기다렸습니다. 이제 해당
  테이블을 **자동으로 다시 읽습니다**(기본 3회, 15초 → 30초 → 60초 백오프 — DNS가 승격된
  writer로 재지정될 시간을 줍니다).
  - 재시도는 죽은 읽기를 마지막 PK에서 **이어받지 않고, 새 consistent snapshot으로 테이블을
    처음부터 다시 읽습니다.** 이어받으면 서로 다른 두 MySQL 스냅샷이 한 테이블에 섞여 어떤
    단일 시점에도 대응하지 않게 되고, gapless Full Load → CDC 핸드오프는 각 테이블이 런의
    워터마크 시점으로 일관되다는 전제에 의존합니다. 이미 쓰인 행은 idempotent 로드가
    건너뛰므로 재시도 비용은 재읽기 I/O뿐이고 중복은 생기지 않습니다. (reader 샤딩을 쓰면
    각 샤드가 이미 자체 스냅샷을 가지므로 해당 샤드만 다시 읽어 비용이 더 줄어듭니다.)
  - **연결 수준** 실패만 재시도합니다(MySQL 2013/2006/2003/2002/2055/1053/1077/1079/1927 및
    소켓 타임아웃). 데이터/스키마 오류는 이전처럼 즉시 실패합니다 — 재시도해도 같은 실패까지
    지연만 늘 뿐입니다.
  - 단일 프로세스 경로뿐 아니라 **멀티프로세스 로드 경로(대규모 기본값)에도 적용**되어 워커
    모드에 따라 런이 다르게 동작하지 않습니다. 재시도 시 대상이 방금 비워졌다는 가정을
    올바르게 해제하므로, 실패한 시도가 이미 쓴 행과 재읽기가 충돌하지 않습니다.
  - 백오프 대기 중에도 사용자 **Stop**이 즉시 반영됩니다(대기가 끝난 뒤가 아니라).
  - 조정 가능: `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS`(1 = 끄기, 기존 동작),
    `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS`.

### Changed

- **소스 연결 끊김이 스스로를 설명합니다.** 재시도가 모두 소진되면, 테이블 오류가 더는
  `OperationalError: (2013, 'Lost connection to MySQL server during query')` 같은 원시
  텍스트로만 보이지 않습니다. 이것이 보통 Aurora failover이며, 소스는 변경되지 않았고(로드는
  읽기만 함), 로드가 idempotent하고 PK 기준으로 재개되므로 재실행이 안전하다(빠진 부분만
  채움)는 설명이 함께 표시됩니다.

## v0.1.138

### Fixed

- **완전히 로드된 Full Load 테이블이 소스 추정치의 과대추정 때문에 미완료로 보고되던 문제 수정.**
  테이블별 `Progress`와 완료 판정이 워터마크의 scan-free `information_schema` 카운트를 분모/
  비교 대상으로 썼습니다. 이 추정치는 InnoDB 인덱스 샘플링에서 나오며 **양방향으로** 틀리므로,
  과대추정이 발생하면 로더가 끝까지 스트리밍한 테이블이 **"91%"로 표시되고 mismatched로
  집계**되어 유실이 없는데도 유실을 암시했습니다.
  - `DONE` 테이블은 이제 정의상 **100%**입니다: export는 PK keyset으로 테이블을 소진할 때까지
    스트리밍하므로 완료 자체가 완전성의 증거이며, 추정치와의 일치에 의존하지 않습니다.
  - `complete`는 부족분이 추정치의 샘플링 허용범위를 넘지 않는 한 완료된 테이블에 `True`를
    반환합니다. 따라서 실제로 잘린 로드는 계속 표시되고, 수 % 차이는 더는 표시되지 않습니다.
  - 추정치보다 **많이** 로드된 경우(흔한 과소추정)는 100% 캡에 가려지지 않고 Rows 툴팁에
    퍼센트와 함께 정상임이 명시됩니다.

### Changed

- **Full Load 테이블이 소스 수치가 근사치임을 명시합니다.** 열 헤더가 **Rows (target / source
  est.)**로 바뀌고, 샘플링 오차·타깃이 소스를 초과하는 것이 정상인 이유·완료 테이블이 100%인
  것은 두 숫자가 일치해서가 아니라 로더가 소진했기 때문임을 설명하는 ⓘ 툴팁이 추가됐습니다.
  정확한 비교는 여전히 Validation(4단계)입니다.

- **CDC 상태 테이블이 정상 테이블을 "target ahead"로 오탐하던 문제 수정.** 이 화면의 Source
  rows는 scan-free `information_schema` **추정치**인데(대규모 소스에 `COUNT(*)` 풀스캔을 하지
  않기 위함), consistency 판정이 여기서 정확한 타깃 `COUNT(*)`를 빼고 그 차이를 이상으로
  취급했습니다. InnoDB는 이 추정치를 인덱스 샘플링으로 구하므로 흔히 수 %를 *적게* 세고,
  따라서 완전히 정상인 타깃이 추정치를 초과합니다 — 실제 11개 테이블 스키마에서 **8개가
  amber "target ahead" 배지**를 달았고, quarantined는 0이고 스트림도 전부 caught up이었습니다.
  - `"target ahead"` 판정을 **제거**했습니다. 타깃이 추정 소스 카운트를 넘는 것은 정상입니다.
  - 판정은 실제로 정확하고 저비용인 신호에 의존합니다: DLQ, 시간 기반 `ReplicationLagMs`,
    `MAX(pk)` 선두 위치. *추정치* 대비 부족분은 샘플링 허용범위를 넘을 때만 "rows missing"으로
    올라가므로, 실제 데이터 유실은 계속 보고되고 통계 노이즈는 보고되지 않습니다.
  - 등호 주장은 **정확한** 소스 카운트일 때만 허용됩니다(`counts_comparable`). `in_sync`는
    소스가 추정치면 거짓 음성 대신 "판정 불가"를 반환합니다.

### Changed

- **CDC 테이블이 Source rows가 근사치임을 명시합니다.** 열 헤더가 **Source rows (est.)**로
  바뀌고, 샘플링 오차를 설명하며 정확한 비교는 Validation(4단계)로 안내하는 ⓘ 툴팁이
  추가됐습니다. 셀마다 반복되던 `(est.)` 접미사는 제거했습니다(이제 드문 *exact* 경우만 표시).
  Consistency 툴팁과 "How to read this table" 범례도 초록색이 "이상 없음"이지 정확한 일치의
  증명은 아니라고 밝힙니다.

## v0.1.137

### Added

- **Fast sweep로 "row count만 검증된" 테이블도 그 자리에서 깊게 재검증 가능.** 기존에는 해당
  각주가 *"Fast sweep을 끄고 전체를 re-run하라"*고만 안내했습니다. 이제 **Deep-check N
  count-only table(s)** 버튼이 생겨, 그 테이블들만 이번 런이 건너뛴 checksum / record
  reconciliation으로 다시 비교하고 결과를 기존 리포트에 머지합니다 — v0.1.136이 실패
  테이블에 도입한 것과 같은 개별 테이블 메커니즘입니다. 통과한 테이블 중에서 재검증이
  실제로 의미 있는 유일한 경우입니다(행 단위 동일성이 증명된 적 없으므로).
  - 무의미한 경우에는 버튼을 숨깁니다: `ROW_COUNT` 모드 + reconciliation 없음이면 더 깊게
    돌릴 검사가 없어 같은 count 비교를 반복할 뿐이므로, 버튼 대신 기존의 정직한 "Fast
    sweep 끄고 re-run" 안내를 그대로 둡니다.
  - 그 외 통과한 테이블에는 여전히 재검증 버튼이 없습니다 — 검사가 추가되는 곳(실패 테이블,
    또는 count-only fast-sweep 테이블)에만 노출됩니다.

## v0.1.136

### Added

- **Validation에서 전체 재실행 없이 개별 테이블만 다시 검증.** row count나 checksum이
  mismatch된 테이블은 "Tables needing attention"의 각 항목에 **Re-check** 버튼이 생깁니다
  (실패 테이블이 여러 개면 **Re-check all N tables**도 함께). 해당 테이블만 다시 비교한 뒤
  결과를 기존 리포트에 **머지**하므로 나머지 테이블의 판정과 전체 cut-over go/no-go가 그대로
  유지되면서 갱신됩니다 — 마지막 실패 테이블을 고치면 몇 시간짜리 전체 재실행 없이 판정이
  "Ready for cut-over"로 바뀝니다.
  - 재검증은 **원래 런의 옵션**(비교 모드, reconciliation, orphan check)을 리포트 자체에서
    복원해 사용하므로 머지된 리포트가 자기모순에 빠지지 않고, 재접속으로 복원된 리포트도
    재검증할 수 있습니다. Fast sweep은 재검증에서 **강제 off** — 이미 다르다고 아는 테이블이라
    checksum/reconciliation이야말로 돌려야 하는 검사입니다.
  - 시점이 섞인 사실을 명시합니다: **"N table(s) re-checked at &lt;시각&gt; — newer than the rest
    of this run"**(대상 테이블 나열). 판정이 두 시점을 함께 반영하기 때문이며, 이 표시는
    재접속 후에도 유지됩니다.
  - 재검증은 완료된 스텝 위에서 돌아갑니다(스텝은 **Done** 유지, 리포트도 화면에 남고) 해당
    행에만 "Re-checking…" 인라인 표시가 붙습니다. validation job 슬롯을 하나만 공유하므로
    재검증 중에는 "Re-run validation"이 비활성화되고 그 역도 같습니다 — 전체 재실행이 재검증
    작업을 고아로 만들거나 머지 대상 리포트를 지우는 일이 없습니다.
  - 재검증을 시작할 수 없는 경우(예: 리포트 생성 이후 단수명 DSQL 타깃 토큰 만료)는 **"Could
    not re-check those tables"** 별도 notice로 알리고 기존 리포트를 건드리지 않습니다 —
    "Validation failed"로 표시되지 않습니다.

## v0.1.135

### Fixed

- **CDC 이후 JSON 컬럼 때문에 Validation이 "data differs"로 오탐하던 문제 수정.** MySQL `JSON`은
  Postgres `json` 컬럼으로 매핑되고 checksum이 원문 텍스트를 비교하는데, MySQL은 공백 있는
  정규형(`{"k": "v"}`), CDC로 쓰인 행은 Debezium compact(`{"k":"v"}`) — 논리적으로 같지만 텍스트가
  달라 JSON 있는 CDC-touched 행이 실패했습니다(Full Load 행은 일치). 이제 JSON을 checksum에서
  제외(FLOAT/DOUBLE처럼)하며 row count·그 외 컬럼은 계속 검증합니다. `customers`/`products`/
  `suppliers` 오탐의 원인이었습니다.

### Changed (checksum 크로스엔진 하드닝)

- **소스 MySQL 세션을 UTC로 고정**(`SET time_zone='+00:00'` — 연결 테스트/인트로스펙션/validation/
  Full Load 스트림의 모든 소스 엔진). MySQL `TIMESTAMP`는 UTC로 저장되지만 세션 타임존으로 읽혀,
  UTC가 아니면 대상의 UTC 렌더링과 checksum에서 어긋날 수 있었습니다. (`DATETIME`은 wall-clock이라
  영향 없음.)
- **Validation이 마이그레이션 제외 컬럼을 스킵**(예: CDC oversized-LOB 제외): 대상에 쓰이지 않은
  컬럼은 checksum에서 빼서 항상 "다름"으로 뜨지 않게 합니다(PK는 절대 제외 안 함).

## v0.1.134

### Changed

- **CDC 스트리밍 중 적용 불가한 액션을 툴팁 경고만이 아니라 시각적으로 비활성(grey out) 처리:**
  - **Start / Re-run Full Load**를 CDC 실행 중엔 **비활성화**(기존엔 경고만 뜨고 클릭 가능했음)
    — 실행 시 스트림과 충돌. 툴팁/힌트로 "Stop CDC 먼저" 안내.
  - **CDC start point**는 잠금 상태였지만 잠긴 것처럼 안 보였는데, 이제 라디오 선택과 수동
    GTID/binlog 입력을 **명확히 흐리게(muted + not-allowed 커서)** 처리해 "Locked" 배지와
    일치시킴.

## v0.1.133

### Changed

- **Inserts / Updates / Deletes 셀을 색상 있는 숫자만 표시** — 앞의 아이콘(＋ / 연필 / −)을
  제거했습니다(컬럼 헤더 + 초록/파랑/빨강 색상으로 이미 구분됨). 헤더 ⓘ 툴팁도 한 문장으로
  간결화했습니다.

## v0.1.132

### Changed

- **테이블별 CDC 모니터에 Inserts / Updates / Deletes 컬럼을 각각 분리**(DMS 스타일)해,
  기존의 단일 "Changes since Full Load" 셀을 대체했습니다. 각 컬럼은 CDC가 스트리밍을 시작한
  이후의 **누적 카운터**이며 색상 코딩(초록 insert / 파랑 update / 빨강 delete)됩니다.

### Fixed

- **I/U/D 카운트가 떴다 사라지는 깜빡임 수정.** applied-ops 읽기는 best-effort라, 폴 한 번이
  비거나 실패하면(CloudWatch throttle/타임아웃, 테이블 일시적 빈 값) 저장값을 빈 맵으로
  덮어써 컬럼이 blank됐습니다. 카운트는 누적(단조 증가)이므로, 이제 비어있지 않은 읽기는
  마지막 값에 **병합**하고 빈 읽기에는 **덮어쓰지 않아** 카운터가 유지되며 증가만 합니다.
- **테이블 헤더 ⓘ 툴팁(Stream lag, Consistency 등)이 hover 중 닫히던 문제 수정.** 기존엔 ~5초
  폴마다 테이블 전체가 다시 그려져 툴팁이 사라졌습니다. 이제 테이블 요소 + 헤더 툴팁은 **한 번만**
  생성하고 행 데이터만 **제자리에서 교체**하므로, 툴팁을 읽는 동안 닫히지 않습니다.
- **Stream lag / Consistency 설명을 더 이해하기 쉽게** 개선(헤더 툴팁과 범례 모두, 딱딱한
  메트릭 정의 대신 평이한 표현으로).

## v0.1.131

### Fixed

- **drain된 파이프라인을 세션 복원했을 때 Stream lag 패널이 사라지던 문제 수정.** 라이브 lag
  추세는 메모리상 롤링 버퍼라 영속화되지 않아, 재연결 시 CloudWatch `ReplicationLagMs`에서
  다시 시드합니다 — 그런데 이 지표는 이벤트 기반이라 소스를 멈춰(caught up) 최근 datapoint가
  없으면 시드할 게 없고, 차트는 ≥2 포인트가 필요하므로 **패널 전체가 숨겨져** 재연결 후
  stream-lag 신호가 아예 안 보였습니다. 이제 CDC가 살아있지만 그릴 추세가 없을 때 **"Caught up
  — no replication lag in the recent window"** 줄을 표시해 지표가 항상 존재하도록 했고,
  스트리밍 시작 전에만 완전히 숨깁니다.

## v0.1.130

### Changed

- **Validation 화면 텍스트 정리.** 5줄짜리 인트로를 한 문장으로 축약하고, 세 개의 상태 알림
  (No export watermark / CDC still streaming / Comparison in progress)은 헤더는 유지하되
  본문을 각각 핵심 한 줄로 줄였습니다. 그래서 세 상태가 동시에 뜨는 경우(watermark 없음 +
  CDC 활성 + 실행 중)에도 더 이상 텍스트 벽이 되지 않습니다. 알림 박스 자체는 실제 조건부
  상태를 전달하므로 유지하되 간결하게만 바꿨습니다.

## v0.1.129

### Changed

- **파이프라인이 drain되면 Change flow가 "idle"로 표시되도록 — 소스 커넥터의 heartbeat
  바닥을 흡수.** 소스(Debezium) 커넥터는 완전히 조용해지지 않습니다 —
  `heartbeat.interval.ms=300000`(5분)이 주기적으로 heartbeat을 방출해
  `SourceRecordPollRate`가 0이 아니라 작은 바닥(CloudWatch 평균 ~0.03/s)에 머뭅니다. 기존
  idle 임계값이 `0.01/s`라 이 heartbeat 잔량 때문에 소스를 멈춘 뒤에도 change-flow가
  "streaming"으로 보였습니다. 임계값을 `0.1/s`로 상향 — heartbeat 바닥 위, 실제 변경
  트래픽(보통 ≥1/s)보다는 훨씬 아래. 판정은 여전히 **source-poll·sink-send 둘 다** 임계값
  미만이어야 하므로, stall(소스는 생산 중인데 sink가 못 보냄)은 idle로 오분류되지 않고
  정확히 "streaming"으로 유지됩니다.

## v0.1.128

### Fixed

- **파이프라인이 drain된 뒤 Stream lag이 마지막 값에 고정되던 문제 수정.** `ReplicationLagMs`
  는 이벤트 기반 지표(싱크가 변경을 적용할 때만 datapoint 방출)라, cut-over를 위해 소스를
  quiesce하면 파이프라인이 방출을 멈춥니다. 그런데 reader가 15분 window 안에 남아있는 마지막
  datapoint를 계속 "현재" lag으로 반환해서, source-poll / sink-send rate는 이미 idle(0)로
  떨어졌는데도 Stream lag 차트/열이 최대 ~15분간 예: 1068 ms 에 flat하게 머물렀습니다. 이제
  최신 datapoint가 freshness 컷오프(~3분)보다 오래됐으면 없는 것으로 처리해, drain된
  파이프라인은 **caught up**으로 읽히고 소스가 조용해진 직후 차트가 0으로 떨어집니다.
  reader 측 수정(싱크 재배포 불필요).

### Changed

- **Data Migration / CDC 화면 정리: 상시 노출되던 장문 설명을 hover ⓘ 툴팁으로 이동(중복은
  삭제).** 상시 안내문은 화면에 익숙해지면 노이즈가 되므로, 설명은 hover로 옮기고 화면을 더
  깔끔하게 했습니다:
  - **Stream lag** 차트 캡션 → 제목 옆 ⓘ (제목 + `lag (ms)` 축이 기본 정보 전달).
  - **Tables to migrate** — "왜 테이블만(뷰/트리거/루틴 제외)" 문단 → 제목 ⓘ; "Locked —
    prerequisite 체크 재실행…" 줄 → 잠금 아이콘 툴팁으로 통합; 사전 선택 안내는
    `Pre-selected: N table(s) already on the target — untick any to skip.` 로 축약.
  - **CDC start point** — "스트리밍 시작 지점 / Automatic은 gapless" 문단 → 제목 ⓘ;
    "CDC has started — locked…" 줄 → **Locked** 배지 툴팁으로 통합.
  - **Stop CDC** — "커넥터가 스트리밍 중… Stop은 커넥터만 제거…" 상시 문단 삭제(스트리밍
    여부는 live status로, 영향은 Stop 확인 대화상자에 이미 명시), 버튼에는 짧은 안심 툴팁만.
  - **Change flow** — "변경이 계속 흐르는지 / cutover 시 idle로 떨어지는지 지켜보라" 문단과
    "CloudWatch, ~last few min" 출처 표기 → "Change flow" 헤더의 ⓘ 하나로 통합, 상태 줄 +
    source/sink rate 게이지만 남김.

## v0.1.127

### Changed

- **테이블별 CDC 모니터가 DMS 스타일 변경 분해(I/U/D)를 표시합니다.** "Net rows since
  Full Load" 열을 **"Changes since Full Load"** 로 교체 — 테이블마다 라이브 카운터 3개:
  **insert**(초록 `add`), **update**(파랑 `edit`), **delete**(빨강 `remove`). 이제 UPDATE
  트래픽이 처음으로 보입니다: 기존 net-rows 값은 insert − delete 만 합산하고 update 는
  건너뛰어서 update 위주 테이블이 유휴처럼 보였습니다. 여전히 스캔 프리(`COUNT(*)` 없음):
  DSQL 싱크가 단일 `NetRowsApplied` 대신 CloudWatch 메트릭 3개 — `InsertsApplied` /
  `UpdatesApplied` / `DeletesApplied`(네임스페이스 `MysqlDsqlMigrator/CDC`, 디멘션
  `Stack` + `Table`) — 를 방출하고, 컨트롤 플레인이 창(window) 단위로 각각 합산합니다.
  net rows 는 필요 시 여전히 (insert − delete)로 파생됩니다. 재빌드된 싱크 플러그인이
  필요하므로(`PLUGIN_VERSION` v21 → v22), 반영하려면 CDC 인프라를 **Delete + Deploy**
  해야 합니다.

## v0.1.126

### Changed

- **CDC Live status 폴리시(가독성↑·노이즈↓):**
  - **Change flow** rate 게이지가 Pipeline health 카드를 벗어나던 문제 수정(고정폭 막대 +
    안쪽 패딩), rate 단위를 `/s` 대신 **`rec/s`**(초당 변경 이벤트 레코드 —
    `SourceRecordPollRate` / `SinkRecordSendRate`)로 명시.
  - **Connectors**에 상태 **색상 배지**(초록 "Running" 등)를 되살려 한눈에 보이게(콤팩트
    한 줄 레이아웃 유지).
  - **"CDC behavior & limits"** 참고 섹션을 **기본 접힘**으로 — 정보성·장문이라 매 방문마다
    노이즈가 되지 않도록.
  - **"Runs on … cdc-stack"** 오리엔테이션 배너는 **cdc-stack 배포 전에만** 표시(배포됨/미확정
    시 숨김) — 매 방문 반복·재연결 시 깜빡임 방지.

## v0.1.125

### Fixed

- **이미 실행 중인 CDC 파이프라인에 재연결했을 때 CDC per-table 상태 뷰(및 라이브 지표)가
  비어 보이던 문제 수정.** per-table 테이블 집합(net rows·stream lag·라이브 lag 차트의 지표
  읽기 범위도 됨)이 **Full Load 잡의 chunk에서만** 유도돼서, Full Load 잡이 없는 세션(실행 중
  파이프라인에 재연결, 또는 CDC-only)은 파이프라인이 실제로 스트리밍 중인데도 빈 테이블 +
  lag/차트 없음으로 보였습니다. 이제 잡이 없으면 **라이브 스택 구성에서 reconcile된 테이블**로
  폴백합니다.

## v0.1.124

### Changed

- **Stream lag 차트가 라이브 in-place 시계열**이 되었습니다(기존엔 5초 폴링마다 통째로 다시
  그려져 깜빡였음). 차트 요소를 유지하고 제자리에서 갱신하므로 CloudWatch 그래프처럼 라인이
  끊김 없이 연장됩니다. **x=시간축, y=lag(ms)**. 데이터는 하이브리드 롤링 시계열 — CloudWatch
  1분 history로 시드(리로드에도 유지) 후 매 ~5초 폴링의 현재 최대 lag으로 연장(따라잡으면 0),
  ~15분으로 bounded. Live status 상단의 전용 "Stream lag" 패널로 이동.
- **Change flow(source poll / sink send)를 비주얼화** — 평문 대신 **동일 스케일의 막대 게이지
  2개**로 표시해, sink가 source를 따라잡는지(막대 일치)/뒤처지는지 한눈에 보입니다.
- **Full Load·CDC 통계 테이블의 상태 배지 통일** — 둘 다 동일한 **아웃라인 칩 + title-case**
  라벨로. (Full Load "Status" 배지는 솔리드·대문자였는데, CDC 테이블의 아웃라인 스타일 및
  디자인 시스템 규칙에 맞춤.)
- **Live status의 "Connectors" 목록을 미니멀하게** — 커넥터당 한 줄(상태 아이콘 + 역할 이름
  + 흐린 detail; raw 커넥터 id는 hover 툴팁으로 이동)로, 기존의 2줄(id + 아웃라인 배지)
  표기를 대체.

## v0.1.123

### Added

- **Stream lag 추세 라인 차트 — CDC "Pipeline health" 카드에 추가.** per-table "Stream lag"
  컬럼은 *현재* lag만 보여줘서, 스트림이 따라잡는 중인지 벌어지는 중인지(정확히 cutover
  판단 질문)를 알 수 없었습니다. 이 차트는 sink의 `ReplicationLagMs` 지표에서 **최근 ~15분
  동안 1분 버킷별 테이블 전체 최대 lag**(초 단위)를 그립니다 — 0 근처에서 평평하면 따라잡음
  (cutover 안전), 우상향이면 파이프라인이 뒤처지는 중. per-table read가 이미 가져오던
  CloudWatch 데이터포인트를 재사용(추가 상태 없음, 리로드에도 유지)하고 앱 내장 ECharts를
  씁니다(신규 의존성 0). 해상도는 ~1분(CloudWatch Period)이라 실시간이 아니라 추세용입니다.

## v0.1.122

### Fixed

- **CloudFormation 삭제 실패가 더 이상 CDC 인프라를 조용히 방치하지 않습니다.** in-VPC
  seeder Lambda의 CloudFormation 응답 PUT이 이제 한 번 실패하면 포기하지 않고 **재시도**
  합니다(바운드, ~4회). 기존에는 teardown 중 PUT이 한 번 실패하면 CloudFormation이 응답을
  받지 못해 자체 ~1시간 커스텀 리소스 타임아웃을 기다렸고, 결국 cdc-stack 전체가
  `DELETE_FAILED`로 빠져 MSK/NAT 과금이 남았습니다. 재시도로 ENI/라우트가 정리되는 동안의
  일시적 S3 게이트웨이 egress 순단을 넘깁니다. (새로 배포되는 CDC 인프라에 적용;
  `PLUGIN_VERSION` v21로 상향.)

### Added

- **teardown 배너가 `DELETE_FAILED`에서 복구합니다.** CDC teardown이 CloudFormation
  `DELETE_FAILED`로 끝나면, 지속 배너가 "진행 중"에서 **"CDC teardown 실패 — 조치 필요"**
  (에러 스타일) 상태로 전환되고, 원클릭 **Retry cleanup**(막힌 리소스를 retain하고 삭제를
  재실행해 MSK/NAT까지 정리)과 **Dismiss**를 제공합니다. Start over로 세션이 초기화된
  뒤에도 재시도가 동작합니다 — 필요한 region/배포 역할/프로필을 durable teardown 마커에
  함께 저장합니다.

## v0.1.121

### Fixed

- **CDC 인프라 삭제(teardown)가 완료될 때까지 화면에 계속 표시됩니다.** Start over에서
  "Delete all CDC infrastructure"(또는 "Remove connectors, keep infrastructure")를
  선택하면 세션은 곧바로 초기화되어 Connect 화면으로 돌아가고 teardown은 백그라운드에서
  진행됩니다. 기존에는 진행 중이라는 표시가 전혀 없어 MSK/NAT가 아직 과금 중인지, 인프라가
  이미 삭제됐는지 알 수 없었습니다. 이제 **모든 화면(Connect 포함)** 상단에 지속 배너
  ("CDC infrastructure teardown in progress…")가 뜨고, teardown이 끝나면 자동으로
  사라집니다. CDC 단계의 Delete/Stop 버튼으로 시작한 teardown도 포함되므로, 다른 단계로
  이동해도 표시가 사라지지 않습니다.
- **Start over가 진행 중인 CDC teardown과 경쟁하지 않습니다.** stop/delete가 도는 중
  reset은 이미 막혀 있었지만, Start over → delete 직후 CloudFormation 스택이 아직
  `DELETE_IN_PROGRESS`로 바뀌기 전의 짧은 창에서 두 번째 reset이 빠져나가 중복 teardown을
  일으킬 수 있었습니다. 세션 초기화에도 살아남는 durable teardown 마커가 이 창을 닫습니다.

### Changed

- CDC 인프라 배포 또는 Start CDC 작업이 진행 중일 때 Start over가 (조용히 진행하는 대신)
  **경고**합니다. 해당 작업은 재검색(re-discover) 가능하고 막으면 stuck 상태 탈출을
  가로막게 되므로 reset은 여전히 허용하되, 백그라운드에서 계속 실행된다는 점을 알려줍니다.

## v0.1.120

### Changed

- **CDC Start가 이제 소스·싱크 커넥터를 한 번의 병렬 pass로 생성**해 커넥터 생성 시간을 대략
  절반으로 줄입니다. 기존에는 순차 2-pass였습니다 — 소스 커넥터 생성 → RUNNING 대기(그래야
  Debezium이 per-table 토픽 자동 생성) → 싱크 생성 — 토픽이 없을 때 싱크가 시작하면
  empty-partition-assignment race에 걸리기 때문이었습니다. 이제 cdc-stack의 start-prep 커스텀
  리소스(일반화된 seeder Lambda)가 매 start마다 **per-table 싱크 토픽을 미리 생성**합니다 —
  도구가 이미 계산하는 결정적 이름 `<prefix>.<db>.<table>` + 파티션 수로 — 그래서 두 커넥터가
  서로가 아니라 미리 생성된 토픽에만 의존해 동시에 배포됩니다. seeder는 여전히 gapless
  Full-Load handoff(watermark 있음)에서만 connect-offsets 레코드를 seed하고, 토픽 선생성은
  무조건이라 CDC-only start도 혜택을 봅니다. Start 진행이 source-then-sink 6스텝에서 단일
  "Waiting for connectors (source + sink)" 스텝으로 축약되며, 커넥터별 상태는 라이브 칩에 유지됩니다.

## v0.1.119

### Fixed

- **샤드 분할된 단일 대형 테이블이 이제 FAILED로 표시되지 않고 정상 적재됩니다.** PK-range 샤드
  워커가 결과를 `rows_skipped=result.rows_skipped`로 만들었는데, `BatchedImportResult`에는 그
  속성이 없습니다(`conflicts`가 있음). 모든 샤드가 반환 지점에서 `AttributeError`를 내고, 잡혀서
  `rows_loaded=0`인 `FAILED`로 보고 → 큰 단일 테이블(엔진이 코어당 1샤드로 분할)이 **모든 행이
  적재됐는데도 FAILED**로 표시됐습니다. 샤드 경로만 영향받았고, 비-샤드 테이블은
  `rows_skipped = result.conflicts`로 올바르게 매핑하므로 멀티테이블 로드(테이블당 1워커, 비샤드)는
  무영향이었습니다. 이제 샤드 워커도 `rows_skipped`를 `conflicts`에서 매핑합니다.
- **샤드 테이블 실패 시 모든 실패 샤드의 status/rows/message를 error log에 기록**합니다(메시지가 있는
  샤드만이 아니라) — "one or more shards failed"가 항상 진단 가능해집니다(이전엔 메시지 없이 실패한
  샤드는 원인이 남지 않음).

## v0.1.118

### Fixed

- **`measure_performance` 하니스가 실패한 런에서 테이블/샤드/배치별 에러 레코드를 덤프합니다.**
  샤드 분할된 테이블은 샤드 중 하나라도 실패하면 `FAILED`로 표시되는데, 샤드의 실제 사유는
  error log에만 기록돼 perf 런은 "one or more shards failed"만 찍고 원인이 없었습니다. 이제
  `FAILURE REASON`과 함께 각 `DATA ERRORS` 항목(테이블/청크·코드·메시지)을 출력해, 대형
  단일 테이블 로드의 막판 단일-샤드 실패까지 로그만으로 진단할 수 있습니다.

## v0.1.117

### Fixed

- **DSQL의 "클러스터당 스키마 10개" 한도가 이제 actionable 에러로 표시됩니다.** 타깃
  클러스터가 이미 10개 한도에 도달하면 마이그레이션 스키마용 `CREATE SCHEMA`가
  `program_limit_exceeded`(SQLSTATE 54000, "more than 10 schemas not allowed")로 실패합니다 —
  `IF NOT EXISTS`가 있어도 DSQL이 존재 확인보다 한도를 먼저 검사하기 때문입니다. 이는 하드
  리밋(재시도해도 안 풀림)이라 즉시 명확한 메시지로 번역해, 미사용 스키마 제거
  (`DROP SCHEMA ... CASCADE`) 또는 다른 클러스터 사용을 안내합니다(불투명한 드라이버 에러 대신).
  OCC/transient 재시도 경로에는 일부러 넣지 않았습니다.
- **`measure_performance` 하니스가 job 실패 사유를 출력합니다.** `run_full_load` 밖으로
  전파된 실패(예: 위의 pre-pass 스키마/DDL 에러 — 테이블 워커가 돌기 전)는 JobManager가
  잡은 예외로만 보관돼, 런이 `status=FAILED`에 모든 테이블 `PENDING`·사유 없음으로 찍혔습니다.
  이제 `FAILURE REASON: <예외>`를 남겨 실패한 perf 런을 로그만으로 진단할 수 있습니다.

## v0.1.116

### Fixed

- **모든 replace 테이블을 병렬 데이터 로드 시작 전에 한 번, 순차적으로 DROP+recreate** 하도록
  바꿔 최대 병렬에서의 startup DDL storm을 막았습니다. 기존에는 각 테이블 워커가 자기 프로세스
  안에서 타깃을 recreate해서, 높은 table-parallelism에선 모든 워커가 동시에 공유 스키마 카탈로그에
  `CREATE SCHEMA`/`DROP`/`CREATE`를 실행했습니다. DSQL은 트랜잭션당 DDL 하나를 낙관적 동시성으로
  처리하므로 이 동시 카탈로그 쓰기가 OC001(`SQLSTATE 40001`, "schema has been updated by another
  transaction")로 충돌하고, DDL 재시도 예산을 소진해 한 행도 적재하기 전에 테이블을 실패시킬 수
  있었습니다. 이제 메타데이터 전용 DROP+recreate를 기존 pre-pass에서 (sharded뿐 아니라) **모든**
  replace 테이블에 대해 수행하고, 워커는 이미 비워진 타깃에 DDL을 다시 실행하지 않고 로드합니다
  (post-load `CREATE INDEX ASYNC` DDL은 적용된 conversion에서 그대로 유도). 최대 병렬 Full Load가
  카탈로그를 경합하지 않고 결정적으로 시작됩니다.

## v0.1.115

### Fixed

- **테이블별 DROP+recreate 연결이 이제 일시적 연결 실패 시 재시도됩니다.** 연결 storm에서
  테이블이 실패하던 마지막 갭을 막습니다. 최대 병렬 Full Load(table-parallelism 16, 테이블
  20개)에서 큐에 대기하던 4개 테이블은 앞선 16개가 완료될 때 비로소 시작되는데, 이 16개가
  거의 동시에 끝나므로 4개가 한꺼번에 새 DSQL 연결을 열어 DSQL의 초당 신규연결 ~100개
  한도를 초과합니다. `recreate_table`(및 `schema_applier`의 다른 DDL 연결 경로)은 이 연결을
  **어떤 재시도로도 감싸지 않고** 열었기 때문에, 발생한 `ConnectionTimeout: connection
  timeout expired`가 **배치 하나 실행되기도 전에 0행으로 테이블 전체를 실패**시켰습니다(OCC
  재시도도, give-up 로그도 없음 — 이전 수정이 강화한 배치 루프 바깥에서 실패). 이제 연결
  open을 배치 로더 풀이 이미 쓰는 것과 동일한 일시적-연결 재시도로 감싸 storm을 견딥니다.
- 일시적-연결 분류기를 `core/target_connection.py`(`is_transient_connection_error`)로 옮겨
  **모든** DSQL 연결/실행 경로가 하나의 정의를 공유합니다(배치 로더 풀 lease와 DDL 연결 모두).
  `batched_import`에는 하위호환 alias를 남겼습니다.

## v0.1.114

### Changed

- **OCC/연결 재시도 루프가 재시도와 give-up을 로깅**하도록 해, 배치 실패를 타이밍
  추론이 아니라 로그로 직접 진단할 수 있습니다. `with_occ_retry`는 조용했지만, 이제
  각 재시도를 DEBUG(attempt N/max, 에러 타입 + SQLSTATE, 백오프 지연)로, budget
  소진 시 WARNING(**재시도 횟수, 총 경과 시간, 마지막 에러 + SQLSTATE**)으로
  남깁니다. 이 WARNING이 *"재시도 budget이 부족했다"* vs *"일시적 스톰이 budget보다
  길었다"* vs *"재시도 불가 에러였다"* 를 구분하는 직접 증거입니다 — 예:
  `occ-retry gave up after 30 attempts over 131.4s; last=ConnectionTimeout
  sqlstate=None`. 순수 추가 로깅이며 재시도 동작 자체는 변경 없음.

## v0.1.113

### Changed

- **Full Load의 배치당 재시도 budget을 더 인내심 있게(10 → 20) 하고 운영자 조정
  가능하게 했습니다** — 배치가 더 긴 일시적 DSQL 연결 스톰을 견디고 테이블 실패로
  가지 않도록. 이 budget(`occ_max_attempts`)은 OCC(`40001`) 충돌과 v0.1.110/112에서
  추가한 일시적 연결 재시도가 공유하는데, 높은 병렬도에서 로드 전환 시(다수 테이블이
  동시에 끝나며 재연결이 몰림) 스톰이 기존 10회(~20초) budget보다 길어져 소진되면,
  오류가 올바르게 재시도 대상으로 분류됐어도 테이블이 `ConnectionTimeout`으로
  실패했습니다. 기본값을 20(지수 백오프로 ~70초 재시도)으로 올리고 — 대규모 로드는
  수 시간 실행되며 이런 블립을 반드시 만납니다 — `DSQL_MIGRATOR_FULL_LOAD_OCC_MAX_ATTEMPTS`
  (1–100)로 노출했습니다. 각 재시도는 여전히 새 연결로 멱등 배치를 재실행하므로,
  인내심만 늘 뿐 중복은 없습니다.

## v0.1.112

### Fixed

- **Full Load가 알려진 메시지 시그니처뿐 아니라 SQLSTATE 없는 연결 오류를 전부
  재시도합니다.** v0.1.110에서 SQLSTATE 없는 연결 드롭을 재시도하도록 했지만
  libpq/OpenSSL 메시지 부분문자열 목록으로 매칭했습니다. 높은 병렬도의 연결 폭풍
  (여러 테이블이 동시에 끝나며 수백 개 동시 연결)에서 DSQL은 드롭을 *다양한* 형태로
  던집니다 — "SSL error: unexpected eof", "Network is unreachable", 그리고
  **"connection timeout expired"** — 목록에 없는 메시지는 영구 실패로 새어나갔습니다
  (512 연결 1TB 실행이 `connection timeout expired`로 테이블을 잃음). 이제 분류기가
  **`sqlstate=None`인 psycopg `OperationalError`/`InterfaceError`는 전부** 일시적
  연결 오류로 취급하며(실제 데이터/제약 오류는 항상 SQLSTATE를 가짐), 예외 타입으로
  게이트해 도구 자체의 SQLSTATE 없는 구조적 오류는 여전히 재시도하지 않습니다.
  메시지 시그니처 목록은 타입이 유실된 래핑 오류용 폴백으로만 유지합니다.

## v0.1.111

### Fixed

- **DSQL 연결을 IPv4로 고정해, IPv4-only 네트워크에서 재연결이 엔드포인트의 도달
  불가능한 IPv6 주소로 실패하지 않도록 했습니다.** Aurora DSQL 엔드포인트는
  dual-stack(A + AAAA 레코드)입니다. IPv4-only VPC(예: IPv6 egress가 없는 ECS
  태스크)에서 libpq가 재연결을 IPv6(AAAA) 주소로 시도하면 *"connection to server
  at … failed: Network is unreachable"* 로 실패합니다. 평소엔 드러나지 않다가,
  일시적 DSQL 이벤트(예: 순간 `XX000 server unavailable`)로 재연결이 한꺼번에
  발생할 때 IPv4가 멀쩡히 도달 가능함에도 IPv6 시도들이 진행 중인 Full Load를
  실패시킵니다(실측: in-VPC 1TB 적재가 DSQL 블립 직후 IPv6 `Network is
  unreachable`로 테이블들을 잃음). 이제 `DsqlConnector.connect`가 엔드포인트의 IPv4
  주소를 resolve해 `hostaddr`로 전달(DNS 이름은 TLS SNI/인증서 검증용으로 `host`에
  유지)하므로, 모든 연결·재연결이 도달 가능한 주소 계열에 머뭅니다. IPv4가 없으면
  기존 host 기반 resolve로 폴백(IPv6-only 환경은 영향 없음). Full Load·Validation·
  프로브 등 모든 DSQL 연결에 적용됩니다.

## v0.1.110

### Fixed

- **Full Load가 SQLSTATE 없는 쿼리 도중 연결 드롭(예: TLS 끊김)에서 테이블 전체를
  실패시키지 않고 복구하도록 수정했습니다.** 배치 로더는 일시적 연결 드롭을 새
  연결로 멱등 배치를 재실행해 재시도하도록 설계돼 있지만, `_is_transient_connection_error`가
  **서버가 보고한 SQLSTATE class `08`** 만 인정했습니다. TLS 소켓이 쿼리 도중 끊기면
  서버가 에러 코드를 못 보내 psycopg가 `sqlstate=None` 인 `OperationalError`와
  *"SSL error: unexpected eof while reading"* / *"server closed the connection
  unexpectedly"* 같은 메시지만 던집니다. 이게 **영구 오류로 오분류** → 재시도 안 함
  → 배치(및 테이블 전체) 실패로 이어졌습니다. 특히 높은 쓰기 병렬수에서 심각(동시
  연결이 많을수록 DSQL이 피크에 일부 연결을 끊음): in-VPC 1TB 적재를
  `table_parallelism=16 × batch_parallelism=32`(512 연결)로 돌리자 완료 직전
  16/20 테이블이 `SSL error: unexpected eof`로 유실됐습니다. 이제 분류기가
  **SQLSTATE 없는 연결 끊김 오류**(libpq/OpenSSL 드롭 시그니처로 매칭)도 transient로
  인정해 재연결·재시도합니다 — CDC sink의 transient 재연결에 대응하는 Full Load판.
  SQLSTATE를 가진 실제 데이터/제약 오류나, 연결 드롭이 아닌 SQLSTATE 없는 구조적
  오류는 영향 없음(계속 표면화, 무한 재시도 안 함).

## v0.1.109

### Changed

- **CDC 테이블별 상태의 "How to read this table"를 훨씬 스캔하기 쉽게 개선하고,
  까다로운 컬럼은 그 자리에서 스스로 설명하도록 했습니다.** 기존 범례는 작은 회색
  글머리표가 통짜로 나열되어 컬럼명이 설명 문장 속에 묻히고, consistency 색상은
  글자로만 서술됐습니다. 이제 옅은 테두리 패널의 **정의 행(definition rows)** 으로
  바뀌어 각 용어가 테이블 헤더와 일치하므로 매핑이 한눈에 보이고, Consistency 항목은
  색상을 글자로 쓰는 대신 테이블 셀과 **동일한 색의 실제 배지 칩**(`consistent` /
  `replicating…` / `rows missing` / `data quarantined`)을 그대로 렌더합니다. 또한
  의미가 자명하지 않은 세 컬럼 헤더(**Net rows since Full Load**, **Stream lag**,
  **Consistency**)에 한 줄 설명이 담긴 **ⓘ 툴팁**을 추가해, 도움말이 눈이 보는 그
  자리에 있게 했습니다. 범례 레이아웃용 재사용 컴포넌트 `definition_row`를 디자인
  시스템(단일 소스)에 추가했습니다.

## v0.1.108

### Fixed

- **부하가 소수 테이블에 집중된(skewed) CDC 워크로드에서 hot 테이블이 하나의 sink
  task에 직렬화되던 문제를 수정 — Kafka 토픽 파티션을 테이블 크기에 비례해
  배분합니다.** 기존 스케일링 기본값은 파티션을 균등 분산했는데, 이는 쓰기 부하가
  테이블에 고르게 분포한다고 가정합니다. 테이블이 많으면(sink 병렬성 상한 이상)
  **토픽당 파티션이 1개**로 줄고, 파티션 1개짜리 토픽은 최대 1개 sink task만
  소비할 수 있습니다. 그래서 소수의 hot 테이블이 대부분의 쓰기를 담당할 때(예:
  9개 중 4개 테이블만 때리는 sysbench) 각 hot 테이블이 단일 task로만 처리되고
  나머지는 유휴 상태가 되어 순수 throughput 손실이 발생했습니다(DSQL은 거의
  유휴). 이제 스캔 없는 테이블별 row-count 추정치(Full Load 워터마크의 값, 또는
  Full Load 전에 CDC 인프라를 배포한 경우 `information_schema`에서 즉석 추정)를
  읽어 큰 테이블에 Debezium `topic.creation` 그룹으로 **파티션을 더** 부여합니다
  (hot 테이블은 2 또는 4개; 단일 테이블은 동시 DSQL upsert 경합으로 이득이
  평탄해지는 4개가 상한). 덕분에 hot 테이블이 여러 task로 병렬 스트리밍됩니다.
  부하가 균등하면 동작하지 않으며(no-op), 크기 신호가 없거나
  `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` 오버라이드가 설정되면 기존 균등 기본값으로
  폴백합니다. 파티션 수는 토픽 생성 시 고정되므로 CDC 인프라 배포 시점에
  결정되며, 순서는 영향받지 않습니다(Debezium이 각 레코드를 PK로 키잉하므로 같은
  키는 항상 같은 파티션). 적용하려면 CDC 인프라를 새로 배포해야 합니다(기존
  토픽의 파티션 수는 변경 불가).

## v0.1.107

### Changed

- **Evaluation의 "Objects by importance" 필터를 혼동스러운 단일 컨트롤 대신 카테고리
  기반의 명확한 두 개 드롭다운으로 변경했습니다.** 기존 세그먼트 컨트롤은 파생된
  "Needs attention" 묶음과 분류(classification)별 값을 한 축에 섞어 놓아 모호하게
  읽혔습니다(예: "Needs attention" vs "Review needed"). 이제 AWS 콘솔 스타일의 필터
  드롭다운 두 개 — **Classification**(Automatic / Review needed / Unsupported)과
  **Estimated manual effort**(Simple / Medium / Significant) — 로 대체했으며, 이는
  요약 배지에 이미 표시되는 색상 구분 카테고리와 동일합니다. 두 필터는 AND로
  결합되며, 필터가 하나라도 활성화되면 **Clear filters** 링크가 나타납니다.
  드롭다운이 Cloudscape "filtering" 스타일과 일치하도록 재사용 가능한
  `filter_select` / `filter_bar`를 디자인 시스템(단일 소스)에 추가했습니다.

## v0.1.106

### Fixed

- **MSK Serverless가 자동 선택된 서브넷의 가용 영역을 거부할 때 CDC 인프라 배포가
  스스로 복구합니다.** MSK Serverless는 리전의 일부 AZ만 지원하며 이를 조회하는
  API가 없습니다. 그래서 배포가 AZ마다 NAT 이그레스 서브넷을 하나씩 자동 선택할 때
  지원되지 않는 AZ(예: `ap-northeast-2d`)의 서브넷을 고를 수 있고, 이 경우
  `MskCluster`가 `CREATE_FAILED … unsupported availability zones: [ap-northeast-2d]`
  로 실패하며 스택 전체가 롤백됩니다. 이제 배포기가 이 특정 실패를 감지해 스택
  이벤트에서 거부된 AZ를 파싱하고, 롤백된 스택을 삭제한 뒤 해당 AZ를 제외하고
  커넥터 서브넷을 다시 선택하여 생성을 자동으로 재시도합니다(무한 반복을 막도록
  횟수 제한). 지원되지 않는 AZ를 제외했을 때 NAT 이그레스 AZ가 2개 미만으로
  남으면, 반복하지 않고 제외된 AZ를 명시한 명확한 메시지와 함께 중단합니다. 새로
  입력할 값은 없으며 사용자는 여전히 VpcId만 제공하면 됩니다.

## v0.1.105

### 추가 (Added)

- **정확한 시간 기반 CDC replication lag — 부정확한 `MAX(pk)` "Stream lag"를 대체.** 기존
  테이블별 "Stream lag (newest)"는 양쪽 `MAX(pk)`를 비교했는데, 이는 PK 개수(시간 아님)이고,
  insert만 반영(UPDATE/DELETE 지연은 못 봄)하며, 단일 정수 PK에서만 동작했습니다. 이제 DSQL 싱크가
  각 변경의 **소스 커밋 시각**(Debezium `source.ts_ms`)을 읽어, 테이블별 **`ReplicationLagMs`**
  CloudWatch 메트릭 = 적용시각 − 소스 커밋시각(오프셋 커밋 창당 최악 lag, 밀리초)을 발행합니다.
  마이그레이션 모니터의 **"Stream lag"** 컬럼이 이제 실제 시간값("8.5s behind", "2m 10s behind",
  "caught up")을 라이브·스캔프리로 보여줍니다 — PK 타입 무관하고 최신 insert뿐 아니라 update/delete
  지연도 반영. 시간 메트릭을 쓸 수 없을 때(구버전 플러그인)나 카운트 미갱신 시에만 기존 `MAX(pk)`
  leading-edge 체크("N behind (PK)")로 폴백합니다. 발행은 철저히 best-effort(복제에 영향 없음)이고
  v18 메트릭 배관/IAM(`cloudwatch:PutMetricData`, `metrics.stack`)을 재사용 — 신규 IAM 없음.
- 반영하려면 재빌드된 커넥터 플러그인(`PLUGIN_VERSION` → `v19`)과 CDC 재배포가 필요하며, 그 전까지는
  컬럼이 `MAX(pk)` 폴백을 사용합니다.

## v0.1.104

### 수정 (Fixed)

- **Query Playground의 "Test on target"이 스키마 없는(unqualified) 테이블 이름을 이제 제대로
  찾습니다 — `relation "orders" does not exist` (42P01) 실패 해결.** MySQL 데이터베이스 대상으로
  작성한 쿼리는 테이블을 스키마 없이 씁니다(`SELECT * FROM orders`). 그런데 마이그레이션은 각 MySQL
  데이터베이스를 같은 이름의 PostgreSQL **스키마**(`ecommerce_demo`)로 매핑하므로, DSQL에선 테이블이
  그 스키마에 있습니다 — 프로브가 돌던 기본 `public` search_path엔 없어서 unqualified 참조가 전부
  거부됐습니다. 이제 프로브가 `EXPLAIN`/dry run 전에 `search_path`를 소스 데이터베이스의 스키마(그 다음
  `public`)로 설정해, MySQL 실행 컨텍스트를 재현하고 변환된 쿼리가 마이그레이션된 테이블 대상으로
  검증되게 합니다. 소스 연결에 데이터베이스가 지정되지 않았으면 영향 없음(search_path 그대로).

## v0.1.103

### 수정 (Fixed)

- **재실행 중 다른 스텝으로 이동했다가 돌아왔을 때 Validation이 "In progress"에 갇혀 "Re-run
  validation" 버튼이 잠기는 문제 수정.** Re-run을 누른 뒤 실행 중에 화면을 벗어나면(예: Data
  Migration 스텝으로 가서 Stop CDC), 스텝을 `DONE`으로 넘기는 poll 타이머가 페이지와 함께 사라집니다
  — 그래서 백그라운드에서 실행이 끝난 뒤 Validation으로 돌아오면 스텝이 **content 렌더 안에서**
  `DONE`으로 정리되는데, 이는 상단 shell(스텝 헤더 배지 + Re-run 버튼)이 이미 stale "In progress"로
  그려진 뒤라 shell이 다시 안 그려집니다. 이제 content 안의 정리가 저장 상태를 바꿀 때마다(실행-중-
  이탈 케이스 및 v0.1.102의 재연결 케이스) **one-shot refresh를 예약해 shell을 다시 그리도록** 했습니다
  → 완료 리포트가 표시되고 Re-run 버튼이 활성화됩니다. (다음 렌더는 `DONE`/`NOT_STARTED`를 보므로
  루프 없음.)

## v0.1.102

### 수정 (Fixed)

- **재연결 후 Validation이 "In progress"에 갇혀 "Re-run validation" 버튼이 영구 잠기는 문제
  수정.** 검증이 막 끝나는 순간 브라우저가 재연결되면(또는 실행 중에 세션이 저장되면), 스텝이
  `IN_PROGRESS`로 복원되면서 완료된 리포트도 함께 복원됩니다 — 그런데 검증 job id는 저장되지 않아
  이를 `DONE`으로 넘겨줄 라이브 job이 없습니다. content 안의 `DONE` 정리는 **너무 늦게**(상단 shell이
  이미 stale "In progress" 배지 + 비활성 Re-run 버튼을 그린 뒤, 그리고 shell이 다시 렌더되지 않음)
  일어나, 완료 리포트가 "In progress" 헤더 아래 표시되고 Re-run이 영구 잠긴 채 남았습니다. 이제
  세션 복원 시 **IN_PROGRESS인데 완료 리포트가 있으면**(리포트가 있다는 건 실행이 끝났다는 증거)
  **shell 렌더 전에** 스텝을 `DONE`으로 정리 → 완료 결과가 표시되고 Re-run 버튼이 활성화됩니다.
  실제 실행 중인 런은 리포트가 없으므로(실행 시작 시 지워짐) 라이브 런을 가리는 일은 없습니다.

## v0.1.101

### 수정 (Fixed)

- **CDC 인프라를 삭제한 뒤 재연결하면, 예전 "Infrastructure deleted" 로그에 갇히지 않고 다시
  "Deploy CDC infrastructure" 버튼이 나타납니다.** 재연결 시 세션 복원이 **완료된** CDC lifecycle
  작업 링크를 다시 적용해(끝난 삭제 작업의 스테이지 로그가 계속 렌더링됨) + 삭제 이전의 **낡은
  커넥터 이름**을 복원해(카드가 파이프라인을 잘못 분류) 재배포 경로를 못 보여줬습니다. 이제 마지막
  CDC 액션이 **teardown(`delete`/`stop`)**이면 복원 시 그 **완료-작업 링크와 낡은 커넥터 이름을
  건너뜁니다** → 카드가 새 읽기 전용 AWS phase 프로브로 구동됩니다: **absent → Deploy CDC
  infrastructure**, **infra → Start CDC**. 스택 식별자는 계속 복원되어 프로브가 어떤 스택을 볼지
  알 수 있고, 진행 중인 teardown은 낡은 작업이 아니라 프로브의 실시간 스택 상태로 반영됩니다.

## v0.1.100

### 추가 (Added)

- **내구성 있는 S3 job 스토어 — 중단된 Full Load와 테이블별 마이그레이션 모니터가 이제 Fargate
  재배포에도 살아남습니다.** JobManager의 작업 상태가 태스크의 **임시 `/tmp`** SQLite 파일에
  있어서, 앱 재배포(ECS 태스크 교체) 시 초기화됐습니다: 중단된 Full Load가 resume 불가였고,
  Full Load 작업에 묶인 **테이블별 모니터가 재배포 후 빈 화면**이 됐습니다(S3 세션 스토어는
  `job_id` 링크만 저장하고 작업 자체는 저장 안 함). 새 `S3JobStore`는 각 작업 스냅샷을 툴의
  **관리형 플러그인 버킷**(세션 스토어와 같은 버킷, 자동 프로비저닝 — 추가 설정 없음) `jobs/`
  접두사에 JSON 객체로 저장해, 태스크 교체에도 작업/resume 상태가 유지됩니다. Fargate에선
  `DSQL_MIGRATOR_JOB_STATE_BUCKET` → 관리형 버킷으로 연결; 로컬 개발은 기존 SQLite 스토어 유지
  (둘 다 `JobStore` 프로토콜 충족이라 JobManager는 무변경).
- **스케일 안전 쓰기(PUT storm 없음).** Full Load drain은 progress tick마다 저장하는데, 로컬
  SQLite엔 저렴하지만 대형 테이블에선 S3를 폭주시킵니다. resume엔 chunk/job **상태 전이만**
  중요하므로(비-`DONE` chunk는 통째 재실행, 하위 progress는 표시용이며 중단된 chunk는 reload 시
  `FAILED`로 조정), `S3JobStore`는 상태 시그니처가 바뀔 때 즉시 PUT하고 순수 progress 쓰기는
  5초당 최대 1회로 throttle합니다 — 행 수와 무관하게 PUT을 상태 전이 수 수준으로 bound. best-effort
  (S3 오류가 라이브 마이그레이션을 절대 깨지 않음), 신규 IAM 없음(태스크 역할의 기존 버킷 `/*`
  권한이 `jobs/` 접두사를 커버). 템플릿+코드 변경만 있고 커넥터/플러그인 변경은 없음 — 다음 앱
  재배포 시 반영.

## v0.1.99

### 수정 (Fixed)

- **테이블별 순증 행수 모니터가 이제 cluster 모드뿐 아니라 single-database 모드에서도
  동작합니다.** DSQL 싱크는 `NetRowsApplied` 메트릭의 `Table` 차원을 항상 **스키마 정규화된**
  형태(`db.table`, 예: `ecommerce_demo.orders`)로 발행하는데, single-database 모드에서는 툴이
  테이블을 **bare** 이름(`orders`)으로 지칭하므로 모니터의 정확 차원 CloudWatch 조회가 빗나가
  "Full Load 이후 순증 행수" 컬럼이 조용히 `COUNT(*)` 기반 값으로 폴백했습니다. 이제 reader가
  해당 스택에 대해 싱크가 실제로 발행한 `Table` 차원 값들을 `ListMetrics`로 발견한 뒤, 요청된
  테이블을 정확 이름으로, 없으면 **모호하지 않은 bare** 테이블 이름으로 매칭합니다 — 그래서 정규화
  방식을 가정하지 않고 cluster(이미 정규화됨)와 single-database(bare) 양쪽에서 스캔 없는 컬럼이
  동작합니다. bare 이름이 모호한 경우(두 스키마에 같은 테이블 이름)는 잘못 귀속시키지 않도록
  건너뜁니다(해당 테이블은 COUNT로 폴백). 앱 태스크 역할에 `cloudwatch:ListMetrics`를 부여합니다
  (리소스 수준 스코핑이 없어 Resource `*`). reader + IAM 변경만 있고 커넥터/플러그인 변경은
  없습니다: 배포하면 역할이 갱신되고 reader가 반영됩니다 — 플러그인 재빌드나 CDC 재배포 불필요.

## v0.1.98

### 추가 (Added)

- **테이블별 "Full Load 이후 순증 행수"를 이제 `COUNT(*)` 없이 표시합니다 — 소스 스캔이
  아니라 싱크 메트릭에서 가져옵니다.** DSQL 싱크 커넥터가 테이블별 `NetRowsApplied` CloudWatch
  메트릭(네임스페이스 `MysqlDsqlMigrator/CDC`, 디멘션 `Stack` + `Table`)을 발행합니다. 커밋마다
  insert − delete를 기록하므로(insert는 +1, update는 0, delete는 −1), 이 메트릭을 합산하면 CDC가
  스트리밍을 시작한 이후 각 테이블에 적용한 순증 행수가 됩니다. 테이블별 마이그레이션 상태 모니터가
  기존 ~5초 CDC 폴링에서 이 값을 읽어 그대로 보여주므로, "Full Load 이후 순증 행수" 컬럼은 더 이상
  소스/타깃에 `COUNT(*)`를 돌릴 필요가 없습니다 — 가볍고, (수십억 행일 수 있는) 소스를 절대 스캔하지
  않습니다. CDC가 스트리밍 중일 때는 테이블별 표가 이제 그 폴링 주기에 맞춰 다시 렌더링되므로(저장된
  메트릭만 읽고 네트워크 호출 없음), "Refresh source/target counts"를 눌러야만 갱신되던 이전과 달리
  컬럼이 **실시간으로** 갱신됩니다(그 버튼은 여전히 정확한 소스/타깃 `COUNT(*)`를 실행하며, 해당
  컬럼들은 그대로입니다). 싱크의 메트릭 발행은 철저히 best-effort이며(메트릭 실패가 복제나 오프셋
  커밋에 전혀 영향을 주지 않음), 메트릭을 쓸 수 없을 때(구버전 플러그인이거나 싱크가 아직 발행 전)는
  기존 `타깃 − Full Load` 값으로 폴백합니다. 이 값은 실시간 진행 모니터이지 최종 정합성 판정이
  아닙니다: Kafka Connect가 이미 적용된 배치를 재전송하면(at-least-once) 약간 과다 집계될 수 있으므로,
  정확한 소스-타깃 판정은 여전히 Validation(4단계)입니다.
- 반영되려면 재빌드된 커넥터 플러그인(`PLUGIN_VERSION` → `v18`)과 CDC 재배포가 필요하며, 그
  전까지는 모니터가 `COUNT(*)` 기반 폴백을 사용합니다. 템플릿 변경으로 싱크의 커넥터 실행 역할에
  네임스페이스 조건으로 범위를 제한한 `cloudwatch:PutMetricData`를 부여하고, 앱 태스크 역할의
  `cloudwatch:GetMetricData`(v0.1.97에서 추가)가 이를 다시 읽습니다.

## v0.1.97

### 수정 (Fixed)

- **라이브 CDC 파이프라인 헬스의 처리율이 이제 실제로 표시됩니다.** UI는 change-flow 패널에서
  커넥터의 `AWS/KafkaConnect` CloudWatch 메트릭(`SinkRecordSendRate`, `SourceRecordPollRate`,
  running/errored 태스크 수)을 읽는데, 앱 태스크 역할에 `cloudwatch:GetMetricData` 권한이 없어서 모든
  읽기가 `AccessDenied`로 실패하고 best-effort로 삼켜져 처리율이 빈 값/unknown으로 보였습니다.
  `cloudwatch:GetMetricData`(Resource `*` — 이 API는 리소스 스코프 미지원)를 부여해 실제 send/poll
  rate가 표시되도록 했습니다. **소스 스캔이 없는** 경량 CDC 활동 신호이며, 다음에 추가할 테이블별
  net-rows 커스텀 메트릭 읽기 준비도 됩니다. 템플릿 IAM 변경만 — 배포로 역할 갱신(이미지 재빌드 불필요).

## v0.1.96

### 수정 (Fixed)

- **CDC 시작/정지가 `kafkaconnect:ListConnectors`에 대한 `AccessDeniedException`으로 더 이상
  실패하지 않습니다.** CDC 배포 역할이 `ListConnectors`를 커넥터 ARN(`connector/mysql-dsql-cdc-*/*`)으로
  스코프했는데, `ListConnectors`는 **계정 수준** list 작업이라 `.../v1/connectors`에 대해 인가되므로 ARN
  스코프로는 아무 권한도 부여되지 않았습니다. deployer는 2-pass Start CDC(및 Stop) 중 소스/싱크 상태를
  읽으려 커넥터를 list하는데, 이 읽기가 AccessDenied로 막혀 작업이 오류("could not read … state")로
  끝났습니다. v0.1.86에서 커넥터 상태 읽기가 (조용히 `None` 반환 대신) 예외를 던지도록 바뀌면서 드러났습니다.
  이제 `ListConnectors`는 별도 문장에서 `Resource: "*"`로 부여되고(태스크 역할의 discovery 권한과 동일),
  나머지 커넥터 작업은 `mysql-dsql-cdc-*` 패밀리로 스코프 유지됩니다. 역할 갱신을 위해 앱 스택 배포 필요(이미지
  재빌드는 불필요).

## v0.1.95

### 수정 (Fixed)

- **스냅샷 직렬화 오류가 UI를 깨뜨리지 못하도록 S3 세션 스토어를 강화.** `S3SessionStateStore.save()`
  에서 `model_dump_json()` 직렬화가 "호출자에게 예외를 절대 던지지 않는다"를 보장하는 `try`/`except`
  바로 바깥에서 실행되어, (평소엔 발생하지 않지만 이론상 가능한) 직렬화 실패가 새어나가 세션 상태를
  저장하는 라이브 UI 요청을 깨뜨릴 수 있었습니다. 직렬화를 가드 안으로 옮겨 모든 경우에 best-effort
  계약을 지키도록 했습니다 — 정상 경로 동작은 그대로. (v0.1.93 변경의 자체 적대적 리뷰에서 발견.)

## v0.1.94

### 수정 (Fixed)

- **CDC 정지가 더 이상 잘못된 "Stack operation timed out"로 보고되지 않습니다.** Stop CDC는
  `MskBootstrapServers`를 비워 커넥터를 제거하는데, 예전엔 in-VPC 오프셋 seeder Lambda까지 함께
  제거했고, 그 Lambda의 Hyperplane ENI 회수가 ~20–40분 걸려 컨트롤 플레인의 10분 정지 대기예산을
  크게 초과했습니다. 그래서 커넥터는 이미 제거됐는데도(= CDC 실제 정지) 정지가 실패로 보고되고,
  스택은 몇 분 뒤 알아서 `UPDATE_COMPLETE`에 도달했습니다. 이제 cdc-stack 템플릿이 새
  `DeploySeederFunction` 조건(seeder 키 + 워터마크에만 의존, `MskBootstrapServers`와 무관)으로
  **정지 중에도 seeder Lambda(+역할)를 유지**하고, 정지 시에는 빠른 `OffsetSeedResource` invoker만
  제거합니다. 그러면 정지 정리는 커넥터 2개 + invoker(모두 빠름)뿐이라 타임아웃 안쪽에서 마무리되고,
  VPC-Lambda ENI 철거는 전체 스택 삭제(그쪽 타임아웃은 이미 이를 수용) 때만 발생합니다. 갱신된
  템플릿이 배포된 뒤 — 즉 다음 Start CDC부터 — 적용됩니다.

## v0.1.93

### 추가 (Added)

- **재배포를 넘어 유지되는 durable 세션 재개 (S3 기반 세션 스토어).** 재연결한 브라우저가
  세션별 워크벤치(워크플로 진행·Step-1 Evaluation 결과·Schema Conversion 선택·CDC 시작점/adopt한
  스택)를 Evaluation 재실행 없이 이어서 작업합니다. 이 스냅샷이 이전엔 컨테이너 **임시 디스크**의
  로컬 SQLite 파일에 있어, Fargate **태스크 교체**(모든 재배포)가 이를 지웠고 — 배포할 때마다
  Evaluation을 다시 해야 했습니다. 새 `S3SessionStateStore`(기존 `SessionStateStore` protocol
  구현)가 각 비밀-아님 스냅샷을 툴의 관리형 플러그인 버킷(`mysql-dsql-migrator-plugins-<account>-<region>`,
  자동 프로비저닝 — 새 파라미터·고객 설정 없음)의 `sessions/` 프리픽스에 기록해, 이제 재배포를 견딥니다.
  Fargate 배포에서 새 `DSQL_MIGRATOR_SESSION_STATE_BUCKET`(템플릿이 관리형 버킷으로 지정)으로 자동
  선택되고, 로컬 dev는 SQLite 경로 유지. 비밀 아닌 상태만 저장(Property 7 — 소스 DB 비밀번호는 Connect에서
  재입력); 영속화는 best-effort(일시적 S3 오류는 로깅만·UI 안 깨짐). 태스크 역할에 세션 삭제/prune용
  `s3:DeleteObject` 추가.

## v0.1.92

### 수정 (Fixed)

- **기존 CDC 파이프라인을 attach하면 이제 테이블 집합까지 reconcile되어, CDC 단계가 "테이블
  미선택"이 아니라 실제 실행 중인 파이프라인을 반영합니다.** 세션이 기존 cdc-stack에 연결
  ("Attach to &lt;stack&gt;", 예: 세션 리셋 후)하거나 파이프라인이 세션 밖에서 시작된 경우,
  세션엔 Full Load 워터마크도 세션 내 테이블 선택도 없어서 — 파이프라인이 실제로 복제 중인데도 —
  CDC 단계가 "Select at least one table before starting CDC"를 띄우고, 빈 집합으로 config
  미리보기를 만들며, 테이블별 상태를 채우지 못했습니다. 이제 렌더 시점 스택 프로브가 라이브 스택의
  `TableIncludeList`(소스 커넥터의 `table.include.list` = 각 테이블 이름)를 읽어 세션에
  reconcile하고, `_cdc_tables_for_config`가 이를 (세션 내 워터마크/선택 다음의) 최종 폴백으로
  사용합니다. 그래서 adopt/out-of-band 파이프라인도 어떤 테이블을 복제 중인지 정확히 해석해 —
  "테이블 선택" 경고가 사라지고, config 미리보기·테이블별 상태가 실제와 일치합니다 — 반면 일반적인
  세션 내 Full Load → Start CDC 흐름은 그대로입니다. 다른 스택을 재adopt하면 이전 reconcile 집합은
  비워지고 새 프로브가 다시 채웁니다.

## v0.1.91

### 수정 (Fixed)

- **CDC "Deploy log"가 라이프사이클 작업 중 몇 초마다 저절로 접히던 문제 수정.** 라이브
  CDC 패널은 새 deploy 로그 라인과 커넥터 상태를 스트리밍하려고 ~5초마다 다시 렌더됩니다.
  "Deploy log" 확장 패널의 열림/닫힘 상태가 패널 렌더 함수의 **지역 변수**에 담겨 있어, 전체
  재렌더 때마다 접힌 상태로 새로 만들어졌습니다 — Start/Stop/Deploy를 지켜보려고 펼쳐 둔 로그가
  몇 초 뒤 스스로 닫혔습니다. 이제 열림/닫힘 상태를 **세션 범위 마이그레이션 상태**에 저장하여
  모든 단계의 재렌더(내부 refreshable + 외부 패널 폴링)를 견디고, 사용자가 닫기 전까지 열린
  채로 유지됩니다.

## v0.1.90

### 수정 (Fixed)

- **Schema Conversion 단계의 "CDC 실행 중이라 스키마 적용 불가" 차단이 이제 막다른 길이
  아니라 실행 가능한 안내로 바뀌었습니다.** CDC 파이프라인이 이미 타깃으로 스트리밍 중이면
  스키마 변환 적용은 (정상적으로) 차단됩니다 — 싱크가 해당 테이블에 쓰고 있고 DDL은 복제되지
  않으므로, REPLACE는 그 테이블을 drop하거나 손상시킬 수 있기 때문입니다. 이전에는 이 안내가
  Apply를 눌렀을 때 잠깐 뜨는 토스트로만 나타나 "먼저 CDC를 중지하라"고만 했는데, CDC를 중지할
  수 있는 유일한 곳인 Data Migration은 Schema Conversion을 선행 조건으로 잠겨 있어 그 화면에서
  앞으로 나아갈 방법이 없었습니다. 이제 Schema Conversion은 CDC가 살아 있을 때 단계 상단에
  **상시 표시되는 경고 알림**을 보여 줍니다 — 타깃 스키마는 (CDC가 스트리밍 중이므로) **이미
  적용된 상태**임을 설명하고, 안전한 한 가지 경로인 **"Skip conversion & continue to Data
  Migration"**(스키마 변환 건너뛰고 Data Migration으로 진행)을 제시합니다. 이 버튼은 진행과
  동시에 Data Migration을 잠금 해제하여, 스키마를 정말로 바꿔야 한다면 그곳에서 CDC를 중지할 수
  있게 합니다. Apply 시 뜨는 토스트도 동일한 실행 가능 안내(계속하려면 Skip, 스키마를 바꾸려면
  Data Migration에서 CDC 중지)를 담도록 바뀌었습니다.

## v0.1.89

### 수정 (Fixed)

- **"기존 CDC 인프라에 연결" 배너가 실제로 CDC를 선택하는 Migration Plan 단계에도 표시됩니다.**
  v0.1.88이 배너를 추가했지만 **Data Migration 단계의 migration-type 선택기에만** 있었습니다.
  **Migration Plan** 단계는 별도 화면("CDC 포함? — 예, keep in sync")인데 거기서 배너가 안 떠서,
  거기서 keep-in-sync를 선택하면 이미 존재하는 파이프라인인데도 여전히 "CDC 인프라 배포" 플로우(그리고
  "already exists" 에러)로 떨어졌습니다. 이제 Migration Plan의 CDC-인프라 섹션이, 다른 스택 이름으로
  기존 `mysql-dsql-cdc-*` 파이프라인이 발견되면 **"Attach to &lt;스택&gt;"** 배너를 보여줍니다(그
  단계에서 이미 도는 phase probe가 발견 결과도 채웁니다). 거기서 연결하면 세션이 그 스택을 대상으로
  잡고, 다음 probe가 배포됨으로 인식해 "CDC infrastructure ready"를 표시합니다. Data Migration 쪽
  노출도 유지됩니다.

## v0.1.88

### 추가 (Added)

- **기존 CDC 인프라를 CDC 단계 깊숙한 곳이 아니라 Migration Plan 단계에서 바로 표시합니다.** 직전
  릴리스가 계정 범위 CDC 발견 + "기존에 연결" 선택을 추가했지만, 그 affordance는 **활성 CDC
  서브스텝 안에서만** 렌더돼, 세션 리셋 후에는 거기까지 도달이 어려웠습니다(앞 단계들을 먼저 통과해야
  함). 이제 **plan에 CDC가 포함되는 순간**, migration-type 선택 옆에 배너가 떠 기존
  `mysql-dsql-cdc-*` 파이프라인을 이름과 함께 **"Attach to &lt;스택&gt;"** 액션으로 보여줍니다 —
  CDC 서브스텝까지 가서 중복 배포 위험을 만나지 않고 그 자리에서 연결할 수 있습니다. 발견은 이미
  plan 시점에 돌고 있었고(plan에 CDC 포함 여부로 게이팅), 이번 변경은 그 결과를 사용자가 있는 곳에
  노출할 뿐입니다. 연결은 read/attach-only이며, 의도적인 두 번째 파이프라인 배포(다른 스택 이름
  suffix)는 여전히 CDC 단계에서 가능합니다 — 즉 선택이지 하드 블록이 아닙니다.

## v0.1.87

### 추가 (Added)

- **CDC 화면이 기존 CDC 인프라를 발견해 "연결(attach)"을 제안합니다 — 무작정 재배포하지 않습니다.**
  도구는 마이그레이션이 쓰는 CDC 스택 이름을 세션 상태에만 두는데, 단일 태스크 앱 재시작
  (ECS/Fargate 태스크 교체)으로 그 값이 사라지면, 재접속 세션이 **다른 이름으로 이미 CDC가 돌고
  있어도** 기본 "CDC 인프라 배포" 플로우로 떨어져 **두 번째(과금되는) MSK 클러스터**를 만들 위험이
  있었습니다. 이제 CDC 단계가 계정에서 `mysql-dsql-cdc-*` 스택을 조회해, 세션이 대상으로 삼지 않는
  스택이 있으면 **"&lt;스택&gt;에 연결" 주 액션**으로 노출합니다(새 배포는 expansion 안으로
  de-emphasize). 연결하면 파이프라인의 **라이브 상태를 AWS에서 재도출**(running/provisioning/infra)
  하므로, 실행 중이면 곧바로 모니터링 뷰로 착지합니다. 스택/커넥터를 절대 변경하지 않으며, 새로
  시작하려면 **Stop CDC**(커넥터만 제거·MSK 유지) 또는 **Delete CDC infrastructure**의 명시적
  경로를 씁니다. 계정 범위 조회를 위해 CDC 배포 역할에 `cloudformation:ListStacks`가 필요합니다(앱
  스택에 추가); 조회는 best-effort라 권한이 없으면 그냥 아무것도 안 보여줍니다.

## v0.1.86

### 수정 (Fixed)

- **CDC가 커넥터 상태를 못 읽을 때 조용히 멈추지 않고 원인을 드러냅니다.** CDC 시작 시 도구는
  소스 커넥터가 `RUNNING`이 될 때까지 기다린 뒤 싱크 커넥터를 요청합니다. 그 대기가 커넥터 상태를
  읽는 헬퍼가 **모든 오류**(자격증명 만료, 스로틀, 일시적 네트워크)를 `None`으로 삼켰는데, 이는
  "아직 생성 중"과 구분되지 않아 — 읽기 실패 시 "creating…"만 영원히 반복하고, 싱크는 요청되지
  않으며, 오류도 표시되지 않았습니다(배포가 멈춘 것처럼 보임). 복구하려면 앱 태스크를 재시작해야
  했는데, Fargate에선 이게 진행 중 세션을 초기화해 모든 워크플로 단계를 다시 해야 했습니다. 이제
  상태 읽기 오류가 **전파**되며, `RUNNING` 대기는 일시적 읽기 실패를 몇 번까지만 관용한 뒤 **실제
  원인과 함께 실패**하고, 회복 불가한 자격증명/인가 오류에는 "Start CDC 재시도" 안내와 함께 **즉시
  실패**합니다. 진짜로 커넥터가 없으면 여전히 `None`으로 읽혀(변경 없음) 정상 프로비저닝 폴링은
  영향받지 않습니다.

## v0.1.85

### 수정 (Fixed)

- **CDC 배포 실패: CDC 배포 역할에 CloudWatch 알람 권한이 없었음.** v0.1.84에서 CDC 스택에
  커넥터별 CloudWatch 알람(`ErroredTaskCount`)을 추가했으나, 앱의 `cdc-deploy` 역할에
  `cloudwatch:PutMetricAlarm` / `DeleteAlarms` / `DescribeAlarms` 권한을 부여하지 않았다.
  그래서 CDC 시작 시 알람 생성에서 `AccessDenied`로 실패하고 CDC 스택이 롤백됐으며(롤백도
  `cloudwatch:DeleteAlarms`에서 실패), 커넥터가 하나도 생성되지 않았다. 이제 역할이 CDC 스택
  계열의 알람 ARN으로 스코프된 알람 권한을 갖는다. **app-stack을 재배포해 권한을 반영한 뒤 CDC
  시작을 재시도**하면 된다(새 이미지 빌드 불필요 — IAM만 바뀌는 템플릿 변경).

## v0.1.84

### 수정 (Fixed)

- **CDC 싱크가 일시적 DSQL 연결 오류에 죽지 않고 자가 복구 (커넥터 재빌드, `PLUGIN_VERSION` v17).**
  일시적 실패(OCC 재시도 예산 소진, 또는 DSQL의 1시간 유휴 종료·IAM 토큰 만료·MSK Connect 워커
  재활용으로 연결이 끊긴 경우) 시 싱크가 일반 `ConnectException`을 다시 던졌는데, Kafka Connect의
  `WorkerSinkTask`는 이를 **치명적**으로 간주해 태스크를 죽이고 오프셋을 전진시키지 않아, 사람이
  커넥터를 재시작할 때까지 CDC가 멈췄다. 이제 이런 일시적 경우에는 `RetriableException`을 던지며,
  `WorkerSinkTask`가 이를 잡아 같은 배치를 재전송(일시정지 후 재시도)하므로 재연결을 거쳐 파이프라인이
  스스로 복구된다. 적용은 멱등이라 같은 오프셋 재생은 안전하다. 일시/영구 분류는 그대로이며, 진짜
  poison 행은 여전히 DLQ로 간다.
- **저트래픽 소스에서의 gapless 재개: Debezium 소스 커넥터에 `heartbeat.interval.ms` 추가.**
  Debezium은 레코드를 방출할 때만 커밋된 binlog 오프셋을 전진시킨다. 캡처 대상 테이블은 유휴인데 다른
  테이블이 binlog를 갱신하면 커밋 오프셋이 live binlog head보다 뒤처질 수 있고, 이후 소스 binlog
  리텐션이 그 지점을 지나 정리(purge)하면 재시작 시 재개가 불가능해진다(갭 → 강제 재-Full-Load).
  주기적 heartbeat가 오프셋을 계속 전진시킨다. `heartbeat.action.query`는 의도적으로 설정하지 않는다
  — 읽기 전용 소스에 쓰기를 하게 되며, MySQL은 heartbeat 레코드 방출만으로 충분하다.

### 추가 (Added)

- **CloudWatch 알람으로 실패한 CDC 커넥터를 자동 감지.** 각 커넥터(Debezium 소스, DSQL 싱크)에
  `AWS/KafkaConnect`의 `ErroredTaskCount` 메트릭 알람을 추가해, 사람이 콘솔을 지켜보지 않아도 errored
  태스크가 드러난다 — 기존에는 복구가 전적으로 누군가 FAILED 커넥터를 발견하는 데 의존했고, 그 공백이
  소스 binlog 리텐션을 넘길 수 있었다. 알람은 항상 생성된다(CloudWatch에서 확인 가능). 새 선택 파라미터
  `AlarmNotificationTopicArn`에 SNS 토픽 ARN을 지정하면 알림도 받는다. 배포에 SNS 연결은 필요 없다
  (기본값은 빈 값).

## v0.1.83

### 수정 (Fixed)

- **미국 외 리전에서도 AI 어시스트 동작: 기본 Bedrock 모델을 리전 무관 `global.*` 프로파일로 변경.**
  코드 기본 모델 id가 `us.anthropic.claude-sonnet-4-6`(미국 geo 전용 교차리전 추론 프로파일)이라
  비-미국 리전에서 `InvokeModel`이 거부했다. 예로 ap-northeast-2(서울)에 배포하고 AI 어시스트를
  켠 뒤 모델 id를 비워두면(자연스러운 경로) 실패했다. 이제 기본값은
  `global.anthropic.claude-sonnet-4-6`(모든 상용 리전에서 호출 가능)로, CloudFormation 템플릿의
  권장과 일치한다. `BEDROCK_MODEL_ID`로 재정의 가능. (리전 이식성 감사로 발견 — 배포 템플릿·리전
  파싱·STS/토큰 리전·S3 엔드포인트/LocationConstraint는 이미 리전-정상이었고, 이 코드 기본값만
  미국 고정이었음.)

## v0.1.82

### 수정 (Fixed)

- **AI 어시스트: 만료/무효 AWS 자격증명이 이제 실행 가능한 메시지를 줌.** 세션 만료·서명 오류
  (`ExpiredTokenException`, `InvalidSignatureException`, `InvalidClientTokenId` 등)가 일반적인
  "unavailable"/"unknown"으로 오분류되어, 재인증하라는 힌트 없이 "AI 없이 계속"만 안내했다. 이제
  이런 오류는 제안 경로와 "Verify AI access" 경로 양쪽에서 `ACCESS_DENIED`로 분류되고, 두 메시지
  모두 자격증명/세션 만료 시 재인증하라고 안내한다.
- **클러스터 전체 스키마 조사: 크로스-스키마 외래키 대상이 이제 스키마로 한정됨.** 클러스터 전체(여러
  스키마)를 조사할 때 테이블명은 `schema.table`로 한정됐지만 외래키의 참조 테이블은 한정되지 않아,
  하위의 고아행 검사/DDL 쿼리가 부모를 search_path(또는 다른 스키마의 동명 테이블)로 잘못 해석했다.
  이제 FK 대상은 FK 자신의 `referred_schema`(동일 스키마 FK는 조사 중인 스키마)로 한정되어 테이블명
  한정 방식과 일치한다.

### 변경 (Changed)

- **AI 어시스트 견고화.** Bedrock 클라이언트에 연결/읽기 타임아웃(10초/60초)을 설정해, 연결이 멈춰도
  "AI is writing…"/"Verifying…" 상태가 무한정 도는 일이 없도록 했다(멈춘 소켓은 분류된 네트워크/타임아웃
  오류로 표면화). "Verify AI access"는 이제 클라이언트 *생성* 중 오류(예: 리전 해석 불가)도 포착해
  예외를 UI로 흘리지 않고 실행 가능한 결과로 보고한다. 연결 화면의 상시 AI 상태 줄은 회색 평문 대신
  디자인 시스템 팔레트로 심각도를 표시한다.
- **소스 개요: 리더가 아니라 Aurora writer의 인스턴스 클래스 표시.** Aurora 클러스터 엔드포인트의
  소스 메타데이터 조회가 임의 멤버가 아니라 `DescribeDBClusters`(`IsClusterWriter`)로 writer를 찾도록
  해, writer/reader 크기가 다른 구성에서 소스 용량을 잘못 표시하지 않는다(best-effort, 실패 시 첫 멤버로 폴백).
- **스키마 적용: `CREATE SCHEMA`가 중복 레이스를 self-heal.** 스키마 생성 시 `42P07`을 이제
  테이블/뷰/인덱스 경로와 동일하게 `CREATED`(스키마 존재)로 흡수해, 잘못된 `FAILED` 대신 수렴한다.

## v0.1.81

### 수정 (Fixed)

- **Evaluation: `TINYINT(1)`·`BIT(n)`·`YEAR`가 더 이상 "완전 자동 호환"으로 보고되지 않음.**
  호환성 assessor에 이 세 타입 규칙이 없어, 이런 컬럼만 있는 테이블이 `AUTO`/`COMPATIBLE`(위험
  0)로 분류됐다 — 스키마 컨버터는 셋 다 의미가 바뀐 *다른* DSQL 타입(`MANUAL`)으로 매핑하고,
  특히 `TINYINT(1)` 값이 `{0,1}` 밖이면 Full Load가 중단되는데도. 즉 Evaluation이 적재에서
  실패할 수 있는 테이블을 "완전 호환·위험 0"으로 보여줘 assessor의 "조용히 호환 처리 안 함"
  보장과 모순됐다. 이제 `TINYINT_BOOLEAN`/`BIT_TYPE`/`YEAR_TYPE` 규칙이 각각을 컨버터 분류에
  맞춰 구체적 위험과 함께 `MANUAL`로 표면화한다.

### 변경 (Changed)

- **Evaluation: 공간(spatial) 컬럼이 `UNSUPPORTED`가 아니라 `MANUAL`로 분류됨.** 공간
  타입(`GEOMETRY`, `POINT`, `POLYGON` 등)이 "타입 치환 또는 컬럼 재설계" 권고와 함께
  `UNSUPPORTED`로 분류돼 테이블이 차단된 것처럼 보였다. 하지만 컨버터는 이미 각 공간 컬럼을
  `bytea`로 자동 치환(원본 WKB 바이트를 Full Load·CDC 전 구간 보존)하므로 테이블은 마이그레이션된다.
  새 `SPATIAL_TYPE` 규칙이 이를 `MANUAL`(원본 `bytea`로 충분한지 검토; 공간 연산자/인덱스는 손실)로
  재분류해, 이미 마이그레이션되는 테이블을 재설계하러 보내지 않는다.

## v0.1.80

### 변경 (Changed)

- **UI: 상태 표시를 임의 글리프/색상 대신 디자인 시스템 팔레트로 통일.** Data Migration ·
  Evaluation 화면 전반의 디자인 시스템 일관화 패스:
  - Full Load "CDC 스트리밍 중" 경고 카드와 CDC 일관성 / 스트림 지연 컬럼에서 리터럴
    `✓`/`✗`/`⚠` 글리프 제거(해당 글리프가 없는 폰트에서 두부(tofu) 박스로 깨질 위험).
    심각도는 기존 색상 알림 박스 / 상태 배지가 전달하며, 헬스 테이블 레전드는 글리프 대신
    색상 배지를 설명하도록 문구를 수정.
  - busy 버튼(Fetch current position, Start CDC, Apply to target)은 디자인 시스템이 금지한
    Quasar `loading` prop 대신 버튼 비활성화 + 라벨 교체(예: "Applying…")로 진행 상태 표시.
  - 경고/파괴적 큐는 orange 대신 디자인 시스템의 amber 사용(Stop CDC 버튼, 점수 게이지,
    노력/충돌 배지).
  - 렌더링되지 않던 죽은 `_format_complete_cell` 헬퍼 제거.

## v0.1.79

### 수정 (Fixed)

- **앱 셸: 단계 렌더링 크래시가 이제 파란 info가 아닌 빨간 error 알림으로 표시됨.**
  최상위 "단계를 표시할 수 없음" 폴백이 `render_notice(tone="negative")`를 호출했는데,
  `negative`는 정의된 알림 톤이 아니어서 조용히 차분한 파란 `info` 스타일로 폴백했다 —
  실제로는 앱에서 가장 경보해야 할 상태(처리되지 않은 렌더링 예외)인데도. 이제 심각도에 맞게
  `tone="error"`(빨강)를 사용한다.

### 정리 (Housekeeping)

- 오픈소스 공개 위생: 내부 세션 핸드오프 노트와 발표 자료의 내부 작성자/리포 식별자를 제거하고,
  커넥터 소스·CloudFormation 템플릿·CDC read-model에서 내부(비공개) 설계/스펙 문서에 대한
  깨진 인용을 인라인 요약으로 대체했다. `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`,
  `SECURITY.md`를 추가했다.

## v0.1.78

### 수정 (Fixed)

- **스키마 변환: `DOUBLE(M,D)`가 이제 유효한 DSQL DDL을 생성함.** MySQL `DOUBLE(M,D)`
  컬럼(예: `DOUBLE(10,2)`)이 타입 매퍼를 그대로 통과했다(sqlglot이 이를 `DOUBLE` 종류로
  파싱하는데 `UDOUBLE`/`FLOAT` 특수 케이스 모두 이를 놓침). 그 결과 두 인자를 가진
  `FLOAT(10, 2)`로 렌더링되었다. PostgreSQL/DSQL의 `double precision`은 인자를 받지
  않으므로 이는 문법 오류였고, 적용 시점에 **테이블 전체**의 `CREATE TABLE`을 실패시켰다.
  이제 `DOUBLE(M,D)`는 (표시용 `(M,D)` 스펙은 저장 의미가 없으므로) 기존 `FLOAT(M,D) -> real`
  처리와 동일하게 순수 `double precision`으로 매핑된다.
- **검증: 큰 `BIGINT UNSIGNED` / `DECIMAL` 값이 더 이상 잘못된 체크섬 불일치를 만들지 않음.**
  PostgreSQL 측 `to_char` 숫자 마스크가 정수 자리를 18개만 제공했지만, MySQL 측은
  `CAST(... AS DECIMAL(65, scale))`로 렌더링하고 `BIGINT UNSIGNED`는 `numeric(20, 0)`으로
  저장된다. 약 10^18 이상의 정수 크기(예: `18446744073709551615`)는 마스크를 넘쳐
  `to_char`가 자릿수 대신 오버플로 표시(`####...`)를 출력했고, 바이트 단위로 동일한 값이
  **체크섬 불일치**로 보고되어 컷오버를 잘못 차단할 수 있었다. 이제 마스크는
  `DECIMAL(65,0)`의 전체 65자리 정수 범위를 커버한다.

## v0.1.77

### 수정 (Fixed)

- **CDC가 소스 재부팅을 수동 개입 없이 견딤.** 소스 RDS/Aurora 인스턴스가 재부팅되면(유지보수 패치,
  페일오버, 인스턴스 클래스 변경) Debezium 소스 커넥터가 retriable binlog 에러를 만나 1회 재시작하고,
  그 재시작이 "Error reading MySQL variables: Communications link failure"(소스가 아직 부팅 중)로
  실패하는데, `errors.retry.timeout`이 기본값 `0`(재시도 안 함)이라 Kafka Connect가 **task를 영구
  종료**("will not recover until manually restarted")시켰습니다 — 조용한 스톨(`SourceRecordWriteRate=0`),
  Stop/Start로만 복구 가능. 이제 소스 커넥터도 sink처럼 `errors.retry.timeout=600000`(10분) +
  `errors.retry.delay.max.ms=60000`을 설정해, 재부팅 구간 내내 재시도하다가 소스가 돌아오면 커밋된
  binlog offset부터 gapless로 재개합니다(사람 개입 불필요). 2026-07-08 소스 2→8 vCPU 스케일업 재부팅에서
  관측·수정.

### 변경 (Changed)

- **CDC: sink MCU를 source와 분리해 별도 지정 (`SinkMcuCount`).** 행당 왕복이 제거된 뒤 sink가 CPU
  바운드가 됨(플러그인 v16: 4 MCU에서 CPU ~80% / ~21,000 rows/s)에 반해 단일 task인 source는 CPU 여유가
  있습니다. 새 `SinkMcuCount` CFn 파라미터(기본 4)로 sink를 독립적으로 스케일; `ConnectorMcuCount`는 이제
  source에만 적용. 측정: sink 4→8 MCU로 처리량 ~21,000 → ~26,200 rows/s, CPU 80% → ~34%.

## v0.1.76

### 변경 (Changed)

- **CDC 싱크: 파라미터 메타데이터를 statement당 1회만 조회 (플러그인 `v16`) — 싱크 처리량 ~9.7배.**
  `bind()`가 모든 change event마다 `getParameterMetaData()`를 호출했는데, pgjdbc에서 이는 서버측
  Parse/Describe 왕복이라 싱크가 **적용 행마다 읽기 전용 트랜잭션 1개**를 발생시키고 있었습니다 —
  DSQL `ReadOnlyTransactions` 메트릭이 ~115,000/분(쓰기 속도의 약 60배)인 반면 `OccConflicts`는
  줄곧 0인 것으로 확인. 즉 진짜 천장은 서버측 쓰기 경합이 아니라 이 숨은 왕복이었고, v13/v15의 배치
  이득 대부분을 상쇄하고 있었습니다. 메타데이터는 같은 SQL이면 모든 행에 동일하므로 이제 prepared
  statement당 1회만 조회해 `bind()`에 전달합니다. 측정 DSQL 적용 속도가 ~1,925 → **~18,672 rows/s**
  (8 파티션/태스크)로 상승; 읽기 전용 트랜잭션 ~150배 감소, 싱크 CPU 10% → ~65%. 싱크 JAR 변경만
  (`PLUGIN_VERSION` → `v16`).

## v0.1.75

### 변경 (Changed)

- **CDC 싱크: multi-row INSERT 재작성 (플러그인 `v15`) — 싱크 처리량 +30%.** 싱크의 JDBC URL이
  이제 pgjdbc `reWriteBatchedInserts=true`를 활성화하여, 단일행 `INSERT` 묶음을 하나의 multi-row
  `INSERT ... VALUES (..),(..) ON CONFLICT ..` 문장으로 접습니다 — N번의 execute 왕복이 1번으로.
  DSQL은 지연 바운드라 이로써 측정 싱크 처리량이 ~1,500 → ~1,925 rows/s(8 파티션/태스크)로
  올랐고, DSQL 적용 속도로 교차검증했습니다. 재작성을 안전하게 하기 위해 `applyChunkBatched`가 각
  동일 SQL run을 PK별 마지막 행으로 먼저 dedup합니다(last-write-wins — 멱등, 순서 보존). 이것이
  없으면 재작성된 multi-row `ON CONFLICT`가 중복 충돌 키를 거부합니다("cannot affect row a second
  time"). 싱크 JAR 변경만(`PLUGIN_VERSION` → `v15`).

## v0.1.74

### 변경 (Changed)

- **CDC 커넥터 스케일링을 하드코딩 대신 추론.** 툴이 테이블별 토픽 파티션 수, 싱크 `tasks.max`,
  MSK Connect MCU 수를 캡처 대상 테이블 수로부터 계산(`compute_cdc_scaling_defaults`)해 cdc-stack
  생성 시 전달합니다. 총 싱크 병렬수(`파티션 × 테이블`)가 상한 8에 도달하는 가장 작은 토픽당
  파티션 수를 고릅니다 — 예: 테이블 1개 → 파티션 8, 4개 → 각 2, ≥8개 → 각 1 — 싱크가 DSQL 쓰기
  지연 바운드라 그 지점을 넘으면 sublinear하게만 증가하기 때문입니다. 파티션 수는 비가역적(토픽
  파티션은 늘리기만 가능)이라 생성 시점에 설정합니다. 이전에는 `topic.creation.default.partitions`가
  `4`로 하드코딩돼 있었으나 이제 `TopicDefaultPartitions` CFn 파라미터입니다. 고급 사용자는
  `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS`, `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX`,
  `DSQL_MIGRATOR_CDC_MCU_COUNT`로 추론을 재정의할 수 있습니다. 매뉴얼 §7.2(CDC)에 모델을 문서화.

## v0.1.73

### 변경 (Changed)

- **CDC 소스 처리량 튜닝 (플러그인 `v14`).** v0.1.72 싱크 배치 이후 병목이 Debezium
  소스로 이동했습니다(~2,000 rec/s, CPU ~12% — binlog 파싱이 아니라 produce/큐 바운드).
  소스 파이프라인 노브를 CFn 파라미터로 노출하여 재배포만으로 넓힐 수 있게 했습니다:
  `SourceMaxBatchSize`(8192)와 `SourceMaxQueueSize`(32768)는 스트리밍 반복당 더 많은 binlog
  이벤트를 배출하고, `SourceProducerBatchSize`(256 KiB) · `SourceProducerLingerMs`(20) ·
  `SourceProducerCompression`(`lz4`)는 Kafka produce 배치를 키우고 압축합니다. producer
  노브는 **소스 워커 설정**에 `producer.*`로 지정합니다 — MSK Connect가 커넥터별 `.override.`
  키를 거부하기 때문입니다 — 따라서 불변 워커 설정은 `PLUGIN_VERSION`을 `v14`로 올려 이름을
  교체합니다(커넥터 JAR 변경 없음).

## v0.1.72

### 변경 (Changed)

- **CDC 싱크 처리량: 배치 적용 (플러그인 `v13`).** DSQL 싱크 커넥터가 이제 동일한 SQL로
  렌더링되는 *연속된* 변경 이벤트의 최대 구간을 행별 `executeUpdate()` 대신 하나의 JDBC
  `executeBatch()`로 묶어서 적용합니다. DSQL은 지연(latency) 바운드입니다 — 각 문(statement)이
  분산 왕복이며, 싱크 태스크는 CPU ~5% / ~550 rec/s로 관측되었습니다(연산이 아니라 왕복에
  묶임). 행별 왕복을 배치 전송으로 접는 것이 처리량의 핵심 지렛대입니다. **적용 순서는
  보존됩니다**: 연속된 동일 SQL 이벤트만 묶이므로 같은 PK에 대한 upsert 뒤의 delete도 도착
  순서대로 적용되고, 테이블/컬럼셋/종류가 바뀌면 구간이 끊깁니다. poison-row 격리, OCC 재시도,
  멱등 재적용은 그대로입니다(영구 실패 시 여전히 행별 적용으로 폴백). `PLUGIN_VERSION`을
  `v13`으로 올림.
- **CDC 싱크 `consumer.max.poll.records` 기본값 3000으로 변경.** 새 `SinkMaxPollRecords`
  CFn 파라미터(기본 3000, 싱크 워커 설정에 지정). Kafka 기본값(500)이 한 `put()` 호출에
  도달하는 레코드 수 — 따라서 하나의 ≤3000행 DSQL 트랜잭션에 배치할 수 있는 수 — 를 제한해
  배치 적용이 덜 채워졌습니다. 트랜잭션 한도에 맞추면 한 번의 poll이 하나의 왕복을 가득
  채웁니다.
- **대규모 소스를 위한 CDC 처리량 기본값 상향:** `ConnectorMcuCount` 4, `SinkTasksMax` 4,
  테이블별 `topic.creation.default.partitions` 4로 설정하여 싱크가 데이터 토픽을 4개
  파티션에 걸쳐 병렬 소비할 수 있게 했습니다(유효 싱크 동시성은 파티션 수로 제한). 앱 스택은
  이제 8/16 vCPU 태스크 크기도 허용합니다.

### 수정 (Fixed)

- **CDC: 非-GTID 소스가 file:position 모드로 안정적으로 폴백.** Debezium이 모든 GTID를
  제외(`gtid.source.excludes=.*`)하고 GTID 부재 시 DML을 필터링하지 않도록
  (`gtid.source.filter.dml.events=false`) 설정하여, `gtid_mode=OFF`인 소스(예: GTID를
  켤 수 없는 RDS MySQL)가 0개 레코드 대신 binlog file:position으로 변경을 캡처합니다.

### UI

- **Start CDC 즉시 피드백:** 배포 요청이 진행 중일 때 버튼이 무응답처럼 보이던 것을, 클릭 시
  로딩 상태와 토스트를 표시하도록 변경.
- **중단된 CDC 단계에 FAILED 아이콘 표시:** 작업이 종료된 후 진행 중 스피너에 멈춰 있던 것을
  실패 아이콘으로 표시.

## v0.1.71

### 수정 (Fixed)

- **CDC: `SnapshotMode`가 실제 CloudFormation 템플릿에 전달되도록 수정.** v0.1.70에서
  Python이 올바른 모드를 계산했지만 CFn 템플릿에 `snapshot.mode: recovery`가 하드코딩.
  `SnapshotMode` 파라미터를 추가하고 Start CDC 시 새 템플릿도 함께 전달하여 기존 스택
  에서도 새 파라미터를 인식. "Could not find existing redo log information" 실패의
  실제 원인 수정.

- **CDC: 소스 DB 포트가 세션에서 실제 값을 읽도록 수정.** 기존에는 항상 3306 기본값
  사용하여 비표준 포트에서 connector 타임아웃 실패 유발.

## v0.1.70

### 수정 (Fixed)

- **CDC: 새 connector에 `snapshot.mode=schema_only` 올바르게 적용.** 기존에는
  `recovery`로 하드코딩되어, schema-history 토픽이 없는 상황에서 "Could not find
  existing redo log information" 에러로 실패. 이제 실제 Full Load 워터마크(binlog
  좌표 포함)가 있을 때만 `recovery` 사용; 그 외(수동 시작, 세션 리셋, CDC-only)는
  모두 `schema_only`.

- **CDC: 배포 전 서브넷 NAT egress 사전 검증.** MSK Connect는 프라이빗 IP만 할당하므로
  NAT 없는 서브넷에서는 Secrets Manager 접근 불가. 사용자 입력 및 자동 발견 서브넷 모두
  배포 제출 전 검증. 다른 스택 삭제로 NAT가 사라지는 race condition도 방지.

- **CDC: deploy/start 진행 중 prerequisites 버튼 비활성화.** CDC 스택 작업 진행 중에도
  Check 버튼이 잠기도록 개선.

## v0.1.69

### 추가 (Added)

- **CDC: Manual 모드에서 "Fetch current position" 버튼 추가.** Full Load 워터마크가
  없는 CDC-only 플로우에서 수동으로 GTID/binlog 위치를 입력해야 할 때, 소스 MySQL에
  `SHOW MASTER STATUS`를 실행해 현재 위치를 자동으로 채워주는 버튼 추가. 사용자가 직접
  소스에서 SQL을 실행하고 복사-붙여넣기할 필요 없음.

## v0.1.68

### 변경 (Changed)

- **Full Load: 멀티프로세스 병렬화 (GIL 우회).** 테이블을 `ProcessPoolExecutor`로
  별도 OS 프로세스에서 로드하여 각 테이블(또는 shard)이 자체 GIL + CPU 코어를 사용.
  단일 정수 PK를 가진 대형 테이블은 자동으로 PK range shard 분할. ECS Fargate 8 vCPU
  측정 결과:
  - 4 테이블 혼합 (tp=8): **34,800 rows/s**, CPU 561% (기존 12,277, CPU 110%)
  - 단일 33.6M행 테이블 shard (tp=8): **51,000 rows/s**, CPU 777%
  - 200GB 테이블 예상: **~2.5시간** (기존 ~46시간, **18× 단축**)
  - 하위 호환: test double은 자동으로 ThreadPool fallback 사용.

## v0.1.67

### 변경 (Changed)

- **Full Load 단일 테이블 처리량 최적화 (GIL-aware).** GIL hold 시간과 네트워크
  round-trip을 줄이는 5가지 변경을 복합 적용:
  1. MySQL keyset page size 1,000 → 5,000행 — 소스 round-trip 5배 감소.
  2. `build_insert_statement` SQL 템플릿 배치 shape별 캐싱 — 배치당 ~40,000
     객체 할당 제거 (대형 테이블에서 99.99% 캐시 적중).
  3. `_iter_batches` 바이트 추정 lazy화 — 배치 첫 행만 샘플링 후 8 MiB 예산
     근처에서만 행별 확인, `_estimate_row_bytes` 호출 90%+ 제거.
  4. `_flatten_params`를 리스트 컴프리헨션으로 전환 (CPython에서 ~40% 빠름).
  5. `convert_row` passthrough fast path — 타입 변환 불필요한 컬럼(int, varchar,
     numeric, text)은 `convert_value`를 건너뜀.

### 수정 (Fixed)

- **"Retry unfinished tables" 버튼 즉시 피드백 제공.** target probe 중
  "Checking target…" 표시 + hourglass 아이콘 + disabled. 재시도 시작 시 toast.
- **Schema Conversion 개별 "Apply to target"이 기존 테이블 감지 후 Replace/Skip
  다이얼로그 표시** (이전: existence checker 미연결로 silent SKIP).
- **"Keep integer PK" → "Keep source PK"** 레이블 수정.
- **"Apply converted to target" → "Apply all to target"** 레이블 수정.

## v0.1.66

### 변경 (Changed)

- **Migration overview 다이어그램을 하나의 통합 패널로 재설계.** 기존 세 개의 개별
  bordered 카드(Source / Migration Tool / Aurora DSQL)를 하나의 공유 surface 안에
  borderless column segment로 통합. 상태 표시는 bordered chip badge 대신 경량
  dot + text 패턴(Cloudscape "StatusIndicator")을 사용하고, flow connector는
  dashed 화살표 + plain text caption으로 단순화. 전체 chrome을 줄이면서 모든 정보
  (endpoint, engine, region, connection state)는 그대로 유지. Design system
  (`ui/design.py`)에 재사용 가능한 `render_status_dot` 컴포넌트 추가.

## v0.1.65

### 변경 (Changed)

- **이미 존재하는 단일 객체를 적용할 때, 조용히 건너뛰지 않고 어떻게 처리할지 묻습니다.**
  Schema Conversion(2단계)에서, 타깃에 이미 존재하는 테이블(그리고 DDL을 편집하지 않은
  경우)에 대해 객체별 **Apply to target**을 누르면 이전에는 그냥 `SKIPPED`로 보고하고
  타깃을 그대로 두었습니다 — 알아채기 어렵고, 그 버튼에서 결정을 바꿀 방법도 없었습니다.
  이제 **Replace / Skip / Cancel** 다이얼로그를 띄워 적용 시점에 명시적으로 선택하게 합니다.
  이는 선택을 **되돌릴 때** 특히 중요합니다 — 예: 복합 키로 바꿨던 테이블을 다시 정수 키로
  되돌리는 경우, SKIP이면 기존 복합 키 테이블이 그대로 남지만 Replace는 새 DDL로 드롭 후
  재생성합니다. (객체의 DDL을 편집했거나 전역 REPLACE 모드인 경우는 기존의 파괴적 교체
  확인 절차를 그대로 탑니다.)
- **객체별 Apply to target 버튼이 실행 중임을 표시합니다.** 적용이 진행되는 동안(잠시
  걸릴 수 있는 타깃 왕복, 또는 확인 다이얼로그 대기) 버튼이 비활성화된 로딩 스피너 상태로
  바뀌고, 적용이 끝나면 원래대로 돌아옵니다 — 느린 적용이 더 이상 먹통 클릭처럼 보이지
  않습니다. 적용이 실패해도 이 상태는 항상 해제됩니다.

## v0.1.64

### 추가 (Added)

- **테이블별 선택형 복합 기본 키(쓰기 핫 파티션 해소).** Aurora DSQL은 기본 키
  순서로 행을 저장하므로, 단조 증가하는 `AUTO_INCREMENT` 키는 모든 삽입을 하나의
  파티션으로 몰아넣어(쓰기 핫 파티션) 처리량 상한을 만듭니다. 이제 Schema
  Conversion(2단계)에서 테이블별 **기본 키** 선택기를 제공합니다: 정수 키 유지(기본,
  변경 없음) 또는 사용자가 고른 고카디널리티 컬럼을 앞에 붙인 **복합 키**(예:
  `(customer_id, id)`)로 전환하여 쓰기를 여러 파티션으로 분산합니다. 소스 MySQL
  스키마는 절대 변경하지 않으며, DSQL 타깃 키만 바뀝니다. 선두 컬럼 후보로는 NOT
  NULL이면서 기존 키에 없는 컬럼만 제시하고, 결과를 DSQL 키 제한(≤ 8개 컬럼, ≤ 1
  KiB)에 대해 검증하며, 원래 키의 유일성을 보존하도록 `CREATE UNIQUE INDEX ASYNC`를
  생성합니다. 선택 시점에 결과를 명시적으로 안내합니다: 전환(cutover) 이후에는
  애플리케이션의 쿼리·조인·업서트가 새 복합 키를 사용해야 하며, 선두 컬럼은 불변이어야
  합니다.
  - **Full Load**는 복합 키 테이블을 올바르게 적재합니다: 멱등 `INSERT ... ON
    CONFLICT`가 이제 **타깃** 기본 키를 기준으로 동작하므로(이전에는 항상 소스 키
    사용), 키가 달라져도 타깃 제약과 불일치하지 않습니다. 타깃 키가 다른 기존
    테이블에 이어붙이기(append)는 명확한 메시지와 함께 거부됩니다(새 키를 적용하도록
    먼저 새로 적재).
  - **CDC**는 커넥터/플러그인 변경 없이 복합 키 테이블을 복제합니다: Debezium
    소스를 `message.key.columns`로 재키잉하여 각 변경 레코드의 키가 타깃 복합 키와
    일치하게 하고, 싱크의 멱등 업서트/삭제가 그대로 적용됩니다. 복합 키 컬럼을 LOB
    제외 대상으로도 선택한 경우에만(키 생성을 위해 반드시 캡처되어야 함) CDC 시작을
    실행 가능한 안내와 함께 막습니다.

## v0.1.63

### 변경 (Changed)

- **Full Load가 큰 테이블을 여러 리더로 동시에 읽을 수 있습니다(reader range
  sharding).** 단일 keyset 리더는 CPU-bound(행마다 타입 변환)이라 한 코어 근처에서
  한계에 이르므로, 큰 테이블의 읽기를 K개의 disjoint 기본 키 범위로 나눠 동시에
  스트리밍하고 모두 하나의 쓰기 풀에 먹입니다. 기본 꺼짐
  (`DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS=1`). 단일 **정수** PK이면서 추정 행수가
  `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS`(기본 1,000,000) 이상인 테이블에만 적용 —
  복합/비정수 PK와 더 작은 테이블은 항상 단일 리더. 총 소스 리더 수
  (`table_parallelism × shards`)가 안전 상한을 넘지 않도록 제한됩니다. 단일 일관
  스냅샷을 유지해야 하는 clean replace 로드(plain INSERT, CDC 없음)에는 샤딩을
  적용하지 않고, watermark + 멱등 재적재로 샤드별 스냅샷 시점 차이가 안전한 기존
  데이터/CDC 경로에만 적용합니다. 재개·OCC 처리·쓰기 측 동작에는 변화 없음.

## v0.1.62

### 변경 (Changed)

- **Full Load가 쓰기 풀보다 앞서 읽습니다(bounded prefetch 큐).** 소스 리더가
  전용 백그라운드 스레드에서 bounded 큐를 채워, page N+1 읽기가 page N 쓰기와
  겹칩니다(기존에는 읽기·쓰기가 직렬). 큐 상한이 쓰기 병렬도의 ~2배로 잡혀 메모리는
  여전히 bounded이고, 적재 순서는 그대로(배치는 여전히 고정 PK 범위에 매핑),
  중단/취소 시 리더 스레드를 join하므로 스레드 누수도 없습니다. 기본 켜짐이며,
  측정용 seam(`DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0`)으로 끄면 이전 경로를 그대로
  재현해 A/B 벤치마크를 할 수 있습니다. 적재 정확성·재개 동작에는 변화 없음.

## v0.1.61

### 변경 (Changed)

- **Full Load 진행 테이블 간소화.** 9열 → 6열로 줄여 한눈에 읽히고 줄바꿈이 사라졌습니다:
  "Rows on target"와 "Source rows"를 **Rows (target / source)** 한 열로 병합하고 큰
  숫자는 축약(`1.18M / 33.6M`), 정확한 수치와 new/already-there 분해는 hover 툴팁으로
  옮겼습니다. **Errors** 열은 **Attempts**에 합쳤고(예: `5 · 1 err`), 중복이던
  **Complete** 열은 제거했습니다(Status + Progress가 이미 표시). **Time** 헤더도 더 이상
  줄바꿈되지 않습니다. 작은 숫자는 천단위 쉼표로 전체 표시됩니다.

## v0.1.60

### 변경 (Changed)

- **Full Load 실행 중에는 사전 조건 검사를 다시 실행할 수 없음.** 이전에는 로드 도중
  Prerequisites 단계로 돌아가 "Check"를 누를 수 있었습니다. 검사는 읽기 전용이고 실행
  중인 job을 건드리지 않아(새 결과는 *다음* 실행에만 적용) 무해했지만, 무의미하고
  혼란스러웠습니다 — 새로 실패한 검사가 로드가 도는 중에 빨간 "차단" 판정을 띄웠고,
  소스에 불필요한 읽기 부하를 더했습니다. 이제 Full Load가 IN_PROGRESS인 동안 Check
  버튼이 비활성화되며, 검사가 다음 실행에 적용되고 실행 중 로드에는 영향을 주지 않는다는
  짧은 안내가 함께 표시됩니다 — migration-type 셀렉터가 이미 실행 중 잠기는 것과 동일한
  방식입니다. 검사를 다시 하려면 로드를 멈추세요.

## v0.1.59

### 변경 (Changed)

- **Full Load "Failure details"를 더 깔끔하게, 긴 에러에도 버튼이 밀리지 않도록 개선.**
  각 실패 행의 per-row "Reload" 버튼을 제거했습니다 — 재시도는 이제 아래의 단일
  "Retry unfinished tables" 체크리스트로만 제어되므로, 두 개의 경쟁하는 컨트롤 대신
  일관된 하나의 방법이 됩니다. 각 실패 행은 안정적인 레이아웃(왼쪽에 테이블 이름 +
  줄바꿈되는 에러 메시지, 오른쪽에 "AI Assist" 액션 고정)이라, 긴 에러 메시지가 버튼을
  두 번째 줄로 밀거나 행마다 정렬이 어긋나던 문제가 사라졌습니다. 격리(quarantine) 행은
  자체 "Reload"를 유지합니다(격리된 테이블은 "미완료"가 아니라 DONE이라 재시도 체크리스트가
  다루지 않음).

## v0.1.58

### 수정 (Fixed)

- **로드 실행 중 Full Load 진행 테이블이 더 이상 1페이지로 되돌아가지 않음.** 테이블별
  진행 테이블은 ~1.5초마다 새로고침되는데, 매번 처음부터 다시 만들어져서 2페이지 이상으로
  넘겨도 다음 tick에 1페이지로 돌아갔습니다. 이제 선택한 페이지가 새로고침 사이에
  유지됩니다(그리고 테이블이 줄어들어 빈 페이지에 갇히지 않도록 범위를 보정).

## v0.1.57

### 수정 (Fixed)

- **실패한 실행이 PENDING으로 남긴 테이블을 이제 재시도할 수 있음(이전엔 갇힘).**
  Full Load가 일부 테이블을 시도하기도 전에 실패로 끝나면 그 테이블들이 `FAILED`가
  아니라 `PENDING`으로 남았는데, 복구 UI가 `FAILED` 청크만 보고 있어서 "Retry" 동작이
  안 뜨고 유일한 탈출구가 전체 "Re-run Full Load"뿐이었습니다. 이제 복구가 **미완료**
  테이블 전체(FAILED 또는 PENDING)를 대상으로 합니다: 종료된 실행에 미완료 테이블이
  있으면 재시도 행이 나타나고, 버튼은 "Retry unfinished tables (N)"으로 표시되며,
  체크리스트가 각 테이블을 이유와 함께 보여줍니다(에러 메시지, 또는 PENDING 테이블은
  "Not loaded yet — the previous run ended first."). 이미 로드된(DONE) 테이블은 그대로
  유지되고 불필요하게 다시 실행되지 않습니다. (v0.1.56 크래시의 복구 경로입니다 —
  업데이트 후 "Retry unfinished tables"를 눌러 PENDING 테이블을 이어서 로드하세요.)

## v0.1.56

### 수정 (Fixed)

- **"Drop & reload"가 `SchemaApplier` TypeError로 전체 실행을 중단시키던 문제 수정.**
  의존 뷰를 드롭/재생성해야 하는 테이블에 Drop & reload를 선택하면
  `TypeError: SchemaApplier.__init__() missing 1 required positional argument:
  'introspector'`가 발생해 Full Load 전체가 중단되고 테이블별 진행 상황 화면이 사라진
  채 "Migration failed"만 표시됐습니다. 이제 의존 뷰 pre-drop/recreate가
  `SchemaApplier`를 잘못 생성하지 않고 introspector가 필요 없는 DDL 헬퍼
  (`drop_object` / `recreate_table`)를 사용해 클린 리로드가 성공합니다. 또한 선택적
  뷰 pre-drop/recreate 단계는 이제 방어적으로 처리되어, 그곳에서 예기치 않은 실패가
  나도 로그만 남기고 건너뛰며 실행을 실패시키지 않습니다 — 그래서 뷰 처리 문제가 다시는
  Full Load 진행 상황을 날리지 않습니다(정말 드롭이 안 되는 테이블은 여전히 일반적인
  테이블별 실패로 표시되어 대응 가능).
- **CDC가 라이브이고 실행 가능한 상태일 때 Full Load 단계가 오류 나던 문제 수정.**
  CDC가 스트리밍 중이고 Start/Re-run 버튼이 활성인 특정 경우에 Full Load 단계 렌더가
  깨지던 `NameError`(오래된 `cdc_live` 참조)를 수정했습니다.

## v0.1.55

### 변경 (Changed)

- **"Retry failed tables"에서 재시도할 실패 테이블을 선택할 수 있습니다.** 재시도
  대화상자가 각 실패 테이블을 체크박스(전부 미리 체크됨)와 실패 이유와 함께 목록으로
  보여줘서, 아직 재시도할 준비가 안 된 테이블(예: 아직 안 고친 소스 값, 미해결
  의존성)은 체크를 풀고 나머지만 재시도할 수 있습니다. 확인하면 체크된 부분집합만
  재시도하고 — "이미 데이터 있음" 읽기 전용 프로브와 Append/Drop 선택도 그 부분집합에
  맞춰집니다. 체크된 게 없으면 확인 버튼이 비활성됩니다. 전부 재시도(가장 흔한 경우)는
  그대로: 전부 체크된 채 확인하면 됩니다. 테이블별 "Reload" 단축 동작도 그대로입니다.

## v0.1.54

### 변경 (Changed)

- **"Retry failed tables"와 테이블별 "Reload"도 이제 Start와 동일한 Drop vs Append
  선택을 제공합니다.** 이전에는 초기 Start Full Load에서만 선택했고 retry는 그 선택을
  조용히 재사용해서, append로 실패한 뒤 깨끗이 다시 로드하려면 전체 Start over밖에
  없었습니다. 이제 retry와 Reload도 동일한 읽기 전용 프로브(재시도할 테이블만 대상)를
  돌리고 동일한 확인 대화상자를 열어, retry 시점에도 **Append** 또는 **Drop & reload**를
  고를 수 있습니다.
- **Drop & reload 선택지가 "편집한 스키마가 반영됨"을 명시합니다.** 대화상자에
  drop & reload는 각 테이블을 **적용된 Schema Conversion(변환 화면에서 편집한 내용
  포함)** 으로 재생성하고 로드 후 보조 인덱스를 다시 만든다는 안내를 추가했습니다.
  스키마 변경이 클린 리로드에 반영된다는 점이 명확해집니다(원래도 반영됐고, 이제
  눈에 보이게 했습니다).

## v0.1.53

### 수정 (Fixed)

- **Full Load에서 기존 데이터가 있는 테이블에 대해 Drop vs Append를 선택할 수 있고,
  retry도 그 선택을 유지합니다.** 이전에는 선택한 타깃 테이블에 이미 데이터가 있으면
  도구가 임의로 결정했고(첫 실행 시 DROP+recreate), **retry는 조용히 append로
  되돌아가** stale 데이터 위에서 "0 new + N already there"로 보고했습니다 — 실패한
  로드가 실제로는 아무것도 갱신하지 않았는데도 깨끗하게 끝난 것처럼 보였습니다. 이제
  Start Full Load 대화상자가 런당 한 번 묻습니다: **Append**(기존 행 유지, 없는 행만
  로드 — 멱등, 기본값) 또는 **Drop & reload**(각 테이블을 먼저 DROP+recreate해 깨끗이
  로드). 이 선택은 저장되어 **retry와 테이블별 Reload가 동일하게 동작**합니다.
- **뷰가 테이블에 의존해도 "Drop & reload"가 더 이상 실패하지 않습니다.** 의존 뷰
  (예: `customer_order_summary`)가 `DROP TABLE`을 `DependentObjectsStillExist`로 막아
  옛 행이 그대로 남던 문제를, 이제 drop 경로가 의존 뷰를 먼저 드롭하고(뷰가 병렬 로드되는
  여러 테이블에 걸칠 수 있으므로 런 레벨 사전 처리) **로드 후 다시 생성**합니다. 그래서
  깨끗한 리로드가 성공하고 뷰도 보존됩니다 — 무딘 `DROP … CASCADE` 없이. CDC 스트리밍
  중에는 억제됩니다(DROP이 라이브 sink와 충돌).

## v0.1.52

### 추가 (Added)

- **Full Load 실패 테이블마다 AI Assist.** Full Load "Failure details" 목록의 각
  테이블에 "Reload" 옆으로 "AI Assist" 버튼이 생겼습니다. 누르면 AI 채팅 드로어가
  열려 해당 실패의 원인과 해결법을 설명합니다 — 실제 에러 텍스트(예: 의존 뷰
  때문에 발생한 `DependentObjectsStillExist` 드롭 충돌, 또는 일시적
  `InternalError_: server unavailable`)뿐 아니라 **이 마이그레이션의 상황**까지
  이해합니다: 마이그레이션 유형(Full Load 전용 vs Full Load + CDC), 해당 테이블이
  기존 타깃을 DROP+재생성 중이었는지, CDC가 이미 스트리밍 중인지. 그래서 일반적인
  답이 아니라 이 마이그레이션에 특화된 안내를 주고, 올바른 복구 방법(스키마 의존성
  수정, 소스 값 수정, 또는 일시적 오류면 그냥 Reload)을 가리킵니다. 옵트인 —
  Connect에서 AI Assist를 켰을 때만 활성화되고, 꺼져 있으면 그쪽을 안내하는
  비활성 버튼이 보입니다. 기존 채팅 드로어/Bedrock 스택을 재사용합니다(새 자격
  증명 경로 없음).

## v0.1.51

### 수정 (Fixed)

- **재접속한 세션에서 "Check"를 눌러도 Prerequisites 섹션이 접히지 않음.** 앱
  재시작 후에는 사전 조건 검사를 (다시) 실행해야 할 수 있습니다. 그런데
  Prerequisites 섹션을 펼치고 Check를 누르면 곧바로 접혔습니다 — 클릭이 재렌더를
  일으키는데, 섹션은 "활성(active)" 하위 단계일 때만 펼쳐진 상태였고, 재접속 후에는
  활성 단계가 더 뒤 단계이기 때문입니다. 이제 이 섹션이 지금 조치가 필요한 상태인
  동안(검사 실행 중이거나 아직 실행을 막고 있을 때)에는 펼쳐진 채로 유지되어,
  실행 중 스피너와 결과가 계속 보입니다.

## v0.1.50

### 변경 (Changed)

- **Schema Conversion의 object browser를 "Tables to migrate"와 동일한 스타일로 통일.**
  2단계의 소스/타깃 브라우저가 이제 3단계 테이블 피커와 같은 모습을 씁니다: 흰
  배경·테두리 스크롤 패널, 연결선 없는(connector-less) 트리. 각 소스 테이블 리프에
  동일한 기본 키 표시가 붙습니다(있으면 초록 체크, 없으면 앰버 경고 — Aurora DSQL은
  기본 키가 필수), 필터 아래에 범례도 있습니다. 뷰/트리거/루틴에는 PK 표시가 붙지
  않습니다(기본 키 개념이 없음). 선택 및 DDL 생성 동작은 그대로입니다.

## v0.1.49

### 수정 (Fixed)

- **"Tables to migrate" 필터가 이제 동작하고, 기본 키 아이콘에 범례를 추가.**
  테이블 트리 위의 이름 필터 입력창이 렌더는 됐지만 트리에 연결되어 있지 않아
  아무 것도 걸러지지 않았습니다. 이제 트리의 필터에 연결되어, 입력하면 일치하는
  테이블 리프만 좁혀 보여줍니다. 또한 헤더 아래에 각 테이블 아이콘을 설명하는 작은
  범례를 추가했습니다: 초록 체크는 기본 키가 있음, 앰버 경고는 기본 키가 없음
  (Aurora DSQL은 기본 키가 필수)을 뜻합니다.

## v0.1.48

### 변경 (Changed)

- **"Tables to migrate" 피커: 다시 스키마 트리로, 모던한 스타일은 유지.** 평면
  데이터 테이블(v0.1.47)을 스키마 → Tables → 리프의 object browser 트리로
  되돌렸습니다. 다만 동일한 AWS/Cloudscape 스타일로 감쌌습니다 — 이름 필터,
  Select all / Unselect all, 그리고 흰 배경·테두리 스크롤 패널 위의 실시간
  "N / M개 선택됨" 카운터. 각 테이블 리프에는 기본 키 표시가 작게 붙습니다(있으면
  초록 체크, 없으면 앰버 경고 — Aurora DSQL은 PK가 필수). 테이블 뷰에 있던 다른
  메타데이터 컬럼(컬럼 수·인덱스·타깃 배지)은 트리를 가볍게 유지하기 위해
  제거했습니다. PK 표시는 클라이언트 측 Quasar 슬롯이라 노드마다 추가 부담이
  없습니다. 선택 동작과 잠금(흐려지고 비활성) 상태는 그대로입니다.

## v0.1.47

### 변경 (Changed)

- **"Tables to migrate"(마이그레이션할 테이블) 피커를 컴팩트한 AWS 콘솔
  (Cloudscape) 데이터 테이블로 개선.** 3단계의 테이블 피커는 스키마 → Tables →
  테이블 리프로 이어지며 각 단계마다 체크박스가 있는 트리였습니다. 이제 체크박스
  열 하나에 테이블당 한 행인 정렬 가능한 데이터 테이블로 바뀌어, 한눈에 더 많은
  정보를 보여줍니다: 스키마, 컬럼 수, 기본 키 유무(있으면 초록 체크, 없으면
  앰버 경고 — DSQL은 PK가 필수), 보조 인덱스 수, 그리고 "exists"/"new" 타깃 상태
  칩. 위에는 이름 필터와 실시간 "N / M개 선택됨" 카운터가 있습니다. 체크박스는
  줄고 정보 밀도는 높아졌지만 선택 동작은 동일합니다 — 선택한 집합이 그대로 Full
  Load / CDC / 사전 조건 검사 대상이 되고, 검사가 실행됐거나 CDC가 라이브면
  피커는 여전히 잠깁니다(흐려지고 비활성).

## v0.1.46

### 변경 (Changed)

- **앱 재시작 후 "사전 조건 다시 실행" 안내 문구 개선.** 데이터 마이그레이션의
  사전 조건(prerequisite)을 이미 통과했지만 아직 Full Load를 시작하지 않은
  상태에서 앱이 재시작되면, 처음 사용자에게 보이는 것과 똑같은 무뚝뚝한 "먼저
  사전 조건 검사를 실행하세요" 문구로 실행이 막혀 마치 진행 상황이 사라진 것처럼
  보였습니다. 검사는 여전히 다시 실행해야 하지만(읽기 전용이고, 재접속 시 소스
  연결이 새로 맺어지므로 이전 결과를 신뢰할 수 없음), 이제 문구가 상황을 정확히
  설명합니다: "재접속됨 — 사전 조건 검사를 다시 실행하면 이어서 진행할 수 있습니다.
  읽기 전용이고 빠릅니다. 진행 상황은 사라지지 않았지만, 검사 결과는 앱 재시작
  간에 보관되지 않습니다." 실제 신규 사용자에게는 기존 문구가 그대로 보입니다.
  (검사를 통과해야만 도달 가능한, 영속화된 활성 하위 단계로 두 경우를 구분합니다.)

## v0.1.45

### 변경 (Changed)

- **성능 튜닝 컨트롤을 콤팩트한 AWS 콘솔(Cloudscape) 폼 스타일로 개선.** 사이드바의
  "Performance tuning" 패널이 이제 숫자 입력 4개를 그냥 나열하지 않습니다. 먼저
  운영 주의사항을 담은 한 줄짜리 info Alert(다음 실행부터 적용, 실시간·앱 전체,
  재시작 시 초기화, 연결 수 ≈ 테이블 수 × 배치 수)를 보여주고, 그 아래에 노브들을
  "Full Load" / "Validation" 섹션 소제목으로 묶어 폼 필드로 배치합니다. 각 노브는
  한 줄로 표시됩니다 — 라벨, 긴 설명을 툴팁으로 담은 info 아이콘, 허용 범위,
  범위가 제한된 숫자 입력 — 좁은 사이드바에서도 패널이 빽빽하지 않게 유지됩니다.
  노브 메타데이터(그룹 / 라벨 / 설명 / 범위)는 모두 `config.py`에 두어 UI와 검증
  메시지가 하나의 기준을 공유합니다. 노브의 동작 자체는 변경되지 않았습니다.

## v0.1.44

### 수정 (Fixed)

- **"Start Full Load"를 두 번 눌러도 확인 팝업이 두 개 뜨지 않음.** 확인 팝업을 열기 전에 ~1~2초
  읽기 전용 프로브(어떤 타깃 테이블에 이미 데이터가 있는지)가 도는데, 빠르게 두 번 누르면 팝업이 두 번
  떴습니다. 이제 프로브가 진행 중이면 두 번째 클릭을 무시하고(재진입 가드), 클릭한 버튼이 비활성화되며
  "Checking…"(모래시계)로 바뀌었다가 팝업이 열리면 복원됩니다. 초기 Start 버튼과 종료 후 Re-run 버튼
  모두에 적용됩니다.

## v0.1.43

### 변경 (Changed)

- **Deploy 로그 타임스탬프에 `UTC` 타임존 표시.** CDC 배포/철거 로그의 각 줄이 `HH:MM:SS UTC - …`로
  표시됩니다(이전에는 타임존 없는 `HH:MM:SS - …`). 모호함이 없어지고, 다운로드하는 활동 로그·
  CloudWatch·CloudFormation 이벤트(모두 UTC)와 일치합니다.

## v0.1.42

### 수정 (Fixed)

- **CDC 스택 이름 필드 정렬 수정.** 고정 `mysql-dsql-cdc-` 접두사를 Quasar `prefix` prop으로 입력창
  **안쪽에**(입력 텍스트와 같은 베이스라인, 금액 앞의 `$`처럼) 렌더하도록 변경했습니다. 이전에는 별도
  왼쪽 라벨로 붙어 입력창 자체 라벨과 정렬이 어긋났습니다. 아래에 완성될 전체 스택 이름을 보여 주는
  한 줄 도움말도 추가했습니다.

## v0.1.41

### 변경 (Changed)

- **CDC 스택 이름 필드를 접미사(suffix) 전용으로 변경 — 커스텀 이름이 조용히 거부되지 않음.** 필수
  `mysql-dsql-cdc-` 접두사를 읽기 전용 고정 애드온으로 보여 주고, 사용자는 접미사만 입력합니다(예:
  `orders` → `mysql-dsql-cdc-orders`). 이전에는 접두사 없는 이름(예: `abcde`)을 넣으면 거부되어
  `mysql-dsql-cdc-stack`으로 되돌아가며 경고가 떴는데 — 접두사는 배포 역할의 IAM 스코프상 필수라
  혼란스러웠습니다. 이제 `abcde`는 그대로 유효한 `mysql-dsql-cdc-abcde`가 되고, 문자 규칙을 위반한
  접미사만 거부됩니다.

## v0.1.40

### 변경 (Changed)

- **"Start over"가 CDC를 프로브하는 동안 "Checking…" 상태를 표시.** Start over를 열면 CDC stop/delete
  타일을 보여줄지 판단하기 위해 ~1~2초 읽기 전용 AWS 프로브가 돌아갑니다. 이제 그동안 버튼이
  비활성화되고 라벨/아이콘이 "Checking…"(모래시계)로 바뀌었다가 다이얼로그가 열리면 복원됩니다 —
  진행 상태를 눈에 보이게 하면서 중복 열림도 막습니다. (앱 관례에 맞춰 라벨/아이콘 교체 방식 —
  flat 버튼에서 테두리 아티팩트를 내는 Quasar `loading` prop은 사용하지 않음.)

## v0.1.39

### 수정 (Fixed)

- **CDC 인프라가 이미 없는데 Start over가 "CDC 과금 계속"을 경고하던 문제 수정.** 라이브 프로브가
  CDC 인프라가 없음을 확인하면(예: 방금 스택 삭제를 마친 경우), Start over 다이얼로그가 더 이상
  "리셋해도 CDC 인프라는 삭제되지 않음 — MSK/NAT 과금 계속" 경고를 표시하지 않습니다. 이미 철거된
  인프라에 대한 오해를 주던 문구였습니다. 프로브가 불확실할 때는(안전 차원의 힌지) 여전히 표시되고,
  CDC가 실제로 배포돼 있으면 당연히 stop/delete 타일 경로가 대신 나옵니다.

## v0.1.38

### 변경 (Changed)

- **CDC 카드가 삭제 중임을 명확히 표시.** cdc-stack이 `DELETE_IN_PROGRESS`인 동안, 파이프라인 카드가
  이전에는 애매하게 "Busy" / "cdc-stack needs cleanup — 현재 작업이 끝나길 기다리세요"로 보였습니다.
  이제 **"Deleting…"** 배지와 함께 안심 알림 — *"CDC 인프라를 삭제하는 중입니다(~15~25분 소요 — VPC
  내부 Lambda의 네트워크 인터페이스 detach에 시간이 걸림); 완료되면 MSK / NAT 과금이 중단됩니다"* — 을
  계속 표시하고, 폴링을 이어가 완료 시 "Not deployed"로 자동 전환됩니다. 정착됐지만 막힌 스택
  (`ROLLBACK_COMPLETE` / `DELETE_FAILED`)은 여전히 "정리 필요 — 삭제 후 재배포" 안내를 보여줍니다.
  (새 순수 헬퍼 `cdc_unstable_message`가 배지와 알림을 한곳에서 결정.)

## v0.1.37

### 수정 (Fixed)

- **"Start over"가 진행 중인 CDC 정리와 더 이상 경쟁하지 않음.** Start over에서 CDC 파이프라인
  stop/delete를 선택하면 CloudFormation 스택이 ~15~25분간 `DELETE_IN_PROGRESS` 상태인데, 그동안
  헤더 "Start over" 버튼이 계속 눌렸고, 리셋이 이미 세션을 지워 버려 두 번째 시도에서는 진행 중인
  정리를 인식하지 못했습니다(혼란스럽고, 커스텀 스택명이면 MSK/NAT 고아 과금 위험). 이제 **CDC
  stop/delete가 실제로 진행 중이면 Start over를 차단**합니다: 다이얼로그가 정리가 진행 중임을 알리고
  Close만 제공(RESET 없음). 감지는 좁게 — 라이브 `*_IN_PROGRESS` 스택 상태 또는 PENDING/RUNNING인
  stop/delete 작업 — 이라, 정착됐지만 막힌 스택(`ROLLBACK_COMPLETE` / `DELETE_FAILED`)은 여전히
  리셋해 정리할 수 있습니다. `run_cdc_delete`의 already-deleting 백스톱은 그대로입니다.

## v0.1.36

### 추가 (Added)

- **UI에서 런타임 성능 튜닝.** 사이드바 푸터에 **Performance tuning** 컨트롤(Diagnostics 옆)을 추가해,
  Full Load / Validation 병렬수 4종(`FULL_LOAD_TABLE_PARALLELISM`, `FULL_LOAD_BATCH_PARALLELISM`,
  `FULL_LOAD_BATCH_ROWS`, `VALIDATE_MAX_WORKERS`)을 **재배포·재시작 없이 실행 사이에** 재튜닝할 수
  있습니다 — 로더와 검증기가 매 실행마다 설정을 다시 읽으므로 여기서 바꾼 값이 다음 실행에 반영됩니다.
  각 필드는 config와 동일한 한도로 제한되고(단일 소스), 앱 전역(단일 태스크)이며 재시작 시 배포/시작
  값으로 리셋됩니다. 영구히 유지할 값은 태스크 정의 `environment`에, 실험은 이 컨트롤로.

## v0.1.35

### 수정 (Fixed)

- **비(非)US 리전(예: 서울 / ap-northeast-2)에서도 AI 어시스트 배포 가능.** `BedrockModelId`
  배포 파라미터가 `us.` 추론 프로파일만 허용했고, 태스크 역할의 `bedrock:InvokeModel` 범위를
  `"us."` 기준으로 분해해 US 멤버 리전(us-east-1/2, us-west-2)에 하드코딩했습니다 — 그래서 US
  밖에서는 AI를 켤 수 없었습니다(비-`us.` ID는 파라미터 검증에서 거부, 파생 IAM 범위도 다른
  지역에는 잘못됨). 이제 파라미터가 `global.` 프로파일(전 리전 이식 가능)도 제공하고,
  foundation-model ID는 `"anthropic."` 기준으로 분해하며(모든 `us.`/`global.`/`apac.` 프로파일
  ID에 존재), foundation-model ARN은 멤버 리전을 열거하는 대신 **리전 무관(`*`, 정확한 모델
  ID)** 으로 범위를 잡습니다. 여전히 최소 권한 — `*`는 리전 필드에만, 모델 ID는 정확히 유지.
- **CDC 배포가 기본적으로 소스 DB에 `0.0.0.0/0` egress를 열지 않음.** CDC 인프라 배포 시 소스
  DB의 보안 그룹을 자동 발견(RDS `DescribeDBInstances`, 읽기 전용)해 커넥터의 소스행 egress를
  그 SG로 범위 제한합니다. 최선 노력 — 비-RDS 호스트나 권한 부재 시 비워 둠(문서화된 폴백).
- **CDC 싱크 로그 정정 + 죽은 인메모리 S3 CSV export 제거.** DLQ 없이 영구 거부된 레코드는
  "logged and skipped"가 아니라 **태스크를 실패**시킨다는(실제 동작) 문구로 정정, 그리고 도달
  불가한 전체-파일-메모리 적재 S3 CSV export 경로를 삭제(실제 경로는 페이지 단위 스트리밍).

### 변경 (Changed)

- **기본 컨테이너 이미지를 `0.1.34`로 상향.** app-stack 기본 `ContainerImageUri`가 아직
  `0.1.31`을 가리켜, 새 배포가 옛 이미지를 실행하던 문제 수정.

### 문서 (Docs)

- **일본어(日本語) 매뉴얼·문서** 추가 및 매뉴얼/README/배포 가이드/체인지로그 전반에 영어/한국어/
  일본어 3중 언어 스위처.
- **한국어 매뉴얼 자연스러움 개선**(문장·용어 통일), 테스트 장 재작성, 성능 장에 실측 예시 추가.
- **아키텍처 다이어그램 PNG**를 README에 임베드(전체 토폴로지는 클릭 확대), 편집용 `.drawio`
  소스는 더 이상 공유하지 않음.
- **배포 가이드**: AWS CLI 예시에 AI 어시스트 인라인 설정(`EnableAiAssist`/`BedrockRegion`/
  `BedrockModelId`), Apache-2.0 `LICENSE` 저작권 기입, 내부 작업 문서 제거.

## v0.1.34

### 추가 (Added)

- **Query Playground의 AI DBA 쿼리 튜닝.** 변환된 `SELECT`가 "Test on target"을 통과하면, 새
  **Tune with AI DBA** 액션으로 우측 AI 채팅 드로어가 열려 해당 쿼리를 Aurora DSQL에 맞게 효율적으로
  재작성합니다. 재작성은 이 쿼리의 **실제 EXPLAIN 플랜과 DPU 비용**, 그리고 Aurora DSQL의 실행 모델(기본
  키가 곧 테이블, 3단계 필터 푸시다운, `Full Scan` vs `Index`/`Index Only Scan`, 비용 단위인 DPU)에
  근거합니다. 무엇을·왜 바꿨고 왜 DSQL에서 더 저렴한지 상세히 설명하며, DSQL에 맞지 않는 일반
  PostgreSQL 튜닝 조언은 명시적으로 배제됩니다. 각 재작성 제안에는 **Test rewrite on target** 액션이
  있어 타깃에서 읽기 전용으로 재실행하고, 개선 전/후 DPU를 같은 채팅에서 AI가 보고합니다. opt-in(AI는
  기본 꺼짐), advisory 전용 — 자동 적용되지 않으며, 개선의 증거는 모델의 설명이 아니라 실측 DPU입니다.

## v0.1.33

### 수정 (Fixed)

- **"Start over"가 어느 단계에서 눌러도 배포된 CDC 파이프라인의 제거 옵션을 제대로 보여줍니다.** 리셋
  다이얼로그는 감지된 CDC 배포 상태를 보고 중지/삭제 선택지를 띄우는데, 그 감지가 CDC 스텝을 열었을 때만
  갱신됐습니다. 그래서 다른 단계에서(또는 재접속 세션에서) Start over를 누르면, 실제로 CDC가 배포돼 있어도
  제거 동작 없이 "리셋해도 CDC 인프라는 삭제되지 않습니다" 경고만 나올 수 있었습니다. 이제 Start over를 열 때
  읽기 전용 AWS 확인을 수행해 실제 배포 상태를 반영합니다.
- **running 상태가 아니어도, 존재하는 CDC 리소스는 모두 제거 대상으로 안내합니다.** 실패했거나 아직
  프로비저닝 중인 커넥터, 멈춰 있거나 롤백된 cdc-스택, 커넥터 없이 인프라만 있는 스택(MSK 클러스터 + NAT)도
  모두 과금되지만 항상 제거 대상으로 안내되지는 않았습니다. 이제 상태(정상 여부)가 아니라 **존재 여부**로
  판단하며, 이는 이미 멈춘/불안정 스택에 Delete를 제공하던 CDC 스텝과 동작을 맞춘 것입니다.
- **커스텀 cdc-스택 이름을 Start over 경고에 명시합니다.** CDC를 커스텀 스택 이름으로 배포했다면(CDC 스텝의
  "Advanced — CDC stack name", 예: 두 번째 병렬 마이그레이션용), 새 세션이 그 이름을 다시 찾지 못합니다(기본
  이름으로 되돌아감). 이제 경고가 정확한 스택 이름을 알려주어, 무엇을 삭제해야 하는지(도구에서 또는 AWS
  콘솔에서) 분명히 알 수 있습니다.
- **작업이 진행 중인 스택에 대해 무리한 삭제 요청을 보내지 않습니다.** CloudFormation 작업이 아직 진행
  중이면 삭제가 그 작업과 충돌해 불투명하게 실패할 수 있었습니다. 이제 진행 중인 작업이 있으면 명확한 "완료 후
  다시 시도" 안내로 멈추고(이미 삭제가 진행 중이면 그대로 완료를 기다림), 안정/실패/롤백 상태의 스택은
  종전처럼 정상적으로 삭제합니다.

## v0.1.32

### 수정 (Fixed)

- **NULL이 포함된 행에서 Validation 체크섬이 잘못 불일치로 나오던 문제 수정.** 행 단위 체크섬은
  NULL을 `'\0'`(NUL) 구분자로 이어 붙였는데, 이 바이트가 두 엔진에서 다르게 표현됩니다 — MySQL에서는 NUL
  한 글자지만, PostgreSQL(`standard_conforming_strings`, DSQL 기본값)에서는 두 글자 문자열 `0x5C30`으로
  나옵니다. 그래서 NULL이 하나라도 든 행은 소스와 타깃의 해시가 달라져, 실제로는 같은데도 차이로
  보고됐습니다. 구분자를 평범한 텍스트 `<NULL>`로 바꿔(PG 텍스트에서 허용되지 않는 NUL도 함께 회피) 같은
  데이터가 두 엔진에서 동일하게 해시되도록 했습니다.
- **binary 및 BIT 컬럼에서 Validation 체크섬이 일치하도록 수정.** MySQL은 `BINARY`/`VARBINARY`/`BLOB`
  (및 공간 타입)을 원시 바이트로 표현하는데 타깃은 16진수를 썼고, `BIT`은 원시 비트 vs 정수로 비교돼 —
  저장된 데이터가 같아도 무조건 엔진 간 불일치가 발생했습니다. 이제 binary 컬럼은 양쪽 모두 소문자
  16진수로 해시하고(MySQL `LOWER(HEX(…))`가 PG `encode(…, 'hex')`와 일치), `BIT(n)`은 정수 값으로
  비교합니다(`CAST(… AS UNSIGNED)` vs `::text`).
- **범위를 벗어난 MySQL `TIME` 값이 타깃 컬럼을 손상시키는 대신 명확히 실패하도록 수정.** MySQL `TIME`은
  `-838:59:59..838:59:59` 범위를 갖지만, DSQL `time` 컬럼은 `00:00:00..23:59:59.999999`만 담을 수
  있습니다. 이 범위를 벗어난 값은 `time`으로 표현할 방법이 없어, 조용히 interval(또는 time이 아닌 텍스트
  셀)로 바인딩되며 컬럼을 손상시켰습니다. 이제 Full Load가 해당 컬럼과 값을 명시한 명확한
  `ValueConversionError`를 발생시키고 해결 방법(Schema Conversion에서 타깃 타입을 `interval`/`text`로
  변경하거나 소스 값 범위를 제한)을 안내합니다 — 기존 `TINYINT(1)` 범위 초과 가드와 동일한 방식으로,
  데이터가 조용히 뭉개지는 일이 없습니다.

## v0.1.31

### 수정 (Fixed)

- **CDC-only 실행 중 Validation 접근 가능(더 이상 "Complete Data Migration first" 아님).** Data
  Migration 스텝은 오직 Full Load 완료로만 DONE에 도달해서, CDC-only 플랜 — 또는 Full Load를 로컬에서
  실행한 적 없는 재접속 세션 — 은 CDC가 실제로 타깃에 복제 중인데도 Validation이 영구 잠겼습니다. 이제
  CDC 스트리밍 중이면 Data Migration 스텝을 다운스트림 게이팅상 DONE으로 취급합니다(순수 함수
  `data_migration_step_after_cdc`; 승격만 하고 터미널 DONE/FAILED는 절대 다운그레이드 안 함).

### 알려진 이슈 (Known issues)

- **재접속한 CDC-only 세션에서 object browser가 여전히 "전부 선택"(잠김)으로 보일 수 있음.** CDC는
  실행 중이지만 이 세션에 Full Load watermark도, 로컬 확정 selection도 없으면(예: Connect부터 새로 시작 후
  재접속) 로컬 상태로 실제 스트리밍 테이블 집합을 알 수 없어, 잠긴 브라우저가 target-existing 기본값으로
  폴백합니다. 완전한 수정은 CDC 상태 discovery 중 배포된 커넥터의 실제 테이블 집합(`describe_connector`)을
  이벤트 루프 밖에서 읽어야 함 — 후속 작업으로 추적. (v0.1.30에서 watermark/selection을 아는 일반 케이스는 이미 수정.)

## v0.1.30

### 수정 (Fixed)

- **Data Migration object browser가 CDC 실행 중 "전부 선택"으로 보이던 문제 수정.** picker가 잠긴
  상태(CDC 스트리밍)에서 재접속하면 "타깃에 있는 전체" 기본값으로 폴백해 모든 테이블이 체크됐고 — 실제 CDC가
  복제 중인 대상을 잘못 표시(게다가 잠겨서 고칠 수도 없음). 이제 잠긴 브라우저는 target-existing 기본값이
  아니라 **실제 스트리밍 대상**(CDC 커넥터의 테이블 집합 = Full Load watermark / 확정 선택)을 반영합니다.
- **CDC 실행 중에는 Schema Conversion "Apply to target"을 차단.** 라이브 CDC 중 스키마 적용은 — 특히
  테이블을 DROP 후 재생성하는 REPLACE — sink가 쓰고 있는 테이블을 손상/절단시켜(Debezium은 DDL을 복제하지
  않음) 데이터 유실/파이프라인 파손 위험이 있습니다. bulk apply와 per-object 인라인 apply 모두 이제 경고와
  함께 중단하고 "CDC를 먼저 중지하라"고 안내합니다. (앱에서 주입된 CDC 상태 프로브로 가드; 없으면 apply는
  영향 없음.)

## v0.1.29

### 추가/변경 (Added / Changed)

- **Schema Conversion: Source/Target DDL 원클릭 복사.** 각 DDL 블록에 클립보드 복사 아이콘을
  추가했습니다 — Source/Target side-by-side diff(각 측 헤더 바)와 편집 불가한 view/trigger/routine
  미리보기("Source DDL" / "Target DDL" 라벨 옆). 복사 성공 시 positive 토스트로 확인하며, 브라우저
  클립보드를 쓸 수 없으면(예: 비HTTPS) "블록에서 선택 복사" 안내로 폴백합니다.

## v0.1.28

### 수정 (Fixed)

- **offset-seeder Lambda의 잔여 ENI로 `DELETE_FAILED`에 걸린 CDC teardown을 자동 복구.**
  offset-seeder는 VPC 안에서 실행돼야 하고(MSK Serverless 부트스트랩이 VPC-private이라 VPC 밖에서는 gapless
  seed 레코드를 produce할 수 없음), VPC Lambda는 AWS 관리형 hyperplane ENI를 남기며 AWS가 이를 비동기로만
  회수합니다(수 분~수십 분). 그 사이 커넥터 서브넷/보안그룹 삭제가 실패해 스택이 `DELETE_FAILED`에
  빠졌습니다 — 이전엔 CLI에서 ENI를 수동 삭제하고 delete-stack을 재실행해야 하는 dead-end였고(이번 세션에
  반복됨) 그동안 MSK/NAT는 계속 과금됐습니다. 이제 `run_cdc_delete`가 `DELETE_FAILED`를 감지해, 실패한
  서브넷/SG를 붙든 *detached(available)* ENI를 삭제하고 delete를 재발행(여전히 막힌 리소스는 retain)해
  teardown을 완료합니다. 아직 회수 중인 in-use ENI는 건드리지 않으며 전 과정 best-effort입니다.
  (offset-seeder ENI known-issue의 현실적 해결책 — Lambda를 VPC 밖으로 옮길 수 없으니, 도구가 teardown을
  스스로 치유하도록 함.)

## v0.1.27

### 수정 (Fixed)

- **CDC 배포가 `UPDATE_ROLLBACK_FAILED`에 빠진 cdc-stack을 자동 복구.** 커넥터
  `UpdateConnector`가 실패하면 커넥터가 RUNNING이 아닌 상태로 남고, CloudFormation의 롤백도 그 리소스에서
  다시 실패("only valid for RUNNING")해 스택이 `UPDATE_ROLLBACK_FAILED`에 고착됩니다 — 이 상태에서는 어떤
  업데이트도 제출할 수 없습니다(이전엔 CLI에서 수동 `continue-update-rollback` 필요). 이제 `discover_stack`이
  이 상태를 감지해 실패 리소스를 스킵하며 롤백을 이어가, 스택을 `UPDATE_ROLLBACK_COMPLETE`로 되돌려 다음
  Start/Retry가 진행됩니다. best-effort: 복구 호출 자체가 실패하면 기존의 "안정 상태 아님" 오류를 표시합니다.

## v0.1.26

### 수정 (Fixed)

- **CDC UI: "테이블 미선택" 가드를 화면에 표면화하고, retry가 Prerequisites로 튕기던 문제 수정.**
  v0.1.25 백엔드 가드에 이어, CDC 스텝이 이제 "테이블을 하나 이상 선택하세요" 안내를 명확히 보여주고(설정
  미리보기가 깨지거나 배포 몇 분 뒤 커넥터 생성에서 실패하는 대신), Start CDC도 job 제출 전에 같은 메시지로
  막습니다. 단, 조기 "인프라 프로비저닝" 배포는 빈 선택을 여전히 허용합니다(`allow_empty=True`) — 커넥터를
  아직 만들지 않으므로; `SinkTopics`는 Start CDC 때 채워집니다.
- **커넥터 배포 후 retry/재렌더 시 CDC 서브스텝이 Prerequisites로 접히던 문제 수정.** active 서브스텝
  resolver에 "cdc"를 영속화하는 곳이 없어, 재렌더(CDC retry, 재접속)마다 full_load/prerequisites로
  폴백해 사용자를 라이브 CDC 화면에서 밀어냈습니다. 플랜에 CDC가 포함되고 커넥터가 존재하면 이제 CDC
  서브스텝을 고정·영속화합니다.

## v0.1.25

### 수정 (Fixed)

- **테이블 미선택 시 CDC start가 깨진 sink를 배포하지 않고 조기 실패하도록 가드 추가.**
  `build_sink_config`가 테이블 목록이 비면 예외를 던집니다: Kafka Connect sink는 비어 있지 않은 topic
  목록이 필수라, 빈 선택은 `SinkTopics=""`를 만들고 MSK Connect가 배포 몇 분 뒤 `POST /connectors`에서
  불투명한 HTTP 400으로 커넥터를 거부했습니다(v0.1.24 참고). 이 가드는 그것을 느리고 비용 드는 배포 이전에
  "테이블을 하나 이상 선택하라"는 명확한 조기 오류로 바꿉니다. (source config는 그대로 — 빈
  `table.include.list`는 유효하며 "전체 테이블"을 뜻함.)

## v0.1.24

### 수정 (Fixed)

- **CDC 커넥터 배포: 커넥터가 실제로 RUNNING에 도달하도록 CdcDeployRole/태스크 롤 IAM을 완성.**
  MSK Connect 커넥터 생성은 여러 권한을 연쇄로 요구하는데, 하나씩 빠져 있어 그때마다 커넥터 CREATE가
  실패(또는 UI가 멈춤)했습니다. 라이브 cdc-stack에 대해 end-to-end로 검증 — Debezium source 커넥터가
  이제 RUNNING에 도달합니다. 추가한 권한:
  - **CdcDeployRole**에 `ec2:CreateNetworkInterface`/`DescribeNetworkInterfaces`/`DeleteNetworkInterface`
    — MSK Connect가 커넥터 ENI를 *호출자(배포 롤)* 자격으로 만듭니다(CloudTrail로 확인:
    `CreateNetworkInterface`가 `kafkaconnect.amazonaws.com`에 의해 호출되나 배포 롤로 authorize).
    커넥터의 ServiceExecutionRole도 MSK Connect service-linked role도 아님. (cdc-stack의
    `ConnectorExecutionRole`에 잘못 넣었던 ENI 권한은 제거 — service execution role엔 불필요.)
  - CdcDeployRole에 CloudWatch Logs *delivery* 액션(`logs:CreateLogDelivery`, `ListLogDeliveries`,
    `PutResourcePolicy` 등) — 커넥터가 CloudWatch 워커 로그 delivery를 켜는데, 이게 없으면 커넥터가
    `InvalidInput.WorkerLogsError`로 FAILED가 되고 워커 로그도 전혀 안 남았습니다.
  - CdcDeployRole에 `kafkaconnect:DescribeConnectorOperation`/`ListConnectorOperations`를 `connector/*`
    **와** `connector-operation/*` 두 ARN 모두로 부여 — UpdateConnector는 비동기이고 그 폴링이 두 ARN 중
    어느 쪽으로든 authorize되어, 둘 다 없으면 CDC 재시도가 롤백됐습니다.
  - **태스크 롤**에 `kafkaconnect:ListConnectors`/`DescribeConnector` — 앱이 커넥터 상태를 폴링해 CDC UI를
    구동(source→sink 전환)하는데, 이게 없으면 AccessDenied가 조용히 삼켜져 실제 RUNNING인 커넥터가
    "creating…"으로 무한 표시됐습니다.
- **DSQL sink 커넥터가 RUNNING 도달 — source→MSK→sink→DSQL 전체 파이프라인을 end-to-end 검증.**
  IAM/인프라가 완성된 뒤 sink가 `POST /connectors` HTTP 400으로 실패했는데, 근본 원인은 **빈 `SinkTopics`**
  파라미터였습니다(Kafka Connect sink는 `topics`/`topics.regex`가 필수라 빈 값은 등록 단계에서 거부).
  `SinkTopics`가 비었던 이유는 2-pass Start가 그 값을 채우지 못했기 때문(아래 UI 알려진 이슈 참고). 이를
  `<TopicPrefix>.<db>.<table>`로 채우니 sink 커넥터가 생성·RUNNING됩니다.

### 알려진 이슈 (Known issues)

- **UI: "Retry CDC"가 배포를 실행하지 않고 화면을 Prerequisites로 리셋**할 수 있고, source→sink 2-pass가
  긴 stack cleanup 이후 재개되지 않으며, 테이블 선택을 건너뛴 Start는 `SinkTopics`/`TableIncludeList`를 빈
  값으로 남깁니다(source는 허용 — 전체 테이블 캡처 — 하지만 sink는 그 뒤 `POST /connectors` HTTP 400 실패).
  후속 UX/가드레일 작업에서: 선택된 테이블이 없으면 CDC start를 막고, 빈 topics 상태를 커넥터 생성 시점이
  아니라 배포 전에 표면화해야 합니다.

## v0.1.23

### 추가/변경 (Added / Changed)

- **"CDC 시작 전" 안내를 더 친화적이고 적절한 타이밍으로 개선.** 이제 Start 버튼 자리에서 어떤 테이블이
  스트리밍될지 바로 보여줍니다(예: "Will stream 3 tables: …") — "선택을 확정하라"를 위로 스크롤하지 않고
  한눈에 확인할 수 있습니다. MSK 용량 주의는 새로 배포한 뒤 첫 시작(happy path)에서는 차분한 info 팁으로
  두고(겁주지 않음), 커넥터가 실제로 존재한 적이 있을 때(이전 start/stop 또는 복원된 실행)에만 warning으로
  승격합니다 — 그때가 커넥터 반복 생성/삭제가 실제로 MSK의 반환되지 않는 용량을 소모하기 시작하는 시점이기
  때문입니다. 문구도 "partition quota … exhaust … force a full teardown" 대신 쉬운 말("MSK의 한정된
  용량이 다시 반환되지 않음")로 바꿨습니다.

## v0.1.22

### 수정 (Fixed)

- **CDC 커넥터 배포가 "Access denied for operation 'AWS::KafkaConnect::Connector'"로 실패하던 문제 수정.**
  `kafkaconnect:CreateConnector`는 리소스 레벨 권한을 지원하지 않는데(생성 시점엔 커넥터 ARN이 없음)
  CdcDeployRole이 `connector/mysql-dsql-cdc-*` ARN으로 좁혀 놔서 DebeziumSourceConnector CREATE가
  거부됐습니다. 이제 (생성 시점 `TagResource`와 함께) `Resource: "*"`로 부여하며 — CreateCustomPlugin /
  CreateWorkerConfiguration과 동일 부류 — 나머지 커넥터 작업은 스코프를 유지합니다.
- **CDC 커넥터 배포가 "not authorized to perform ec2:CreateNetworkInterface"로 실패하던 문제 수정.**
  MSK Connect는 커넥터의 ServiceExecutionRole을 assume해 커넥터 서브넷에 ENI를 배치하는데, 그 롤
  (cdc-stack의 `ConnectorExecutionRole`)에 EC2 네트워크 인터페이스 권한이 없었습니다. MSK Connect의
  `EC2NetworkAccess` 세트(`ec2:CreateNetworkInterface`/`DescribeNetworkInterfaces`/`DeleteNetworkInterface`
  + attach/detach/permission, `Resource: "*"`)를 추가해 커넥터가 ENI를 만들고 정리할 수 있게 했습니다.
  (이 둘은 잠복 이슈였음 — 이전 CDC 실패는 커넥터 CREATE 단계 이전에 멈춰서, 커넥터가 실제로 생성된 적이
  없었기 때문.)

### 추가/변경 (Added / Changed)

- **Full-load-only 실행 완료 후 Full Load 단계가 CDC를 제안.** Full-load-only 마이그레이션은 CDC 단계가
  없어("Continue to CDC" 버튼 없음), 완료 시 실시간 복제 추가 방법을 info 노티스로 안내합니다 — migration
  type을 "CDC only"로 바꾸면 이 Full Load의 watermark부터 이미 적재된 타깃으로 스트리밍(재스냅샷 없음),
  CDC 인프라가 없으면 먼저 배포가 필요하다는 점도 함께.

## v0.1.21

### 추가/변경 (Added / Changed)

- **Migration Plan이 3택 migration-type 타일 대신 "CDC 포함?" 단일 질문으로 변경.** 이 단계의 유일한
  실질 효과는 CDC 스트리밍 인프라(MSK, ~15~20분)를 미리 프로비저닝하느냐이므로, 과한 3택 대신 그것만
  (Yes/No) 묻습니다 — 타입은 Data Migration에서 자유롭게 바꿀 수 있고 Full Load는 항상 실행됩니다.
  No → `FULL_LOAD_ONLY`, Yes → `FULL_LOAD_AND_CDC`이며, Full Load + CDC vs CDC-only 세부 선택은 Data
  Migration 단계에 남습니다(Yes 재선택이 CDC-only 선택을 덮어쓰지 않음). 내부 `migration_type` enum,
  서브스텝, prerequisite, 세션 스냅샷은 무변경입니다.
- **Migration Plan 단계에서는 "Migration type:" 배너를 숨김**(이후 단계에서는 연속성을 위해 계속 표시).
  플랜 단계에서는 바로 아래 "CDC 포함?" 컨트롤이 진실의 원천이라, 2값 결정 위에 3값 배너
  ("Full load + CDC")가 겹쳐 보이는 것이 중복이며 혼란스러웠습니다.

## v0.1.20

### 수정 (Fixed)

- **IPv4-only Fargate 태스크에서 Aurora DSQL 연결이 타임아웃되던 문제 수정.** DSQL 클러스터
  엔드포인트는 dual-stack이라 DNS가 A와 AAAA 레코드를 모두 반환하는데, IPv4-only 서브넷/ENI(IPv6 CIDR
  없음, IPv6 SG egress 없음)의 Fargate 태스크는 IPv6 주소로 가는 경로가 없습니다. glibc가 AAAA를 먼저
  돌려주면 드라이버(psycopg/libpq)가 도달 불가한 IPv6에 `connect_timeout`까지 묶여, IPv4:5432는
  열려 있는데도 UI에는 "Connection failed: connection timeout expired"로 나타났습니다. 이제 컨테이너
  이미지가 모든 아웃바운드 이름 해석에서 IPv4를 우선하도록 하여(`/etc/gai.conf`:
  `precedence ::ffff:0:0/96 100`) `getaddrinfo`가 도달 가능한 IPv4 주소를 먼저 반환해 연결이 성공합니다.
  진짜 dual-stack 태스크에서도 무해합니다(IPv4를 먼저 시도할 뿐).
- **teardown 이후 CDC 소스 시크릿 재프로비저닝이 AccessDenied로 실패하던 문제 수정.** 태스크 롤의
  `provision-cdc-source-secret` 정책에 `secretsmanager:RestoreSecret`이 빠져 있었는데, upsert는 이전
  teardown이 삭제 예약(recovery window)한 동일 이름 시크릿을 먼저 복구한 뒤 새 값을 씁니다. 이제 삭제 이후
  CDC 소스 시크릿 재생성이 성공하며, 액션은 `mysql-dsql-migrator/cdc/*` prefix로만 스코프됩니다.

### 추가/변경 (Added / Changed)

- **배포 가이드 + 스택 세부 정보 폼 정리.** "Specify stack details"가 필수 필드 표와 self-signed 인증서
  1줄 명령으로 시작하도록 개선; 데스크톱 브라우저 접속 조합(`AlbScheme=internet-facing` + 퍼블릭
  `AlbSubnetIds` + `AllowedIngressCidr=<내IP>/32`)을 명시; `HttpsEgressCidr`는 "`0.0.0.0/0` 기본값
  유지"로 문서화(PrivateLink일 때만 좁힘). `ServiceSubnetIds`는 VPC에 프라이빗/NAT 서브넷이 없으면
  ALB 서브넷 재사용 + `AssignPublicIp=ENABLED`가 가능하다고 안내.

## v0.1.19

### 수정 (Fixed)

- **완료된 Validation이 "in progress"로 표시되고 새로고침 시 "not started"가 되던 문제 수정.**
  IN_PROGRESS→DONE 전이는 Validation 화면에서만 도는 폴 타이머가 담당해서, 실행 중 다른 화면(예: Data
  Migration)으로 이동하면 잡이 끝나도 IN_PROGRESS에 고착되고, orphan 상태 reconcile이 완료 리포트를
  "not started"로 버렸습니다. 이제 **실행이 실제로 끝났고(리포트 존재) 라이브 잡이 없으면 DONE으로
  reconcile**하여 리포트를 표시합니다.

### 추가/변경 (Added / Changed)

- **CDC 라이프사이클 + 커넥터 상태전이 activity 로깅.** 컨트롤플레인 액션(deploy/start/stop/delete CDC
  infrastructure)과 커넥터 RUNNING/FAILED 전이를 **이산 마일스톤**으로 activity log에 기록(중복 제거;
  연속 lag/throughput는 로그가 아니라 라이브 패널에 유지).
- **Cut over: "Steps to cut over" 1~6 런북 폰트 확대**(중요한 가이드가 너무 작았음) — cut-over 런북에만 적용.
- **배포 가이드: 완전 teardown 순서.** Teardown 섹션에 전체 해체 순서를 추가 — 비용 큰 **cdc-stack 먼저**
  제거("Start over → Delete all CDC infrastructure" 또는 수동 `delete-stack`), 이어서 app-stack,
  build-stack, 그리고 `mysql-dsql-*` 스택/Route 53/빌드 버킷 잔여 확인 — 리소스·비용이 남지 않도록.

## v0.1.18

### 수정 (Fixed)

- **Full Load 재실행이 CDC 시작 전이라면 "Full load + CDC" 패턴이어도 확인한 테이블을 DROP+재생성합니다.**
  기존엔 패턴이 Full load+CDC이기만 하면 DROP+재생성을 비활성화해서, **CDC 시작 전 "Re-run all tables"가
  깨끗이 재적재하지 않고 기존 행을 남긴 채 병합**(이전 행이 "already there"로 잔존)됐습니다. 이제 억제 조건을
  **CDC가 실제 스트리밍 중인지**로 변경했습니다: CDC 시작 전 재실행은 확인한 테이블을 DROP+재생성(깨끗이
  재적재)하고, **실제 스트리밍 중일 때만** 안전하게 기존 행은 건너뛰는 `SKIP_EXISTING`(DROP 없음)으로 적재해 라이브 싱크와의
  충돌을 막습니다. 확인창의 "will be DROPPED" 경고도 **실제로 DROP될 때(CDC 미시작)에만** 표시됩니다.
  (DROP 없이 재적재해도 중복은 생기지 않습니다 — `INSERT ... ON CONFLICT (PK) DO NOTHING` — 다만 소스에서
  삭제된 행이 잔존할 수 있어, 깨끗한 재적재가 그 모호함을 없앱니다.)

## v0.1.17

### 수정 (Fixed)

- **"Start / Re-run Full Load" 확인 다이얼로그가 몇 초 뒤 사라지던 문제 수정.** 다이얼로그가
  주기적으로 재렌더되는 콘텐츠 안에서 생성되고 일회성 플래그로 열려, 약 1.5초 진행 폴링 재렌더가
  뜨자마자 닫아버렸습니다. 이제 **클라이언트 최상위 컨텍스트에서 on-demand로 생성·오픈**하여,
  Confirm/Cancel 할 때까지 유지됩니다.

## v0.1.16

### 수정 (Fixed)

- **Full Load 재실행이 커스터마이즈한 타깃 스키마를 더 이상 되돌리지 않습니다.** 객체별
  **편집한 타깃 DDL**(예: `TINYINT(1)` → `smallint` 리매핑)을 durable 세션 스냅샷에 영속화하고
  재접속/재시작 시 복원합니다. 이전엔 편집이 메모리에만 있어, 재시작 후 "Re-run all tables"가
  기본(결정적) 변환으로 테이블을 재생성(예: `smallint`을 다시 `boolean`으로)해 범위를 벗어난 값이
  다시 적재 실패했습니다. 이제 재실행의 DROP+재생성이 커스터마이즈한 DDL을 사용합니다.

> 참고: 복원은 세션 ID로 매칭되므로, 재시작에도 세션(과 편집)이 유지되려면
> `DSQL_MIGRATOR_STORAGE_SECRET`을 설정하세요. 컨테이너 재배포는 새 임시 스토리지를 쓰므로,
> 재배포 후에는 편집을 다시 적용하세요.

## v0.1.15

### 수정 (Fixed)

- **Schema Conversion: "Apply to target"가 이제 REPLACE 확인 다이얼로그를 확실히 표시합니다.**
  다이얼로그가 객체별 에디터의 (중첩) 슬롯 안에서 생성돼 페이지 오버레이로 안 뜨는 경우가 많아
  버튼이 무반응처럼 보였습니다. 이제 **클라이언트 최상위 컨텍스트**에서 생성되어 항상 표시됩니다.
- **Schema Conversion: 느린 apply가 "parent slot deleted"로 더 이상 크래시하지 않습니다.**
  await 이후 UI 피드백(notify/refresh)이 원래 client 컨텍스트로 재진입하고 best-effort로 동작합니다.
- **우측 상단 UI 버전이 실제 릴리스 버전을 표시합니다.** `__version__`을 하드코딩 대신 설치된 패키지
  메타데이터에서 읽어, 빌드된 이미지가 항상 실제 버전을 보여줍니다.

### 추가/변경 (Added / Changed)

- **Schema Conversion·Data Migration: Select all / Unselect all** — 두 object 브라우저에서
  일괄 선택/해제.
- **Schema Conversion: "Generate DDL for selected"가 생성 후 잠기고**, "Reset all" 후 다시
  활성화됩니다(같은 범위를 두 번 눌러 조용히 재실행되는 혼란 제거).
- **Data Migration: 사전 선택 캡션 명확화** — 몇 개가 왜(타깃에 이미 존재) 미리 선택됐는지 표시 +
  Select all/Unselect all.
- **격리(quarantine) 행을 "실패"가 아니라 재프레이밍** — 적재 가능분은 다 적재했고 DSQL 하드
  리밋(예: 값당 ~1 MiB 초과)으로 한 행을 영구 드롭한 테이블은 **"Done — quarantined"**(앰버)로,
  재시도 가능한 실제 실패(레드)와 분리 표시.
- **테이블별 Reload** — 한 테이블만(DONE이어도) Full Load 재실행. 예: 초과 소스 값을 고친 뒤
  이전에 격리된 행을 적재. 나머지 테이블은 그대로 유지.
- **격리 행 인지 후 CDC 진행(오버라이드)** — Full Load가 **영구 격리 행 때문에만** incomplete일 때,
  갭을 인지하고 재실행 없이 CDC를 진행할 수 있습니다(갭은 Validation에 계속 표시). 재시도 가능한
  실제 실패는 여전히 차단됩니다(오버라이드가 복구 가능한 실패를 가리지 않음).

## v0.1.14

### 수정 (Fixed)

- **Schema Conversion: 편집이 이제 REPLACE로 확실히 적용됩니다(이전엔 여전히 SKIP되기도 함).**
  v0.1.13은 자동 REPLACE를 UI 측 존재 검사에 의존했는데, 그 값이 오래됐거나 없으면 편집한 객체가
  여전히 "SKIPPED — already existed; left unchanged"로 처리됐습니다. 이제 편집한 객체 Apply는
  **항상 REPLACE 확인 경로**로 가며(REPLACE의 `DROP ... IF EXISTS`가 아직 없는 객체도 안전 처리),
  확인하면 편집이 적용됩니다.
- **Schema Conversion: Apply 시 열려 있던 Generated DDL 패널이 더 이상 접히지 않습니다.** apply 후
  재렌더가 객체별 펼침/접힘 상태를 보존합니다.

### 참고 (Notes)

- UI 수정 — `:0.1.14` 이미지에 포함.

## v0.1.13

### 변경 (Changed)

- **Schema Conversion: 편집한 기존 객체를 Apply하면 이제 조용히 SKIP하지 않고 REPLACE(확인 후)로
  적용됩니다.** 이전엔 변환 DDL을 편집(예: 컬럼 타입 리매핑)하고 기본 SKIP 모드로 "Apply to target"을
  눌러도, 타깃에 이미 있는 객체는 건드리지 않아 **편집이 조용히 반영되지 않았고**, 피드백도 짧은 SKIPPED
  토스트뿐이라 "반응이 없다"고 느껴졌습니다. 이제 per-object Apply가 **기존 객체에 대한 편집을 감지하면
  REPLACE 확인 다이얼로그**("DROP 후 재생성 …")로 보내, 확인하면 변경이 실제로 적용됩니다. 편집하지 않은
  기존 객체는 여전히 SKIP(다시 적용해도 그대로), 아직 없는 편집 객체는 정상 생성됩니다.

### 참고 (Notes)

- UI/동작 변경 — `:0.1.13` 이미지에 포함.

## v0.1.12

### 변경 (Changed)

- **DSQL 미지원 소스 컬럼을 차단/NULL 대신 `bytea`로 보존 — Full Load와 CDC 양쪽에서.** MySQL
  spatial 컬럼(geometry/point/…)이 있는 테이블이 이전엔 Schema Conversion에서 통째로 실패
  (UNSUPPORTED 읽기전용 주석)했습니다. 이제:
  - **Schema Conversion**: spatial 컬럼을 `bytea`로 매핑해 실제 편집 가능한 `CREATE TABLE` 생성
    (MANUAL + "원본 바이트(WKB)로 보존" 경고). 원하면 `text`(WKT)/드롭/`bytea` 유지로 편집 가능.
  - **Full Load**: `ST_AsBinary(col)` → WKB 바이트 → `bytea`.
  - **CDC**: 커스텀 DSQL 싱크가 Debezium geometry 로지컬 타입
    (`io.debezium.data.geometry.Geometry`/`Geography`/`Point`)의 WKB 바이트를 추출 → `bytea`.
    **Full Load와 동일한 바이트**(SRID는 양쪽 모두 드롭, plain WKB) → FL/CDC 일치. 예상치 못한
    형태는 그대로 바인딩되어 DLQ로 시끄럽게 실패 — **조용한 NULL 없음.**
  - 공유 write contract(`converter.DSQL_WRITE_CONTRACT_CASES`)에 geometry → `bytea`를 기록해
    Full Load(Python)와 CDC(Java) 쓰기 경로가 어긋나지 않게 함.

### 참고 (Notes)

- CDC geometry 처리가 라이브 파이프라인에 반영되려면 DSQL 싱크 커넥터 플러그인 재빌드/재게시가
  필요하며, 다음 이미지+플러그인 빌드에 포함됩니다.

## v0.1.11

### 변경 (Changed)

- **Full Load 값 변환이 적용된 타깃 스키마를 따릅니다.** 이전엔 값 변환기가 각 컬럼의 타깃 타입을
  *소스* MySQL 타입에서 재유도해, Schema Conversion에서 리매핑한 컬럼(예: `TINYINT(1)` ->
  `boolean` 대신 `smallint`)을 무시하고 0/1이 아닌 값이 테이블 전체를 실패시켰습니다. 이제 Full
  Load는 *적용된* 타깃 타입(변환/편집된 DDL에서 parse)에 맞춰 값을 변환하므로, smallint/integer로
  리매핑한 컬럼은 0/1이 아닌 값도 정수로 적재됩니다(진짜 boolean 컬럼은 영향 없음).
- **fresh/replace 재적재가 custom 리매핑 스키마를 보존합니다.** fresh-load 재생성이 deterministic
  재유도가 아니라 적용된(편집된) DDL로 DROP+재생성하므로, full re-load 시 사용자의 리매핑이 덮이지
  않습니다.

### 수정 (Fixed)

- boolean 값 변환 충돌 메시지가 이제 "Schema Conversion에서 해당 컬럼 타깃 타입을 smallint/integer로
  리매핑(이제 실제로 적용됨) 후 재시도"로 안내합니다(소스 변경만 제안하던 것에서 개선).

### 참고 (Notes)

- 아직 새 컨테이너 이미지를 발행하지 않았습니다(v0.1.10과 함께 배치). 로컬은 UI 재시작으로 반영,
  ECS는 다음 이미지 빌드에 포함됩니다.

## v0.1.10

### 수정 (Fixed)

- **Schema Conversion 미리보기: 자동 변환 불가 객체를 "Unsupported"로 라벨링하고 "Apply to target"
  버튼을 제거.** 특정 플레이스홀더(예: MySQL spatial 타입)를 쓰는 테이블이 이전엔 단순 "N warning"으로만
  표시되고, 편집 가능하며, Apply 버튼까지 제공됐습니다(눌러도 no-op/SKIP). 이제 미리보기는 (1) 객체 헤더에
  변환 심각도("Unsupported" / "Review needed")를 표기하고, (2) generic 미변환 주석뿐 아니라 **CREATE가
  아닌 모든 플레이스홀더**를 미변환으로 취급합니다 — 읽기 전용으로 재설계 사유와 AI 제안 옵션만 보이고 apply
  대상이 되지 않습니다. apply 경로에서 이미 SKIP 처리하는 v0.1.9를 보완합니다.

## v0.1.9

### 수정 (Fixed)

- **Schema Conversion: 자동 변환 불가 테이블이 FAILED가 아니라 SKIPPED로 표시.** 컨버터가 자동
  변환할 수 없는 테이블(예: MySQL spatial/geometry 컬럼 — Aurora DSQL에 대응 타입 없음)은
  `target_ddl`이 `CREATE`가 아닌 **주석 플레이스홀더**여서, apply 시 모호한
  `SchemaApplyError: target DDL must be a CREATE ...`가 발생했습니다. 이제 해당 테이블은 applier로
  보내지 않고 **SKIPPED + 재설계 사유**(평가 결과와 동일)로 보고되며, 함께 선택한 나머지 테이블은
  정상 적용됩니다.

## v0.1.8

### 수정 (Fixed)

- **CDC offset-seeder(무손실 Full Load -> CDC 핸드오프) 배포 가능.** Full Load watermark로 CDC를
  배포하면(`SeedOffset`) cdc-stack이 in-VPC offset-seeder Lambda와 그 IAM 역할을 만들고
  커스텀 리소스가 이를 호출합니다. 앱이 assume하는 `CdcDeployRole`에 권한이 없어
  `AccessDenied`로 실패·롤백되던 문제. `CdcDeployRole`에 추가:
  - `function:mysql-dsql-cdc-*`에 대한 `lambda:*` 라이프사이클
    (`CreateFunction`/`DeleteFunction`/`InvokeFunction` 등);
  - IAM 역할 관리 스코프를 `*-ConnectorExecutionRole-*` → `role/mysql-dsql-cdc-*`로 확장해
    자동 명명되는 offset-seeder 역할까지 포함;
  - `lambda.amazonaws.com`으로의 `iam:PassRole`(MSK Connect에 추가).
- **CDC 인프라: MSK Serverless 클러스터 생성.** MSK Serverless 생성은 caller 자격으로 VPC를
  검증하므로 assume된 `CdcDeployRole`에 `ec2:DescribeVpcAttribute`(및
  `ec2:DescribeAvailabilityZones`)도 필요합니다. 없으면 `DescribeVpcAttribute 권한 없음`으로
  실패·롤백됩니다.
- **CDC 인프라: 커넥터 역할 생성 + 롤백 정리.** `logs:DescribeLogGroups`(CFN이 `!GetAtt`로
  LogGroup `Arn`을 해석할 때 호출)는 리소스 레벨 스코프를 지원하지 않아, 특정 로그 그룹에
  고정하지 않고 계정/리전 로그 그룹 범위의 별도 문장으로 부여; MSK Serverless 클러스터 삭제는
  `kafka:DeleteCluster`가 필요(`DeleteClusterV2`는 없는 액션) — 없으면 롤백/teardown이 클러스터를
  고아로 남깁니다.
- **죽은 Glue Schema Registry 권한 제거**: 파이프라인은 (v0.1.5부터) 내장 JSON 컨버터를 쓰고 Glue
  레지스트리를 만들지 않으므로 `glue:*` 권한은 미사용이었습니다.

### 참고 (Notes)

- 배포 템플릿(app-stack IAM)만 변경 — **컨테이너 이미지 변경 없음**. 게시된 `:0.1.7` 이미지가
  그대로 기본값입니다.

## v0.1.7

### 수정 (Fixed)

- **CDC 인프라(cdc-stack) 배포 성공.** 앱이 assume하는 `CdcDeployRole`의 IAM 권한 누락과
  템플릿 버그로 cdc-stack 배포가 실패·롤백되던 문제. 수정:
  - `CdcDeployRole` IAM: 오버사이즈 템플릿을 플러그인 버킷에 stage(`s3:PutObject`/`GetObject`);
    MSK Connect 플러그인/워커설정 태그 권한(`kafkaconnect:TagResource`/`ListTagsForResource`/
    `UntagResource`)과 리소스 레벨 미지원 create 액션을 위한 `Resource: "*"`; VPC 엔드포인트
    권한(`ec2:CreateVpcEndpoint` 등).
  - `cdc-stack.yaml`: 잘못된 `!GetAtt ConnectorS3Endpoint.PrefixListId` 제거
    (`AWS::EC2::VPCEndpoint`엔 없는 속성), 보안그룹 규칙 설명을 EC2 제약(256자 미만/제한 문자셋)에
    맞게 단축.

### 변경 (Changed)

- 기본 `ContainerImageUri` -> 게시된 `:0.1.7` 이미지.

> 참고: CDC **인프라** 경로는 end-to-end 검증 완료. 커넥터 시작("Start CDC")과 offset-seeder
> (watermark/무손실 핸드오프) 경로는 별도로 보강 중.

## v0.1.6

### 수정 (Fixed)

- **공개 이미지에서 CDC 인프라 배포가 동작.** "Deploy CDC infrastructure"가 런타임에 읽는
  cdc-stack CloudFormation 템플릿(`deploy/cdc-stack/cdc-stack.yaml`)이 컨테이너 이미지에
  포함되지 않아(Dockerfile 미복사 + `.dockerignore`가 `deploy/` 제외) 깨끗한 이미지에서
  "Could not read the cdc-stack template"로 실패하던 문제. 이제 템플릿을 이미지에 포함.

### 변경 (Changed)

- 기본 `ContainerImageUri`를 게시된 `:0.1.6` 이미지로 갱신(새로 배포해도 CDC 템플릿 수정 포함).

## v0.1.5

### 변경 (Changed)

- **CDC 배포 비용 추정을 월(month)이 아닌 시간(hour) 단위로 표시** — 이 도구는 cut-over 기간
  동안만 CDC를 띄우는 특성에 맞춤. 그리고 파이프라인이 사용하지 않는 **Glue를 비용 항목에서 제거**.

## v0.1.4

### 수정 (Fixed)

- **Schema Conversion이 미지원 spatial 타입에서 더 이상 전체가 멈추지 않음.** MySQL
  spatial 타입(`POINT`, `LINESTRING`, `POLYGON` 등)을 쓰는 테이블이 `sqlglot`
  `ParseError`를 일으켜 Schema Conversion 스텝 전체가 렌더되지 않던 문제. 이제 테이블
  단위로 실패를 격리해, 해당 테이블만 사유(문제 컬럼명 포함)와 함께 `UNSUPPORTED`로
  분류하고 나머지 테이블은 정상 변환합니다.
- **Migration plan의 "Deploy CDC infrastructure" 버튼 동작.** async 확인
  다이얼로그/배포 핸들러를 `await` 없이 호출해(코루틴 미실행) 클릭이 조용히 무시되던
  문제. 이제 await 처리되어 확인 다이얼로그가 열리고 배포가 시작됩니다.

### 변경 (Changed)

- **app-stack 네트워크 가드레일.** `AllowedIngressCidr` 안내를 명확화(공개 ALB →
  본인 공인 IP `x.x.x.x/32`)하고, 태스크가 소스 DB로 나갈 egress 경로를 항상 갖도록
  `SourceDbSecurityGroupId` / `SourceDbCidr` 중 최소 하나를 요구하는
  `SourceReachabilityRequired` 규칙을 추가(배포 후 "소스 연결 불가"가 조용히 생기는 것 방지).
- **AI 보조 모델 선택.** `BedrockModelId`를 Anthropic 큐레이션 드롭다운으로 만들고,
  선택한 모델로부터 태스크 역할의 `bedrock:InvokeModel` 스코프를 자동 도출합니다.
  `BedrockModelArns`는 선택적 override가 됩니다.
- **`CertificateArn` 테스트 경로 문서화.** 배포 가이드(EN/KO) 정리: optional 섹션 명확화,
  공인 IP / 테스트 인증서 사전 준비를 앞쪽에 노출.

## v0.1.3

- 이전 게시 베이스라인(ECR Public 이미지 `:0.1.3`).
