# 변경 이력 (Changelog)

_언어: [English](CHANGELOG.md) | **한국어** | [日本語](CHANGELOG.ja.md)_

이 프로젝트의 주요 변경 사항을 기록합니다. [유의적 버전(semver)](https://semver.org/)을
따르며, 버그 수정은 패치 릴리스로 올립니다.

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
  리밋(예: 값당 ~1 MiB 초과)으로 한 행을 영구 드롭한 테이블은 **"Done — quarantined"(앰버)**로,
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
