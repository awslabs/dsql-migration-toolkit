# 변경 이력 (Changelog)

_언어: [English](CHANGELOG.md) | **한국어** | [日本語](CHANGELOG.ja.md)_

이 프로젝트의 주요 변경 사항을 기록합니다. [유의적 버전(semver)](https://semver.org/)을
따르며, 버그 수정은 패치 릴리스로 올립니다.

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
