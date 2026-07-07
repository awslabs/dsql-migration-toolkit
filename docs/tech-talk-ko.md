---
marp: true
theme: default
paginate: true
title: MySQL to DSQL Migrator — Migration Architecture & Data Path Deep Dive
class: dense
style: |
  /* 모든 슬라이드에 dense 적용 (frontmatter class: dense). 특정 슬라이드만 예외로 두려면 그 슬라이드 맨 위에 <!-- _class: 다른클래스 --> */
  section.dense { font-size: 21px; }
  section.dense h1 { font-size: 30px; }
  section.dense h2 { font-size: 22px; }
  section.dense table { font-size: 19px; }
  section.dense pre { font-size: 16px; }
  section.dense li { line-height: 1.3; }
---

<!--
발표 자료 (DB 전문가 대상, 한국어). Marp / reveal-md 로 렌더 가능. 그냥 Markdown으로 읽어도 됨.
슬라이드 구분은 수평선(3개 하이픈), 화자 노트는 HTML 주석 블록.
시간 배분(발표 20분 + 데모 5분): 도입 2 · 아키텍처 4 · Evaluation/Schema 3 · Full Load 4 · CDC 5 · Validation/AI 2 · 마무리(핫파티션) 1 (+예비).
-->

<style scoped>
section h1 { font-size: 60px; }
section h2 { font-size: 34px; }
</style>

# MySQL to DSQL Migrator
## Migration Architecture & Data Path Deep Dive

발표자: dalyoung@ · 2026-07-06

Gitlab - https://gitlab.aws.dev/dalyoung/mysql-dsql-migration-tool-public

<!--
- (구두) 내부 기술 공유 · 발표 20분 + 데모 5분 · 대상: DB 전문가.
- 위 GitLab 링크가 공개 리포지토리 — 참석자가 clone해서 따라올 수 있음.
- 이 툴은 RDS/Aurora MySQL을 Aurora DSQL(PostgreSQL-16 호환, 분산형)로 옮기는 웹 툴.
- 오늘은 "무엇을 하냐"보다 "어떻게 동작하냐" — 특히 아키텍처, Full Load, CDC 내부를 깊게.
- DSQL은 분산 아키텍처라 수평 확장에 안 맞는 기능(FK, 트리거, 동기 인덱스 등)을 의도적으로 제외 → 그래서 이건 '업그레이드'가 아니라 '이종 마이그레이션'이다.
-->

---

# 오늘의 관점 3가지

1. **이종(heterogeneous) 마이그레이션이다** — 업그레이드가 아니다
   - MySQL → PostgreSQL 방언 → DSQL 제약, 2-hop 변환

2. **두 개의 데이터 경로가 DSQL로 수렴한다**
   - Full Load(일회성 벌크) + 선택적 CDC(연속 스트리밍)
   - **Full Load는 Debezium 스냅샷이 아니다** — 툴 자체 Python 벌크 로더

3. **툴은 컨트롤 플레인**이다
   - 설정·벌크 로드·워터마크·모니터링만 담당
   - 데이터 무결성 원칙: **조용한 손실보다 시끄러운 실패**

<!--
- 세 관점이 오늘 발표 전체를 관통. 특히 (2)의 "Full Load ≠ Debezium 스냅샷"과 (3)의 "loud fail over silent loss"를 기억.
- 목표는 완전 자동 무중단이 아니라: 마이그레이션 가능성 평가 → 결정론적으로 되는 건 자동화 → 사람 손이 필요한 지점을 명확히 드러내기.
-->

---

# 왜 이 툴이 필요한가 — MySQL ≠ Aurora DSQL

| | **RDS/Aurora MySQL (소스)** | **Aurora DSQL (타깃)** |
|---|---|---|
| 엔진 계열 | MySQL | **PostgreSQL(-16) 방언·호환** → 이종(heterogeneous) |
| 아키텍처 | 단일 노드 스토리지(heap) | **분산 · PK로 스토리지 파티셔닝** |
| 외래 키 | 지원 | **없음** (앱 계층 강제) |
| 트리거·저장프로시저 | 지원 | **없음** |
| 인덱스 생성 | 동기 | **`CREATE INDEX ASYNC`** (적재 후 백필) |
| 트랜잭션 | 큰 트랜잭션 OK | **≤3000행 · DDL 1개 · 값당 1 MiB · ≤5분** |
| 동시성 제어 | 잠금 기반 | **낙관적(OCC) — 충돌 시 40001 재시도** |
| 인증 | 비밀번호 | **단기 IAM 토큰 (~15분)** |
| PK | 선택 | **필수** (AUTO_INCREMENT는 핫 파티션 유발) |

> 단순 덤프/복원·표준 JDBC 로더로는 이 제약들을 못 넘는다 → **DSQL을 이해하는 전용 툴**이 필요

<!--
- 이 슬라이드가 "왜 이 툴인가"의 답. 하나하나가 뒤 슬라이드의 설계 근거가 된다:
  분산+PK파티셔닝 → 핫 파티션/PK 전략, ≤3000행 → 배치 로더, OCC 40001 → statement 재시도, IAM 토큰 → 커스텀 싱크, FK 없음 → preserve-in-report.
- 핵심 한 줄: mysqldump나 범용 도구의 'full load'는 DSQL의 배치 한도·OCC·IAM 토큰·타입 차이를 다루지 않는다.
-->

---

# 아키텍처 (한눈에) — 두 데이터 경로가 DSQL로 수렴

![w:1000](../deploy/architecture-aws-simple.png)

- **Migration Tool**(ECS Fargate·web UI)이 소스를 읽어(**convert + bulk load**) DSQL에 씀 = Full Load
- **CDC 파이프라인**(점선 박스)은 관리형 MSK Connect 위: Debezium → MSK → **커스텀 DSQL 싱크**
- **Offset-seeder Lambda**가 Full Load → CDC를 gapless로 이음

<!--
- 큰 그림 먼저. 상단 경로 = Full Load(변환+벌크 로드), 하단 박스 = 선택적 CDC.
- "CDC pipeline runs on managed MSK Connect — no servers owned" 라벨 강조: 우리가 컴퓨팅을 운영하지 않는다.
- 다음 슬라이드에서 프로덕션 전체 아키텍처(네트워킹/IAM/보안)로 확대.
-->

---

# 아키텍처 (전체) — app-stack + cdc-stack

![w:1080](../deploy/architecture-aws.png)

- **app-stack**(항상): ALB(선택 Cognito) · ECS Fargate(ECR Public) · Secrets Manager · (선택)Bedrock
- **cdc-stack**(선택, VPC 프라이빗): MSK + MSK Connect(Debezium 소스 + 커스텀 싱크) · Offset-seeder Lambda · S3 Gateway VPC 엔드포인트
- **컨트롤 플레인 vs 데이터 플레인**: 툴은 설정·벌크로드·워터마크·모니터링만, 싱크 실행은 관리형 MSK Connect

<!--
- 왜 단일 태스크(desiredCount=1)? 컨트롤 플레인이라 상태 분할 불요(no-bloat). 이미지 롤링 교체 시 짧은 다운타임 → 다음 상태 3계층 슬라이드로 관리.
- 소스 MySQL은 고객 소유, 두 스택 밖. 브라우저는 UI만(데이터 경로엔 없음).
- 보안: Cognito는 기본 off, 인터넷 노출(0.0.0.0/0) 조합은 템플릿이 강제로 막음. 최소권한 IAM(task/execution role 분리).
- 시간 짧으면 이 슬라이드는 "왼쪽=app-stack 항상, 오른쪽 VPC 박스=cdc-stack 선택" 두 줄로 넘어가도 됨.
-->

---

# 상태(state) 관리 — 계층별 저장 위치와 수명

| 계층 | 저장 위치 | 태스크 교체 시 |
|---|---|---|
| **① 자격증명** | 세션별 **프로세스 메모리에만** | 소멸 (원래 안 남김) |
| **② 워크벤치/작업 상태** | 로컬 SQLite (`/tmp`, 휘발성) | 소멸 → 재연결 후 재실행 |
| **③ 마이그레이션된 데이터·스키마** | **DSQL 자체** | 유지 (무관) |

- **Property 7**: 자격증명은 디스크·로그·리포트·작업상태에 **절대** 안 남긴다. 세션 끝나면 폐기.
- 태스크 롤링 교체 → ①②는 사라지고 ③은 살아남음 → **재연결 시 읽기전용 Evaluation 재실행으로 자동 복구**.
- 세션 쿠키 서명 시크릿은 스택이 자동 생성(운영자 입력 없음, DB 자격증명 아님).

<!--
- DB 전문가에게 어필 포인트: 자격증명이 절대 디스크에 안 닿는다는 게 강제 규칙(코드 리뷰 게이트).
- ③이 DSQL이라 태스크가 죽어도 데이터/스키마는 안전. 재연결하면 타깃을 introspect해서 상태 복원.
-->

---

# 배포 모델 — 최소 설정으로 바로 배포

- 이미지는 **ECR Public** 게시 → 빌드 불필요
- 커넥터 플러그인 아티팩트가 **커밋돼 있음** → Java/Maven 툴체인 불필요
- 툴이 **자체 S3 버킷 생성** → 아티팩트 직접 업로드
- CDC 인프라 **자동 발견** → 추론 불가한 것(VpcId 등)만 입력
- 리전은 **DSQL 엔드포인트에서 도출** (`…dsql.ap-northeast-2.on.aws` → `ap-northeast-2`)

**최소 권한 IAM 분리**
- task role: `dsql:DbConnect(+Admin)`, 읽기전용 `GetCluster`, 소스 시크릿 범위 `secretsmanager:GetSecretValue`
- `bedrock:InvokeModel`은 **AI 켤 때만** (허용 모델 ARN 범위)

<!--
- 배포 편의성이 설계 최상위 원칙. "새 clone + 최소 설정"이 목표.
- 크로스 리전 마이그레이션은 미지원 — CDC 데이터 플레인이 DSQL 리전 VPC 안에서 소스에 프라이빗하게 도달해야 하기 때문. 단일 리전 전제.
-->

---

# 6단계 마이그레이션 워크플로우

```
Connect → 1.Migration plan → 2.Evaluation → 3.Schema Conversion
        → 4.Data Migration → 5.Validation → 6.Cut over
```

- 각 단계는 독립 상태(시작 안 함 / 진행 중 / 완료 / 실패), 개별 실행·재실행 가능
- **Migration plan**의 유일한 지속 효과: CDC 인프라를 미리 프로비저닝할지 여부
  - 세부 방식(Full+CDC vs CDC only)은 뒤 Data Migration에서, **되돌릴 수 있음**
- **Cut over**는 사람이 하는 작업 → 툴은 런북만 제공(Run 액션 없음)

<!--
- 강제 마법사가 아니라 가이드형 흐름. 사전 단계 미완료면 UI가 안내만.
- 오늘 딥다이브는 2·3(Evaluation/Schema)과 4(Full Load/CDC), 5(Validation).
-->

---

# 2·3단계: Evaluation & Schema Conversion
## 데이터를 옮기기 전에, DSQL이 무엇을 거부할지 미리 파악한다

**객체별 3단계 분류** — 모든 소스 객체를 AUTO / MANUAL / UNSUPPORTED 중 하나로 판정
- 규칙에 걸리지 않으면 기본 AUTO. 여러 규칙에 걸리면 **가장 엄격한 등급을 채택**: UNSUPPORTED > MANUAL > AUTO
- 대신 걸린 **모든 규칙의 사유와 권장 조치는 리포트에 함께 기록**되어, 어떤 지적도 묻히지 않는다

**규칙 기반 2단계 변환** (`sqlglot`)
```
MySQL DDL → [sqlglot: MySQL→PostgreSQL 방언] → [DSQL 제약 레이어] → DSQL DDL
                                                FK 제거 · 인덱스→ASYNC
                                                DDL 트랜잭션 분리 · 타입 매핑
```
- 결정론적 변환이 **항상 먼저** 수행되고, AI(Bedrock)는 MANUAL/UNSUPPORTED만 보강(검토·승인 후 반영)

<!--
- 핵심 메시지: 연결과 데이터 이동 사이의 '결정론적 게이트' — 시행착오가 아니라 예측 가능한 변환.
- Aurora MySQL 사용자에게 이 단계는 "DSQL이 받아들이지 않을 것"을 단 한 행도 옮기기 전에, 읽기전용으로 저렴하게 알게 되는 지점.
- AI는 결정론 경로를 대체하지 않고 보강만 하며(기본 off), review-only + 명시적 승인 게이트를 통과해야 타깃에 반영됨.
-->

---

# Schema 변환의 핵심 결정들

| 항목 | 처리 |
|---|---|
| **타입 매핑** | TINYINT(1)→boolean, BIT(n)→int, ENUM/SET→text+CHECK, BLOB→bytea, DATETIME→timestamp |
| **외래 키** | DSQL엔 FK 없음 → DDL에서 제거하되 **리포트에 보존** + "앱 계층에서 강제" 권장 |
| **보조 인덱스** | `CREATE INDEX ASYNC` (적재 후 비동기 백필) — FULLTEXT/SPATIAL은 UNSUPPORTED |
| **DDL 적용** | 트랜잭션당 **DDL 1개**, 40001/OC001 안전 재시도(idempotent) |
| **PK 전략** | 정수 유지 / UUID / 캐시 identity / **복합 PK(신규)** |

- **손실 투명성**: 파싱 불가한 뷰도 조용히 버리지 않고 **placeholder + MANUAL 플래그**
- **fail-loud**: `{0,1}` 밖 TINYINT(1) 값은 조용히 true로 뭉개지 않고 테이블 Full Load 중단

<!--
- FK "preserve-in-report"가 손실 투명성 원칙의 상징. 사라지는 게 아니라 드러난다.
- PK 전략의 이유(핫 파티션)는 Full Load 딥다이브에서 상세히. 여기선 "선택지가 있다"만.
- DSQL 하드 제약: 컬럼 ≤255/테이블, 테이블 ≤1000/DB, DB 1개/클러스터, DECIMAL ≤38, 값당 1 MiB.
-->

---

# 4단계 A: Full Load 엔진 내부 (1/2)
## Debezium 스냅샷이 아니다 — 전용 Python 벌크 로더

**읽기: PK keyset 페이지네이션 (OFFSET 아님)**
```sql
SELECT <cols> FROM <table>
WHERE pk > :last ORDER BY pk LIMIT 1000        -- 복합 PK는 행-값 튜플 비교
-- START TRANSACTION WITH CONSISTENT SNAPSHOT (InnoDB REPEATABLE READ), server-side cursor
```
- OFFSET은 매 페이지 앞부분 재스캔 → 소스에 O(n²). keyset은 인덱스 seek, 페이지당 ~1000행만 in-flight
- **메모리는 테이블 크기 무관, 한 페이지로 bounded** — 전체 테이블을 RAM에 안 올림
- 단일 일관 스냅샷 → 라이브 소스가 바뀌어도 안전. **PK 필수**(없으면 UNSUPPORTED)

**쓰기: 배치 `INSERT ... ON CONFLICT`**
- 배치 크기 = min(≤2000행/**≤3000 하드캡**, 파라미터 ≤65535, 바이트 ≤8 MiB)
- idempotent(ON CONFLICT) → 같은 배치 여러 번 적재해도 중복 없음. CDC 동시 시 '기존 건너뛰기'

<!--
- 왜 전용 로더인가: 범용 도구의 full load는 내부적으로 JDBC INSERT라 DSQL 특화 OCC 처리가 없다.
- 일관 스냅샷 안에서 워터마크도 같은 트랜잭션으로 캡처 → 스냅샷 시점과 binlog 좌표가 정확히 일치(뒤 CDC 핸드오프의 근거).
- 주의: REPEATABLE READ 스냅샷이 테이블 읽는 내내 열려 있어 쓰기 많은 소스에선 InnoDB undo purge를 막을 수 있음(History List Length).
-->

---

# 4단계 A: Full Load 엔진 내부 (2/2)
## 재개·병렬·실패 격리

**결정론적 재개**: 행이 keyset(PK) 순서로 흐름 → **배치 i = 항상 같은 PK 범위**
→ 배치가 안정적 재개 단위. 중단/재시도는 **미완료 범위만** 재실행(중복 없음)

**병렬 모델**: `table_parallelism`(기본4,≤16) × `batch_parallelism`(기본8,≤32)
→ 동시 DSQL 연결 ≈ 4×8=32 (클러스터 한도 10,000연결·100신규/초 내 여유)

**OCC 재시도는 statement 단위** (배치 전체 아님)
- 40001(OC000 데이터/OC001 스키마) → 충돌한 `INSERT` **문 하나만** 백오프+지터 최대 10회
- 배치 전체 재제출은 충돌 안 한 99% 행을 다시 지불 + 넓은 키 범위 = livelock 위험

**실패 격리 두 갈래** (의도적으로 다름)
- **행 quarantine**: DSQL이 SQLSTATE로 행 거부(1MiB초과/제약위반) → 배치 이진분할로 그 행만 격리(**PK+사유만, 값 절대 미기록**), 나머지 적재, 실행은 실패로 판정
- **table-fatal**: 무손실 변환 불가(예 TINYINT(1)=2) → `ValueConversionError`(SQLSTATE 없음) → 테이블 적재 시끄럽게 중단

<!--
- statement 단위 재시도가 이 툴의 시그니처. 뒤 CDC에서 커스텀 싱크가 이걸 그대로 미러링.
- 행 quarantine vs table-fatal 차이: 전자는 DSQL이 거부(SQLSTATE 있음), 후자는 DSQL 묻기 전 read/convert 중 발생(SQLSTATE 없음). 둘 다 "조용히 넘어가지 않는다".
- 인덱스는 적재 후 마지막에 CREATE INDEX ASYNC (적재 중이면 모든 INSERT가 인덱스 유지비 지불).
-->

---

# Full Load 성능 — 실측으로 배운 것

**핵심 발견: 네트워크가 아니라 CPU-bound**
- 소스 리더가 행마다 MySQL→DSQL 타입 변환을 **순수 Python(GIL, Global Interpreter Lock 점유)**으로 수행
- payments+orders 실측: **4 vCPU가 0.5 vCPU(512) 대비 ~3.8배**

**GIL 벽**: reader 샤딩(K개 리더 스레드)도 변환은 ~1코어 못 넘음
→ `reader_shards=4`가 in-VPC 4vCPU에서 단일 리더 대비 **~0%** (그래서 기본 꺼짐)

**prefetch 큐**(리더가 다음 페이지 미리 읽음): in-VPC 4vCPU **~+19%**

**병렬수 가드레일 (실측)**: 32→128 연결로 2배 → 처리량 **+5%만**,
재시도 배치 비율 **9.6%→12.8%** (단조 PK가 같은 키 범위로 몰림)
→ **병렬수 무작정 올리기보다 PK 전략을 먼저**

<!--
- 이 슬라이드가 DB 전문가에게 가장 흥미로울 부분. "병렬수는 처리량 다이얼이 아니라 스로틀"로 다뤄라.
- 메모리는 table_par × batch_par × ~8 MiB (테이블 크기 무관). Fargate CPU/메모리 짝(4vCPU→≥8GiB)이 이미 충족.
- 다음 CDC로 넘어가는 다리: 이 CPU-bound 발견이 "핫 파티션이 항상 병목은 아니다"라는 마지막 슬라이드 고민으로 이어짐.
-->

---

# 성능 케이스 스터디 — Composite PK A/B (in-VPC 실측)

**실험**: `orders`+`payments`, prefetch ON, PK 전략만 변경 → keep(정수) vs composite `(customer_id, id)`
(orders=처리군 / payments는 `customer_id` 없어 정수 PK 유지=대조군)

| 조건 | keep 전체 rows/s | composite 전체 rows/s | CPU |
|---|---|---|---|
| **0.5 vCPU · bp8** | 4,270 | 4,243 (**0.99x**) | ~50% (1코어 벽) |
| **4 vCPU · bp16** | **10,055** | **10,088 (1.00x)** | 109~111% |

- **CPU 8배 → 처리량 ~2.4배** (4,270 → 10,055). **CPU가 확실한 병목** → vCPU 상향이 실무 레버
- **composite는 두 조건 모두 0% 차이.** DSQL `CommitLatency`: 양쪽 **p50 ~47ms · p99 60~120ms** → **핫 파티션 롱테일 없음** (서버 쓰기가 병목에 도달 못 함)

> **교훈: 최적화 전에 병목을 측정하라.** 이 워크로드의 벽은 서버 쓰기가 아니라 **클라이언트 CPU**였다. composite(서버 분산 레버)는 고칠 서버 병목이 없어 이득 0 — 기능은 정상(적용/skip/무손실 확인). composite는 훨씬 높은 write 동시성·진짜 단조-PK 핫스팟에서만 값어치.

<!--
- 이건 이번 주 직접 돌린 실측. 대조군(payments)이 처리군(orders)과 똑같이 0.99x → 환경 노이즈가 아니라 진짜 무효과.
- 핵심: composite PK는 "핫 파티션이 실제 병목일 때" 값어치. 여기선 p99가 120ms로 안정 → 서버가 병목이 아님 → GIL이 벽.
- composite가 빛나는 조건: (a) 클라 GIL 병목을 먼저 해소한 뒤, (b) 훨씬 높은 write 동시성에서 monotonic PK가 파티션을 달굴 때.
- 부수 관찰: 양쪽 run 끝에 SSL EOF로 8/8964 배치(~0.09%) 실패 → 로더에 transient 재연결 추가 여지(후속 과제).
-->

---

# 4단계 B: CDC 파이프라인 토폴로지

![w:960](../deploy/architecture-cdc-pipeline.png)

- CDC는 **선택적**. Full Load가 기존 행 복사, CDC가 이후 insert/update/delete 반영 → 최소 다운타임
- 테이블당 토픽 + **PK 키잉** → 한 행의 모든 변경이 한 파티션에 **순서 보존**
- 스키마는 런타임 내장 JSON 컨버터로 전달(별도 스키마 레지스트리 불필요)

<!--
- 짧은 동결이 허용되면 Full Load만으로 충분. CDC는 대규모/지속 마이그레이션용.
- 툴은 이 파이프라인을 in-process로 돌리지 않는다 → 컨트롤 플레인일 뿐(다음다음 슬라이드).
-->

---

# 왜 커스텀 싱크인가 — 설계 결정의 핵심

**표준 관리형 JDBC 싱크 vs 커스텀 싱크**

| | 표준 JDBC 싱크 | **커스텀 DSQL 싱크** |
|---|---|---|
| OCC(40001) 재시도 | **batch 단위** | **statement 단위** |
| 고경합 대규모 CDC | 처리량 **붕괴(collapse)** | 충돌 문장만 재시도, 배치 진행 |
| IAM 단기 토큰 | ✗ | 15분 토큰 2분 여유 갱신 |
| ≤3000행 배치 | ✗ | 청크당 commit 1회 |

> "Java is a consequence of the runtime, not a preference."

- 관리형 MSK Connect = 관리형 Kafka Connect → 플러그인이 **JVM jar**여야 함
- Python `core/`의 토큰 생성·OCC 재시도·DSQL dialect 로직을 **Java로 미러링**
- **제한적(bounded) cross-language 중복은 관리형 런타임의 대가** — write-contract 패리티 테스트로 강제

<!--
- "결정 변경 8"이 이 근거. 표준 JDBC 싱크가 40001을 batch로 재시도 → 3000행 통째 재생 → 넓은 키 범위 → livelock.
- Full Load(Python)와 CDC 싱크(Java)가 같은 타입 매핑을 따르도록 공유 패리티 테스트로 강제 → 어느 경로로 옮겨도 같은 행이 동일하게 적재.
- CDC 특유: BIGINT UNSIGNED는 precise 모드, JSON은 PGobject 래핑, GEOMETRY는 .wkb 추출.
-->

---

# gapless 핸드오프 — Full Load에서 CDC로 이어지기

**gapless는 파이프라인의 양 끝에서 지켜진다 — 시작점과 적용 양쪽**

**① 입구(시작점): Full Load가 끝난 그 지점부터 스트리밍을 시작한다**
- Full Load가 스냅샷을 뜬 순간의 **워터마크**(binlog 위치 + GTID)를 기록해 둔다
- CDC 시작 시, 소스 커넥터가 뜨기 **전에** VPC 안의 Lambda가 그 워터마크를 Debezium 시작 오프셋으로 심는다
  - Lambda인 이유: MSK Serverless 접속 주소가 VPC 전용이라 앱이 직접 못 심음
- 그 결과 Debezium은 **"지금"이 아니라 "스냅샷 직후 첫 변경"부터** 읽는다 → 앞부분 유실 없음
  - `snapshot.mode=recovery`: 행을 다시 읽지 않고 스키마 이력만 재구성한 뒤 시드된 오프셋부터 재개

**② 출구(적용): 같은 변경이 중복 적용돼도 안전하다**
- 싱크가 PK 기준 `ON CONFLICT` upsert / PK delete로 적용 → **재시도·재생돼도 중복 없음**(idempotent)
- 커넥션이 끊기면 그 지점의 오프셋을 다시 재생(replay) → 결과적으로 **정확히 한 번(effectively-once)**

⚠️ **꼭 챙길 전제**: 워터마크가 가리키는 **binlog가 CDC 시작 시점까지 남아 있어야** 한다
- Aurora MySQL 기본 보존 24시간인데 CDC 스택 배포에만 15~20분 → **시작 전에 보존 기간을 늘려두기**(예: 7일). 없어졌으면 Full Load를 다시 돌려 새 워터마크를 얻어야 함

<!--
- 핵심 프레이밍: gapless는 한 지점이 아니라 '입구 + 출구' 두 계층에서 지켜진다. Lambda(입구)는 앞부분 유실 방지, 싱크 idempotent(출구)는 중간 유실 방지.
- 실제 겪은 손실은 '출구' 버그였다: 커넥션 끊김을 poison으로 오분류해 미적용 행의 오프셋을 넘겨버림 → isTransient 재분류로 수정(재시도). Lambda(입구)는 원래 정상이었다.
- 전제조건(binlog 보존)은 현장에서 가장 자주 놓침. binlog_format=ROW, binlog_row_image=FULL도 필수.
-->

---

# CDC 데이터 무결성 — 조용한 손실 없음

**transient vs permanent 오류 구분이 재시도/DLQ의 기준**
- **transient**(재시도, DLQ 안 감): OCC 40001, 커넥션 끊김(idle close/토큰 만료/워커 교체)
  - 죽은/half-open 커넥션 감지 → 새 토큰 재연결 → 같은 오프셋 재적용(PK idempotent, 중복無)
- **permanent**(DLQ 격리): 타입 불일치, 제약 위반, 없는 타깃 컬럼(전파 안 된 소스 ALTER), 초대형 값
- 둘 다 불가하면 → **조용히 건너뛰지 않고 태스크를 시끄럽게 실패**

**값당 1 MiB 한도 — 3구간**
- ≤1 MiB 정상 / 1–8 MiB 싱크가 write 前 측정해 **DLQ 격리**(Kafka 한도 4→8 MiB 상향) / >8 MiB **캡처 단계서 `column.exclude.list`로 드롭**

**DLQ는 Kafka가 아니라 CloudWatch에서 본다**
- 격리 사유엔 **SQL 템플릿(컬럼명+`?`)만** — 행 값·자격증명 절대 없음 → 툴이 파싱해 UI에 "테이블별 Quarantined + 다운로드 에러 로그"

<!--
- "CDC는 스키마가 아니라 데이터를 복제한다"(include.schema.changes=false). 소스 DDL 변경은 전파 안 됨 → 먼저 DSQL에 직접 재적용. 그 전까진 안 맞는 행이 DLQ로 격리(조용히 사라지지 않음).
- 커넥션 끊김을 poison row로 오인하지 않는 게 핵심 — 예전 데이터 손실 모드였음.
- 복합 PK: message.key.columns로 소스에서 재키잉 → 한 행 변경이 같은 파티션 순서 유지 + 싱크가 ON CONFLICT(pk...)/DELETE WHERE 정확히 구성.
-->

---

# 5단계: Validation — 최종 확정 판정은 여기서만

Full Load/워터마크 행 수는 **스캔 없는 추정**(소스 아끼려고). 정확한 판정은 **오직 Validation**.

**테이블당 3단계 점증 엄밀도** (비용↑)
1. **행 수** = 소스 vs 타깃 정확한 `COUNT(*)` (저렴)
2. **체크섬** = 순서무관 테이블 체크섬 양쪽 동일 계산 → "행 수는 같지만 값 다름" 포착(전 행 읽기)
   - 로직: 행별 `MD5(컬럼들)` 앞 60비트를 정수화 → 테이블 전체 `SUM`(순서 무관) → 소스=타깃 비교. 크로스 엔진 정규화(NULL 센티넬·타입 렌더 일치)로 같은 데이터=같은 해시, FLOAT는 제외
3. **대조** = 양쪽 전체 PK 정렬 병합 → `missing_on_target`/`extra_on_target` 정확히 짚음(**단일 정수 PK만**)

**판정 AND-체인**: 테이블 matched = (COUNT 동일) AND (체크섬 동일) AND (PK셋 일관)
→ 리포트 is_match = (∀테이블 matched) AND (orphan==0). 증거 없으면 'not deeply checked'(거짓 일치 아님)

**라이브 소스 변화 보정** (검증 중에도 소스는 계속 바뀐다)
- 스냅샷 시점 GTID(워터마크)와 **지금** 소스 GTID를 비교 → 소스가 그새 전진했는지 확인
- 전진했다면, 소스가 타깃보다 많은 건 **마이그레이션 버그가 아니라 스냅샷 이후 늘어난 신규 데이터**로 구분해 리포트
- → "설명되는 차이"(신규 활동 · 의도된 quarantine · 미수렴 CDC)를 걸러내고, **설명 안 되는 누락만** 진짜 문제로 표시

<!--
- 타깃 부족 진단: (a) 드리프트, (b) 의도된 quarantine(1MiB초과), (c) 아직 수렴 안 된 CDC 삭제 → 이 셋으로 설명되면 건전. 설명 안 되는 누락/잉여 PK가 진짜 잡을 대상.
- 차이 샘플도 PK+체크섬 토큰만, 행 값 절대 노출 안 함. 읽기전용 CLI(compare_rows.py / cdc_consistency_check.py, exit 0 게이팅)도 있음.
-->

---

# AI DBA & Query Playground (선택)
## 증거 기반 — 실측 DPU로 증명

- AI 보조는 **opt-in**(기본 off), **컨트롤 플레인 전용**, **데이터 경로엔 절대 없음**
- 쿼리 변환도 스키마와 **같은 sqlglot 엔진** + AUTO/MANUAL/UNSUPPORTED + 안티패턴 태깅(`SELECT ... FOR UPDATE` 등)
- **타깃 안전 실행**: SELECT→EXPLAIN(ANALYZE 읽기전용), DDL→dry-run+**ROLLBACK**, DML→**차단**

**DSQL 쿼리 튜닝 규칙 (일반 PG 조언과 다름)**
- **PK가 곧 테이블** — PK 정렬 B-tree, heap 없음. 인덱스 없으면 Seq Scan이 아니라 **Full Scan**
- **compute↔storage 분리** → 넘어오는 모든 행이 **DPU** 유발. **필터를 아래로 밀기**가 핵심 지렛대
- 필터 3계층: Query Processor Filter(최악) → Storage Filter(INCLUDE) → **Index Condition(최선)**
- 배제: VACUUM/REINDEX/fillfactor/플래너 GUC/`cost=` 낮추기 (DSQL에 안 맞음)

<!--
- Tune with AI DBA 버튼은 변환 SELECT가 Test on target 통과 후에만 노출 — AI가 추측이 아니라 실제 플랜에 근거하도록.
- 증명 루프: EXPLAIN ANALYZE로 전/후 DPU 델타를 채팅에 되먹임 → 모델 주장이 아니라 측정치가 근거. 무개선이면 솔직히 말함. 자동 적용 안 함(human-review 게이트).
-->

---

# 마무리 고민: 핫 파티션과 애플리케이션 쿼리 변경

**DSQL은 PK로 스토리지를 분산** → 단조 증가 PK(AUTO_INCREMENT/타임스탬프)는 쓰기가 한 파티션으로 몰림(핫 파티션). 대책: UUID/캐시 identity/**복합 PK `(고카디널리티 선행컬럼, 원본 PK)`**.

**그런데 — 핫 파티션이 항상 병목은 아니다 (실측)**
- keep vs composite A/B(orders+payments): **0.5 vCPU·bp8도, 4 vCPU·bp16도 처리량 차이 없음(0.99~1.00x)**
- DSQL CommitLatency: 양쪽 **p50 ~47ms / p99 60~120ms** — 수 초짜리 핫 파티션 롱테일 **없음**
- 병목은 **서버 쓰기가 아니라 클라이언트 CPU**(§Full Load). CPU 8배 주면 처리량 2.4배지만, DSQL은 여전히 여유 → composite 이득 0

**복합 PK의 진짜 비용: 애플리케이션 쿼리가 바뀐다**
- PK가 `(customer_id, id)`가 되면 앱의 조회·조인·**upsert가 새 복합 키를 써야 함**, 선행 컬럼은 **불변**이어야 함
- 원본 키 유일성 보존 위해 `UNIQUE INDEX ASYNC` 별도 필요, CDC는 `message.key.columns` 재키잉 필요

> **결론**: 핫 파티션 대책(PK 변경)은 **서버 쓰기 벽에 실제로 부딪힐 때** 값어치가 있다. 먼저 병목을 측정하고(클라 vs 서버), composite는 **쿼리 변경 비용**과 함께 판단하라.

<!--
- 이건 이번 주 실측에서 나온 정직한 진단. composite PK 기능은 정상 동작(적용/skip/무손실 확인)하지만, 이 워크로드에선 이득이 0이었다 — 왜냐면 병목이 서버가 아니라 클라이언트 GIL이었기 때문.
- 메시지: "핫 파티션은 실재하지만, 대책을 넣기 전에 측정하라. 그리고 복합 PK는 공짜가 아니다 — 앱 쿼리가 바뀐다."
- composite가 빛나는 조건: (a) 클라 GIL 병목을 먼저 없앤 뒤, 또는 (b) 훨씬 높은 write 동시성에서 monotonic PK가 파티션을 달굴 때.
-->

---

# Demo (5분)

## 지금부터 실제 툴을 실행합니다

- 6단계 워크플로우를 UI로: Connect → Evaluation/Schema Conversion → Full Load → Validation
- 오늘 이야기한 것을 화면에서 확인:
  - 3분류(AUTO/MANUAL/UNSUPPORTED) 리포트 · side-by-side DDL diff
  - Full Load 진행(테이블별 rows/s) · 실패 격리
  - Validation 판정 · (선택) AI DBA 증명 루프

**Q&A 는 데모 중·후에 자유롭게**

<!--
- 데모 시작. (내부 테스트 환경에서 실행 — 실행 방법은 발표 자료에 넣지 않음)
- 시간 없으면 Full Load 진행 화면 + Validation 판정만 보여줘도 핵심 전달됨.
-->

---

# 감사합니다 / 참고

- 매뉴얼: `docs/manual/ko/` (0~11장, 각 단계 상세)
- 배포 가이드: `deploy/DEPLOYMENT.ko.md`
- 커스텀 싱크: `connectors/dsql-sink/`

**핵심 3줄 요약**
1. 이종 마이그레이션 — 결정론 우선, 사람 손 필요한 곳을 드러냄
2. Full Load(스트리밍 벌크, CPU-bound) + CDC(커스텀 싱크, statement-OCC, gapless) → DSQL
3. 조용한 손실보다 시끄러운 실패 — 자격증명은 메모리에만, 판정은 Validation에서만

<!--
- 마무리. 질문 유도.
-->
