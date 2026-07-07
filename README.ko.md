# mysql-dsql-migration-tool-with-AI

_언어: [English](README.md) | **한국어** | [日本語](README.ja.md)_

Amazon RDS MySQL / Aurora MySQL을 **Amazon Aurora DSQL**로 마이그레이션하는 웹 기반
All-In-One 도구이며, 판단이 필요한 부분을 위한 **선택적 AI 보조(Amazon Bedrock)**를
내장했습니다.

Aurora DSQL은 MySQL이 아니라 PostgreSQL 16 호환 *분산* 데이터베이스이므로, 이것은
두 개의 변환이 겹치는 **이종(heterogeneous) 마이그레이션**입니다: MySQL → PostgreSQL
방언, 이어서 PostgreSQL → DSQL 제약(외래 키 없음, 낙관적 동시성, 트랜잭션당 행/시간
한도, 비동기 인덱스, `C` collation 등).

목표는 완전 자동화된 무중단 마이그레이션이 아닙니다. **마이그레이션 가능성을 평가하고,
결정론적으로 변환 가능한 것(`sqlglot`)은 자동화하며, 사람의 작업이 필요한 지점을 명확히
드러내는 것**입니다. 소스 데이터베이스는 항상 읽기 전용으로만 접근합니다.

> **여기서 시작하세요:** [**고객 FAQ**](docs/manual/ko/11-customer-faq.md)(무엇을 계획해야
> 하는지 — Full Load vs CDC, DSQL 제약, 검증, 컷오버, 비용)를 먼저 읽고,
> [**사용자 매뉴얼**](docs/manual/ko/README.md)의 단계별 안내를 따라가세요.

## 한눈에 보기

<p align="center">
  <img src="docs/demo-ui.gif" alt="UI 데모 — 6단계 가이드 마이그레이션 워크플로우" width="720">
</p>

두 개의 데이터 경로가 Aurora DSQL로 수렴합니다: 도구가 주도하는 일회성 **Full Load**와,
관리형 MSK Connect에서 돌아가는 선택적 연속 **CDC** 스트림. binlog/GTID 워터마크가 둘을
무손실로 이어 줍니다.

![아키텍처 다이어그램](deploy/architecture-aws-simple.png)

## 할 수 있는 것 / 없는 것

**✅ 할 수 있는 것**

- **평가** — MySQL 스키마를 introspect해 모든 객체를 분류(`AUTO`/`MANUAL`/`UNSUPPORTED`),
  작업량 추정·이름 충돌 감지 포함.
- **스키마 변환** — MySQL → DSQL DDL 변환·적용(타입 매핑, FK 제거, 비동기 인덱스, PK 전략),
  객체 트리에서 검토 후 적용.
- **Full Load** — 일관성 스냅샷을 스트리밍으로 벌크 적재. 재개 가능, 대용량 대응.
- **CDC**(선택) — 거의 무중단 전환을 위한 연속 변경 복제.
- **Validation** — 행 수·체크섬·PK 대조로 소스↔타깃 일치 검증, 드리프트 보고.
- **AI 보조**(선택, 기본 off) — 어려운 항목의 변환 제안(검토 후에만 적용).

**❌ 하지 않는 것 / 범위 밖**

- **완전 자동·무중단이 아님** — 어려운 변환과 최종 **Cut over**는 사람이 결정·수행.
- **소스에 절대 쓰지 않음** — 읽기 전용, 롤백 앵커로 유지.
- **CDC는 DDL을 복제하지 않음** — 스키마 변경은 Schema Conversion으로.
- **크로스 리전 불가** — 소스와 타깃이 동일 리전이어야 함.
- **DSQL이 제외한 기능은 그대로 제약** — 외래 키·트리거·저장 프로시저 없음, 트랜잭션당 행
  한도, 값당 1 MiB 한도 등.

> 강제되는 한계의 전체 목록과 우회법은 사용자 매뉴얼
> [6장 — 한계](docs/manual/ko/06-limitations.md).

## 워크플로우

웹 UI는 **Connect**를 사전 단계로 한 6단계를 안내합니다:

`Connect → Migration plan → Evaluation → Schema Conversion → Data Migration → Validation → Cut over`

| 단계 | 하는 일 |
| --- | --- |
| Connect | 소스(RDS/Aurora MySQL)와 타깃(Aurora DSQL) 연결 정보 입력. 자격증명은 세션별 메모리에만 있다가 세션 종료 시 폐기. |
| 1. Migration plan | 이 마이그레이션에 **CDC를 쓸지**만 결정. 이 선택은 스트리밍 인프라를 미리 프로비저닝할지만 좌우하며, 되돌릴 수 있음(Full-Load만으로 시작한 뒤 CDC 추가 가능). |
| 2. Evaluation | 소스 **와** 타깃을 introspect해 호환성 리포트(`AUTO`/`MANUAL`/`UNSUPPORTED`) 생성. 작업량 추정·이름 충돌 감지·선택적 AI 전략 포함. |
| 3. Schema Conversion | 객체를 탐색하고 소스 vs 변환 DDL을 나란히 비교, 타깃에 적용(SKIP / REPLACE), 안전 재시도 포함. |
| 4. Data Migration | 사전 점검·테이블 선택 후 **Full Load**(워터마크 → export → 로드, 테이블별 진행률 + 에러 로그). 선택적으로 **CDC**(별도 cdc-stack)로 확장. |
| 5. Validation | 워터마크 시점 기준으로 타깃을 소스와 비교 — 행 수/체크섬 결과와 드리프트를 보고, 리포트 export. |
| 6. Cut over | Validation 통과 후 앱을 MySQL → DSQL로 전환하는 운영 런북 — 도구가 대신 실행하지 않는 유일한 단계. MySQL 소스는 롤백 앵커로 유지. |

각 단계는 상태(시작 안 함 / 진행 중 / 완료 / 실패)를 표시하며 독립적으로 실행/재실행할 수
있습니다. 기능 단위 상세는 [사용자 매뉴얼](docs/manual/ko/README.md)에 있습니다.

## 빠른 시작

같은 도구·같은 UI이며 **어디서 실행하느냐**만 다릅니다. 평가·소규모는 **로컬**, 실제
마이그레이션은 **ECS Fargate**를 권장합니다.

### 로컬 (가장 빠름)

내 머신이 마이그레이션 엔진이므로, 소스 MySQL과 DSQL **양쪽**에 도달할 수 있어야 합니다
(프라이빗 소스는 VPN/SSM 포워딩). AWS 자격증명은 실행하는 셸에서 사용 가능하면 됩니다
(`aws sso login`, `AWS_PROFILE=…`).

```bash
git clone <repo-url> mysql-dsql-migrator
cd mysql-dsql-migrator
uv sync                       # .venv 가상환경 생성·채움 (uv 필요)
cp .env.example .env          # 선택: 연결 정보 미리 채우기 (git-ignore 됨)
uv run mysql-dsql-migrator ui
```

기본적으로 `http://127.0.0.1:8080`에 바인딩됩니다. 출력된 URL을 열고 **Connect** 단계부터
시작하세요.

### ECS Fargate (실제 마이그레이션)

CloudFormation으로 app-stack을 배포하면(이미지 빌드 불필요 — 게시된 ECR Public 이미지 사용)
도구가 내 **VPC 안**의 단일 태스크 Fargate 서비스로 뜨고, 출력된 ALB URL로 UI에 접속합니다.
이때 **마이그레이션 트래픽은 전부 AWS 내부에서 발생**(소스 → Fargate → DSQL)하고 내 브라우저는
UI만 띄우므로, 대용량 마이그레이션과 프라이빗 소스에 적합합니다.

**전체 절차: [`deploy/DEPLOYMENT.ko.md`](deploy/DEPLOYMENT.ko.md)**(빠른 배포, 파라미터,
Dev/Test vs Prod, DNS·Cognito, teardown, 문제 해결).

## 아키텍처

이 도구는 운영자가 고객 환경 안에서 실행하는 **Python 앱**(NiceGUI UI + import 가능한 엔진)으로,
평가 → 변환 → 일관성 스냅샷 벌크 로드 → 검증을 수행합니다. 배포 시 단일 태스크
**Amazon ECS Fargate** 서비스로 **HTTPS ALB**(기본 `internal`, 선택적 Cognito) 뒤에서 돌고,
이미지는 **Amazon ECR**에서 가져옵니다.

[![전체 AWS 아키텍처 토폴로지](deploy/architecture-aws.png)](deploy/architecture-aws.png)

> 다이어그램을 클릭하면 원본 해상도로 열립니다.

- **AI 보조는 컨트롤 플레인만** — 켜면 Amazon Bedrock이 변환 제안·CDC 준비도·DLQ 분류를 더해
  주지만, Full Load / CDC 행 데이터는 보지도 건드리지도 않고 스키마/DDL/플랜 메타데이터만
  사용합니다. 기본 off, 서드파티 API 키 없음(범위 제한된 `bedrock:InvokeModel`).
- **CDC는 선택적 별도 경로**(`cdc-stack`) — Amazon MSK + Debezium → 관리형 MSK Connect 위의
  **커스텀 Aurora DSQL 싱크 커넥터**([`connectors/dsql-sink/`](connectors/dsql-sink)). 표준
  JDBC 싱크로는 DSQL의 단기 IAM 토큰·구문 단위 OCC 재시도·≤3,000행 배치를 감당할 수 없어 직접
  만들었습니다. 이 도구는 컨트롤 플레인만 맡고 싱크 실행 자원은 직접 운영하지 않습니다.

> 더 보기: [CDC와 DSQL 제약](docs/manual/ko/04-cdc-and-dsql-constraints.md) ·
> [성능과 튜닝](docs/manual/ko/07-performance-and-tuning.md).

<details>
<summary><b>사용되는 AWS 서비스</b> (app-stack은 항상, cdc-stack은 선택)</summary>

마이그레이션 **소스**(RDS / Aurora MySQL)는 고객 소유이며 두 스택 모두와 무관합니다.
Debezium은 MSK Connect *위에서* 실행되는 오픈소스 소프트웨어입니다.

**컨트롤 플레인 & 공유 (app-stack)**

| 서비스 | 역할 |
| --- | --- |
| Amazon ECS (Fargate) | 단일 태스크 컨트롤 플레인 앱(NiceGUI + 엔진)을 실행. |
| Amazon ECR | 앱 컨테이너 이미지를 저장(기본은 ECR Public 이미지). |
| Elastic Load Balancing (ALB) | 앱으로 전달하는 HTTPS 진입점(기본 `internal`). |
| Amazon Route 53 | 앱 도메인 DNS(공개 도메인 사용 시, 운영자 제공). |
| AWS WAF | ALB 앞단의 웹 보호(공개 노출 시 권장). |
| Amazon Cognito | ALB의 OIDC 인증 게이트(공용 인터넷 노출 시 필수). |
| AWS Certificate Manager | ALB HTTPS 리스너용 TLS 인증서. |
| Amazon VPC | 프라이빗 서브넷, 보안 그룹, NAT / VPC 엔드포인트. |
| AWS IAM | 최소 권한 역할 및 DSQL IAM 토큰 인증. |
| AWS Secrets Manager | UI 세션 쿠키 서명 시크릿(자동 생성). 기존 소스 자격증명 시크릿 재사용은 선택. |
| Amazon Aurora DSQL | 마이그레이션 타깃(PostgreSQL 호환, IAM 인증, OCC). |
| Amazon S3 | Full Load 스테이징, 커넥터 플러그인 아티팩트, CodeBuild 소스. |
| Amazon CloudWatch (Logs) | 앱·커넥터 로그, CDC lag / 메트릭. |
| Amazon Bedrock | 선택적 AI 보조(컨트롤 플레인만). |
| AWS CloudFormation | 두 스택의 IaC. |

일반 배포는 ECR Public 이미지를 그대로 사용하므로 빌드가 없습니다. **AWS CodeBuild**는 런타임
구성요소가 아니라, 로컬 Docker가 없는 제한된 네트워크에서 자체 이미지를 빌드해야 할 때만 한 번
쓰는 선택적 빌드 도구(`deploy/codebuild.yaml`)입니다.

**선택적 CDC 데이터 플레인 (cdc-stack)**

| 서비스 | 역할 |
| --- | --- |
| Amazon MSK (Serverless) | Kafka 백본: PK로 파티셔닝된 테이블당 토픽 + DLQ 토픽. |
| Amazon MSK Connect | Debezium 소스와 커스텀 DSQL 싱크 커넥터를 호스팅하는 관리형 Kafka Connect(JSON 컨버터, `schemas.enable=true` — 스키마 레지스트리 불필요). |
| AWS Lambda | in-VPC 오프셋 시더(CFN 커스텀 리소스) — 무손실 핸드오프를 위해 Debezium GTID 워터마크를 자동 시드. |
| Amazon VPC (전용) | CDC는 자체 VPC에서 실행되어 소스 MySQL에 프라이빗하게 도달. |

</details>

## 사전 요구사항

- 스키마·데이터를 읽을 수 있는 사용자를 가진 소스 **RDS / Aurora MySQL**.
- 소스와 **동일 리전**의 타깃 **Aurora DSQL** 클러스터(IAM 토큰 인증, 비밀번호 없음).
- 표준 체인(환경 변수 / `~/.aws` / 프로필)으로 도달 가능하고 `dsql:DbConnect` 권한이 있는
  **AWS 자격증명**. 선택적으로 `secretsmanager:GetSecretValue`, `bedrock:InvokeModel`.
- **로컬 실행 시에만:** Python 3.10+(3.12 고정), [`uv`](https://docs.astral.sh/uv/).

> 소스 DB·CDC 설정(binlog 등)을 포함한 전체 체크리스트:
> [사용자 매뉴얼 §1.1](docs/manual/ko/01-setup.md).

## 설정 (고급 — 보통은 건드릴 필요 없음)

모든 작업은 UI에서 이뤄지고 합리적 기본값이 적용됩니다. 아래는 자동화·튜닝용 운영자 레퍼런스로,
환경 변수에서 읽으며(config 파일 없음, 자격증명 미영속화) Fargate에서는 ECS 태스크 정의에
설정합니다. Full Load / Validation 병렬수 4개는 사이드바의 **Performance tuning** 컨트롤에서
**런타임에** 재튜닝할 수도 있습니다(재배포 불필요, 재시작 시 리셋).

| 변수 | 기본값 | 설명 |
| --- | --- | --- |
| `DSQL_MIGRATOR_APP_HOST` | `127.0.0.1` | UI가 바인딩할 호스트/인터페이스. |
| `DSQL_MIGRATOR_APP_PORT` | `8080` | UI가 수신할 포트. |
| `DSQL_MIGRATOR_AWS_REGION` | _(미설정)_ | boto3 클라이언트용 AWS 리전. |
| `DSQL_MIGRATOR_AWS_PROFILE` | _(미설정)_ | 선택적 글로벌 AWS named profile. 미설정 시 표준 체인으로 폴백. 프로필 이름(비밀 아님)만 저장. |
| `DSQL_MIGRATOR_JOB_STATE_PATH` | `job_state.sqlite` | Full Load 작업 스냅샷(상태, 테이블별 진행률, 워터마크) — 재시작 후 재개용. |
| `DSQL_MIGRATOR_ACTIVITY_LOG_PATH` | `migration_activity.log` | 구조화된 활동 로그(이벤트당 UTC 타임스탬프 JSON 한 줄); UI에서 다운로드, 크기 제한·회전(~20 MB × 백업 4개). |
| `DSQL_MIGRATOR_SESSION_STATE_PATH` | `session_state.sqlite` | 세션별 비밀 아닌 워크벤치 상태 — 재연결 브라우저가 이어서 작업. `DSQL_MIGRATOR_STORAGE_SECRET`과 함께 사용. |
| `DSQL_MIGRATOR_STAGING_BUCKET` | _(미설정)_ | Full Load 스테이징용 S3 버킷(스트리밍 멀티파트 업로드 — 대형 테이블 확장 경로). 미설정 시 제한된 로컬 임시 CSV(개발 / 소형). |
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | `4` (≤16) | 동시에 로드하는 테이블 수. 총 DSQL 연결을 클러스터 쿼터 안에서 유지. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | `8` (≤32) | 테이블당 in-flight `INSERT … ON CONFLICT` 배치 수. 높을수록 처리량↑, OCC(40001) 충돌↑. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | `2000` (≤3000) | 배치 쓰기당 행 수, DSQL의 트랜잭션당 3000행 한도로 하드캡. |
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | `4` (≤32) | Validation에서 동시에 비교하는 테이블 수. `1` = 순차. |
| `DSQL_MIGRATOR_LOG_LEVEL` | `INFO` | 시작 로그 레벨. `DEBUG`는 실패 이벤트에 stacktrace(콜 스택만) 추가. 런타임에 **Diagnostics**에서도 변경 가능. |
| `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` | `false` | 활동 로그 이벤트를 stdout에도 미러링(ECS에서는 → CloudWatch). 런타임에 **Diagnostics**에서 토글 가능. |
| `BEDROCK_MODEL_ID` | `us.anthropic.claude-sonnet-4-6` | AI 보조용 Bedrock 모델 / 추론 프로파일 id. |
| `BEDROCK_REGION` | _(미설정)_ | Amazon Bedrock 호출용 리전. |

AI 보조는 기본 off이며 UI에서 켭니다. UI는 Bedrock 도달 가능 여부를 확인하고 조치 가능한 실패
이유를 보고하는 **Verify AI access** 사전 점검도 제공합니다. 튜닝 노브의 배경은 매뉴얼
[성능과 튜닝](docs/manual/ko/07-performance-and-tuning.md).

## 프로젝트 구조

| 경로 | 내용 |
|---|---|
| `src/dsql_migrator/core/` | import 가능한 마이그레이션 엔진(UI 의존성 없음). |
| `src/dsql_migrator/ui/` | NiceGUI 웹 애플리케이션 — **주 인터페이스**. |
| `src/dsql_migrator/cli/` | 자동화용 명령줄 진입점. |
| `connectors/dsql-sink/` | 커스텀 Aurora DSQL Kafka Connect **싱크 커넥터**(Java; 선택적 CDC 플러그인). |
| `deploy/` | `Dockerfile`, CloudFormation 템플릿, 빌드/teardown 스크립트, 다이어그램. [`deploy/DEPLOYMENT.ko.md`](deploy/DEPLOYMENT.ko.md) 참고. |
| `docs/manual/` | 단계별 사용자 매뉴얼(영·한·일). |

## 배포

이 도구는 고객의 프라이빗 RDS/Aurora와 DSQL에 고객의 IAM 컨텍스트로 연결하므로 **고객 환경
안에서(단일 테넌트)** 실행됩니다 — 프로덕션에서는 `deploy/cloudformation.yaml`로 배포하는 단일
태스크 **ECS Fargate** 서비스(이미지 빌드 없음). 선택적 스트리밍 CDC는 별도 **cdc-stack**입니다.

**▶ 전체 단계별 절차: [`deploy/DEPLOYMENT.ko.md`](deploy/DEPLOYMENT.ko.md).**

> [!IMPORTANT]
> **단일 리전만 지원.** 이 도구는 Aurora DSQL을 제공하는 모든 리전에서 동작하지만, 소스(RDS /
> Aurora MySQL)와 타깃(Aurora DSQL)은 **동일 리전에 있어야 하며**(리전은 DSQL 엔드포인트에서
> 도출), 프로비저닝되는 모든 인프라 — 특히 소스에 프라이빗하게 도달해야 하는 CDC VPC — 가 그
> 리전에 배포됩니다. 크로스 리전 소스/타깃은 지원되지 않습니다.

## 버전 / 변경 이력

현재 버전: [`pyproject.toml`](pyproject.toml); 버전별 변경 내용:
[**CHANGELOG.ko.md**](CHANGELOG.ko.md).

## 라이선스

**Apache License 2.0**으로 배포됩니다 — [`LICENSE`](LICENSE)와 [`NOTICE`](NOTICE) 참고.
`connectors/plugins/` 아래에 미리 빌드된 서드파티 커넥터 아티팩트(Debezium 및 런타임 의존성)를
번들로 포함하며, 각 라이선스는 [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md)에 정리되어
있습니다. 번들된 의존성 중 MySQL Connector/J는 GPL-2.0(Universal FOSS Exception 포함)이므로
재배포 전 확인이 필요합니다.
