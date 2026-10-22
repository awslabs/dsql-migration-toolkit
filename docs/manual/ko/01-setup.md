# 1. 설정

_언어: [English](../en/01-setup.md) | **한국어** | [日本語](../ja/01-setup.md)_

> **이전:** [0. 시작하기 전에](00-before-you-begin.md)

이 장은 "Amazon RDS 또는 Aurora 데이터베이스(MySQL 또는 PostgreSQL)가 있다"에서 "도구가
브라우저에 열려 있고 소스와 Aurora DSQL 타깃 모두에 연결됐다"까지를 안내합니다.

> **이미 AWS에 배포했다면?** [`deploy/DEPLOYMENT.ko.md`](../../../deploy/DEPLOYMENT.ko.md)를
> 따라 UI가 이미 `AppUrl`에서 열려 있다면, [§1.5 연결](#15-소스와-타깃-연결)로 바로 넘어가세요.
> 이 장은 배포 가이드가 다루지 않는 로컬 실행도 함께 설명합니다.

도구를 실행하는 방법은 세 가지입니다:

- **로컬** — 평가나 비교적 작은 마이그레이션을 위해 노트북/워크스테이션에서 실행. 시작이 가장 빠름.
- **AWS(ECS Fargate)** — 실제 마이그레이션에서 대부분의 팀이 쓰는 배포 형태. Application Load
  Balancer 뒤의 웹 엔드포인트로 접속.
- **AWS(단일 EC2 호스트, 소스에서)** — 컨테이너/ECR나 AWS Lambda를 쓸 수 없는 계정용: VPC 안의
  단일 EC2 호스트에서 소스로 바로 실행(`git clone` + `uv sync` + **systemd** 서비스)하고, SSM
  포트포워딩으로 접속(ECS/ALB/이미지 없음). → §1.4 참조.

어느 방법이든 **동일한** 소스/타깃에 연결합니다. 도구 *프로세스*가 어디서 실행되느냐만 다릅니다.

---

## 1.1 사전 요구사항

**데이터베이스**

- 네트워크로 접근 가능한 소스 데이터베이스 — **Amazon RDS 또는 Aurora MySQL**, 혹은 **Amazon
  RDS 또는 Aurora PostgreSQL**. 스키마와 데이터를 읽을 수 있는 사용자면 됩니다. Evaluation,
  Schema Conversion, Full Load, Validation에는 읽기 전용으로 충분하며, MySQL 소스(그리고 Full
  Load만 하는 PostgreSQL 마이그레이션)에는 절대 쓰지 않습니다. `gtid_mode=ON`이 아닌 소스에서
  MySQL **Full load + CDC** 실행은 CDC 시작 지점을 기록한 직후 `FLUSH BINARY LOGS`도 한 번
  실행하므로, 그 실행에 쓰는 사용자에게는 `RELOAD`가 필요합니다(데이터를 쓰지 않고 테이블도 잠그지
  않으며, binlog가 새 파일로 넘어갑니다. [§3.5](03-full-load.md#35-워터마크--cdc로의-다리) 참조). 쓰기를
  하는 유일한 예외는 **PostgreSQL CDC**입니다. Full Load 일관성 지점에서 도구가 마이그레이션 대상
  테이블에 정확히 한정된 논리 복제 슬롯과 publication을 생성하고(AUTOCOMMIT, 소규모 허용 목록으로
  제한, 감사 기록), 정리(teardown) 시 삭제합니다. 이것이 도구가 어떤 소스에든 하는 유일한 쓰기입니다.
  - **지원 소스 엔진 및 버전** — 두 소스 경로(Schema Conversion + Full Load + CDC + cut over)
    모두 실제 인프라에서 end-to-end로 검증됨:
    - **RDS for MySQL** 5.7 / 8.0 / 8.4, **Aurora MySQL** 5.7(v2) / 8.0(v3) / 8.4.
      MySQL 5.7은 표준 지원이 종료됐지만(RDS/Aurora Extended Support 적용될 수 있음), 도구는
      마이그레이션 소스로 5.7을 완전히 지원합니다.
    - **RDS for PostgreSQL / Aurora PostgreSQL**, PG **13–16**(end-to-end 테스트됨). 도구에는
      엄격한 버전 게이트가 없습니다 — 서버 버전은 표시용으로만 읽으며 어떤 메이저 버전도 거부하지
      않습니다. PG 13–16이 검증된 범위입니다.
- 도구를 실행할 리전과 **동일한 리전**의 타깃 **Amazon Aurora DSQL** 클러스터. (DSQL은 IAM 토큰
  인증을 쓰므로 관리할 비밀번호가 없습니다.)

**로컬 실행 시**

- Python 3.10+ (프로젝트는 `.python-version`으로 3.12 고정).
- 의존성 관리를 위한 [`uv`](https://docs.astral.sh/uv/).
- 표준 자격증명 체인(환경 변수, `~/.aws`, 또는 명명된 프로파일)으로 도달 가능한 AWS 자격증명으로,
  **Aurora DSQL IAM 토큰 생성**(`dsql:DbConnect` / `dsql:DbConnectAdmin`) 권한 필요. 선택적으로
  `secretsmanager:GetSecretValue`(소스 자격증명이 Secrets Manager에 있을 때),
  `bedrock:InvokeModel`과 `bedrock:InvokeModelWithResponseStream`(AI 어시스트를 켤 때; 채팅은 응답을
  스트리밍하므로 앞의 것만으로는 부족합니다).

**AWS 배포 시** — 전체는 [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md) 참조. 요약은 §1.3.

> **DSQL 타깃 참고:** 설정 파일에 복사해 넣을 DSQL "엔드포인트 + 비밀번호"는 없습니다.
> 도구에 DSQL **클러스터 엔드포인트**와 AWS 신원을 주면, 도구가 연결마다 단기 IAM 토큰을 발급합니다.
> 실행에 사용하는 신원이 해당 DSQL 클러스터에 연결할 권한이 있는지 확인하세요.

**CDC(선택적 스트리밍 파이프라인) 사용 시** — 거의 무중단 전환을 위해 CDC를 쓸 때만 해당합니다.
**Full Load만** 하는 마이그레이션은 이 중 아무것도 필요 없습니다. 아래는 CDC가 소스의 변경
스트림 — **MySQL 바이너리 로그**, 또는 PostgreSQL 소스라면 복제 슬롯 + publication을 통한
**논리 디코딩 WAL** — 을 읽을 수 있게 하는 소스 측 요구사항입니다. 도구의 사전 점검 게이트가 CDC
시작 전에 각 항목을 검사해 무엇이 빠졌는지 정확히 알려 주지만, 설정 자체는 한 번 해 두는 소스 측
작업입니다.

> **관리형 RDS/Aurora는 직접 운영하는(community) 서버와 설정 방법이 다릅니다.** 직접 운영하는
> MySQL이나 PostgreSQL 서버라면 `my.cnf` / `postgresql.conf`를 편집하고 `SET GLOBAL`을 실행하지만,
> **Amazon RDS / Aurora에서는 둘 다 불가능합니다** — 서버 변수는 **파라미터 그룹**으로 설정하고
> (MySQL: `binlog_format` 등, **PostgreSQL: `rds.logical_replication`**), binlog 보존 같은 MySQL
> 운영 설정은 **RDS 저장 프로시저**(`mysql.rds_*`)로 바꿉니다. PostgreSQL의 활성화 파라미터는
> **정적(static)이라 재부팅이 필요합니다.** 아래 단계는 이 도구가 대상으로 하는 관리형(RDS/Aurora)
> 방식입니다.

- **MySQL 소스 — 바이너리 로깅이 ROW 포맷·full row image로 켜져 있어야 합니다** — `log_bin=ON`,
  `binlog_format=ROW`, `binlog_row_image=FULL`, 그리고 MySQL 8.0 이상에서는 `binlog_row_value_options`를
  비워 둡니다(`PARTIAL_JSON`은 JSON 업데이트를 CDC가 적용할 수 없는 diff로 기록합니다. 비운 뒤에는 소스를 재부팅하거나 애플리케이션의 커넥션 풀을 재활용하세요. 변경 전에 열린 커넥션은 계속 diff를 기록합니다). CDC의 **필수 요건**입니다(충족 안 되면 게이트가 CDC를
  실패 처리). 관리형 MySQL에서 설정하는 방법:
  - **RDS for MySQL:** `log_bin`을 **직접 켤 수 없고** `my.cnf`도 **편집할 수 없습니다.** 대신
    **자동 백업**을 켜면(백업 보존 기간 > 0) 바이너리 로깅이 켜지고, 그다음 인스턴스에 연결된 **커스텀
    DB 파라미터 그룹**에서 `binlog_format=ROW`와 `binlog_row_image=FULL`을 설정합니다.
    (`binlog_row_image`는 기본값이 `FULL`이지만 확실히 하려면 명시적으로 설정하세요.)
  - **Aurora MySQL:** `binlog_format`은 **클러스터 수준** 파라미터입니다 — **커스텀 DB *클러스터*
    파라미터 그룹**(기본 그룹은 변경 불가)에서 `ROW`로 설정한 뒤, `OFF`에서 바꿨다면 클러스터를
    **재부팅**하세요. 기본값이 `OFF`라 이 작업 전에는 바이너리 로깅이 꺼져 있습니다.
  - **Community / 직접 운영 MySQL(대조용):** 그쪽에서는 `log_bin`/`binlog_format`/`binlog_row_image`를
    `my.cnf`(또는 런타임 `SET GLOBAL`)에 설정하고 재시작하지만 — **RDS/Aurora에는 어느 것도
    해당하지 않습니다.**
- **PostgreSQL 소스 — 논리 디코딩이 켜져 있어야 합니다** — **`wal_level=logical`**(**필수 요건**,
  충족 안 되면 게이트가 CDC를 실패 처리). **RDS for PostgreSQL / Aurora PostgreSQL**에서는 커스텀
  DB(클러스터) 파라미터 그룹에서 정적 파라미터 **`rds.logical_replication=1`**을 설정한 뒤
  **재부팅**하세요(Aurora: 라이터를 재부팅). 직접 운영하는 PostgreSQL에서는 `postgresql.conf`에
  `wal_level=logical`을 설정하고 재시작합니다. (PostgreSQL에는 `binlog_format`/`binlog_row_image`에
  해당하는 것이 없습니다.)
- **복제 권한을 가진 소스 사용자.** **MySQL:** `SELECT`, `REPLICATION CLIENT`, `REPLICATION SLAVE`
  (그리고 초기 스냅샷 정리에 쓰이는 `RELOAD`, `LOCK TABLES`). `RELOAD`는 Full load + CDC 실행이
  `FLUSH BINARY LOGS`를 실행하는 데에도 쓰입니다. 소스가 `gtid_mode=ON`이 아닐 때 이 명령이 CDC 시작
  지점을 정확하게 맞춥니다([3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)). enhanced binlog를
  쓰는 Aurora MySQL에서는 그런 실행에 대신 `gtid_mode=ON`이 필요합니다. MySQL 8.0.19 이상에서는
  애플리케이션이 XA 트랜잭션을 쓰면 `XA_RECOVER_ADMIN`도 부여하세요. **PostgreSQL:** CDC 사용자는 논리
  복제 슬롯을 생성·읽을 수 있어야 합니다 — **superuser**이거나, **REPLICATION 역할 속성**
  (`pg_roles.rolreplication`)을 갖거나, **`rds_replication`** 멤버(RDS/Aurora에서는 REPLICATION
  속성을 직접 부여할 수 없음)이면 통과합니다. 스냅샷을 위해 마이그레이션 대상 테이블에 대한
  **SELECT** 권한도 필요합니다. (이는 community의 `REPLICATION` 객체 권한이 아닙니다.) admin 계정이
  아니라 전용 최소 권한 CDC 사용자를 쓰세요.
- **MySQL 소스 — CDC가 따라잡기 전에 로그가 삭제되지 않도록 binlog 보존을 늘리세요.** RDS/Aurora는
  기본적으로 바이너리 로그를 공격적으로 삭제합니다 — **Aurora MySQL은 24시간만 보존**하고, RDS for
  MySQL은 백업 보존 기간에 따릅니다. CDC는 Full Load 중 캡처한 **워터마크**부터 재개하므로, 그 위치의
  binlog가 **CDC 시작 시점에 여전히 존재해야** 합니다 — 게다가 CDC 스택(MSK + MSK Connect) 배포에만
  **약 15~20분**이 걸린 뒤에야 스트리밍이 시작됩니다. RDS 저장 프로시저로 넉넉한 창을 설정하세요(단위:
  시간, RDS for MySQL·Aurora MySQL 모두 동일):

  ```sql
  CALL mysql.rds_set_configuration('binlog retention hours', 168);  -- 예: 7일
  ```

  Full Load와 CDC 시작 사이의 간격 + 예상 따라잡기 시간을 넉넉히 덮는 창을 고르세요(7일이 안전한
  기본값). Aurora MySQL 최댓값은 **2160**(90일)이며, 전환 후 다시 줄여도 됩니다. 이제 게이트가
  보존이 너무 짧거나(24시간 미만) 미설정이면 **경고(WARN, 비차단)** 로 알려주므로 binlog가 삭제되기
  전에 알아챌 수 있습니다 — 다만 Full Load가 얼마나 걸릴지는 사용자만 알기에 차단하지는 않습니다.
- **PostgreSQL 소스 — 바꿔야 할 binlog 보존 설정이 없습니다.** 대신 도구가 Full Load 일관성
  지점에서 생성하는 **논리 복제 슬롯**이 해당 LSN부터 필요한 WAL을 **자동으로 고정(pin)**하므로 설정할
  것이 없습니다. 슬롯이 시작 위치를 잡아두므로 약 15~20분의 CDC 스택 프로비저닝도 문제가 되지
  않습니다. 대신 반대 방향의 위험이 있습니다: **비활성/미소비 슬롯이 계속 WAL을 고정해 소스 디스크를
  가득 채울 수 있으므로** WAL/슬롯 상태를 모니터링하세요 — 도구가 `wal_status`를 노출하며,
  `wal_status='lost'`는 슬롯이 무효화됐다는 뜻으로 → gapless 재개가 깨졌으니 → Start CDC가 차단하며 해결
  경로를 알려 줍니다([§4.3](04-cdc-and-dsql-constraints.md)): Full load + CDC에서는 **Drop & reload**로
  Full Load를 다시 실행하되, 스택의 커넥터를 전에 시작했다면 먼저 Delete CDC infrastructure 후 다시
  배포합니다.
- **MySQL 소스 — GTID는 권장이지만 필수는 아니며, Full Load 전에 정하세요.** `gtid_mode=ON`이면
  Debezium이 GTID로 위치를 잡습니다: CDC는 시작 지점과 함께 기록된 GTID 셋(Full Load 워터마크, 또는
  Manual 지점이면 **Fetch current position**) 다음부터 시작하고, 마지막으로 커밋한 GTID 셋 다음부터
  재개하므로 바이너리 로그 파일이 다른 복제본으로 장애 조치(failover)한 뒤에도 이어집니다. `OFF`,
  `OFF_PERMISSIVE`, `ON_PERMISSIVE`이면 binlog `file:position`을 쓰며, 한 소스에서는 똑같이
  동작합니다. Full Load부터 cut over까지 모드를 바꾸지 마세요: `gtid_mode`가 `ON`이 아닐 때 기록된
  시작 지점으로는 — 또는 `gtid_mode`가 `ON`이 아닐 때 마지막으로 시작한 CDC 스택으로는 — `ON`이 된
  뒤 CDC를 시작할 수 없습니다([6장](06-limitations.md)). 게이트는 GTID 부재를 정보성으로 보고합니다.
  다만 GTID가 없으면 Full load + CDC 실행은 `FLUSH BINARY LOGS`로 그 `file:position` 시작 지점을
  정확하게 맞추며, 여기에는 `RELOAD`가 필요합니다. enhanced binlog를 쓰는 Aurora MySQL에서는 도구가
  이에 의존하지 않으므로, 그런 소스의 Full load + CDC 실행에는 `gtid_mode=ON`이 필요하고(또는
  enhanced binlog를 꺼야 합니다 — 어느 쪽이든 라이터 재부팅이 필요합니다), 그때까지 **CDC start point
  can be made exact**가 실행을 차단합니다([§3.5](03-full-load.md#35-워터마크--cdc로의-다리)).
  (PostgreSQL에는 GTID / `file:position` 개념이 없습니다.)
- **PostgreSQL 소스 — CDC는 클러스터 라이터에서 실행해야 합니다.** 소스는 **스탠바이가 아니라
  라이터**여야 합니다(`pg_is_in_recovery()`가 false여야 함) — 스탠바이는 복제 슬롯을 호스팅할 수
  없으므로 CDC를 라이터 엔드포인트로 향하게 하세요.
- **PostgreSQL 소스 — 복제되는 모든 테이블에 사용 가능한 REPLICA IDENTITY가 필요합니다.**
  `DEFAULT`에는 `DEFERRABLE`이 아닌 유효한 기본 키가 필요하고(PostgreSQL은 키가 삭제된 뒤에도
  `DEFAULT`를 유지하므로 검사는 소스의 카탈로그를 읽습니다), `USING INDEX`에는 유효한 고유 인덱스가
  필요하며, `FULL`(`ALTER TABLE … REPLICA IDENTITY FULL`)은 어떤 테이블에도 동작하지만 PostgreSQL
  18에서 생성 컬럼이 있는 테이블은 예외입니다(거기서는 identity에 생성 컬럼이 들어가면 안 되므로 키에도
  들어가면 안 됨). 파티션된 테이블은 모든 파티션에 하나씩 있어야 합니다. 없으면 테이블이 publication에 들어간
  뒤 그 테이블의 모든
  UPDATE/DELETE가 소스에서 실패하므로, CDC 사전 요구사항 검사가 실패로 판정하고 Full Load는 소스에서
  아무것도 바꾸기 전에 멈춥니다(publication도 슬롯도 만들지 않음). 복제 슬롯 /
  `max_wal_senders`(walsender) **여유**는 비차단 **경고(WARN)**로 검사됩니다(슬롯 항목이 남아 있어도
  walsender 풀이 가득 차면 새 슬롯이 막힘).

---

## 1.2 로컬에서 실행

새로 클론한 뒤:

```bash
# 로컬 가상환경에 의존성 설치
uv sync

# 웹 UI 실행 (기본 127.0.0.1:8080 바인딩)
uv run mysql-dsql-migrator ui
```

브라우저에서 **http://127.0.0.1:8080** 을 엽니다.

선택 편의: `.env.example`을 `.env`로 복사해 연결 항목을 채우면, **Connect** 화면이 이를 미리
채워 매 세션 재입력하지 않아도 됩니다. `.env`는 git-ignore되며 로컬 개발 전용입니다.

```bash
cp .env.example .env
# .env 편집: 소스 DB host/port/user, 타깃 DSQL 엔드포인트, 리전 등
```

> 앱은 `reload=False`로 실행되므로 코드 변경을 **자동으로 다시 불러오지 않습니다(핫 리로드 없음)** —
> 편집을 반영하려면 재시작하세요. 도구 자체를 수정할 때만 해당됩니다.

---

## 1.3 AWS에서 실행 (ECS Fargate)

실제 마이그레이션에서는 대부분 도구를 **단일 태스크 ECS Fargate 서비스**로 Application Load
Balancer 뒤에(선택적으로 Amazon Cognito OIDC 인증) 배포하고, 컨테이너 이미지는 Amazon ECR에
둡니다. 파라미터화된 전체 CloudFormation 흐름은
[`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)에 있고, 핵심은:

```bash
# 1. 이미지 빌드 + 푸시. 로컬 Docker가 없으면 AWS CodeBuild 사용:
AWS_REGION=us-east-1 deploy/build_in_codebuild.sh      # 이미지 URI 출력

# 2. app-stack(ECS Fargate + ALB + IAM) 배포. 위 이미지 URI와
#    VPC/서브넷/인증서/DSQL/소스 정보를 파라미터로 전달.
#    정확한 `aws cloudformation deploy` 명령은 deploy/DEPLOYMENT.md 참조.
```

**배포 편의성 우선 설계:** 새 `git clone`만으로 최소 설정으로 배포 가능 — 커넥터 플러그인
아티팩트가 커밋돼 있고(Java/Maven 툴체인 불필요), 도구가 자체 S3 버킷을 만들어 아티팩트를 직접
업로드하며, 선택적 CDC 인프라는 자동 발견됩니다(VpcId처럼 추론 불가능한 것만 입력).

> **VPC와 서브넷은 배포하는 계정이 소유해야 합니다.** RAM으로 공유된(교차 계정) 서브넷은
> 지원하지 않습니다 — CDC 배포 역할의 EC2 권한이 이 계정의 리소스로 스코핑돼 있어서,
> 공유 서브넷에 커넥터의 네트워크 인터페이스를 만들 때 `AccessDenied`로 실패합니다.

> **보안 참고:** 배포된 앱은 **자체 인증을 하지 않으며** ALB의 선택적 Cognito 게이트에 의존합니다.
> `0.0.0.0/0`으로 전체 개방된 인터넷 노출 ALB를 Cognito **없이** 두면 배포 템플릿의 `Rules`
> (`CognitoRequiredWhenIngressOpen`)가 이를 막습니다. Cognito(`EnableCognitoAuth=true`)를 켜거나
> `AllowedIngressCidr`를 내 네트워크로 한정하세요.

### 고객 배포 권장 설정

스택은 파라미터화돼 있어 빠른 테스트라면 지름길을 택할 수도 있지만, 실제 배포에서는 아래가 더
안전하고 견고한 선택입니다. 각 항목은 [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)의
CloudFormation 파라미터에 대응합니다:

| 설정 | 권장값 | 이유 |
|---|---|---|
| `AlbScheme` | **`internal`** (권장) | 도구를 공용 인터넷에서 떼어 놓음 — VPN / Direct Connect / VPC 피어링으로 접속. `internet-facing`은 `AllowedIngressCidr`를 내 네트워크로 한정했을 때만 사용(`0.0.0.0/0`으로 열린 ALB는 Cognito 없이는 차단됨). |
| `EnableCognitoAuth` | **`true`** (권장; `AllowedIngressCidr=0.0.0.0/0`일 때만 **필수**) | 앱 자체 인증이 없으므로 Cognito가 유일한 게이트. `CognitoDomainPrefix`와 **`CognitoAdminEmail`**을 함께 설정하세요 — 템플릿이 셋을 함께 요구합니다. 유저풀에 self sign-up이 없어 첫 사용자가 없으면 아무도 로그인할 수 없는 앱이 되기 때문입니다. |
| `AllowedIngressCidr` | **내 네트워크로 한정** (권장) | ALB 접근 범위를 제한. `0.0.0.0/0`로 열어 두지 말 것. |
| `AssignPublicIp` | **`DISABLED` + NAT 게이트웨이 또는 VPC 엔드포인트** (운영 권장) | 퍼블릭 서브넷의 `ENABLED`는 NAT를 생략하는 **테스트 전용** 지름길. |
| 태스크 egress | **VPC 엔드포인트** (가능하면 권장) | DSQL / Secrets Manager / ECR / Logs (/ Bedrock)에 공용 경로 없이 프라이빗으로 도달. 아니면 NAT 게이트웨이. |
| 이미지 참조 | **immutable 태그 또는 digest** (권장) | 재현 가능한 배포 — 움직이는 `:latest` 피하기. |
| 활동 로그 CloudWatch 미러 | **켜기** (권장) | 영속 감사 추적 — 태스크의 `/tmp` 사본은 태스크 교체 시 사라짐. |
| 작업/세션 상태 | **S3 백업** — 관리형 버킷 (템플릿이 설정) | 태스크 교체 후에도 살아남음: 작업 화면 상태와 Full Load 기록이 유지됨. 그 순간 실행 중이던 적재는 중단되고, Retry unfinished tables가 끝나지 않은 테이블마다 처음부터 다시 적재. `/tmp` SQLite 경로는 버킷이 설정되지 않았을 때(로컬 실행)의 대체 경로일 뿐임. |

> *동작을 가장 빨리 보는* 길은 **Dev/Test 프로파일**(`internal` ALB,
> `EnableCognitoAuth=false`, 자체 서명 인증서) — 여전히 실제 Fargate이며 구성 요소만 더 적습니다.
> 평가 이상이면 **Prod 프로파일**(Cognito + 실제 도메인/인증서)로 승격하세요. 두 프로파일 모두
> `deploy/DEPLOYMENT.md`에 있습니다.

---

## 1.4 단일 EC2 호스트에서 실행 (소스에서)

**컨테이너/ECR나 AWS Lambda를 쓸 수 없는 계정**에서는 같은 도구를 **VPC 안의 단일 EC2 호스트에서
소스 그대로** 실행합니다 — 빌드하거나 가져올 이미지가 없습니다. 파라미터화된 전체 CloudFormation
절차(`deploy/cloudformation-ec2.yaml`)는
[`deploy/DEPLOYMENT.ko.md`](../../../deploy/DEPLOYMENT.ko.md#단일-ec2-호스트에서-실행-소스에서-lambda-free)에
있으며, 요지는:

- 호스트가 소스에서 부트스트랩합니다(`git clone` + `uv sync` + **systemd** 서비스) — **Docker·ECR
  없음**.
- UI에는 **SSM 포트포워드**(Session Manager)로 접속하므로 **ALB·공인 IP·인바운드 규칙이 필요 없고**,
  ACM 인증서나 Cognito도 없습니다.
- 앱 상태는 **보존형 EBS 볼륨**(S3 아님)에 있어 인스턴스 교체를 넘어 유지됩니다.
- CDC는 Kafka를 **인프로세스로** 시드하므로 **오프셋 시더 Lambda가 생성되지 않습니다**(CDC는 커넥터
  아티팩트를 위해 S3 플러그인 버킷은 여전히 자동 프로비저닝).

VPC 내 프라이빗 데이터 경로(소스 → EC2 → DSQL)는 Fargate와 동일하며 구성 요소는 훨씬 적습니다.
파라미터와 SSM 포트포워드 명령은
[배포 가이드](../../../deploy/DEPLOYMENT.ko.md#단일-ec2-호스트에서-실행-소스에서-lambda-free)를 참고하세요.

---

## 1.5 소스와 타깃 연결

도구를 열고 **Connect** 단계에서 시작합니다. 입력 항목:

| 항목 | 입력 내용 |
|---|---|
| **Source** | **소스 엔진**(MySQL 또는 PostgreSQL)을 고른 뒤 host, port, user, password — **또는** 이를 담은 Secrets Manager 시크릿 ARN/이름 — 을 입력. 기본 포트는 엔진에 따릅니다(MySQL은 **3306**, PostgreSQL은 **5432**). PostgreSQL은 연결할 단일 **데이터베이스**도 지정합니다. 두 엔진 모두 인증은 password 또는 Secrets Manager입니다(IAM 토큰 인증은 타깃 DSQL 전용). |
| **Target** | Aurora DSQL **클러스터 엔드포인트**, 리전, 데이터베이스(`postgres`로 고정, 읽기 전용 표시), 사용자명(기본 `admin`). **비밀번호 없음** — 도구가 IAM 토큰 생성. |

각 연결을 **테스트**합니다. 도구는:

- 소스를 **읽기 전용**으로 읽어 도달성·권한을 확인하고,
- DSQL IAM 토큰을 생성해 타깃 연결을 확인합니다.

**자격증명은 세션별 프로세스 메모리에만 존재합니다.** 디스크·로그·리포트·작업 상태에 절대 기록되지
않으며, 세션이 끝나면 폐기됩니다(이 도구의 엄격한 비타협 규칙입니다). 재시작하면 다시 입력합니다.

> **단일 리전.** 이 도구는 Aurora DSQL을 지원하는 어느 리전에서나 동작하지만, **소스와 타깃은
> 동일 리전에 있어야 하며** 크로스 리전 마이그레이션은 **지원되지 않습니다.** 도구도 그 리전에서
> 실행하세요.

두 연결이 모두 정상이면 곧바로 **Evaluation**으로 넘어갑니다 — 도구가 양쪽
DB를 조사(introspect)해 호환성 리포트를 만듭니다. 이후 안내 흐름은 Schema Conversion,
Data Migration(여기서 Full Load만 할지 CDC를 추가할지 고르고 실행), Validation, 그리고 마지막으로
**Cut over**(애플리케이션을 DSQL로 전환하는 런북)로 이어지며, 각각 다음 장에서 다룹니다.

---

**다음:** [2. Evaluation과 Schema Conversion →](02-evaluation-and-schema-conversion.md)
