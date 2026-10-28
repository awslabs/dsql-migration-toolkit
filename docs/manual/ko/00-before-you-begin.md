# 0. 시작하기 전에

_언어: [English](../en/00-before-you-begin.md) | **한국어** | [日本語](../ja/00-before-you-begin.md)_

마이그레이션을 시작하기 **전에** 이 짧은 페이지를 읽으세요. Aurora DSQL은 MySQL이나 PostgreSQL을 그대로
대체하는 드롭인 대체재가 **아니라** 고유한 규칙을 가진 분산형 엔진이므로, 이를 **이종(heterogeneous)
마이그레이션**으로 대하세요. (PostgreSQL 소스는 양쪽 모두 PostgreSQL-16 와이어 프로토콜을 쓰므로 더
가깝지만, DSQL의 제약은 그대로 적용됩니다.) 도구는 차이를 처음에 명확히 드러내고(**Evaluation**), 가능한
것을 결정론적으로 변환하며(**Schema Conversion**, **Full Load**, **CDC**), 결과를 증명합니다
(**Validation**). 아래 각 항목은 짧은 "반드시 알아야 할 것"과 이를 깊이 다루는 장으로 이어지는
링크이며, 강제되는 전체 한계는 [6장 — 한계](06-limitations.md)에 있습니다.

---

## 사전 점검 체크리스트

**범위와 접근**

- [ ] **소스와 타깃은 동일 AWS 리전에 있어야 합니다** — 도구도 그 리전에서 실행하세요. 크로스 리전
      마이그레이션은 **지원되지 않으며**, 선택적 CDC 파이프라인도 단일 리전/VPC에서 실행됩니다.
      → [§1.5](01-setup.md#15-소스와-타깃-연결),
      [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)

- [ ] **두 가지 소스 엔진을 지원합니다:** **RDS for MySQL** / **Aurora MySQL**(**5.7 / 8.0 / 8.4**)과
      **RDS for PostgreSQL** / **Aurora PostgreSQL**(PG **13–18** 모두 종단 간 검증; 버전별 엣지는
      [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계) 참고). 두 엔진 모두 비밀번호 또는
      **AWS Secrets Manager**의 사용자명/비밀번호로 인증합니다 — IAM 인증은 타깃 DSQL 전용입니다.
      → [§1.1](01-setup.md#11-사전-요구사항)

- [ ] **소스는 거의 모든 경로에서 읽기 전용입니다** — 읽기 가능한 사용자면 충분합니다. 쓰기는 CDC에서만
      일어납니다. `gtid_mode=ON`이 아닌 소스에서 MySQL **Full load + CDC** 실행은
      `FLUSH BINARY LOGS`를 한 번 실행하며(그 사용자에게 `RELOAD`가 필요), **PostgreSQL CDC**는
      논리 복제 슬롯과 publication을 생성하고(teardown 시 삭제), CDC 키가 바뀐 테이블에
      `REPLICA IDENTITY FULL`을 설정하며, Full Load 이후 선택한 테이블을 publication에
      추가합니다(자동 커밋, 소규모 allowlist로 제한, 감사 기록). 그 밖의 쓰기는 없습니다.
      → [§1.1](01-setup.md#11-사전-요구사항)

- [ ] **DSQL은 PostgreSQL 호환·IAM 인증·분산형입니다.** PostgreSQL 와이어 프로토콜을 쓰고, **단기 IAM
      토큰**(비밀번호 없음)과 **낙관적 동시성**(락 없음)을 씁니다. 도구에 DSQL **클러스터 엔드포인트**와
      그에 연결할 수 있는 AWS 신원을 줍니다. → [§1.1](01-setup.md#11-사전-요구사항),
      [§1.5](01-setup.md#15-소스와-타깃-연결)

- [ ] **자격증명은 세션 메모리에만 있습니다.** 디스크·로그·리포트에 절대 기록되지 않고 세션이 끝나면
      폐기되므로, 재시작하면 연결 정보를 다시 입력합니다. → [§1.5](01-setup.md#15-소스와-타깃-연결)

**스키마**

- [ ] **마이그레이션하는 모든 테이블에는 기본 키(PK)가 있어야 합니다.** DSQL은 PK로 데이터를 분산하고 Full
      Load도 PK 순서로 읽으므로, PK 없는 테이블은 마이그레이션할 수 없습니다 — Evaluation이
      `UNSUPPORTED`로 표시하고 Full Load도 거부합니다. 적재 전에 PK를 추가하세요(단일 컬럼·복합 PK 모두
      지원). → [§6.1](06-limitations.md#61-aurora-dsql-기능-한계-스키마가-이에-맞아야-함)

- [ ] **DSQL은 소스 엔진이 가질 수 있는 일부 기능을 의도적으로 제외합니다:** **트리거 / 저장 프로시저 /
      이벤트** 없음, **트랜잭션당 행 수 제한(≤ 3000)**, **값당 1 MiB 제한**, **공간 타입** 없음.
      (**외래 키**는 반대로 DSQL이 지원·강제합니다 — 도구가 이를 보존해 데이터 적재 후, CDC
      마이그레이션이라면 cut over 시점에 다시 생성합니다.) 직접 찾을 필요는 없습니다 — **Evaluation**이
      각각을 권장 조치와 함께 `AUTO` / `MANUAL` / `UNSUPPORTED`로 플래그합니다. **PostgreSQL 소스**의
      경우 DSQL이 저장할 수 없는 PG 전용 컬럼 타입도 다룹니다: 대부분은 같은 데이터를 담는 타입으로 바꿔
      (배열 → `jsonb`, `money` → `numeric`, `inet`/`enum` → `text`) 검토하게 하고, 대체 타입이 없는
      타입(composite, domain, pgvector 같은 확장 타입)만 `UNSUPPORTED`입니다. 데이터 적재 전에
      `UNSUPPORTED` 항목을 해결하고 `MANUAL` 항목을 결정하세요.
      → [2장](02-evaluation-and-schema-conversion.md), [6장](06-limitations.md)

- [ ] **PostgreSQL 행 수준 보안에는 모든 행을 읽는 마이그레이션 사용자가 필요합니다.** Aurora DSQL에는
      행 수준 보안이 없으므로 Evaluation은 정책이 있는 테이블을 `UNSUPPORTED`로 분류합니다(규칙은
      애플리케이션으로 옮겨야 함). 도구는 `pg_dump`처럼 `row_security = off`로 소스를 읽으므로,
      마이그레이션 사용자는 슈퍼유저이거나 `BYPASSRLS` 역할이거나 테이블 소유자(`FORCE ROW LEVEL
      SECURITY`가 아닐 때)여야 합니다 — 그렇지 않으면 Full Load와 Validation이 일부 행만 복사하는
      대신 그 테이블에서 멈춥니다. → [2장](02-evaluation-and-schema-conversion.md),
      [6장](06-limitations.md)

**CDC 선택**

- [ ] **CDC는 선택이며, 실행 중 과금됩니다.** 스트리밍 CDC는 대규모이거나 거의 무중단인 전환에만
      필요하며, MSK + MSK Connect(경우에 따라 NAT 게이트웨이)를 프로비저닝해 내릴 때까지 비용이 듭니다.
      짧은 동결을 허용하는 일회성 마이그레이션이라면 **Full Load만**으로 충분하고 스트리밍 인프라를
      프로비저닝하지 않습니다. → [4장](04-cdc-and-dsql-constraints.md),
      [§10.1](10-conclusion.md#101-어떤-경로가-필요한가)

- [ ] **CDC는 소스 측 설정을 먼저 해야 하며 — 엔진마다 다릅니다.** 사전점검 게이트는 준비될 때까지 CDC를
      **차단**하며, Full Load만 한다면 필요 없습니다. **MySQL:** ROW 포맷·full row image 바이너리 로깅,
      복제 권한을 가진 사용자, 충분한 binlog 보존(게이트는 **경고만** 하지만, 너무 짧은 보존은 실제
      silent-gap 위험입니다). **PostgreSQL:** `wal_level=logical`, 복제 권한을 가진 사용자, 클러스터
      **writer** 엔드포인트, 그리고 복제 대상 모든 테이블의 사용 가능한 `REPLICA IDENTITY`(`FULL`은 어떤
      테이블에도 동작하지만 PostgreSQL 18에서 생성 컬럼이 있는 테이블은 예외). RDS/Aurora에서는 파라미터
      그룹과 `mysql.rds_*` 저장 프로시저를 씁니다 — `my.cnf`/`SET GLOBAL`이 아닙니다.
      → [§1.6](01-setup.md#16-cdc를-위한-소스-준비)

- [ ] **CDC는 스키마가 아니라 데이터를 복제합니다.** CDC 중 소스 **DDL** 변경은 DSQL로 **전파되지
      않습니다** — Aurora DSQL에 대응하는 변경을 직접 합니다(추가되거나 이름이 바뀐 컬럼은
      **Fix target schema…**, 이제 NULL을 허용하는 컬럼은 `ALTER … DROP NOT NULL`, 타입·키 변경은 CDC를 중지한
      상태에서 다시 만들기. CDC가 스트리밍하는 동안 Schema Conversion의 Apply는 차단됨). 일부 변경은 dead letter
      없이 쓰이므로 검증하세요.
      → [§4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)

**도구 실행**

- [ ] **AI 어시스트는 선택이며 Amazon Bedrock으로만 가능합니다.** **기본 꺼짐**이고, 켜면 AWS
      자격증명(`bedrock:InvokeModel`과 `bedrock:InvokeModelWithResponseStream` 권한)으로 **오직 Amazon
      Bedrock을 통해서만** 동작하며, 선택한 모델은 AWS 계정에서 한 번 활성화되어 있어야 합니다(모델의 AWS
      Marketplace 약관(agreement) 수락 — 2장). **직접 API 키 입력은 지원하지 않습니다.** 제안은 모두
      **검토 전용**이며 AI는 데이터 경로에 절대 없습니다. 같은 어시스턴트가 **모든 단계**에서 read-only
      **AI DBA** 패널로 제공됩니다. → [2장](02-evaluation-and-schema-conversion.md),
      [9장](09-query-validation.md)

- [ ] **(AWS 배포 시) 단일 태스크 컨트롤 플레인이며 인증은 선택입니다.** 일반적인 형태는 ALB 뒤의
      **ECS Fargate** 태스크 하나이며 ALB의 **선택적 Cognito** 게이트를 씁니다(인터넷에 노출되는 배포에선
      반드시 켜세요). 작업/세션 상태는 관리형 S3 버킷에 보관돼 태스크 교체 후 재개됩니다. **단일 EC2
      호스트(소스에서)** 모드(상태는 보존 EBS 볼륨, SSM 포트포워딩, ALB/Cognito 없음)도 있습니다.
      → [§1.4](01-setup.md#14-단일-ec2-호스트에서-실행-소스에서),
      [§6.3](06-limitations.md#63-배포-한계-aws-호스팅-형태)

---

**다음:** [1. 설정 →](01-setup.md)
