# 0. 시작하기 전에

_언어: [English](../en/00-before-you-begin.md) | **한국어** | [日本語](../ja/00-before-you-begin.md)_

마이그레이션을 시작하기 **전에** 이 짧은 페이지를 읽으세요. Aurora DSQL은 소스 엔진(MySQL 또는
PostgreSQL)을 그대로 대체하는 드롭인 대체재가 **아니라** 고유한 규칙을 가진 분산형 엔진이므로, 몇 가지
사실이 첫 단계부터 계획을 좌우합니다. (PostgreSQL 소스는 DSQL에 더 가깝습니다 — 양쪽 모두 PostgreSQL-16
와이어 프로토콜을 사용합니다 — 그래도 DSQL의 제약을 그대로 받으므로 이종(heterogeneous) 마이그레이션
마음가짐은 여전히 유효하며, 다만 방언 격차가 MySQL보다 작을 뿐입니다.) 아래 각 항목은 한 줄 "반드시
알아야 할 것"과 이를 깊이 다루는 장으로 이어지는 링크입니다.

> 이 페이지는 **사전 점검 체크리스트**입니다. 상세한 각 장을 대체하지는 않으며, 어떤 방식을 *이미 정한 뒤에야*
> 제약을 발견하는 일을 막아 줍니다. 강제되는 전체 한계는 [6장 — 한계](06-limitations.md)에 있습니다.

---

## 사전 점검 체크리스트

- [ ] **소스와 타깃은 동일 AWS 리전에 있어야 합니다.** 이 도구는 Aurora DSQL을 지원하는 어느
      리전에서나 동작하지만, **소스와 타깃은 동일 리전에 있어야 하며** **크로스 리전 마이그레이션은
      지원되지 않습니다.** 선택적 CDC 파이프라인도 단일 리전/VPC에서 실행됩니다. 도구도 그 리전에서
      실행하세요. → [§1.5](01-setup.md#15-소스와-타깃-연결),
      [§6.2](06-limitations.md#62-마이그레이션-프로세스-한계)

- [ ] **소스는 거의 모든 경로에서 읽기 전용입니다.** MySQL, 그리고 PostgreSQL Full Load 전용
      마이그레이션에서는 도구가 소스에 절대 쓰지 않으며 읽기 가능한 사용자면 충분합니다. 유일한 예외는
      **PostgreSQL CDC**입니다: Full Load 일관성 지점에서 도구가 논리 복제 슬롯과 마이그레이션 대상
      테이블에만 한정된 퍼블리케이션을 생성하고 — teardown 시점에 다시 삭제합니다(자동 커밋, 소규모
      allowlist로 제한, 감사 기록). 이것이 도구가 어떤 소스에든 수행하는 유일한 쓰기입니다.
      → [§1.1](01-setup.md#11-사전-요구사항)

- [ ] **두 가지 소스 엔진을 지원합니다.** **RDS for MySQL**과 **Aurora MySQL**(**5.7 / 8.0 / 8.4**
      버전), 그리고 **RDS for PostgreSQL**과 **Aurora PostgreSQL**(PG **13–16** 검증; 도구는 PG 버전
      하한/상한을 강제하지 않음). 소스 엔진은 Connect 화면에서 선택합니다(MySQL 기본 포트 **3306**,
      PostgreSQL은 **5432**; PostgreSQL 소스는 단일 데이터베이스에 연결). 두 엔진 모두 비밀번호 또는
      **AWS Secrets Manager**의 사용자명/비밀번호로 인증합니다 — IAM 인증은 타깃 DSQL 전용입니다.
      → [§1.1](01-setup.md#11-사전-요구사항)

- [ ] **DSQL은 PostgreSQL 호환·IAM 인증·분산형입니다.** PostgreSQL 와이어 프로토콜을 쓰고, **단기 IAM
      토큰**(관리할 비밀번호 없음)을 사용하며, **낙관적 동시성**(락 없음)을 씁니다. 도구에 DSQL
      **클러스터 엔드포인트**와 그에 연결할 수 있는 AWS 신원을 줍니다. → [§1.1](01-setup.md#11-사전-요구사항),
      [§1.5](01-setup.md#15-소스와-타깃-연결)

- [ ] **DSQL로 마이그레이션되는 모든 테이블에는 기본 키(PK)가 반드시 있어야 합니다.** DSQL은 PK로
      데이터를 분산·저장하므로 PK가 필수이며, 도구의 Full Load도 PK 순서로 데이터를 읽습니다. PK 없는
      테이블은 마이그레이션할 수 없습니다 — Evaluation이 `UNSUPPORTED`로 표시하고 Full Load도 거부합니다.
      적재 전에 그런 테이블에 PK를 추가하세요. (단일 컬럼·복합 PK 모두 지원.)
      → [§6.1](06-limitations.md#61-aurora-dsql-기능-한계-스키마가-이에-맞아야-함)

- [ ] **DSQL은 소스 엔진(MySQL 또는 PostgreSQL)이 가질 수 있는 일부 기능을 의도적으로 제외합니다.**
      **트리거 / 저장 프로시저 / 이벤트** 없음, **트랜잭션당 행 수 제한(≤ 3000)**, **값당 1 MiB 제한**,
      `DECIMAL` **정밀도 ≤ 38**, **공간 타입** 없음. (**외래 키**는 반대로 DSQL이 지원·강제합니다 — 도구가
      이를 보존해 데이터 적재 후, CDC 마이그레이션이라면 cut over 시점에 다시 생성합니다.) 이를 직접 찾을
      필요는 없습니다 — **Evaluation** 단계가 스키마를 검사해 각각을
      `AUTO` / `MANUAL` / `UNSUPPORTED`로 권장 조치와 함께 플래그합니다. **PostgreSQL 소스**의 경우
      Evaluation은 DSQL이 저장할 수 없는 PG 전용 컬럼 타입 — 배열, 기하(geometric), 네트워크
      (`inet`/`cidr`/`macaddr`), `xml`, `money`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange,
      `enum`, composite, `pgvector` — 도 `UNSUPPORTED`로 플래그하니 리모델링하세요. 데이터 적재 전에
      `UNSUPPORTED` 항목을 해결하고 `MANUAL` 항목을 결정하도록 계획하세요.
      → [2장](02-evaluation-and-schema-conversion.md), [6장](06-limitations.md)

- [ ] **CDC는 선택이며, 실행 중 과금됩니다.** 스트리밍 CDC는 대규모이거나 거의 무중단인 전환에만
      필요하며, MSK + MSK Connect(경우에 따라 NAT 게이트웨이)를 프로비저닝해 내릴 때까지 비용이 듭니다.
      짧은 동결을 허용하는 일회성 마이그레이션이라면 **Full Load만**으로 충분하고 스트리밍 인프라를
      프로비저닝하지 않습니다. → [4장](04-cdc-and-dsql-constraints.md),
      [§10.1](10-conclusion.md#101-어떤-경로가-필요한가)

- [ ] **CDC는 소스 측 설정을 먼저 해야 하며 — 엔진마다 다릅니다.** CDC를 쓴다면 사전점검 게이트는
      소스가 준비될 때까지 CDC를 **차단**합니다. Full Load만 한다면 이 중 아무것도 필요 없습니다.
      → [§1.1](01-setup.md#11-사전-요구사항)
    - **MySQL 소스:** **바이너리 로깅을 ROW 포맷·full row image로 활성화**하고(`binlog_format=ROW`,
      `binlog_row_image=FULL`) **복제 권한**을 가진 사용자를 둡니다. 또한 Full Load 워터마크 시점의
      binlog이 CDC 시작 시까지 남아 있도록 **binlog 보존을 늘리세요**(이 항목은 게이트가 **경고만**
      하지만, 너무 짧은 보존은 실제 silent-gap 위험입니다). RDS/Aurora에서는 **파라미터 그룹**과
      `mysql.rds_set_configuration` **저장 프로시저**로 설정합니다 — community MySQL처럼
      `my.cnf`/`SET GLOBAL`이 아닙니다.
    - **PostgreSQL 소스:** **`wal_level=logical`**을 설정합니다(RDS/Aurora에서는 커스텀 DB/클러스터
      파라미터 그룹에 정적 파라미터 **`rds.logical_replication=1`**을 설정한 뒤 **재부팅** — Aurora는
      writer를 재부팅; 자체 관리형은 `wal_level=logical` 설정 후 재시작). **복제 권한**을 가진
      사용자(superuser, **REPLICATION** 역할 속성, 또는 RDS/Aurora의 **`rds_replication`** 멤버십)가
      필요하며, 소스는 standby가 아니라 클러스터 **writer** 엔드포인트여야 합니다(`pg_is_in_recovery()`
      = false). 또한 **복제 대상 모든 테이블에 사용 가능한 `REPLICA IDENTITY`가 필요합니다** — 기본 키가
      있으면 기본값으로 충족되고, 없으면 `ALTER TABLE … REPLICA IDENTITY FULL`을 설정하세요
      (`REPLICA IDENTITY NOTHING`은 거부됩니다). 복제 슬롯 / walsender **여유량**은 차단하지 않는
      **경고**로 확인합니다. **퍼블리케이션**과 **논리 복제 슬롯**은 도구가 자동으로 생성합니다.

- [ ] **CDC는 스키마가 아니라 데이터를 복제합니다.** CDC 중 소스 **DDL** 변경은 DSQL로 **전파되지
      않습니다** — Schema Conversion으로 직접 다시 적용합니다.
      → [§4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요)

- [ ] **자격증명은 세션 메모리에만 있습니다.** 디스크·로그·리포트에 절대 기록되지 않고 세션이 끝나면
      폐기됩니다 — 재시작하면 연결 정보를 다시 입력합니다. → [§1.5](01-setup.md#15-소스와-타깃-연결)

- [ ] **AI 어시스트는 선택이며 Amazon Bedrock으로만 가능합니다.** AI는 **기본 꺼짐**이고, 켜면 AWS
      자격증명(`bedrock:InvokeModel` 권한)으로 **오직 Amazon Bedrock을 통해서만** 동작합니다.
      **직접 API 키 입력은 지원하지 않습니다**(Anthropic/OpenAI 키 입력란 없음) — AI를 쓰는 유일한
      경로는 Bedrock입니다. 모든 제안은 **검토 전용**이며 AI는 데이터 경로에 절대 없습니다. 같은
      어시스턴트가 **모든 단계**에서 **AI DBA** 패널로 제공됩니다 — Full Load / CDC 실패 진단, DLQ
      트리아지, 사전점검 확인을 하는 read-only 도우미입니다.
      → [2장](02-evaluation-and-schema-conversion.md), [9장](09-query-validation.md)

- [ ] **(AWS 배포 시) 단일 태스크 컨트롤 플레인이며 인증은 선택입니다.** 일반적인 형태는 ALB 뒤의
      **ECS Fargate** 태스크 하나이며 ALB의 **선택적 Cognito** 게이트를 씁니다(인터넷에 노출되는 배포에선
      반드시 켜세요); AWS 호스팅 배포에서는 작업/세션 상태가 관리형 S3 버킷에 durable하게 보관돼 태스크
      교체 후 재개됩니다. **단일 EC2 호스트(소스에서)** 모드도 있습니다 — 상태는 보존 EBS 볼륨, 접속은
      SSM 포트포워딩, ALB/Cognito 없음. → [§1.4](01-setup.md#14-단일-ec2-호스트에서-실행-소스에서),
      [§6.3](06-limitations.md#63-배포-한계-aws-호스팅-형태)

---

## 마음가짐

이것을 업그레이드가 아니라 **이종(heterogeneous) 마이그레이션**으로 대하세요. 도구의 전체 역할은 DSQL의
차이를 처음에 명확히 드러내고(**Evaluation**), 가능한 것을 결정론적으로 변환하며(**Schema Conversion**,
**Full Load**, **CDC**), 결과를 증명하는 것(**Validation**)입니다. 위 체크리스트를 체득하면, 매뉴얼의
나머지는 그 세부일 뿐입니다.

---

**다음:** [1. 설정 →](01-setup.md)
