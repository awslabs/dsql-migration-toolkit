# 6. 한계

_언어: [English](../en/06-limitations.md) | **한국어** | [日本語](../ja/06-limitations.md)_

> **이전:** [5. Validation](05-validation.md)

계획에 반드시 반영해야 하는 **실제로 강제되는** 한계입니다. 대부분은 Aurora DSQL 자체에서 비롯됩니다(분산형이라
수평 확장에 맞지 않는 기능을 의도적으로 제외했기 때문), 일부는 배포 방식에서 비롯됩니다. 어느 것도 런타임에서
갑자기 튀어나오지 않습니다. 도구가 **Evaluation** 단계에서 플래그하고, 처리할 수 있는 것은 처리하며, 처리할 수
없는 것은 **요란하게** 실패시킵니다.

---

## 6.1 Aurora DSQL 기능 한계 (스키마가 이에 맞아야 함)

| 한계 | 결과 | 도구 동작 |
|---|---|---|
| **외래 키(지원·강제됨)** | DSQL은 강제되는 외래 키를 지원합니다. 다만 참조/피참조 테이블에 대한 DML에는 추가 읽기 비용이 있고, `CASCADE`/`SET NULL`/`SET DEFAULT` 동작은 트랜잭션당 3,000행 한도에 포함됩니다. | FK를 제거하지 않고 **보존** — 적재 후 실행할 `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY`로 렌더링(`CREATE TABLE` 밖)해 데이터 적재 뒤 다시 생성하며, CDC 마이그레이션은 cut over 시점에 적용. Evaluation은 이를 **권고(RECOMMENDED, 필수 작업 아님)**로 분류하고, 원하면 떼어 낼 수도 있음(`preserve_foreign_keys=False`). |
| **소스 `CHECK` 제약** | MySQL `CHECK`(8.0.16+)는 타깃으로 변환되지 않음. | DDL에서 드롭되고 **MANUAL** 플래그 — DSQL 호환 `CHECK`를 직접 다시 추가하거나 앱에서 강제. (도구가 `ENUM`용으로 *생성*하는 `CHECK … IN (...)`은 영향 없음.) |
| **기본 키 필수** | PK 없는 테이블은 마이그레이션 불가. | **UNSUPPORTED** 플래그; Full Load도 차단(keyset export에 PK 필요). |
| **트리거/저장 프로시저/함수/스케줄 이벤트 없음** | 서버 측 로직은 옮겨지지 않음. | **UNSUPPORTED** — 애플리케이션으로 재구현(이벤트 → EventBridge/Lambda). |
| **네이티브 파티셔닝 없음** | DSQL이 직접 자동 분산. | 파티션 테이블 **MANUAL**(파티셔닝 제거). |
| **값당 1 MiB 한도** | ~1 MiB를 초과하는 단일 `TEXT`/`BLOB` 값은 저장 불가. | 1–8 MiB 값은 에러 로그/DLQ로 **격리(quarantine)**; > 8 MiB 컬럼은 **캡처 단계에서 제외**해야 함. 대형 LOB 컬럼은 `OVERSIZED_LOB`(MANUAL). |
| **`DECIMAL` 정밀도 > 38** | 더 높은 정밀도 미지원. | Evaluation은 **UNSUPPORTED**(`NUMERIC_PRECISION`)로 표시하지만, 변환된 DDL을 적용하면 Schema Conversion이 **`numeric(38,37)`로 clamp**(손실)하며 경고 — 스케일도 37로 제한. |
| **공간/geometry 타입** | `bytea`로 대체(원본 WKB가 Full Load·CDC 전 구간에서 그대로 보존됨). | 변환기가 각 컬럼을 `bytea`로 자동 대체하고, 평가기가 해당 테이블을 검토 대상 **MANUAL**로 표시. |
| **FULLTEXT / SPATIAL 인덱스** | 미지원. | **UNSUPPORTED**. |
| **테이블당 ≤ 255 컬럼, DB당 ≤ 1000 테이블** | 초과 시 미지원. | **UNSUPPORTED**(`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT`). |
| **테이블당 ≤ 24 인덱스** (PK가 포함되므로 보조 인덱스는 ≤ 23; MySQL은 64) | 초과분 `CREATE INDEX ASYNC`가 Full Load로 모든 행을 쓴 **뒤에** 실패. | 계획 단계에서 **MANUAL**(`TOO_MANY_INDEXES`)로 표시하고 Schema Conversion에도 동일 노트. |
| **기본 키·인덱스의 컬럼 수 ≤ 8** (MySQL은 16) | 더 넓은 키는 error 54011로 실패. | PK가 넓으면 **UNSUPPORTED**(`CREATE TABLE` 자체가 거부되어 아무 데이터도 적재되지 않음), 인덱스가 넓으면 **MANUAL**(`TOO_MANY_KEY_COLUMNS`). 변환은 적재 후 반드시 실패할 DDL을 내보내지 않고 그 인덱스를 **생략**하며, 어떤 인덱스인지 노트에 명시. |
| **키 합산 크기 ≤ 1 KiB** (PK 각각, 인덱스 각각) | DDL이 아니라 **`INSERT`/`UPDATE` 시점의 값**에 대해 검사 — 따라서 실제 키가 너무 긴 행만 실패(error 54000 `key size too large`). | *선언된* 폭이 이를 넘을 수 있으면 변환이 경고(**MANUAL** 권장)하며, utf8mb4 기준 문자당 4바이트로 계산해 해당 키와 최악의 크기를 명시. 차단하지 않음 — 선언은 넓어도 실제 값이 짧으면 정상 마이그레이션되므로. |
| **클러스터당 하나의 DB** | DSQL은 다중 DB가 아니라 스키마로 구성. | 다중 DB 소스 **MANUAL**(스키마로 통합 또는 클러스터 분리). |
| **`TRUNCATE` 없음; 트랜잭션당 DDL 한 개; 낙관적 동시성** | MySQL과 다른 쓰기/DDL 의미. | 투명하게 처리: TRUNCATE 대신 DROP+재생성, 단일 DDL 단위, 필요한 곳마다 `40001` 재시도. |
| **IAM 토큰 인증(비밀번호 없음); 단기 토큰** | 정적 DB 비밀번호 없음. | 도구(와 CDC 싱크)가 IAM 토큰을 자동 발급·갱신. |

> **스키마 설계의 결론:** 대형 blob, 서버 측 로직, 매우 높은 정밀도의 숫자는 DSQL에
> 맡기기 *전에* 데이터베이스 밖의 애플리케이션으로 옮기세요. 어떤 객체에 이 작업이 필요한지는
> Evaluation이 정확히 알려 줍니다.

---

## 6.2 마이그레이션 프로세스 한계

- **단일 리전 — 크로스 리전 마이그레이션 없음.** 도구는 DSQL을 지원하는 어느 리전에서나 동작하지만,
  **소스와 타깃은 동일 리전에 있어야 하며** 선택적 CDC 파이프라인은 단일 리전/VPC에서 실행됩니다. 크로스
  리전은 지원되지 않습니다.
- **CDC는 소스 측 사전 요건(binlog)이 있음.** CDC를 쓰려면 소스에서 바이너리 로깅이 **ROW 포맷,
  전체 행 이미지**(full row image)로 켜져 있고(`binlog_format=ROW`, `binlog_row_image=FULL`)
  **복제 권한**을 가진 사용자가 있어야 합니다. 사전 점검 게이트가 이 조건이 충족될 때까지 **CDC를
  차단**합니다. 또한 Full Load 워터마크 시점의 로그가 CDC가 시작될 때까지 남아 있도록 **binlog 보존
  기간을 늘려야** 합니다(Aurora MySQL은 기본적으로 24시간만 보존).
  RDS/Aurora에서는 `my.cnf`가 아니라 파라미터 그룹과 `mysql.rds_set_configuration`으로 설정합니다 —
  [§1.1](01-setup.md#11-사전-요구사항) 참조. (Full Load만 한다면 이 중 아무것도 필요 없습니다.)
- **CDC는 데이터를 복제할 뿐 DDL은 복제하지 않음.** CDC 도중 소스에서 발생한 스키마 변경은 DSQL로
  **전파되지 않으므로** 동일한 DDL을 DSQL에 직접 적용해야 합니다
  ([4장 §4.2](04-cdc-and-dsql-constraints.md#42-cdc는-스키마가-아니라-데이터를-복제--중요) 참조).
- **캐스케이드 FK 동작은 CDC로 복제되지 않음.** InnoDB의 `ON DELETE/UPDATE CASCADE`(및 `SET NULL`/
  `SET DEFAULT`)는 MySQL *내부*에서 발생해 binlog에 남지 않으므로 CDC가 적용할 수 없습니다 — 소스가
  캐스케이드한 자식 행이 타깃에 orphan으로 남습니다. 이 orphan은 조용히 어긋나 버리는 대신 **cut over
  시점의 외래 키 적용(`ADD CONSTRAINT`)을 차단**하며(Validation의 **고아 검사**가 먼저 잡아냄), 그래서
  cut over 전에 정리하게 됩니다. Evaluation이 해당 테이블을 표시합니다.
- **CDC는 배포되어 있는 동안 과금됨.** 스트리밍 파이프라인(MSK Serverless + MSK Connect, NAT 게이트웨이를
  만들었다면 그것까지)은 실행되는 내내 비용이 발생합니다. Cut over 후에는 cdc-stack을 내리세요. Full Load만
  수행하면 스트리밍 인프라가 프로비저닝되지 않습니다.
- **TINYINT(1) 범위 초과는 설계상 테이블 전체 실패.** `{0,1}`을 벗어난 `TINYINT(1)` 값은 조용히 `true`로
  뭉개지 않고, 해당 테이블의 Full Load를 요란하게 중단시킵니다. 소스 데이터를 정리(또는 해당 컬럼 제외)한 뒤
  다시 실행하세요.

---

## 6.3 배포 한계 (AWS 호스팅 형태)

- **단일 태스크 컨트롤 플레인.** 도구는 **하나의** ECS Fargate 태스크로 배포되며, 하나를 초과해 올리면
  안 됩니다. AWS 호스팅 배포에서는 **작업/세션 상태가 모두 관리형 S3 버킷**(`jobs/` + `sessions/` 프리픽스)에
  durable하게 보관되므로, 태스크가 교체돼도(배포, 크래시) 진행 상태를 잃지 않고 **재개**합니다 — 재개는
  마지막으로 기록된 상태 전이 지점으로 복귀하며, 진행 중이던 정확한 배치는 아닙니다. 단일 태스크 제한은
  상태 durability가 아니라 **단일 writer 오케스트레이션**(CDC 배포 조율 + 인메모리 세션 상태) 때문입니다;
  임시 스토리지의 로컬 SQLite는 로컬 개발 폴백일 뿐입니다.
- **앱 자체 인증 없음.** 앱은 ALB의 **선택적 Cognito** 게이트에 의존합니다. 인터넷에 노출되는 배포라면
  Cognito를 켜세요. 배포 템플릿은 Cognito 없이 인터넷에 노출하는 안전하지 않은 조합을 막습니다.
- **자격증명은 세션별로만, 메모리에만 존재.** 절대 디스크에 남지 않으며, 재시작하면 소스/타깃 연결 정보를
  다시 입력해야 합니다.

---

**다음:** [7. 성능과 튜닝 →](07-performance-and-tuning.md)
