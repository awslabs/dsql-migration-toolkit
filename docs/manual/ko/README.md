# mysql-dsql-migrator — 사용자 매뉴얼 (한국어)

_언어: [English](../en/README.md) | **한국어** | [日本語](../ja/README.md)_

이 도구로 **Amazon RDS / Aurora MySQL 또는 PostgreSQL** 데이터베이스를 **Amazon Aurora DSQL**로
마이그레이션하는 안내서입니다. 이 도구는 **두 가지 소스 엔진 — RDS / Aurora MySQL과 RDS / Aurora
PostgreSQL —** 을 지원하며, 이는 **Connect** 화면에서 선택합니다. 주 대상은 MySQL에는 익숙하지만
**Aurora DSQL을 이제 사용해 보려는 DB 운영(DB Operation) 담당자**입니다. DSQL은 분산 데이터베이스
설계로 인해 PostgreSQL과도 상당히 많은 부분이 다르며, 이 매뉴얼은 그 차이를 넘어 변환하는 일을 도구가
*어떻게* 돕는지 설명합니다. PostgreSQL 소스도 **완전히 지원**되며, 두 경로는 다릅니다 — MySQL 소스는
**이종(heterogeneous)** 변환이고, PostgreSQL 소스 → DSQL은 (둘 다 PostgreSQL-16 와이어를 쓰므로)
**거의 동일한(near-identity)** 변환입니다.

> 프로젝트가 처음이라면, 아키텍처와 사용 AWS 서비스 개요는 [최상위 README](../../../README.ko.md)를
> 먼저 읽어 주세요. 이 매뉴얼은 실제로 마이그레이션을 **실행하는** 과정을 단계별로 안내하는
> 동반 문서입니다.
>
> 이 매뉴얼은 도구가 **이미 실행 중**(로컬 또는 AWS)이라고 가정합니다. 아직 띄우지 않았다면 먼저
> [`deploy/DEPLOYMENT.ko.md`](../../../deploy/DEPLOYMENT.ko.md)로 배포한 뒤 돌아오세요 —
> [설정](01-setup.md) 장은 로컬 실행도 함께 다룹니다.

## 이 도구란

**결정론(deterministic) 우선 마이그레이션**을 수행하는 **웹 도구**(및 임포트 가능한 엔진)이며,
경로는 소스 엔진에 따라 달라집니다. **MySQL 소스**는 **이종(heterogeneous)** 경로인 MySQL →
PostgreSQL 방언 → DSQL 제약을 거치고, **PostgreSQL 소스**는 방언 단계를 건너뛰어 **거의 동일한
(near-identity)** PostgreSQL-16 → DSQL 경로(값 그대로 전달, 동일한 와이어, DSQL 제약과 미지원
타입만 반영)를 거칩니다. **MySQL 소스(및 PostgreSQL을 Full Load 전용으로만 쓰는 마이그레이션)에는
절대 쓰기를 하지 않습니다.** 유일한 예외는 **PostgreSQL CDC**로, Full Load 일관성 지점에서 도구가
정확히 마이그레이션 대상 테이블로 한정된 pgoutput 논리 복제 슬롯과 publication을 생성했다가 정리
(teardown) 시 삭제합니다(autocommit, 허용 목록 한정, 감사 기록). 이것이 도구가 소스에 하는 유일한
쓰기입니다. 마이그레이션은 **Connect**를 사전 단계로 두는 5단계 안내 흐름입니다:

```
Connect → 1. Evaluation → 2. Schema Conversion → 3. Data Migration → 4. Validation → 5. Cut over
```

Data Migration은 **Full Load**(도구 자체의 벌크 로더)와, 선택적으로 스트리밍 **CDC**(거의 무중단
전환을 위한 별도의 선택적 파이프라인)로 구성됩니다. 마지막 단계 **Cut over**는 Validation을 통과한
뒤 애플리케이션을 DSQL로 전환하는 운영 런북입니다.

## 매뉴얼 목차

| # | 장 | 배우는 내용 |
|---|---|---|
| 0 | [시작하기 전에](00-before-you-begin.md) | 사전 점검 체크리스트 — 첫 단계부터 계획을 좌우하는 반드시 알아야 할 사실(동일 리전 필수, 읽기 전용 소스 — 단 PostgreSQL CDC의 감사되는 슬롯/publication은 예외, DSQL이 제외한 기능, CDC는 선택·과금). **여기서 시작하세요.** |
| 1 | [설정](01-setup.md) | 사전 요구사항, 도구 실행 방법(로컬 또는 AWS), 소스/타깃 연결 방법. |
| 2 | [Evaluation과 Schema Conversion](02-evaluation-and-schema-conversion.md) | DSQL로 옮길 수 있는 것/없는 것을 평가하는 방식(AUTO / MANUAL / UNSUPPORTED, 작업량 추정, 이름 충돌)과 스키마 변환·적용. 완전한 **MySQL → DSQL 및 PostgreSQL → DSQL 타입·제약 참조**를 포함합니다(PostgreSQL 참조는 거의 동일하며, DSQL이 지원하지 않는 PG 타입 — 배열, 기하(geometric), 네트워크, xml, money, bit/varbit, range/multirange, enum, composite, tsvector/tsquery, pgvector — 은 자동 치환하지 않고 Evaluation과 Schema Conversion에서 표시합니다). |
| 3 | [Full Load](03-full-load.md) | 벌크 스냅샷 로드 동작 방식: 스트리밍 export, 다시 적용해도 안전한 배치 로드, 워터마크, 실패 격리 방식. |
| 4 | [CDC와 DSQL 제약](04-cdc-and-dsql-constraints.md) | 스트리밍 CDC 동작, 무손실 Full Load → CDC 핸드오프, 그리고 DSQL 제약(트랜잭션당 행 한도, 1 MiB 값 한도, OCC, IAM 인증)을 데이터 경로에서 처리하는 방식. |
| 5 | [Validation](05-validation.md) | 타깃이 소스와 일치함을 증명하는 방식: 행 수, 체크섬, 전체 PK 대조, 라이브 소스 드리프트. |
| 6 | [한계](06-limitations.md) | 계획에 반드시 반영해야 하는 실제 제약(DSQL 제약, 단일 리전 CDC, 단일 태스크 컨트롤 플레인). |
| 7 | [성능과 튜닝](07-performance-and-tuning.md) | 데이터 경로를 이렇게 설계한 이유(AWS 근거: OCC 재시도, 핫 파티션 PK, 트랜잭션 한도, 비동기 인덱스, IAM 토큰)와 Full Load / Validation / CDC 병렬수 튜닝 — 로컬 및 Fargate — 그리고 설계 근거를 뒷받침하는 실측 예시(재현 가능). |
| 8 | [테스트 및 검증](08-testing-and-verification.md) | 당신의 데이터에서 어긋날 수 있는 상황(큰 테이블·1 MiB 값·애매한 타입·OCC 경합·긴 스트림·무손실 핸드오프·드리프트)마다 도구가 대신 해 주는 일과, 그 결과를 어디서 확인하는지 — 그리고 일부러 까다롭게 만든 데이터로 실제 AWS에서 낸 100% 일치 결과. |
| 9 | [Query Converter와 AI DBA](09-query-validation.md) | 선택적 Query Converter: MySQL 쿼리 하나를 Aurora DSQL로 변환하고, 타깃에서 읽기 전용으로 테스트(`EXPLAIN` / `EXPLAIN ANALYZE` + DPU 비용)하며, **AI DBA**가 DSQL에 맞게 효율적으로 재작성하고 재테스트로 개선을 증명. |
| 10 | [결론](10-conclusion.md) | 어떤 경로를 언제 쓸지, 권장 end-to-end 흐름, 다음 단계. |
| 11 | [고객 FAQ](11-customer-faq.md) | 고객이 가장 많이 묻는 질문 — Full Load, CDC, 제약, 타입 매핑, 검증, 컷오버/롤백, 운영 — 을 도구의 실제 동작에 근거해 답하고 상세 장으로 연결. |
| 12 | [부록: 성능 테스트 결과](12-performance-test-results.md) | 튜닝 근거를 뒷받침하는 실측 Full Load / Validation / CDC 처리량 예시와 방법론. |

## MySQL·PostgreSQL 사용자를 위한 Aurora DSQL 안내

가장 먼저 알아 둘 점: **Aurora DSQL은 MySQL이 아니고, Aurora MySQL을 그대로 갈아 끼우는 대체재도
아닙니다.** 이름은 비슷하지만 완전히 다른 엔진입니다. 그래서 이 이동은 "버전 업그레이드"가 아니라,
규칙이 다른 새 데이터베이스로 옮기는 **이기종(異種) 마이그레이션**입니다.

MySQL을 쓰던 입장에서 미리 알아 두면 좋은 주요 차이는 다음과 같습니다.

| MySQL에서는 | Aurora DSQL에서는 | 그래서 무엇이 달라지나 |
|---|---|---|
| MySQL 프로토콜 | **PostgreSQL 프로토콜** | 접속 드라이버·SQL 방언이 PostgreSQL 계열로 바뀝니다. |
| 아이디/비밀번호로 접속 | **단기 IAM 토큰**으로 접속(비밀번호 없음) | 고정 비밀번호 대신, 수명이 짧은 토큰을 계속 발급받아 접속합니다(도구가 자동 처리). |
| 한 서버에서 락으로 동시성 제어 | **분산형 + 낙관적 동시성(OCC)** | 락을 잡지 않고, 커밋 시점에 충돌을 감지해 재시도합니다. |
| 트리거·저장 프로시저 사용 | **없음** | 서버측 로직은 애플리케이션 쪽으로 옮겨야 합니다. (외래 키는 DSQL이 지원·강제하므로 도구가 보존해 적재 후 다시 생성합니다.) |
| 큰 트랜잭션·큰 값도 대체로 허용 | **트랜잭션당 행 수 제한, 값당 1 MiB 제한** | 대량 쓰기는 나눠서, 초대형 값(예: 큰 LOB)은 미리 걸러야 합니다. |

PostgreSQL 사용자라면: DSQL이 PostgreSQL 와이어를 쓴다고 해도 **Aurora PostgreSQL을 그대로 갈아
끼우는 대체재는 아닙니다.** IAM 토큰 인증과 위의 트랜잭션당·값당 제한이 더해지고, 트리거·저장
프로시저·생성 열(generated column)·`DEFAULT` 지원이 빠진 **제약된 분산형 PostgreSQL**이므로, 평범한
Aurora PostgreSQL처럼 다룰 수는 없습니다. (이 DSQL 차이는 소스 엔진과 무관하게 적용됩니다.)

이런 제약은 DSQL이 **수평 확장**(대규모 분산)을 위해 의도적으로 택한 설계입니다. 걱정하지 않아도
됩니다 — 이 매뉴얼은 각 차이가 **실제로 문제가 되는 지점마다** 짚어 주고, 그 자리에서 **도구가
무엇을 대신 처리해 주는지**를 함께 보여 줍니다. 그러니 DSQL의 규칙을 미리 다 외울 필요 없이,
매뉴얼을 따라가며 필요할 때 이해하면 됩니다.
