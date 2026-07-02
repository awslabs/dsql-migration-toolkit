# 7. 성능, 튜닝, 그리고 이 설계의 이유

_언어: [English](../en/07-performance-and-tuning.md) | **한국어**_

이 장은 이 도구의 데이터 경로가 **왜** 이렇게 설계됐는지를 — Aurora DSQL이 실제로 동작하는 방식에
근거해 — 설명하고, 워크로드에 맞춰 병렬수를 **어떻게** 튜닝하는지 안내합니다. TB 규모 마이그레이션에 이
도구를 신뢰할지 평가 중이라면, 이것이 기술적 근거입니다.

> 아래 모든 설계 선택은 문서화된 Aurora DSQL 동작·한계에 매핑됩니다. 출처:
> [DSQL 쿼터/한계](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html),
> [동시성 제어](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html),
> [PostgreSQL→DSQL 마이그레이션 가이드](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html),
> [비동기 인덱스](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html),
> [기본 키](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html).

---

## 7.1 이 설계의 이유 (기술적 근거)

### 배치 단위가 아닌 문장 단위 OCC 재시도

Aurora DSQL은 **락 프리(lock-free)** 입니다: 행 락을 전혀 잡지 않고, 쓰기 충돌을 **커밋 시점**에
감지해 `SQLSTATE 40001`(직렬화 실패 — `OC000` 데이터 충돌 또는 `OC001` 스키마 충돌)을 반환합니다.
지는 트랜잭션은 **재실행**해야 하며, AWS는 OCC에서 애플리케이션이 락 기반 DB보다 이 로직을 "**더 자주**"
수행해야 한다고 명시합니다.

표준 JDBC 싱크는 `40001`에 **배치 전체**를 재시도합니다. 한정 병렬의 TB 규모에서는 잘못된 단위입니다:
~3000행 전부를 재제출하면 충돌하지 않은 99%+ 행의 읽기/쓰기 작업을 다시 지불하고, 트랜잭션이 걸치는 키
범위가 넓을수록 재시도 커밋 전에 *다른* 워커가 그 범위를 건드릴 확률이 커져 — 바쁜 적재를 라이브락으로
밀어붙입니다. 이 도구는 **문장 단위**로 재시도합니다: 충돌한 `INSERT … ON CONFLICT`만 재실행하므로 각
충돌이 국소적이고 한정됩니다. 이것이 표준 JDBC 싱크 대신 커스텀 DSQL 싱크 커넥터가 존재하는 바로 그
이유입니다([4장 §4.1](04-cdc-and-dsql-constraints.md#41-파이프라인) 참조).

### 기본 키 전략 — 핫 파티션 회피

DSQL은 **기본 키로 스토리지를 파티셔닝·분산**하며, 문서는 명확합니다: **"무작위 기본 키를 선택하라…
단일 키에 경합을 늘리는 패턴을 피하라."** MySQL `AUTO_INCREMENT` PK는 단조 증가라 — 모든 insert가
같은 "오른쪽 끝" 키 범위를 노립니다. 고처리량 적재 중 모든 워커가 한 파티션으로 수렴해, **행이 논리적으로
충돌하지 않아도** OCC 충돌률이 치솟고 쓰기 핫스팟이 생깁니다.

이 도구는 이를 **Evaluation**에서 드러내고(`AUTO_INCREMENT` → `MANUAL`), **Schema Conversion**에서 PK
전략을 제공합니다 — 정수 PK 유지, **UUID** 변환, 또는 **캐싱 있는 identity 컬럼** — 그래서 쓰기를 키
범위에 분산할 수 있습니다. 동일 엔진(MySQL→MySQL) 마이그레이션은 결코 신경 쓸 필요 없는 DSQL 특유의
고려사항입니다.

### DSQL 트랜잭션 한도에 맞춘 배치 적재

DSQL은 트랜잭션당 하드 한도를 강제합니다: **≤ 3000행**, **수정 데이터 ≤ 10 MiB**, **≤ 5분**, 트랜잭션당
DDL 1개. 로더는 **≤ 3000행** + **8 MiB** 바이트 예산(10 MiB 천장 아래 여유)으로 배치하며, 65,535
바인드 파라미터 한도로 추가 클램프됩니다. 이로써 두 함정을 피합니다:

- **행 단위** 로더는 *모든 행*마다 DSQL 트랜잭션 오버헤드(및 쓰기당 DPU 최소값)를 지불 — 배치로 분산하는
  것보다 몇 배 비쌉니다.
- **테이블 전체를 한 트랜잭션에** 넣는 로더는 **아예 성공할 수 없습니다** — 3000행 천장(과 대형 테이블의
  5분 한도)에 걸려 실패합니다.

### 적재 *후* 비동기 인덱스 빌드

DSQL은 **논블로킹** 인덱스 빌드를 위한 `CREATE INDEX ASYNC`를 제공합니다. 도구는 데이터를 **먼저**
적재한 뒤 보조 인덱스를 `CREATE INDEX ASYNC`(트랜잭션당 DDL 1개)로 빌드합니다. 적재 중 인덱스를 빌드하면
모든 `INSERT`가 각 보조 인덱스 항목의 쓰기 비용도 지불하고(전환 전 이후 CDC 변경이 덮어쓸 행 포함) 모든
쓰기에 유일성 읽기가 추가됩니다. 미루면 안정된 데이터셋 위에서 그 비용을 한 번만 지불합니다.

### 초기 복사는 벌크 로더, 전환은 스트리밍 CDC

DSQL은 shared-nothing·서버리스라 — 타깃으로 삼을 PostgreSQL 논리 복제 슬롯이 없고, 범용 도구의
"full load"는 내부적으로 JDBC `INSERT`이며 DSQL 특화 OCC 처리가 **없습니다**. 도구의 전용 로더는 keyset
스트리밍(재개 가능, 한정 메모리), DSQL 한도 인지 배치, 문장 단위 OCC 재시도, PK 리매핑을 한 경로에
담습니다. 전환에는 **Debezium → MSK → 커스텀 싱크**가 소스 binlog와 적용을 분리합니다: Kafka가 변경을
내구성 있게 버퍼링하므로 싱크가 OCC 재시도 폭주 중 뒤처져도 이벤트를 잃거나 소스 binlog 로테이션을
막지 않습니다.

### 장기 CDC에서 단기 IAM 토큰 갱신

DSQL은 **IAM 토큰 인증만**(정적 비밀번호 없음) 쓰고, 토큰은 **단기**(~15분)이며 **연결은 60분 후
타임아웃**됩니다. 토큰 하나를 캐시한 장기 CDC 싱크는 풀 eviction이나 60분 타임아웃 후 *재연결*에
실패합니다 — 네트워크 오류처럼 보이지만 실은 만료된 토큰입니다. 커스텀 싱크는 **새 연결마다 새 토큰**을
발급(15분 TTL, 2분 갱신 여유)하므로 수 시간 CDC가 인증에 멈추지 않습니다.

---

## 7.2 병렬수 튜닝

네 단계 모두 합리적인 한정 기본값으로 실행되며, 큰 하드웨어에서 처리량을 위해 올리거나 바쁜 소스를 보호하기
위해 낮출 수 있습니다. **단계마다 튜닝 방식이 다릅니다** — Full Load와 Validation은 앱 환경 변수로, CDC는
CloudFormation 파라미터로.

### Full Load

| 설정 (env var) | 기본값 | 상한 | 효과 |
|---|---|---|---|
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | 4 | ≤ 16 | 동시에 적재하는 테이블 수. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | 8 | ≤ 32 | 테이블당 동시 진행 `INSERT … ON CONFLICT` 배치 수. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | 배치 쓰기당 행 수(DSQL 3000행 한도로 하드캡). |

> **연결 쿼터 가드레일.** 총 동시 DSQL 연결 ≈ `table_parallelism × batch_parallelism`(기본 4 × 8 = 32).
> DSQL은 **클러스터당 최대 10,000 연결**을 허용하지만 **초당 신규 100 연결**만 가능하므로, 곱을 쿼터 내에
> 여유 있게 두고 두 값을 이유 없이 최대로 설정하지 마세요. 병렬수를 올리면 핫 키 범위의 **OCC 충돌률**도
> 오릅니다 — 좋은 PK 전략(위)과 함께 쓰세요.

### Validation

| 설정 (env var) | 기본값 | 상한 | 효과 |
|---|---|---|---|
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | 4 | ≤ 32 | 동시에 비교하는 테이블 수(각자 읽기 전용 소스 + 타깃 연결). `1` = 순차. |

너무 많은 동시 스캔으로부터 소스를 보호하기 위해 32로 제한.

### CDC (데이터 플레인)

CDC 병렬수는 앱 env가 아니라 **cdc-stack CloudFormation 파라미터**로 설정합니다(커넥터는 앱이 아니라
관리형 MSK Connect에서 실행):

| CloudFormation 파라미터 | 기본값 | 효과 |
|---|---|---|
| `SinkTasksMax` | 2 | 싱크 커넥터 쓰기 병렬수(**토픽 파티션 수로 상한**). |
| `SourceTasksMax` | 1 | Debezium 소스 태스크 — MySQL은 서버당 사실상 단일 태스크; 1로 유지. |
| `ConnectorMcuCount` | 1 | 워커당 MSK Connect 컴퓨트 단위(MCU)(1/2/4/8). |
| `ConnectorWorkerCount` | 1 | 커넥터당 MSK Connect 워커 수. |
| `SinkBatchMaxRows` | 3000 | 싱크의 DSQL 쓰기 트랜잭션당 행 수(**3000 초과 금지**). |

CDC 처리량은 **MSK 파티션 × 싱크 `tasks.max` × 워커 MCU/수**로 확장되며, 궁극적으로 파티션 수로
제한됩니다; 부하 시 실제 천장은 핫 기본 키의 OCC입니다 — 역시 PK 전략이 가장 중요합니다.

### AWS(ECS Fargate)에서도 — 네, 모두 튜닝 가능합니다

Full Load와 Validation 노브는 평범한 `DSQL_MIGRATOR_*` **환경 변수**이며 앱이 런타임에 읽습니다. Fargate
배포에서는 **ECS 태스크 정의의 컨테이너 `environment` 블록**(템플릿이 이미 `DSQL_MIGRATOR_LOG_LEVEL`,
`/tmp` 상태 경로 등을 설정하는 그곳)에 설정합니다 — 위 키들을 `deploy/cloudformation.yaml`의 컨테이너
environment(또는 자체 태스크 정의)에 추가하고 재배포하세요. CDC 노브는 도구가 cdc-stack을 배포할 때
전달하는 cdc-stack CloudFormation 파라미터입니다. 또한 **Fargate 태스크 CPU/메모리**(`ContainerCpu` /
`ContainerMemory`)를 병렬수에 맞게 사이징하세요: 메모리는 테이블 크기가 아니라
`table_parallelism × batch_parallelism × ~8 MiB` 행 버퍼로 한정되므로, 다중 테이블 Full Load에는
~1 vCPU / 2 GiB가 합리적 시작점입니다.

> **로컬 실행**도 동일 환경 변수를 읽습니다 — `mysql-dsql-migrator ui` 실행 전 셸이나 `.env`에 설정하세요.

---

## 7.3 개별 쿼리 튜닝

위의 병렬수 노브 외에, 개별 쿼리를 Aurora DSQL의 분산 실행 모델에 맞게 튜닝할 수 있습니다 — 기본 키가 곧
테이블이고, 필터 푸시다운이 비용을 좌우하며, 비용 단위는 (PostgreSQL의 `cost=`가 아니라) **DPU**입니다.
선택적 **Query Playground**는 MySQL 쿼리를 변환하고 `EXPLAIN` / `EXPLAIN ANALYZE`로 읽기 전용
프로브한 뒤, AI 보조를 켜면 **AI DBA**가 DSQL에 맞게 재작성하고 재테스트로 DPU 개선을 증명합니다.

> 전체 흐름은 [9장 — 쿼리 검증과 AI DBA](09-query-validation.md)를 참고하세요.

---

**다음:** [8. 테스트와 검증 →](08-testing-and-verification.md)
