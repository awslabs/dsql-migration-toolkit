# 7. 성능, 튜닝, 그리고 이 설계의 이유

_언어: [English](../en/07-performance-and-tuning.md) | **한국어** | [日本語](../ja/07-performance-and-tuning.md)_

> **이전:** [6. 한계](06-limitations.md)

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
충돌에서 밀린 트랜잭션은 **재실행**해야 하며, AWS는 OCC 환경에서는 애플리케이션이 락 기반 DB보다 이
재시도 로직을 "**더 자주**" 거쳐야 한다고 명시합니다.

표준 JDBC 싱크는 `40001`에 **배치 전체**를 재시도합니다. 한정 병렬의 TB 규모에서는 잘못된 단위입니다:
~3000행 전부를 재제출하면 충돌하지 않은 99%+ 행의 읽기/쓰기 작업을 다시 지불하고, 트랜잭션이 걸치는 키
범위가 넓을수록 재시도 커밋 전에 *다른* 워커가 그 범위를 건드릴 확률이 커져, 부하가 높은 적재가
라이브락(livelock — 재시도만 반복하며 진행이 멈추는 상태)에 빠지기 쉬워집니다. 이 도구는 **문장 단위**로
재시도합니다: 충돌한 `INSERT … ON CONFLICT`만 재실행하므로, 각 충돌의 영향 범위가 그 문장 하나로 좁게
한정됩니다. 이것이 표준 JDBC 싱크 대신 커스텀 DSQL 싱크 커넥터가 존재하는 바로 그
이유입니다([4장 §4.1](04-cdc-and-dsql-constraints.md#41-파이프라인) 참조).

### 기본 키 전략 — 핫 파티션 회피

DSQL은 **기본 키로 스토리지를 파티셔닝·분산**하며, 문서는 명확합니다: **"무작위 기본 키를 선택하라…
단일 키에 경합을 늘리는 패턴을 피하라."** MySQL `AUTO_INCREMENT` PK는 단조 증가하므로, 모든 insert가
키의 가장 큰 쪽(항상 최근에 생성된 같은 키 구간)으로 몰립니다. 고처리량 적재 중 모든 워커가 한 파티션으로
수렴해, **행이 논리적으로 충돌하지 않아도** OCC 충돌률이 치솟고 쓰기 핫스팟이 생깁니다.

이 도구는 이를 **Evaluation**에서 드러내고(`AUTO_INCREMENT` → `MANUAL`), **Schema Conversion**에서 PK
전략을 제공합니다: 정수 PK 유지, **UUID** 변환, 또는 **캐시를 적용한 identity 컬럼**. 이렇게 하면 쓰기를
키 범위 전체로 분산할 수 있습니다. 동일 엔진(MySQL→MySQL) 마이그레이션은 결코 신경 쓸 필요 없는 DSQL
특유의 고려사항입니다.

### DSQL 트랜잭션 한도에 맞춘 배치 적재

DSQL은 트랜잭션당 하드 상한을 강제합니다: **≤ 3000행**, **수정 데이터 ≤ 10 MiB**, **≤ 5분**, 트랜잭션당
DDL 1개. 로더는 **≤ 3000행** + **8 MiB** 바이트 예산(10 MiB 상한 아래 여유)으로 배치하며, 65,535개
바인드 파라미터 한도에 맞춰 배치 크기가 한 번 더 제한됩니다. 이로써 두 함정을 피합니다:

- **행 단위** 로더는 *모든 행*마다 DSQL 트랜잭션 오버헤드(및 쓰기당 DPU 최소값)를 지불 — 배치로 분산하는
  것보다 몇 배 비쌉니다.
- **테이블 전체를 한 트랜잭션에** 넣는 로더는 **아예 성공할 수 없습니다** — 3000행 상한(과 대형 테이블의
  5분 상한)에 걸려 실패합니다.

### 적재 *후* 비동기 인덱스 빌드

DSQL은 **논블로킹** 인덱스 빌드를 위한 `CREATE INDEX ASYNC`를 제공합니다. 도구는 데이터를 **먼저**
적재한 뒤 보조 인덱스를 `CREATE INDEX ASYNC`(트랜잭션당 DDL 1개)로 빌드합니다. 적재 중 인덱스를 빌드하면
모든 `INSERT`가 각 보조 인덱스 항목의 쓰기 비용도 지불하고(전환 전 이후 CDC 변경이 덮어쓸 행 포함) 모든
쓰기에 유일성 읽기가 추가됩니다. 미루면 안정된 데이터셋 위에서 그 비용을 한 번만 지불합니다.

### 초기 복사는 벌크 로더, 전환은 스트리밍 CDC

DSQL은 노드가 저장소를 공유하지 않는(shared-nothing) 분산·서버리스 구조라, 타깃으로 삼을 PostgreSQL 논리
복제 슬롯이 없고, 범용 도구의
"full load"는 내부적으로 JDBC `INSERT`이며 DSQL 특화 OCC 처리가 **없습니다**. 도구의 전용 로더는 keyset
스트리밍(재개 가능, 한정 메모리), DSQL 한도 인지 배치, 문장 단위 OCC 재시도, PK 리매핑을 한 경로에
담습니다. 전환에는 **Debezium → MSK → 커스텀 싱크**가 소스 binlog와 적용을 분리합니다: Kafka가 변경을
내구성 있게 버퍼링하므로 싱크가 OCC 재시도 폭주 중 뒤처져도 이벤트를 잃거나 소스 binlog 로테이션을
막지 않습니다.

### 장기 CDC에서 단기 IAM 토큰 갱신

DSQL은 **IAM 토큰 인증만**(정적 비밀번호 없음) 쓰고, 토큰은 **단기**(~15분)이며 **연결은 60분 후
타임아웃**됩니다. 토큰 하나를 캐시한 장기 CDC 싱크는 커넥션 풀에서 연결이 회수(eviction)되거나 60분
타임아웃이 지난 뒤 *재연결*에 실패합니다 — 네트워크 오류처럼 보이지만 실은 만료된 토큰입니다. 커스텀 싱크는 **새 연결마다 새 토큰**을
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
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | 배치 쓰기당 행 수(DSQL 3000행 하드 상한). |

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

CDC 처리량은 **MSK 파티션 수 × 싱크 `tasks.max` × 워커 MCU·워커 수**에 비례해 늘어나며, 최종적으로는
파티션 수가 상한이 됩니다. 다만 부하가 높을 때 실제 상한은 핫 기본 키에서 발생하는 OCC이므로, 여기서도
PK 전략이 가장 중요합니다.

### AWS(ECS Fargate)에서도 — 네, 모두 튜닝 가능합니다

Full Load와 Validation 설정은 평범한 `DSQL_MIGRATOR_*` **환경 변수**이며 앱이 런타임에 읽습니다. Fargate
배포에서는 **ECS 태스크 정의의 컨테이너 `environment` 블록**(템플릿이 이미 `DSQL_MIGRATOR_LOG_LEVEL`,
`/tmp` 상태 경로 등을 설정하는 그곳)에 설정합니다 — 위 키들을 `deploy/cloudformation.yaml`의 컨테이너
environment(또는 자체 태스크 정의)에 추가하고 재배포하세요. CDC 설정은 도구가 cdc-stack을 배포할 때
전달하는 cdc-stack CloudFormation 파라미터입니다.

> **재배포 없이 실행 사이에 재튜닝.** 이 Full Load / Validation 값을 이리저리 바꿔 보며 테스트할 때는
> 사이드바 푸터의 **Performance tuning** 컨트롤(Diagnostics 옆)을 쓰세요. 로더와 검증기가 매 실행마다
> 설정을 다시 읽으므로, 여기서 바꾼 값은 **다음** Full Load / Validation에 즉시 반영됩니다 — 태스크 정의
> 수정·재배포 불필요. 앱 전역(단일 태스크)이며 재시작 시 배포/시작 값으로 리셋되므로, **영구히 유지**할
> 값은 태스크 정의 `environment`에 설정하고, UI 컨트롤은 **실험**용으로 쓰세요.

또한 **Fargate 태스크 CPU/메모리**(`ContainerCpu` /
`ContainerMemory`)를 병렬수에 맞게 사이징하세요. 메모리 사용량은 테이블 크기가 아니라 행 버퍼 크기, 즉
`table_parallelism × batch_parallelism × 약 8 MiB`로 정해집니다. 따라서 여러 테이블을 함께 적재하는 Full
Load라면 약 1 vCPU / 2 GiB가 합리적인 출발점입니다.

> **로컬 실행**도 동일 환경 변수를 읽습니다 — `mysql-dsql-migrator ui` 실행 전 셸이나 `.env`에 설정하세요.

---

## 7.3 개별 쿼리 튜닝

위의 병렬수 설정 외에, 개별 쿼리를 Aurora DSQL의 분산 실행 모델에 맞게 튜닝할 수 있습니다 — 기본 키가 곧
테이블이고, 필터 푸시다운이 비용을 좌우하며, 비용 단위는 (PostgreSQL의 `cost=`가 아니라) **DPU**입니다.
선택적 **Query Playground**는 MySQL 쿼리를 변환하고 `EXPLAIN` / `EXPLAIN ANALYZE`로 읽기 전용
프로브한 뒤, AI 보조를 켜면 **AI DBA**가 DSQL에 맞게 재작성하고 재테스트로 DPU 개선을 증명합니다.

> 전체 흐름은 [9장 — 쿼리 검증과 AI DBA](09-query-validation.md)를 참고하세요.

---

## 7.4 실측 예시 — §7.1·§7.2를 뒷받침하는 한 번의 실행

아래는 위 설계 근거가 실제로 관측되는지 확인하려고 라이브 인프라에서 한 번 돌려 본 예시입니다. **성능
규격이나 보장치가 아니라 방법을 보여 주는 예시**로만 읽어 주세요 — `scripts/measure_performance.py`로
누구나 자신의 환경에서 재현할 수 있습니다.

> [!note] 이 수치가 나온 조건
> RDS MySQL 8.0.42 소스 + Aurora DSQL 타깃 + MSK, `us-east-1`, 1회 실행. **하드웨어(소스 RDS 등급,
> Fargate/로컬 CPU·메모리, DSQL 웜 상태, 네트워크 RTT)를 고정하지 않았으므로** 절대 처리량·지연은
> 환경에 따라 달라집니다. 또 이 실행은 `AUTO_INCREMENT` 정수 PK 스키마 — §7.1에서 설명한 **핫 파티션
> 최악 조건** — 를 그대로 쓴 것으로, 이 장이 권장하는 **UUID·캐시 identity PK**를 쓰면 경합은 더
> 낮아집니다. 당신의 수치는 다를 것입니다.

**Full Load — 병렬수를 올려도 처리량보다 경합이 먼저 오른다(§7.2 가드레일).** 테이블·배치 병렬수를
둘 다 두 배로(4×8=32 → 8×16=128 연결) 올렸더니 처리량은 **약 +5%**만 늘었는데, **재시도가 한 번이라도
발생한 배치의 비율**은 약 1/3 늘었습니다(약 9.6% → 12.8%). 논리적으로 충돌하지 않는 행이라도 단조 증가
PK의 같은 키 구간으로 쓰기가 몰리기 때문으로, §7.1의 핫 파티션 설명과 정확히 일치합니다. 즉 **병렬수를
무작정 올리기보다 PK 전략을 먼저 손보는 것이 이득**입니다.

**CDC 복제 지연 — 기본 사이징에서의 하한선(latency floor).** 소스 커밋 → DSQL 가시화까지의 지연은,
꾸준한 부하에서 p50 약 0.8초·p95 약 1.3초, 버스트 부하에서 p50 약 0.5초·p95 약 0.7초로 관측됐습니다.
이는 **기본 CDC 사이징(MCU=1, 워커 1, 단일 컬럼 테이블)에서의 좋은 조건 하한선**이며, 다중 테이블·높은
변경률에서는 더 커질 수 있습니다. 처리량이 필요하면 §7.2의 CDC 파라미터로 확장하세요.

> **직접 재현:** `scripts/measure_performance.py full-load`(처리량·경합)와
> `scripts/measure_performance.py cdc-lag`(복제 지연)로 자신의 소스·타깃에 대해 같은 지표를 얻을 수
> 있습니다. 단, `full-load`는 대상 테이블을 **DROP 후 재생성**하므로(`--yes` 필요) **비프로덕션
> 타깃에서만** 쓰고, `cdc-lag`는 활성 CDC 파이프라인이 필요합니다.

---

**다음:** [8. 테스트 및 검증 →](08-testing-and-verification.md)
