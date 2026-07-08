# 7. 성능, 튜닝, 그리고 이 설계의 이유

_언어: [English](../en/07-performance-and-tuning.md) | **한국어** | [日本語](../ja/07-performance-and-tuning.md)_

> **이전:** [6. 한계](06-limitations.md)

이 장은 이 도구의 데이터 경로가 **왜** 이렇게 설계됐는지를 — Aurora DSQL이 실제로 동작하는 방식에
근거해 — 설명하고, 워크로드에 맞춰 병렬수를 **어떻게** 튜닝하는지 안내합니다. 대용량 마이그레이션에 이
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

표준 JDBC 싱크는 `40001`에 **배치 전체**를 재시도합니다. 한정 병렬의 대용량에서는 잘못된 단위입니다:
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

#### 복합 키 옵션 — 서버 측 한계를 실제로 움직이는 유일한 레버

이 옵션이 존재하는 이유: VPC 내부 실측에서 대형 테이블 Full Load는 **클라이언트** 측 튜닝을 아무리 해도 처리량이 거의 같은 수준에서 정체됐습니다 — 읽기 선행 prefetch 큐, PK 범위 샤딩, 배치 크기·쓰기 병렬수 증가 각각 처리량을 ~0% 움직였습니다. 한계는 클라이언트가 아니었습니다. DSQL의 `CommitLatency`는 p50 약 50 ms로 양호했지만, 주기적으로 **수 초(p99)와 수십 초(max)** 에 달하는 긴 꼬리(long tail)가 반복됐고, OCC 충돌률은 0에 가까운 상태였습니다. 이 긴 꼬리가 쓰기 **핫 파티션**의 전형적 증상입니다: 단조 증가 `AUTO_INCREMENT` 키에서는 모든 insert가 동일한 맨 오른쪽 키 범위에 집중되어, 행이 논리적으로 충돌하지 않아도 한 파티션이 쓰기를 직렬화합니다. 핫 파티션은 *서버 측* 한계이므로, 쓰기를 파티션 전체로 분산하는 변화만이 이를 해소할 수 있습니다 — 복합 키가 바로 그 변화입니다.

따라서 Schema Conversion은 테이블별 네 번째 전략을 제공합니다: 원래 키 앞에 고분산 컬럼을 직접 지정해 테이블의 타깃 기본 키를 **복합 키**로 전환 — 예: `(id)` 대신 `(customer_id, id)`. DSQL은 기본 키 순서로 행을 저장하므로, `customer_id`를 앞에 두면 insert가 하나의 맨 오른쪽 파티션으로 집중되지 않고 수많은 키 범위(고객 수만큼)에 분산됩니다. 주요 사항:

- **소스 MySQL 스키마는 절대 변경되지 않습니다** — 오직 DSQL 타깃 키만 바뀝니다. MySQL에 변경을 되돌릴 필요가 없는, 타깃 측 마이그레이션 결정입니다.
- **원래 키의 고유성이 유지됩니다.** 도구는 복합 PK와 함께 원래 키에 `CREATE UNIQUE INDEX ASYNC`를 생성하므로, 이전 키 기반 조회나 제약이 계속 유효합니다.
- **도구가 선택 사항의 유효성을 검사합니다.** 적용 전 DSQL 키 한도에 대해 검증하며(앞에 추가할 컬럼은 `NOT NULL`이어야 하고, 이미 키의 일부가 아니어야 하며, 복합 키는 ≤ 8개 컬럼, ≤ 1 KiB 내에 있어야 함), 선택 시점에 결과를 명확히 안내합니다: **전환 후 애플리케이션의 쿼리, 조인, upsert는 새 복합 키를 사용해야 하며, 앞에 추가한 컬럼은 불변이어야 합니다**(DSQL 기본 키는 제자리 업데이트가 불가).
- **Full Load와 CDC 모두 처리합니다.** Full Load의 멱등성 `INSERT ... ON CONFLICT`는 타깃 복합 키 기준으로 키를 맞추고, CDC는 커넥터나 플러그인 변경이 **전혀 필요 없습니다** — Debezium 소스가 (`message.key.columns`를 통해) 재키잉(re-keyed)되어 각 변경 레코드의 키가 타깃 복합 키와 일치하고, 싱크의 upsert/delete는 그대로 적용됩니다.

테이블의 쓰기가 명확히 핫 파티션 상태(낮은 OCC에서 부하 시 `CommitLatency` 긴 꼬리 발생)이고, **자연스러운 고분산 그룹핑 컬럼이 있을 때** 이 옵션을 사용하세요. 부하가 다른 요인 — 클라이언트 CPU, 또는 핫 파티션이 아닌 쓰기 왕복 지연 — 에 묶여 있다면 복합 키는 아무것도 바꾸지 않습니다. 움직일 서버 측 한계 자체가 없기 때문입니다. 먼저 측정하세요(§7.5 참조).

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

### 쓰기 풀보다 앞서 읽기 (bounded prefetch 큐)

하나의 소스 테이블은 **하나의** MySQL 커넥션에서 스트리밍됩니다: 다음 keyset 페이지를
가져오려면 다음 `SELECT … WHERE pk > :last LIMIT :page`와 행별 타입 변환이 필요합니다.
이 읽기가 쓰기를 디스패치하는 같은 스레드에서 인라인으로 돌면, 페이지 *N+1* 읽기는 페이지
*N*의 배치가 제출된 **뒤에야** 시작됩니다 — 읽기·쓰기가 직렬화되고, 각 읽기 동안 한정 쓰기
풀이 놀게 됩니다. 로더는 대신 **전용 리더 스레드**가 **bounded 큐**(깊이 ≈ 쓰기 병렬수의
2배)를 채워, 페이지 *N*의 `INSERT … ON CONFLICT` 배치가 아직 빠지는 **동안** 페이지 *N+1*을
읽습니다. 상한이 스트리밍 메모리 보장을 유지하고(큐가 차면 리더가 멈춤 — 무한정 앞서 못 감),
적재 순서는 불변입니다(배치는 여전히 고정 PK 범위에 매핑되어 stop/retry가 결정적).

이 겹침은 실제 마이그레이션이 도는 곳 — **in-VPC, 적정 태스크 CPU** — 에서 가장 큰 효과를
냅니다. 리더가 뒤에 숨길 수 있는 쓰기 측(DSQL로의 네트워크 왕복, 이미 병렬로 발행됨)이 거기서
지배적이기 때문입니다. in-VPC 4 vCPU 실측에서 큐를 끈 것보다 **약 19% 빨랐습니다**. 기본
켜짐이며, 측정용 seam(`DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0`)으로 끄면 pre-prefetch 경로를
그대로 재현해 A/B 벤치마크를 할 수 있습니다. **CPU가 부족한** 태스크(§7.2)나 읽기가 크리티컬
패스가 아닌 **고RTT** 링크에서는 이득이 0에 수렴합니다 — 그래서 도구는 이 최적화 하나가 아니라
적정 CPU에 기댑니다.

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
| `DSQL_MIGRATOR_FULL_LOAD_PREFETCH` | `1`(켜짐) | 켜짐/꺼짐 | 읽기 선행 prefetch 큐(§7.1). **켜 두세요** — A/B 벤치마크로 pre-prefetch 경로를 재현할 때만 `0`. |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `1`(꺼짐) | ≤ 8 | 리더 범위 샤딩: **큰 단일 정수 PK** 테이블의 읽기를 K개 동시 리더로 분할(§7.1). **기본 꺼짐이며 켤 가치가 드묾** — 아래 주의 참고. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | 1000000 | — | 이 추정 행수 이상인 테이블만 샤딩; 더 작은 테이블은 항상 단일 리더. |

> **리더 샤딩은 언제 효과가 있나? 드묾 — in-VPC 테스트에서 ~0%였음.** 행별 타입
> 변환이 순수 Python이라 GIL을 점유 → K개 리더 *스레드*를 띄워도 변환 처리량이 ~1코어를
> 못 넘습니다. 스레드 샤딩은 리더의 I/O 대기만 겹칠 뿐(그건 prefetch가 이미 함) 변환에
> 두 번째 코어를 더하지 못합니다. in-VPC 4vCPU 실측에서 `reader_shards=4`는 단일 리더 대비
> **~0%**였고 CPU가 ~1코어에 고정됐는데, 그 로드가 리더가 아니라 **DSQL 쓰기 왕복 지연**에
> 묶여 있었기 때문입니다. run이 read/convert-starved(리더가 쓰기 풀을 못 채움)라는 *증거*가
> 있을 때만 올리세요. 벽이 쓰기 측이면 대신 **`BATCH_PARALLELISM`**을 올리세요. 총 소스
> 리더 = `table_parallelism × reader_shards`이며 소스 커넥션 한도 보호를 위해 상한이 걸립니다.

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

**CDC 스케일링은 툴이 추론하므로 UI에서 설정할 것이 없습니다.** 커넥터는 관리형 MSK Connect에서
실행되며, 그 스케일링 노브는 병렬수를 예측하는 유일한 입력 — **캡처 대상 테이블 수** — 로부터 계산됩니다.
각 테이블은 자체 Kafka 토픽이고, 싱크는 토픽 **파티션**을 병렬로 소비하므로(파티션당 싱크 태스크 1개)
총 싱크 병렬수 = `토픽당 파티션 수 × 테이블 수`입니다. 툴은 이 곱이 싱크 병렬수 상한에 도달하는 가장 작은
파티션 수를 고르고 거기서 멈춥니다:

| 캡처 테이블 수 | 토픽당 파티션 | `SinkTasksMax` | 유효 병렬수 |
|---|---|---|---|
| 1 | 8 | 8 | 8 |
| 2 | 4 | 8 | 8 |
| 3 | 3 | 8 | 8 |
| 4 | 2 | 8 | 8 |
| ≥ 8 | 1 | 8 | 8 |

UI 필드가 아니라 추론·숨김으로 둔 이유:

- **파티션 수는 비가역적입니다.** 토픽 파티션은 *늘리기*만 가능하고 줄일 수 없으며 — 실무상 늘리려면 MSK
  클러스터를 재생성해야 합니다. 잘못된 값은 영구적이므로, 만지작거릴 대상이 아니라 생성 시점에 맞아야 합니다.
- **CDC 변경은 15~20분 커넥터 재배포**이지 Full Load 같은 값싼 재튜닝 루프가 아니므로, 빠른 실험
  주기가 없습니다.
- **노브들이 상호작용하고 MCU는 비용이 듭니다** — 자유 조합은 틀리기 쉽고 조용히 과금될 수 있습니다.

상한이 존재하는 이유는 싱크가 **DSQL 쓰기 지연 바운드**이기 때문입니다: 측정된 처리량은 파티션 수에
**sublinear**하게 증가합니다(4 → 8 파티션에서 2배가 아니라 ~1.4배) — 동일 테이블에 대한 동시 upsert가
DSQL 내부에서 경합하기 시작하기 때문입니다. 상한을 넘으면 파티션을 더 늘려도 처리량 없이 MCU 비용만
늘어납니다. 소스 측은 MySQL 서버당 단일 태스크(단일 binlog 스트림)이지만 **병목이 아닙니다** — 기본 제공되는
producer 튜닝으로 초당 수만 레코드를 소화합니다. 언제나처럼 부하 시 실제 상한은 핫 기본 키의 OCC이므로
**PK 전략이 가장 중요합니다**(§7.1 참고).

**추론 재정의(고급).** 추론값에서 벗어날 이유가 있다면, 툴이 cdc-stack을 배포하기 전에 아래 환경 변수를
설정하세요(빈 값/유효하지 않은 값은 스마트 기본값으로 폴백):

| 환경 변수 | 재정의 대상 | 비고 |
|---|---|---|
| `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` | 테이블별 토픽 파티션 수 | 토픽 생성 후 **비가역적**. |
| `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX` | 싱크 커넥터 `tasks.max` | 실효값은 파티션 수로 상한. |
| `DSQL_MIGRATOR_CDC_MCU_COUNT` | 워커당 MSK Connect MCU | 1 / 2 / 4 / 8 중 하나여야 함. |

관련된 두 cdc-stack 파라미터는 추론이 아니라 고정입니다: `SourceTasksMax` = 1(MySQL은 서버당 단일 태스크),
`SinkBatchMaxRows` = 3000(DSQL 트랜잭션당 행 한도 — **3000 초과 금지**).

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

또한 **Fargate 태스크 CPU**(`ContainerCpu`)를 넉넉히 사이징하세요 — **Full Load는 네트워크가
아니라 CPU-bound**입니다. 소스 리더가 행마다 MySQL 타입을 DSQL 형식으로 Python에서(셀 단위,
GIL 점유) 변환하므로 처리량이 CPU에 비례합니다: payments+orders 로드 실측에서 **동일 데이터의
0.5 vCPU(512) 기본값보다 4 vCPU에서 약 3.8배 빨랐습니다**. **평가용은 0.5–1 vCPU**, **실제 대용량
Full Load에는 2–4 vCPU**를 쓰세요. 단일 큰 테이블은 ~4 vCPU를 넘으면 수확체감입니다 — 리더가
한 스레드라 한 코어 근처에서 한계에 이르므로, 다음 레버는 vCPU가 아니라 PK 범위로 읽기를 샤딩하는
것(향후 개선)입니다. **메모리**(`ContainerMemory`)는 테이블 크기가 아니라 행 버퍼의
`table_parallelism × batch_parallelism × 약 8 MiB`로 제한되며, Fargate의 CPU/메모리 짝(2 vCPU면
≥ 4 GiB, 4 vCPU면 ≥ 8 GiB)이 이미 이를 충족합니다.

> **로컬 실행**도 동일 환경 변수를 읽습니다 — `mysql-dsql-migrator ui` 실행 전 셸이나 `.env`에 설정하세요.

---

## 7.3 소스 부하 최소화

Full Load는 프로덕션 소스를 읽으므로 자연스러운 걱정이 생깁니다: **"여러 테이블을 한꺼번에 로드하면 무거운
읽기가 되는데, 소스에 영향을 주지 않을까?"** 설계 자체가 이미 그 읽기를 가볍게 유지합니다(keyset 스트리밍,
`OFFSET` 재스캔 없음, 테이블당 한 번에 ~1000행 페이지 하나만 in-flight, 전역 락 없음, `COUNT(*)` 대신
스캔 없는 `information_schema` 추정치, 그리고 암묵적 백프레셔 — 현재 페이지가 적재된 뒤에야 다음 페이지를
읽음). 남은 관리 대상은 **동시 읽기 압력**이고, 이는 거의 전적으로 **하나의 레버**로 결정됩니다.

### 유일한 레버: 테이블 병렬수

`DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM`(기본 **4**, 최대 **16**)는 *소스에서 동시에 몇 개
테이블을 읽는가* — 동시 테이블당 소스 스트리밍 커넥션 하나. 동시 소스 읽기 압력의 다이얼입니다.
`BATCH_PARALLELISM`·`BATCH_ROWS`는 **DSQL 쓰기** 압력이지 소스 읽기 부하가 아니므로, DSQL이 병목이
아니면 그대로 두세요. (소스 읽기 **페이지 크기는 1000행으로 고정**되어 튜닝 불가 — 소스 읽기 스로틀은
테이블 병렬수가 유일합니다.)

> 사이드바 **Performance tuning** 컨트롤에서 실행 사이에 실험하거나, 영속화하려면 환경 변수로 설정하세요(§7.2).

### 낮게 시작해서 관찰하며 튜닝

병렬수를 **처리량 다이얼이 아니라 스로틀**로 다루세요 — 테이블 개수가 아니라 측정된 여유에 맞춰 올립니다:

1. **낮게 시작(2~4)** — 큰 인스턴스라도. 느리게 끝나는 게 프로덕션 지연 사고보다 훨씬 쌉니다.
2. **소스를 지속 관찰** — 순간 스파이크가 아니라 5~10분 구간으로.
3. **여유가 명확할 때만 조금씩 올리고** 재관찰. (병렬수는 런당 1회 읽히므로 변경은 **다음** 런부터 적용)

**관찰할 신호** (Amazon RDS/Aurora — CloudWatch + Performance Insights):

- **올려도 되는 신호:** `CPUUtilization`이 라인 한참 아래, `ReadIOPS`가 올라가도 `ReadLatency` 평평,
  `DiskQueueDepth` 낮음, `BurstBalance` / `EBSIOBalance%` / `CPUCreditBalance` 100% 근처, Aurora
  `BufferCacheHitRatio` 유지(~99%).
- **즉시 낮출 신호:** `CPUUtilization` 지속 > ~85~90%; `ReadLatency`가 IOPS 정체 중 2~5배 상승;
  burst/credit balance가 **0으로 하강**(0 되기 *전에* 스로틀 — 회복이 느림); `FreeableMemory` 바닥 /
  `SwapUsage`; Aurora `BufferCacheHitRatio` 하락(콜드 스캔이 프로덕션 hot page를 밀어냄); 그리고 무엇보다
  **애플리케이션 자체 쿼리 지연 상승** — 인스턴스 지표와 무관하게 최종 back-off 트리거.

> [!note] 내장 rate limiter 없음
> 이 도구에는 **처리량/QPS 제한이 없습니다** — 소스 읽기 압력은 테이블 병렬수에 선형 비례합니다. 레버를 낮추는
> 것이 조절 수단입니다. 벌크 로드는 **오프피크**에 스케줄하고 인스턴스의 백업/유지보수 창과 겹치지 않게 하세요
> (스냅샷 + 풀 스캔 동시 실행은 gp2 `BurstBalance`에 최악의 경우).

### 시작 전 여유 점검 체크리스트

대표 피크 구간에서 다음을 baseline으로 잡고, 로드를 얹을 여유가 있는지 확인하세요: **CPU** 여유; **스토리지**
— gp2 `BurstBalance`(지속 스캔이 0으로 드레인되면 baseline IOPS로 붕괴 — 프로덕션을 해치는 가장 흔한 경로),
그리고 `ReadIOPS`/`ReadThroughput`/`ReadLatency`/`DiskQueueDepth`(Aurora: `VolumeReadIOPS`);
**버퍼 풀** `BufferCacheHitRatio`; **커넥션** `DatabaseConnections` vs `max_connections`(16-way + 앱
풀이 여유를 두고 한도 아래); **여유 공간/메모리**. 바쁜 RDS 소스라면 gp3 + 여유 IOPS가 풀 스캔에서 가장 자주
발목 잡는 gp2 버스트 절벽을 제거합니다.

### 쓰기 많은 소스의 대형 테이블

각 테이블 읽기는 `REPEATABLE READ` 스냅샷 안에서 실행되며 **그 테이블 읽기 내내** 열려 있습니다. 쓰기 많은
소스에서는 이것이 InnoDB undo 히스토리(**History List Length**)의 purge를 막아 — undo/디스크 증가와
읽기 지연 — 16개 테이블이 in-flight면 **가장 오래된** read view가 purge 지평을 정합니다. 완화책: 병렬수
낮추기(열린 read view 감소); **가장 큰 테이블을 PK 범위로 쪼개** 별도의 짧은 런으로(페이지 크기 노브가 없으니
개별 스냅샷을 짧게 하려면 테이블 셋을 쪼갬); History List Length 모니터링, 시작 전 여유 공간 확보로 undo 증가가
storage-full을 유발하지 않게.

### read replica에서 읽으면 어떨까?

Full Load를 **RDS read replica / Aurora reader endpoint**로 향하게 하면 스캔 IOPS·CPU·버퍼풀 churn이
전부 primary에서 벗어납니다 — 프로덕션을 보호하는 가장 강력한 방법이고, 병렬수도 더 세게 밀 수 있습니다.

- ✅ **Full Load 전용 마이그레이션: read replica는 좋은 선택입니다.** 로더는 일관 스냅샷 안에서 읽기 전용
  keyset `SELECT`만 하고, CDC가 꺼져 있으면 watermark는 캡처만 되고 사용되지 않습니다. (replica lag 때문에
  스냅샷이 약간 과거 시점을 반영하는 것은 정상)
- ⛔ **CDC도 함께 실행한다면, replica에서 읽지 마세요 — primary(writer)를 쓰세요.** 이 도구는 CDC 핸드오프
  **watermark**(binlog 파일/위치, GTID, `server_uuid`)를 연결한 소스에서 캡처하고, CDC 커넥터도 같은
  호스트에서 스트리밍합니다. replica의 binlog 좌표는 primary와 **다른 네임스페이스**에 있고, RDS replica는
  binlog가 **비활성이거나 미보존**인 경우가 많아 — replica에서 잡은 watermark는 조용히 **CDC 데이터 갭**을
  만들 수 있습니다(스냅샷 시점과 CDC 시작 사이 변경 유실). Full Load + CDC 마이그레이션에서는
  **primary/writer**에 연결하고, 그것을 단일 소스로 유지하며, **binlog retention을 Full Load 전체 시간보다
  길게** 설정하세요 (`CALL mysql.rds_set_configuration('binlog retention hours', N)`).

> [!tip] 바쁜 프로덕션 소스를 위한 빠른 권장
> **Full Load 전용:** **read replica**에서 읽기(gp3, 필요하면 창 동안 up-size); 병렬수 4~8에서 시작해
> 여유가 명확하면 램프. **Full Load + CDC:** **primary/writer**에서 읽기(replica 금지), 병렬수 **2~4**에서
> 시작해 여유가 명확할 때만 램프, **오프피크** 실행, binlog retention을 로드보다 길게. 어느 경우든 가장 큰
> 테이블 몇 개는 PK 범위로 쪼개세요.

---

## 7.4 개별 쿼리 튜닝

위의 병렬수 설정 외에, 개별 쿼리를 Aurora DSQL의 분산 실행 모델에 맞게 튜닝할 수 있습니다 — 기본 키가 곧
테이블이고, 필터 푸시다운이 비용을 좌우하며, 비용 단위는 (PostgreSQL의 `cost=`가 아니라) **DPU**입니다.
선택적 **Query Playground**는 MySQL 쿼리를 변환하고 `EXPLAIN` / `EXPLAIN ANALYZE`로 읽기 전용
프로브한 뒤, AI 보조를 켜면 **AI DBA**가 DSQL에 맞게 재작성하고 재테스트로 DPU 개선을 증명합니다.

> 전체 흐름은 [9장 — 쿼리 검증과 AI DBA](09-query-validation.md)를 참고하세요.

---

## 7.5 실측 예시 — §7.1·§7.2를 뒷받침하는 한 번의 실행

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
