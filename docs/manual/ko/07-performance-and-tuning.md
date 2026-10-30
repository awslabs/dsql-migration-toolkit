# 7. 성능, 튜닝, 그리고 이 설계의 이유

_언어: [English](../en/07-performance-and-tuning.md) | **한국어** | [日本語](../ja/07-performance-and-tuning.md)_

> **이전:** [6. 한계](06-limitations.md)

기본값은 Aurora DSQL의 한도에 맞춰져 있고 소스에 주는 부하도 크지 않으므로, **대부분의
마이그레이션은 튜닝이 필요 없습니다**: 큰 테이블은 이미 기본 키 구간으로 나누어 읽고 쓰며, 동시 진행
배치는 Aurora DSQL이 더 늘려도 빨라지지 않는 지점에서 상한이 걸려 있습니다. 큰 실행이라면
*Tables in parallel*을 태스크의 vCPU 수로 설정하세요([§7.2](#72-병렬수-튜닝)). 소스가 프로덕션
트래픽을 처리하고 있다면 대신 2–4에서 시작해 소스를 지켜보며 올리세요([§7.3](#73-소스-부하-최소화)).
도구가 Aurora DSQL에 맞추려고 이미 하고 있는 일은
[§7.1](#71-aurora-dsql에서-도구가-대신-해-주는-일)에 있습니다.

> 아래 모든 설계 선택은 문서화된 Aurora DSQL 동작·한계에 매핑됩니다. 출처:
> [DSQL 쿼터/한계](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html),
> [동시성 제어](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html),
> [PostgreSQL→DSQL 마이그레이션 가이드](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html),
> [비동기 인덱스](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html),
> [기본 키](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html).

---

## 7.1 Aurora DSQL에서 도구가 대신 해 주는 일

각 행은 문서화된 Aurora DSQL의 동작·한도와 그에 대해 도구가 하는 일입니다:

| Aurora DSQL의 동작 | 도구가 하는 일 |
|---|---|
| 쓰기 충돌은 커밋 시점에 감지되고, 밀린 트랜잭션은 `40001`로 실패합니다(행 락 없음) | 모든 쓰기 트랜잭션을 작고 멱등하게 유지해(Full Load 배치나 CDC 청크는 최대 3,000행이며 기본 키 기준으로 씀) 재실행이 저렴하고 안전합니다 |
| 트랜잭션 하나는 최대 3,000행·10 MiB, 최대 5분이며 DDL은 하나만 담을 수 있습니다 | 최대 3,000행과 8 MiB 바이트 예산으로 배치하고, 트랜잭션당 DDL 하나만 실행합니다 |
| 스토리지가 기본 키로 파티셔닝되므로, 기본 키 순서로 들어오는 insert 스트림은 한 번에 키 범위 하나로 갑니다: 실측하면 배치 크기나 동시 진행 배치 수와 관계없이 약 20,000 rows/s입니다 | 큰 테이블을 기본 키 구간으로 나누어 읽어 쓰기가 여러 키 범위로 동시에 가게 하고, 동시 진행 배치를 키 범위당 2개(*Batches per table*이 더 작지 않으면 테이블 또는 부분당 최소 4개), 실행당 128개로 제한합니다: 그보다 많으면 기다리기만 합니다(§7.2) |
| `CREATE INDEX ASYNC`는 인덱스를 백그라운드에서 빌드합니다 | 테이블의 보조 인덱스를 행이 들어가기 전, 테이블이 비어 있을 때 만듭니다: Schema Conversion의 적용이 만들고, **Drop & reload**가 다시 만든 뒤 적재 전에 빌드를 최대 10분 기다립니다. 실측하면 인덱스는 빈 테이블에서 훨씬 빨리 빌드되므로, 적재와 인덱스 빌드가 더 일찍 끝납니다(§7.5) |
| IAM 토큰은 약 15분 유효하고, 연결은 60분 후 끊깁니다 | 토큰이 만료되기 전에, 그리고 Aurora DSQL이 토큰을 거부하면 즉시 갱신하므로, 수 시간짜리 적재나 CDC 스트림도 인증에서 멈추지 않습니다 |
| 타깃으로 삼을 논리 복제 슬롯이 없습니다 | 자체 벌크 로더(keyset 스트리밍: 재개 가능, 한정 메모리)로 복사한 뒤 Debezium → MSK → 커스텀 싱크로 변경을 스트리밍합니다. Kafka가 변경을 버퍼링하므로 싱크가 충돌 폭주 중 뒤처져도 이벤트를 잃지 않습니다 |

**쓰기가 실패하면.** `40001` 충돌이 나면 도구는 **트랜잭션 전체**를 지수 백오프와 jitter로 다시
실행하므로, 충돌한 워커들이 같은 박자로 재시도하지 않습니다. 끊긴 연결이나 만료된 IAM 토큰도 다시
연결해 재시도합니다. Aurora DSQL의 `XX000`("server unavailable")도 재시도합니다(일시적인 서비스
상태). 재시도가 예산을 넘겨도 계속되면 Full Load 배치는 실패하고(**Retry unfinished tables**로 다시
적재하며, 그 때문에 격리되는 행은 없음) 싱크는 Kafka Connect가 배치를 다시 전달하게
합니다(dead-letter 처리되는 행 없음). 계속 반복되면 AWS Support에 문의하세요. 그 밖의 오류는
재시도하지 않습니다: Full Load는 배치를 거부된 행까지 나눠 그 행을 격리(quarantine)하고, 싱크는 그
행을 DLQ로 보내며, 정상 행은 모두 반영됩니다.

### 기본 키 전략 — 핫 파티션 회피

Aurora DSQL은 **기본 키로 스토리지를 파티셔닝·분산**하며, AWS의 안내는 명확합니다: **"무작위 기본
키를 선택하라… 단일 키에 경합을 늘리는 패턴을 피하라."** MySQL `AUTO_INCREMENT` 키는 단조 증가하므로
새 행이 모두 키의 가장 오른쪽 구간으로 몰립니다: 컷오버 후 애플리케이션의 insert(그리고 CDC가
스트리밍하는 동안에는 CDC의 insert)가 한 파티션으로 수렴해, **행이 논리적으로 충돌하지 않아도** 쓰기
지연이 올라갑니다. Full Load는 큰 테이블의 쓰기를 스스로 여러 키 범위로
분산합니다([§7.2](#72-병렬수-튜닝)).

도구는 이런 키를 **Evaluation**에서 알려 주고(`AUTO_INCREMENT` → `MANUAL`), **Schema
Conversion**에서 테이블마다 전략을 고르게 합니다: 캐시를 적용한 identity 컬럼(`bigint`가 담을 수
있는 단일 컬럼 키의 기본값), 정수 키 유지, 또는 복합
키입니다([기본 키 전략 선택](02-evaluation-and-schema-conversion.md#기본-키-전략-선택) 참고).

#### 핫 파티션을 위한 복합 키 옵션

Schema Conversion은 사용자가 고른 고분산 컬럼을 테이블의 원래 키 앞에 둘 수 있습니다. 예를 들어
`(id)` 대신 `(customer_id, id)`로 두면 insert가 가장 오른쪽 파티션 하나가 아니라 수많은 키 범위에
분산됩니다.

테이블의 쓰기가 명확히 핫 파티션 상태이고(낮은 OCC 충돌률에서 부하 시 `CommitLatency`에 긴 꼬리가
생김: p99에서 수 초, 최대 수십 초), **자연스러운 고분산 그룹핑 컬럼이 있을 때** 쓰세요. Aurora
DSQL에서 실측해 보면 배치를 키우거나 동시 진행 배치를 늘려도 핫 키 범위는 빨라지지 않았고(키 범위
하나가 동시 진행 수와 관계없이 약 20,000 rows/s), 쓰기를 여러 키 범위로 분산해야만 빨라졌습니다. Full
Load는 큰 테이블을 키 범위로 나누어 읽어 이를 해 주고(§7.2), 복합 키는 컷오버 후 애플리케이션의
쓰기와 CDC의 쓰기를 포함해 그 테이블의 모든 쓰기에 이를 해 줍니다. 부하가 다른 요인(예: 클라이언트
CPU)에 묶여 있다면 복합 키는 아무것도 바꾸지 않으니, 먼저 측정하세요
([§7.5](#75-라이브-인프라-실측--71과-72의-근거) 참조).

- **소스는 절대 변경되지 않습니다.** 바뀌는 것은 Aurora DSQL 타깃 키뿐입니다.
  `CREATE UNIQUE INDEX ASYNC`가 원래 키의 고유성을 유지하므로 그 키 기반 조회와 제약은 계속
  유효합니다.
- **애플리케이션도 함께 바뀝니다.** 전환 후 쿼리, 조인, upsert는 복합 키를 사용해야 하며, 앞에
  추가한 컬럼은 절대 바뀌면 안 됩니다(Aurora DSQL 기본 키는 제자리 업데이트가 불가). 선행 컬럼
  목록에는 Aurora DSQL의 키 한도를 통과하는 컬럼(`NOT NULL`, 인덱싱 가능한 타입, 전체 8개 컬럼·1 KiB
  이내)만 표시됩니다.
- **Full Load와 CDC 모두 처리합니다**(두 엔진 모두). CDC에는 이 버전이 배포한 인프라가
  필요합니다([6장](06-limitations.md)). **PostgreSQL** 소스에서는 **실행 내내 테이블과 모든 파티션에
  `REPLICA IDENTITY FULL`**도 필요합니다. 그렇지 않으면 `DELETE`가 추가된 키 컬럼을 싣지 못하기
  때문입니다. 이것이 없으면 사전 점검이 Start를 차단하고, CDC가 실행되는 동안 CDC 모니터가 다시
  확인합니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버)).

---

## 7.2 병렬수 튜닝

단계가 너무 느리고 하드웨어에 여유가 있으면 설정을 올리고, 바쁜 소스를 보호하려면 내리세요. **Full
Load**와 **Validation**은 환경 변수나 **Settings**(§7.6)로 튜닝하고, **CDC**는 도구가 알아서
사이징합니다. Full Load에서 중요한 설정은 *Tables in parallel*입니다: 키 범위와 동시 진행 배치 수는
§7.5의 실측에 맞춰 이미 정해져 있습니다.

### Full Load

| 설정 (env var) | 기본값 | 상한 | 효과 |
|---|---|---|---|
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | 4 | ≤ 16 | 동시에 적재하는 테이블, 또는 큰 테이블의 부분 수: 각각 worker 프로세스 하나와 소스 연결 하나를 씁니다. 큰 실행에서는 태스크의 vCPU 수로 설정하세요. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | 8 | ≤ 32 | 테이블 또는 부분당 동시 진행 배치의 최대 수(배치마다 자신의 Aurora DSQL 연결). 그 안에서 도구는 읽는 키 범위당 최대 2개, 이 값이 더 작지 않으면 최소 4개를 두므로(실행 전체로는 최대 128개), 키 범위 한두 개로 읽는 테이블에서는 4보다 큰 값이 아무것도 바꾸지 않습니다. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | 배치 쓰기당 행 수(DSQL 3000행 하드 상한). |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `0`(자동) | ≤ 32 | **큰** 테이블을 몇 개의 기본 키 구간(K)으로 나누어 읽고 쓰는지([§3.8](03-full-load.md#38-병렬-적재)). `0`: 추정 5,000,000행부터 2,500,000행마다 구간 하나, 최대 32; `1`: 꺼짐; `2`–`32`: 정확히 그 수. *Tables in parallel*이 1보다 크고 선행 기본 키 컬럼이 정수여야 합니다. **Settings → Full Load**의 *Reader shards per large table*로도 설정. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | 1000000 | — | 이 추정 행수 이상인 테이블만 나눔(자동은 5,000,000행도 필요); 더 작은 테이블은 단일 리더. |

> **키 범위: 왜 도움이 되고, 대가는 무엇인가.** Aurora DSQL은 테이블의 키 범위 하나를 배치 크기나
> 동시 진행 배치 수와 관계없이 약 20,000 rows/s로 쓰므로, 큰 테이블은 쓰기를 여러 키 범위로
> 분산해야 빨리 적재됩니다(150M 행 PostgreSQL 테이블을 32개 구간으로 나누면 약 205,000 rows/s,
> §7.5). 구간을 읽는 방식은 소스에 따라 다릅니다:
>
> - **PostgreSQL:** 구간들이 스냅샷 하나를 공유하며, 실행 전체의 추정 행 수 중 그 테이블이 차지하는
>   몫만큼의 worker 프로세스(최대 *Tables in parallel*)가 읽습니다.
> - **MySQL:** 리더 세션 하나가 스냅샷 하나에서 모든 구간을 읽으므로, 테이블은 소스 연결 하나와
>   worker 프로세스 하나를 쓰며 이 프로세스가 코어 하나를 거의 다 씁니다: 거대한 MySQL 테이블은
>   코어 하나의 속도로 적재됩니다(측정에서 호스트에 따라 약 25,000–55,000 rows/s).
>   binlog 시작 지점이 있는 **Full load + CDC**에서 2 이상을 명시하면, 모든 unique 키가 선행 키
>   컬럼을 포함하는 테이블은 대신 별도 스냅샷으로 읽는 부분으로 나누어 부분마다 프로세스 하나가
>   읽습니다(150M 행을 16개 부분으로: 전체 약 164,000 rows/s, 세션 하나로는 안정 구간 약 55,500).
>   그러면 복사본은 CDC가 재생해야 일관되므로, 도구는 그 재생을 건너뛰는 경로인 그 테이블의 체크
>   해제, Full load only로의 전환, 더 나중 지점에서의 CDC 시작을
>   거부합니다([§3.8](03-full-load.md#38-병렬-적재)).
>
> **트레이드오프:** PostgreSQL에서, 그리고 MySQL의 별도 스냅샷 부분에서 K > 1은 놀던 slot을 같은
> 테이블에 대한 동시 읽기로 바꾸며, 그 테이블을 다 읽을 때까지 이어지므로, 그동안 소스 읽기
> 속도·DSQL 연결·메모리가 최대 `TABLE_PARALLELISM`배까지 늘어나고, PostgreSQL 소스는 그 읽기가 모두
> 끝날 때까지 공유 스냅샷을 유지합니다. 여전히 프로덕션 트래픽을 처리하는 소스라면 `Threads_running`
> 스로틀(`FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`, §7.3)과 함께 쓰세요. *Batches per table*을 올려도
> 키 범위 하나는 빨라지지 않습니다(§7.5): 거대한 MySQL 테이블을 코어 하나의 속도보다 빠르게
> 적재하려면 위의 별도 스냅샷 부분을 쓰세요.

> **연결 쿼터 가드레일.** 실행의 Aurora DSQL 연결 수는 많아야 동시 진행 배치 수(도구가 키 범위당
> 2개, *Batches per table*이 더 작지 않으면 테이블 또는 부분당 최소 4개, 실행당 128개로 제한)에
> 실행 중인 테이블 또는 부분마다 하나를 더한 수입니다. DSQL은 **클러스터당 최대 10,000 연결**을
> 허용하지만 **초당 신규 100 연결**만 가능하므로, 이 상한이 실행을 쿼터 안에 넉넉히 둡니다. 키 범위가
> 소화하는 것보다 많은 동시 진행 배치는 rows/s는 늘리지 않고 커밋 지연과 Aurora DSQL 컴퓨트
> 비용(`ComputeDPU`)만 늘립니다: 키 범위당 2개 대신 4개를 두자 같은 rows/s에서 둘 다 약 두 배가
> 됐습니다(§7.5).

### Validation

| 설정 (env var) | 기본값 | 상한 | 효과 |
|---|---|---|---|
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | 4 | ≤ 32 | 동시에 비교하는 테이블 수(각자 읽기 전용 소스 + 타깃 연결). `1` = 순차. |

너무 많은 동시 스캔으로부터 소스를 보호하기 위해 32로 제한.

### CDC (데이터 플레인)

**CDC는 도구가 알아서 사이징하므로 UI에서 설정할 것이 없습니다.** 캡처하는 테이블마다 Kafka 토픽이
하나이고 싱크는 토픽 파티션마다 태스크 하나를 실행하므로, 싱크 병렬수 = 토픽당 파티션 수 × 테이블
수입니다. 도구는 이 곱이 싱크 태스크 8개라는 상한에 도달하는 가장 작은 파티션 수를 고르고 거기서
멈춥니다:

| 캡처 테이블 수 | 토픽당 파티션 | `SinkTasksMax` | 유효 병렬수 |
|---|---|---|---|
| 1 | 8 | 8 | 8 |
| 2 | 4 | 8 | 8 |
| 3 | 3 | 8 | 8 |
| 4 | 2 | 8 | 8 |
| ≥ 8 | 1 | 8 | 8 |

**편향된 워크로드에서는 큰 테이블에 파티션을 더 줍니다.** 위 규칙은 부하가 균등하다고 가정합니다.
테이블이 많으면 토픽당 파티션이 1개가 되고, 파티션 1개는 최대 싱크 태스크 1개만 소비하므로 hot
테이블이 단일 태스크로만 스트리밍되고 나머지는 유휴 상태가 됩니다. 그래서 도구는 스캔 없는 테이블별
행 수 추정치(Full Load 워터마크의 값, 또는 Full Load 전에 CDC 인프라를 배포했다면 배포 시점에
소스에서 읽은 값)를 읽어 큰 테이블에 파티션을 2개 또는 4개 줍니다.
MySQL 8.0 이상(Aurora MySQL 3)에서는 둘 다 MySQL이 캐시해 두는 사본(기본 하루,
`information_schema_stats_expiry`)이 아니라 InnoDB에서 읽습니다. 이 동작은 **자동**이며, 실제로
편향된 캡처에서만 작동하고 부하가 균등하면 아무것도 하지 않습니다. 토픽의 파티션 수는 생성 시
고정되므로 변경하려면 CDC를 새로 배포해야 합니다.

기본 키를 유지하는 변경이라면 행 단위 순서는 지켜집니다(Debezium이 각 레코드를 기본 키로 키잉).
파티션을 넘나드는 변경은 두 가지입니다: 기본 키를 바꾸는 `UPDATE`와, 한 행에서 다른 행으로 넘어가는
고유 값입니다. 기본 키 외에 고유 인덱스가 있는 테이블에서는 이 버전이 배포한 CDC 인프라가 그런
변경을 다른 행의 변경이 적용될 때까지 보류합니다: 보류된 변경마다 최대 5분입니다. 다른 파티션은 계속
흐르며, 대기는 그 테이블의 Stream lag으로 보입니다. 이전 버전이 배포한 CDC 인프라는 그 변경을 즉시
dead-letter 처리하므로, 대기 동작을 받으려면 CDC 인프라를 Delete 후 다시
Deploy하세요([6장](06-limitations.md)).

**추론 재정의(고급).** 재정의할 이유는 거의 없습니다: 싱크 태스크 8개를 넘으면 파티션을 더 늘려도
처리량은 거의 그대로인 채 MCU 비용만 늘어납니다(4 → 8 파티션에서 2배가 아니라 약 1.4배). 싱크가
Aurora DSQL 쓰기 지연에 묶이기 때문입니다. 그래도 필요하면 툴이 cdc-stack을 배포하기 전에 아래 환경
변수를 설정하세요(빈 값/유효하지 않은 값은 추론된 기본값으로 폴백):

| 환경 변수 | 재정의 대상 | 비고 |
|---|---|---|
| `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` | 테이블별 토픽 파티션 수 | 토픽 생성 후 **비가역적**. 또한 **균등 파티션 수를 강제** — 위의 크기비례 배분을 비활성화합니다. |
| `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX` | 싱크 커넥터 `tasks.max` | 실효값은 파티션 수로 상한. |
| `DSQL_MIGRATOR_CDC_MCU_COUNT` | 워커당 MSK Connect MCU, **소스** 커넥터 | 1 / 2 / 4 / 8 중 하나여야 함. |

#### 싱크 컴퓨트 (가장 먼저 손댈 노브)

싱크가 따라오지 못하면 소스가 아니라 **싱크의** 컴퓨트를 올리세요: 고부하에서 싱크는 CPU에 묶이는
반면 단일 태스크인 소스는 CPU에 여유가 있습니다. **Settings → CDC → "Sink compute (MCU)"**(또는
`DSQL_MIGRATOR_CDC_SINK_MCU_COUNT`)로 설정하며, 워커당 1 / 2 / 4 / 8 MCU를 제공합니다(1 MCU = 1 vCPU
+ 4 GiB, 기본값 4). 측정 곡선은 부록(§12)을 참고하세요.

**언제 적용되나.** Full Load 노브와 달리, Start CDC가 싱크 커넥터를 생성/업데이트할 때 적용됩니다:

| 파이프라인 상태 | 값 변경 시 |
|---|---|
| CDC 미배포 | 다음 인프라 배포와 Start CDC에 사용됩니다. |
| 인프라만 배포, 미시작 | 다음 Start CDC에 사용됩니다. |
| **이미 스트리밍 중** | **다음 Start CDC까지 아무것도 바뀌지 않습니다: Stop CDC 후 Start CDC.** |

CDC가 스트리밍하는 동안 CDC 단계는 **Stop CDC**만 제공하므로, 새 싱크 용량은 Stop CDC 후 Start CDC를
다시 할 때 적용됩니다. 그때 커넥터가 새 용량으로 다시 만들어지고(커넥터 생성·삭제마다 MSK Serverless
파티션을 씀), Stop 때 싱크가 적용하지 않은 변경은 CDC 토픽에서 적용되므로, 그 재시작은 토픽이 그
변경을 아직 보존하는 동안만 빈틈이
없습니다([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)).

> **소스 재부팅은 자동 처리됩니다.** 소스 커넥터가 10분 동안 재시도하므로 소스 RDS/Aurora
> 재부팅(유지보수, 페일오버, 리사이즈)이 흡수됩니다: 커넥터가 재부팅 구간 내내 재시도하고 커밋된
> binlog offset부터 gap 없이 재개 — 운영자 개입 불필요. 재부팅 후 CDC를 Stop/Start할 **필요
> 없습니다**.

### AWS(ECS Fargate)에서

Full Load와 Validation 환경 변수는 태스크 정의의 컨테이너 `environment`
블록(`deploy/cloudformation.yaml` 또는 자체 태스크 정의)에 설정하고 재배포하세요. CDC 설정은 도구가
cdc-stack을 배포할 때 전달됩니다.

> **재배포 없이 실행 사이에 재튜닝.** 사이드바 푸터의 **Settings**를 열고 **Full Load** 또는
> **Validation** 탭으로 가세요. 로더와 검증기가 매 실행마다 설정을 다시 읽으므로, 여기서 바꾼
> 값은 **다음** 실행에 즉시 반영됩니다. 앱 전역이며 재시작 시 초기화되므로, 유지할 값은 태스크
> 정의에 설정하고 **Settings**는 실험용으로 쓰세요.

작업 규모에 맞게 태스크 크기를 잡고, *Tables in parallel*을 그 vCPU 수로 설정하세요: 동시에 적재되는
테이블, 또는 큰 테이블의 부분 하나가 worker 프로세스 하나입니다. **메모리는 hard limit**입니다.

| 설정 | 평가용 | 실제 대용량 Full Load |
|---|---|---|
| `ContainerCpu` | 0.5–1 vCPU(`512`–`1024`) | 4–16 vCPU(`4096`–`16384`) |
| `ContainerMemory` | `1024` MiB | 4 vCPU에서 ≥ `8192` MiB, 16 vCPU에서 ≥ `32768`(그 CPU에 대한 Fargate의 최솟값) |

- **MySQL 적재는 태스크의 CPU에 묶입니다.** MySQL 행을 읽고 변환하느라 실행 중인 테이블 또는
  부분마다 코어 약 하나가 바쁘므로, 처리량은 *Tables in parallel*까지 vCPU에 비례해
  늘어납니다(16 vCPU에서 20M 행 테이블 16개: 약 193,000 rows/s, 모든 코어 사용).
- **PostgreSQL 적재는 Aurora DSQL에 묶입니다.** 값이 거의 그대로 통과하므로 태스크에 필요한 코어가
  적고(336,000 rows/s에서 약 3.5개), 처리량은 키 범위(위)에서 나옵니다.
- **호스트가 달라집니다.** Fargate는 CPU가 서로 다른 호스트에 태스크를 배치하며, 같은 MySQL 적재가 한
  태스크에서 다른 태스크보다 최대 약 1.8배 빨랐습니다: 변경의 효과는 두 번 이상의 실행으로 판단하세요.
- **메모리.** `ContainerMemory`를 넘으면 커널이 태스크를 OOM-kill하며 앱 종료 로그가
  없습니다(CloudWatch 급등과 ELB 타임아웃만 보임). 넓은 행(큰 `TEXT`/`BLOB` 값)이 배치를 약 8 MiB
  상한까지 채우므로, 기본 `512` CPU / `1024` MiB는 동시 소스 쓰기가 있는 실제 적재에서 OOM-kill된
  적이 있습니다. 앱이 메모리 high-water와 한계의 ~80% 압박 경고를 로그(태스크가 죽어도 남는 활동
  로그 포함)에 남기므로 근거로 적정 크기를 잡을 수 있습니다.

둘 다 올리려면 재배포해야 합니다. Fargate는 태스크 메모리를 자동 스케일하지 않습니다. 전체
CPU↔메모리 짝 표는
[DEPLOYMENT.ko.md → 태스크 사이징](../../../deploy/DEPLOYMENT.ko.md#태스크-사이징--containercpu--containermemory)을
참고하세요.

> **로컬 실행**도 동일 환경 변수를 읽습니다 — `mysql-dsql-migrator ui` 실행 전 셸이나 `.env`에
> 설정하세요.

---

## 7.3 소스 부하 최소화

**소스 읽기는 이미 가볍습니다**: keyset 스트리밍(`OFFSET` 재스캔 없음), 테이블당 한 번에 페이지
하나만 in-flight, 전역 락 없음, `COUNT(*)` 대신 스캔 없는 행 수 추정치. 남은 관리 대상은 동시 읽기
압력이고, 거의 전적으로 설정 하나로 결정됩니다.

### 유일한 레버: 테이블 병렬수

`DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM`(기본 **4**, 최대 **16**)은 소스에서 동시에 읽는 테이블,
또는 큰 테이블의 부분 수이며, 각각 소스 스트리밍 연결 하나를 씁니다(PostgreSQL에서는 테이블이
스냅샷을 공유하는 동안 하나 더). `BATCH_PARALLELISM`과 `BATCH_ROWS`는 Aurora DSQL 쪽만 바꾸며 소스
읽기 부하는 바꾸지 않습니다. 선택형 `Threads_running` 상한이 두 번째 안전밸브입니다(아래 노트 참조).
병렬수는 **Settings → Full Load**에서 실행 사이에 실험하거나, 영속화하려면 환경 변수로
설정하세요(§7.2).

### 낮게 시작해서 관찰하며 튜닝

병렬수를 **처리량 다이얼이 아니라 스로틀**로 다루세요 — 테이블 개수가 아니라 측정된 여유에 맞춰
올립니다:

1. **낮게 시작(2~4)** — 큰 인스턴스라도. 느리게 끝나는 게 프로덕션 지연 사고보다 훨씬 쌉니다.
2. **소스를 지속 관찰** — 순간 스파이크가 아니라 5~10분 구간으로.
3. **여유가 명확할 때만 조금씩 올리고** 재관찰. (병렬수는 런당 1회 읽히므로 변경은 **다음** 런부터
   적용)

**관찰할 신호** (Amazon RDS/Aurora — CloudWatch + Performance Insights):

- **올려도 되는 신호:** `CPUUtilization`이 라인 한참 아래, `ReadIOPS`가 올라가도 `ReadLatency` 평평,
  `DiskQueueDepth` 낮음, `BurstBalance` / `EBSIOBalance%` / `CPUCreditBalance` 100% 근처, Aurora
  `BufferCacheHitRatio` 유지(~99%).
- **즉시 낮출 신호:** `CPUUtilization` 지속 > ~85~90%; `ReadLatency`가 IOPS 정체 중 2~5배 상승;
  burst/credit balance가 **0으로 하강**(0 되기 *전에* 스로틀 — 회복이 느림); `FreeableMemory` 바닥 /
  `SwapUsage`; Aurora `BufferCacheHitRatio` 하락(콜드 스캔이 프로덕션 hot page를 밀어냄); 그리고
  무엇보다 **애플리케이션 자체 쿼리 지연 상승** — 인스턴스 지표와 무관하게 최종 back-off 트리거.

> [!note] 소스 부하 안전밸브 **QPS 제한기는 없지만**, 선택형 동시성 스로틀이 **있습니다**:
> `DSQL_MIGRATOR_FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`(기본 `0` = 꺼짐, **Settings → Full
> Load**에서도 설정)이 소스의 전역 `Threads_running`이 상한을 넘는 동안 새 소스 읽기를 일시
> 중지합니다 — gh-ost의 `--max-load`류 가드. 그 외에는 테이블 병렬수를 낮추는 것이 조절 수단입니다.
> 벌크 로드는 **오프피크**에 스케줄하고 인스턴스의 백업/유지보수 창과 겹치지 않게 하세요(스냅샷 + 풀
> 스캔 동시 실행은 gp2 `BurstBalance`에 최악의 경우).

### 시작 전 여유 점검 체크리스트

대표 피크 구간에서 다음을 baseline으로 잡고, 로드를 얹을 여유가 있는지 확인하세요: **CPU** 여유;
**스토리지** — gp2 `BurstBalance`(지속 스캔이 0으로 드레인되면 baseline IOPS로 붕괴 — 프로덕션을
해치는 가장 흔한 경로), 그리고 `ReadIOPS`/`ReadThroughput`/`ReadLatency`/`DiskQueueDepth`(Aurora:
`VolumeReadIOPS`); **버퍼 풀** `BufferCacheHitRatio`; **커넥션** `DatabaseConnections` vs
`max_connections`(16-way + 앱 풀이 여유를 두고 한도 아래); **여유 공간/메모리**. 바쁜 RDS 소스라면
gp3 + 여유 IOPS가 풀 스캔에서 가장 자주 발목 잡는 gp2 버스트 절벽을 제거합니다.

### 쓰기 많은 소스의 대형 테이블

각 테이블 읽기는 그 테이블 읽기 내내 열려 있는 `REPEATABLE READ` 스냅샷 안에서 실행됩니다.
`READ-COMMITTED`로 설정된 MySQL 소스도 마찬가지입니다(도구가 자기 세션을 `REPEATABLE READ`로 설정:
[§3.2](03-full-load.md#32-스트리밍-export--어떤-크기든-한정된-메모리)). Validation도 비교하는
테이블마다 같습니다. 쓰기 많은 소스에서는 열린 read view가 InnoDB undo 히스토리(**History List
Length**)의 purge를 막아 undo/디스크 사용량이 늘고 읽기가 느려지며, 테이블이 여럿 in-flight이면
**가장 오래된** read view가 purge 지평을 정합니다. 줄이는 방법:

- 병렬수를 낮춥니다(열린 read view 감소).
- 큰 테이블의 read view는 그 테이블을 다 읽을 때까지 유지된다고 보세요: 자동 분할은 모든 구간을 세션
  하나에서, 스냅샷 하나로 읽습니다. binlog 시작 지점이 있는 Full load + CDC에서는
  *Reader shards per large table*을 2 이상으로 명시하면 이를 줄여 각 read view가 자기 구간을 읽는
  동안만 유지되며, 그 대가는 그 테이블에 대한 동시 읽기(§7.2)와 [§3.8](03-full-load.md#38-병렬-적재)의
  거부입니다.
- History List Length를 모니터링하고, 시작 전에 여유 공간을 확보해 undo 증가가 storage-full을
  유발하지 않게 합니다.

### read replica에서 읽으면 어떨까?

Full Load를 **RDS read replica / Aurora reader endpoint**로 향하게 하면 스캔 IOPS·CPU·버퍼풀 churn이
전부 primary에서 벗어납니다 — 프로덕션을 보호하는 가장 강력한 방법이고, 병렬수도 더 세게 밀 수
있습니다.

|  | Full Load 전용 | Full Load + CDC |
|---|---|---|
| **읽을 곳** | **read replica**(gp3, 필요하면 창 동안 up-size) | **primary/writer**, replica는 안 됨 |
| **시작 병렬수** | 4~8, 여유가 명확할 때만 램프 | 2~4, 여유가 명확할 때만 램프 |
| **그 밖에** | 스냅샷이 replica lag만큼 약간 과거 시점을 반영하는 것은 정상 | **오프피크**에 실행하고, binlog retention을 Full Load 전체보다 길게 설정 |

**CDC와 함께일 때 replica를 쓰면 안 되는 이유.** 도구는 CDC 핸드오프 **워터마크**(binlog 파일/위치,
GTID, `server_uuid`)를 연결한 소스에서 캡처하고, CDC 커넥터도 같은 호스트에서 스트리밍합니다.
replica의 binlog 좌표는 primary와 **다른 네임스페이스**에 있고 RDS replica는 binlog가 비활성이거나
미보존인 경우가 많아, replica에서 잡은 워터마크는 조용히 **CDC 데이터 갭**을 만들 수 있습니다(스냅샷
시점과 CDC 시작 사이 변경 유실). retention은
`CALL mysql.rds_set_configuration('binlog retention hours', N)`으로 설정하세요.

---

## 7.4 개별 쿼리 튜닝

위의 병렬수 설정 외에, 개별 쿼리를 Aurora DSQL의 분산 실행 모델에 맞게 튜닝할 수 있습니다 — 기본
키가 곧 테이블이고, 필터 푸시다운이 비용을 좌우하며, 비용 단위는 (PostgreSQL의 `cost=`가 아니라)
**DPU**입니다. 선택적 **Query Converter**는 MySQL 쿼리를 변환하고 `EXPLAIN` / `EXPLAIN ANALYZE`로
읽기 전용 프로브한 뒤, AI 보조를 켜면 **AI DBA**가 DSQL에 맞게 재작성하고 재테스트로 DPU 개선을
증명합니다.

> 전체 흐름은 [9장 — Query Converter와 AI DBA](09-query-validation.md)를 참고하세요.

---

## 7.5 라이브 인프라 실측 — §7.1과 §7.2의 근거

아래 실행은 위 설계가 실제로 나타나는지 확인한 것입니다.
**성능 규격이나 보장치가 아니라 방법을 보여 주는 예시**로만 읽어 주세요: 소스, 태스크가 놓인 호스트,
Aurora DSQL의 상태가 모두 수치를 움직이며, `scripts/measure_performance.py`(아래)로 자신의 환경에서
재현할 수 있습니다.

> [!note] 이 수치가 나온 조건 서울(`ap-northeast-2`), 16 vCPU / 120 GB ECS Fargate 태스크 하나에서
> 실행한 도구, `db.r8g.16xlarge`의 Aurora MySQL·Aurora PostgreSQL 소스, 그리고 *Tables in parallel*
> 16을 뺀 모든 설정이 기본값(Reader shards는 행에 따로 적지 않으면 자동, Batches per table 8).

| 적재 | 결과 |
|---|---|
| PostgreSQL, 테이블 16개 × 20M 행 | 전체 약 336,000 rows/s, 안정 구간 600,000 |
| PostgreSQL, 150M 행 테이블 하나(구간 32개) | 전체 약 205,000 rows/s |
| MySQL, 테이블 16개 × 20M 행 | 전체 약 193,000 rows/s, 태스크의 16 vCPU 포화 |
| MySQL, 150M 행 테이블 하나, Full load only(세션 하나) | 안정 구간 약 55,500 rows/s, 코어 하나에서 프로세스 하나(25분 후 중단, 75M 행까지) |
| MySQL, 같은 테이블을 Full load + CDC에서 Reader shards 16으로 | 전체 약 164,000 rows/s, 이전 사본의 DROP이 실행되는 가운데(아래) |

이 실행들과, `db.r7g.large` 소스를 둔 4 vCPU 태스크의 더 작은 실행들이 보여 준 것:

- **벽은 배치가 아니라 키 범위 하나입니다.** 테이블 하나로 들어가는 기본 키 순서 스트림 하나는 동시
  진행 배치가 8개든 32개든, 배치가 3,000행이든 약 20,000 rows/s였습니다. 동시 진행을 늘리면 같은
  rows/s에서 Aurora DSQL의 커밋 지연(가벼운 부하에서 26 ms, 동시 진행 8개에서 0.7초, 32개에서
  3초)과 컴퓨트 비용(`ComputeDPU`, 약 4배)만 올랐습니다. 키 범위 31–32개로 나누어 프로세스마다 두
  개씩 읽은 80M 행 PostgreSQL 테이블은 키 범위당 동시 진행 배치가 2개든 4개든 약 150,000 rows/s로
  적재됐고, 2개일 때 커밋 지연(1.4–1.6초 대 2.8초)과 `ComputeDPU`가 약 절반이었습니다(행당
  `TotalDPU`는 약 5% 낮음). 그래서 도구는 키 범위당 최대 2개, *Batches per table*이 더 작지 않으면
  테이블 또는 부분당 최소 4개를 유지합니다: 키 범위 하나는 4개로 20,000 rows/s에 도달했습니다. 키
  범위 4개로 분산하자 10M 행 테이블이 전체 약 37,000–40,000 rows/s(피크 54,000–60,000)로 적재됐고,
  엔진마다 하나씩 그런 테이블 두 개는 합쳐서 127,000에 이르며 계속 오르고 있었습니다. 키 범위 하나의
  약 20,000 rows/s와 비교하면, 위의 150M 행 테이블은 PostgreSQL에서 약 10배, 부분으로 나눈
  MySQL에서 8배(둘 다 전체 기준), 세션 하나의 MySQL에서 2.8배(안정 구간 기준) 빨리 적재됐습니다.
- **보조 인덱스는 빈 테이블에 만듭니다.** 8.5M 행에 보조 인덱스 25개(MySQL)인 경우, 행보다 먼저
  인덱스를 만든 Drop & reload는 처음부터 끝까지 512초에 끝났고, 적재 후에 인덱스를 빌드하면
  873초였습니다. 인덱스가 있는 테이블에는 행이 더 느리게 적재됐지만(485초 대 262초), 빌드는 빈
  테이블에서 13–41초, 적재 후에는 10–11분이 걸렸습니다.
- **새 테이블은 워밍업이 필요합니다.** Aurora DSQL은 새 테이블을 파티션 하나로 시작해 커지면서
  분할하므로, 적재의 처음 1–2분은 키 범위 하나 정도의 속도로 진행됩니다: 짧은 테스트는 안정 구간으로
  판단하세요.
- **이전 사본의 DROP은 계속 실행됩니다.** 적재된 테이블의 삭제(Drop & reload도 합니다)는 Aurora
  DSQL의 백그라운드 작업입니다: 수 분 동안, 75M 행 사본은 1시간 넘게, 다음에 적재되는 것과 나란히
  실행됐습니다.

**CDC 복제 지연 — 하한선(latency floor).** 별도로 측정했습니다(RDS MySQL 8.0 소스, MSK, `us-east-1`,
1 MCU·워커 1개의 싱크, 단일 컬럼 테이블): 소스 커밋 → DSQL 가시화까지 꾸준한 부하에서 p50 약 0.8초·p95
약 1.3초, 버스트 부하에서 p50 약 0.5초·p95 약 0.7초. 다중 테이블·높은 변경률에서는 더 커질 수 있으니,
처리량이 필요하면 §7.2의 CDC 파라미터로 확장하세요.

> **직접 재현:** `scripts/measure_performance.py full-load`는 처리량(테이블별, 시간 구간별), 쓰기
> 경로의 재시도와 지연, 그리고 Aurora DSQL이 인덱스 빌드를 끝낸 시점을 보고합니다.
> `--table-parallelism`, `--reader-shards`, `--cdc-mode`로 위의 각 행을 구성하고,
> `scripts/measure_performance.py cdc-lag`로 복제 지연을 잽니다(활성 CDC 파이프라인이 필요).
> `full-load`는 대상 테이블을 **DROP 후 재생성**하므로(`--yes` 필요) **비프로덕션 타깃에서만**
> 쓰세요. 공정하게 비교하려면 이전 사본을 먼저 삭제하고 Aurora DSQL이 백그라운드 작업을 끝내게
> 하세요(`--wait-sys-jobs`가 실행 시작 전에 이를 기다립니다).

---

## 7.6 Settings 다이얼로그 (튜닝·진단·활동 로그)

위 knob들은 재시작 없이 **UI에서도** 편집할 수 있습니다: **Settings**(사이드바 하단)가 탭
다이얼로그를 엽니다 —

- **Full Load**, **CDC**, **Validation** 탭 — §7.2의 튜닝 knob. 각 탭은 변경이 언제 적용되는지로
  시작합니다(*Changes apply to the next run.*; CDC 탭에서는 *the next Start CDC*):
  - **Full Load**: *Tables in parallel*, *Batches per table*, *Rows per batch*,
    *Reader shards per large table*, *Source-load throttle (max Threads_running)*. 그 위에 알림
    *Batches in flight are capped at 2 per key range (never fewer than 4 per table or part
    unless Batches per table is lower) and 128 per run*이 있습니다;
  - **CDC**: *Sink compute (MCU)*;
  - **Validation**: *Tables in parallel*.
- **Activity log** 탭 — 하나의 활동/감사 로그에 관한 모든 것을 한곳에: 앱 **로그 레벨**(DEBUG / INFO)
  설정, **"Mirror to CloudWatch Logs"** 토글(로그를 컨테이너 stdout으로도 스트리밍 → ECS에서
  CloudWatch로 전달되어 태스크 교체에도 남는 사본 확보), 그리고 세션 활동 로그 **다운로드**(AI DBA가
  읽는 그 피드).

다이얼로그 변경은 **재시작 시 되돌아갑니다**; 유지하려면 해당 `DSQL_MIGRATOR_*` 환경변수(§7.2
목록)를 설정하세요.

---

**다음:** [8. 테스트 및 검증 →](08-testing-and-verification.md)
