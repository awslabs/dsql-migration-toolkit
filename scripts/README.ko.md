# 명령줄 스크립트 — Full Load 실행 및 마이그레이션 검증

_언어: [English](README.md) | **한국어**_

이 헬퍼 스크립트들은 웹 UI를 열지 않고도 명령줄에서 **Full Load를 실행**하고 그 결과를 독립적으로
**검증**할 수 있게 해 줍니다 — 도구 자체 엔진을 사용합니다. 소스 MySQL은 항상 **읽기 전용**으로만
접근합니다.

| 스크립트 | 하는 일 | DSQL에 쓰나? |
|---|---|---|
| [`run_full_load.py`](run_full_load.py) | 지정한 스키마/테이블에 대해 **Full Load 실행**: MySQL에서 keyset 스트리밍 export → Aurora DSQL로 배치 적재(여러 번 실행해도 중복 없음; 도구 자체 벌크 로더). | 예(타깃만). `--yes` 필요; 없으면 계획만 출력. |
| [`compare_rows.py`](compare_rows.py) | **행 수 확인:** 테이블별로 소스 vs 타깃 카운트(및 PK min/max)가 일치하나? | 아니요 — 읽기 전용. |
| [`cdc_consistency_check.py`](cdc_consistency_check.py) | **무손실 확인:** 양쪽에서 전체 기본 키 집합을 읽어, 타깃에 **없는** PK(손실된 행)와 타깃에만 **있는** PK(아직 적용 안 된 소스 삭제)를 정확히 지목. | 아니요 — 읽기 전용. |

두 검증 스크립트는 **양쪽 모두 읽기 전용**이고, 테이블별 리포트를 출력하며, **모든 것이 일치/정합일
때만 `0`으로 종료**(아니면 non-zero)하므로 셸 스크립트의 게이트로 쓸 수 있습니다.

> 이 스크립트들은 선택적 독립 유틸리티입니다 — 마이그레이션 도구의 **Validation** 단계(매뉴얼 5장)가
> 권위 있는 go/no-go이며 체크섬 + 전체 조정(reconciliation)까지 수행합니다. 이 스크립트들은 명령줄에서
> 간편하게 스팟 체크하는 용도입니다.

---

## 1. 사전 준비

- **Python 3.10+** 와 이 리포의 의존성. 리포 루트에서:
  ```bash
  python -m venv .venv && .venv/bin/pip install -e .
  ```
- **연결 설정은 환경변수 / `.env`로** (`.env.example` → `.env` 복사). 스크립트가 읽는 값:

  | 변수 | 대상 | 비고 |
  |---|---|---|
  | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` | 소스 **MySQL** | 읽기 전용 연결 |
  | `TARGET_ENDPOINT` | 타깃 **Aurora DSQL** | 예: `your-cluster-id.dsql.<region>.on.aws` |
  | `TARGET_REGION` | DSQL | 선택 — 엔드포인트에서 자동 도출 |
  | `TARGET_DATABASE`, `TARGET_USERNAME` | DSQL | 기본값 `postgres` / `admin` |

  DSQL 쪽은 짧은 수명의 **IAM 토큰**으로 인증(비밀번호 없음)하므로, AWS 자격증명이 클러스터에 연결할 수
  있어야 합니다.

- per-PK 검증(`cdc_consistency_check.py`)에는 **단일 컬럼 정수 기본 키**가 필요합니다. 행 수 비교는
  어떤 테이블에서도 동작합니다.

먼저 `.env`를 셸에 로드하세요:
```bash
set -a; source .env; set +a
```

---

## 2. `run_full_load.py` — 명령줄에서 Full Load 실행

도구 자체 벌크 로더로 소스 테이블을 Aurora DSQL에 적재합니다. **먼저 계획(쓰기 없음)을 확인하고,
`--yes`로 재실행하세요.**

```bash
# 1) 계획 — 무엇이 적재될지 확인 (소스를 읽기 전용으로 introspect; 쓰기 없음)
.venv/bin/python scripts/run_full_load.py --schema sales --tables orders customers

# 2) 해당 테이블 적재 (여러 번 실행해도 중복 없음; 타깃 테이블이 이미 있어야 함)
.venv/bin/python scripts/run_full_load.py --schema sales --tables orders customers --yes

# 스키마의 모든 테이블을 새로 적재 (타깃 DROP+재생성) + 워터마크 저장
.venv/bin/python scripts/run_full_load.py --schema sales --clean --yes \
    --watermark-out sales_watermark.json
```

- `--schema` (필수): 적재할 소스 MySQL 데이터베이스.
- `--tables …` (선택): 특정 테이블들; **생략하면 스키마의 모든 테이블**을 적재.
- **기본 모드는 재실행해도 안전**합니다(`INSERT ... ON CONFLICT DO NOTHING`) — 몇 번을 다시 돌려도
  중복이 생기지 않습니다. 타깃 테이블이 먼저 존재해야 합니다(도구의 Schema Conversion 단계로 생성하거나
  `--clean` 사용).
- `--clean`: 적재 전 각 타깃 테이블을 변환된 DDL로 DROP + 재생성(DSQL에는 TRUNCATE 없음).
  **⚠️ 파괴적** — 해당 테이블의 기존 타깃 데이터를 버립니다; 새로 적재할 때 사용.
- `--watermark-out <파일>`: 캡처한 binlog/GTID 워터마크를 저장해, 이후 CDC를 바로 그 지점부터 누락 없이
  시작할 수 있게 합니다.

테이블 크기와 무관하게 메모리는 테이블당 한 페이지로 제한(keyset 스트리밍)되므로, 매우 큰
테이블에도 안전합니다. 모든 테이블이 적재되면 `0`으로 종료; 한 테이블이라도 실패하거나 행이
격리(영구 거부, 예: DSQL의 ~1 MiB 값당 한도 초과)되면 non-zero.

적재 후 아래 읽기 전용 검사로 확인하세요.

---

## 3. `compare_rows.py` — 빠른 행 수 확인

`-t schema.table`(반복 가능)로 **자신의 테이블**을 지정하세요. 스키마 없는 테이블명은 `cdc_demo`
스키마로 기본 처리되므로 항상 스키마를 붙이세요.

```bash
# 하나 이상의 테이블
.venv/bin/python scripts/compare_rows.py -t sales.orders -t sales.customers

# 일치할 때까지 10초마다 재확인 (CDC 따라잡는 동안 유용); Ctrl-C로 중지
.venv/bin/python scripts/compare_rows.py -t sales.orders --watch 10
```

테이블별로 `SOURCE` vs `TARGET` 카운트와 `MATCH` / `DIFFER (Δ=…)`, 그리고 불일치 시 PK min..max
범위를 출력합니다. `0` 종료 = 전부 일치.

**언제 쓰나:** Full Load 직후(카운트 일치 기대), 또는 CDC 중 타깃이 수렴하는지 지켜볼 때.

---

## 4. `cdc_consistency_check.py` — 무손실(zero-data-loss) 조정

카운트보다 강력합니다: 양쪽의 **전체 기본 키 집합**을 비교합니다.

```bash
# 특정 테이블 확인 (스키마 없는 이름 + --schema)
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders customers payments

# 기계 판독용 출력
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders --json
```

> **기본값은 도구 내부 샘플 스키마**(`customers_sample_new`, 11 테이블)입니다. 항상 **자신의**
> 데이터베이스에 맞게 `--schema` + `--tables`를 넘기거나, 스키마는 `CDC_WORKLOAD_SCHEMA`로 설정하세요.

테이블별로 `source_count` / `target_count`, **`missing_on_target`**(도착하지 않은 행)과
**`extra_on_target`**(아직 복제 안 된 소스 삭제)을 샘플 PK와 함께 리포트합니다. 판정이
**`ZERO DATA LOSS`**가 되는 것은 모든 테이블에서 `missing == 0` 이고 `extra == 0` 일 때뿐입니다.

**언제 쓰나:** 소스 변경이 멈추고 CDC가 드레인된 뒤 — 컷오버 전, 아무것도 손실되지 않았다는 증거.

### 선택: op-log 교차 확인
op-log(`{ts, op, table, pk}` 형식의 JSONL)를 기록하는 스크립트로 변경을 주입했다면, `--op-log <파일>`을
넘겨 손실된 INSERT / 남은 DELETE를 카운트 비교와 무관하게 정확한 연산에 귀속시킬 수 있습니다.

---

## 5. 전형적인 흐름

```bash
set -a; source .env; set +a

# Full Load (계획 먼저, 그다음 실행) — 스키마 전체 새로 적재
.venv/bin/python scripts/run_full_load.py --schema sales --clean          # 계획
.venv/bin/python scripts/run_full_load.py --schema sales --clean --yes    # 실행

# Full Load 후: 카운트가 일치해야 함
.venv/bin/python scripts/compare_rows.py -t sales.orders -t sales.customers

# CDC 중: 타깃이 따라잡는지 확인
.venv/bin/python scripts/compare_rows.py -t sales.orders --watch 10

# 컷오버 전(소스 정지, CDC 드레인): 무손실 증명
.venv/bin/python scripts/cdc_consistency_check.py --schema sales --tables orders customers payments
```

이 스크립트들은 명령줄 기반 Full Load와 읽기 전용 검증을 다룹니다; 체크섬, 전체 조정, 드리프트 귀속은
도구 내장 **Validation** 단계(매뉴얼 5장)를 사용하세요.
