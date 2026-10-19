# 10. 결론

_언어: [English](../en/10-conclusion.md) | **한국어** | [日本語](../ja/10-conclusion.md)_

> **이전:** [9. Query Converter와 AI DBA](09-query-validation.md)

이제 전체 여정을 살펴보았습니다: 연결, 계획, **평가(Evaluation)**, **스키마 변환(Schema Conversion)**,
**Full Load**, 선택적 **CDC**, **검증(Validation)**, 그리고 **전환(Cut over)**. 이 장은 이 모두를
하나로 엮습니다 — 어떤 경로를 고를지, 권장하는 전체(end-to-end) 흐름, 전환 런북, 그리고 다음 단계.

---

## 10.1 어떤 경로가 필요한가?

| 상황 | 사용 |
|---|---|
| 일회성 마이그레이션; 짧은 점검 동결 허용 | **Full Load만**(스트리밍 인프라 없음, 지속 비용 없음). |
| 대규모/지속적; **거의 무중단** 전환 필요 | **Full Load + CDC**(무손실 핸드오프로 전환 시점까지 DSQL을 라이브 유지). |
| 타깃이 이미 적재되어 있음; 스트리밍만 연결/재개 | **CDC only**(이미 적재된 타깃에 스트리밍만 연결 — 예: 이전 워터마크부터 CDC 재개, 또는 외부에서 적재된 타깃에 반영 — 새 Full Load 없이). |

**PostgreSQL**에서는 무손실 핸드오프를 적재 *이전에* 선택해야 합니다. WAL을 보관하는 복제 슬롯은
과거 위치에 만들 수 없으므로, **Full Load + CDC**를 선택하면 도구가 스냅샷 시점에 슬롯을 만듭니다.
**Full Load만** 실행한 뒤에는 그 선택지가 사라지고, 나중에 CDC를 시작하면 캡처 대상 테이블을 모두
다시 스냅샷합니다 — 여전히 무손실이지만 소스를 한 번 더 읽고 모든 행이 MSK를 통과합니다(커넥터를 전에
시작한 CDC 스택에서는 대신 Start CDC가 그 스택 자체의 슬롯에서 이어 가되, 그 스택의 소스 커넥터가
스트리밍하지 않았다면 Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다: 싱크가
적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만 빈틈이 없음). MySQL은
소비자가 없어도 바이너리 로그가 히스토리를 보관하므로, 스트리밍한 적 없는 CDC 스택에서는 나중에 CDC를
시작해도 재읽기 없이 워터마크부터 시작합니다. 스트리밍한 적 있는 스택에서는 Start CDC가 그 더 새 시작
지점을 거부하므로, 마이그레이션 유형을 CDC only로 바꾼 뒤 그 CDC 단계에서 Delete CDC infrastructure 후
다시 배포하고 Start CDC 하세요(Full load only에는 CDC 단계가 없습니다.
[3장 §3.5](03-full-load.md#35-워터마크--cdc로의-다리)). 어느 쪽이든 CDC는 워터마크에서 시작해 그 뒤의
변경만 가져오므로, 그 Full load only 실행이 앞선 적재가 타깃에 남긴 행에 append했다면 먼저
**Drop & reload**로 Re-run Full Load 하세요.

CDC는 실제로 움직이는 구성 요소(MSK, MSK Connect, 싱크 커넥터)와 **배포된 동안 계속 발생하는 비용**을
더합니다. 지속적인 복제가 정말 필요할 때만 사용하세요. 그렇지 않으면 Full Load만으로도 더 단순하고 저렴합니다.

---

## 10.2 권장 end-to-end 흐름

1. **Connect** — 소스(읽기 전용)와 타깃(DSQL, IAM 토큰)에 연결.
2. **Evaluation** — 호환성 리포트를 읽습니다. 모든 **UNSUPPORTED** 항목(PK 없음, 트리거, 루틴,
   DSQL에 대응이 없는 PostgreSQL 타입)을 해결하고 각 **MANUAL** 항목(공간 타입, FULLTEXT/SPATIAL 인덱스, 대소문자 무시 collation, `ENUM`/`SET`, 크기부터
   점검할 초대형 LOB·JSON 컬럼 등)을 결정하세요. *건너뛰지 마세요* — "적재가 영문도 모른 채 실패했다"를 "그 객체는 변경이 필요하다는 걸
   알고 있었다"로 바꿔 주는 단계입니다. 또한 캐스케이딩 외래 키가 CDC에 어떤 영향을 주는지도 여기서 알게
   됩니다: 캐스케이딩 외래 키는 바이너리 로그에 남지 않아 CDC가 복제하지 못하므로, 소스가 캐스케이드한
   행이 타깃에 고아로 남으면 cut over 시점에 그 외래 키에 대한 Aurora DSQL의 검증이 실패하고, 고칠 때까지
   cut over를 붙잡습니다(Validation의 고아 검사도 이를 보고함).
3. **Schema Conversion** — 소스 vs 변환 DDL을 검토하고 DSQL에 적용.
4. **Data Migration** — 리포트로 상황을 파악한 뒤 마이그레이션 타입(**Full Load만**, **CDC only**, 또는 **Full Load + CDC**)을
   선택합니다. 사전 점검을 실행하고 나면 **Full Load**가 행을 벌크 복사하고 워터마크를
   캡처합니다(에러 로그를 확인하세요: 격리(quarantine)된 행은 예상된 것(예: 초대형 값)이거나 조치가
   필요한 것입니다). CDC 타입이면 적재를 시작하기 **전에** Prerequisites 하위 단계에서 스트리밍
   인프라를 배포하세요 — 약 15~20분의 생성이 스냅샷과 겹쳐서 진행됩니다. 그 다음 그 워터마크부터
   **CDC**를 시작해 타깃이 수렴해 가는 과정을 지켜보세요.
5. **Validation** — 행 수 + 체크섬을 실행합니다(전환 전 + 대조 실행). 모든 차이가 설명될 때만(드리프트,
   의도된 격리, 아직 수렴하지 않은 CDC) 판정이 **MATCH**가 됩니다. CDC 마이그레이션이면 dead-letter 처리된
   CDC 변경이 있는 테이블은 자동으로 CHECKSUM으로도 비교됩니다.
6. **Cut over** — Validation이 깨끗한 MATCH가 되면(CDC이면 [§5.5](05-validation.md#55-cdc-dead-letter와-컷오버)의 dead-letter 확인과
   [§5.7](05-validation.md#57-소스-스키마-변경과-컷오버)의 소스 스키마 재확인까지 해소되면) 애플리케이션을
   DSQL로 전환합니다. 도구가 대신
   수행해 주지 않는 유일한 단계이며, 경로에 맞춘 런북(아래 §10.3)을 따르세요.

---

## 10.3 Cut over: 애플리케이션을 DSQL로 전환

"Cut over"는 마지막 워크플로 단계이자, 애플리케이션이 MySQL 사용을 멈추고 Aurora DSQL을 쓰기
시작하는 순간입니다. 도구가 대신 수행해 주지 **않는** 유일한 단계입니다 — 도구는 타깃이 정확함을
증명하고 무손실 복제 스트림을 제공하지만, 애플리케이션을 *언제* 어떻게 전환할지는 사용자만 내릴 수 있는
운영상의 결정입니다. Validation이 깨끗한 MATCH가 되면(CDC이면 [§5.5](05-validation.md#55-cdc-dead-letter와-컷오버)의 dead-letter 확인과
[§5.7](05-validation.md#57-소스-스키마-변경과-컷오버)의 소스 스키마 재확인까지 해소되면) UI의 **Cut over**
단계에 경로별 런북이 표시되며, 아래는 그와 동일한 안내입니다.

### Full load only (짧은 점검 동결)

짧은 쓰기 동결이 허용될 때 사용합니다. 스트리밍이 없으므로 동결은 적재 + 검증에 걸리는 시간만큼 지속됩니다.

1. **소스 쓰기 동결** — 애플리케이션을 점검/읽기 전용 모드로 두어 MySQL에 새 행이 쓰이지 않게 합니다.
   (도구는 소스를 읽기 전용으로 유지하며, 쓰기를 멈춰야 하는 건 *애플리케이션*입니다.)
2. **스냅샷 이후 소스에 쓰기가 있었다면 "Drop & reload"로 Full Load 재실행** — 기본값 *Append*는 빠진 행만 채우고 그 뒤의 UPDATE/DELETE는 적용하지 않습니다 — 그다음 CHECKSUM으로 검증. 다시 적재하면 테이블의 외래 키도 함께 사라지므로, Data Migration 단계(또는 Cut over)에서 **Apply foreign keys**를 다시 클릭하세요 — 각 키가 바로 추가되어 모든 신규 쓰기를 강제하고, 이미 적재된 행은 Aurora DSQL이 백그라운드에서 검증합니다.
3. **Validation** — 깨끗한 **MATCH** 확인(go/no-go 게이트). PostgreSQL 소스라면 어떤 컬럼도 소유하지
   않는 시퀀스(Evaluation의 `PG_STANDALONE_SEQUENCE`)는 이제 직접 소스의 최종 값 다음으로 옮겨야 합니다:
   Cut over 단계가 *Before you repoint: move standalone sequences past the source* 아래에 각각을, 소스에서
   실행할 읽기(`SELECT last_value, is_called FROM <seq>`)와 그 읽기가 돌려준 두 값으로 Aurora DSQL에서
   실행할 문장(`SELECT setval('<seq>', <last_value>, <is_called>)`)과 함께 나열합니다.
4. **애플리케이션을 DSQL 엔드포인트로 전환**(PostgreSQL 와이어, IAM 토큰 인증 — 비밀번호 없음).
   핵심 읽기/쓰기 경로 스모크 테스트.
5. **동결 해제** — 애플리케이션이 DSQL에서 라이브.

### Full load + CDC (거의 무중단)

애플리케이션이 MySQL에 계속 쓰는 동안 CDC가 DSQL을 수렴시켜 주므로, 동결은 마지막 잔여 변경분
소진(drain) + 스모크 테스트만큼으로 줄어듭니다.

1. **CDC 따라잡기 대기** — 복제 지연(lag)이 0에 가까워질 때까지.
2. **소스 쓰기 동결**(잠시 읽기 전용 / 점검 모드).
3. **마지막 소진(drain) 대기** — CDC가 아직 처리 중인(in-flight) 마지막 변경까지 적용해 지연이 다시
   0이 되고, 소스의 마지막 쓰기 후 최소 5분이 지나도록 합니다: 고유 인덱스를 기다리는 변경은 적용되거나
   dead-letter 처리되기 전까지 그만큼 보류되고, 그 지연이 나타나기까지 1~2분 걸릴 수 있습니다. 이제 MySQL과
   DSQL이 같은 행을 가집니다.
4. **Validation 재실행**으로 최종 go/no-go 판단을 하고, 깨끗한 MATCH일 때만 전환합니다. 최종 검증은
   마이그레이션이 적재한 모든 identity / `AUTO_INCREMENT` 키의 **시퀀스를 CDC가 전달한 행들 이후로
   전진**시키며 — 실행에서 빠진 테이블(**Objects to validate**에서 칩을 끈 테이블)도 포함됩니다 — Cut
   over의 *Sync identity sequences* 동작도 마찬가지이므로, 전환 후 앱의 첫 insert가 마이그레이션된 id와
   충돌(`23505`)하지 않습니다. 이 동작이 닿지 못한 마이그레이션 identity 테이블은 빨간 알림에 이름이
   표시되며(다시 실행하세요), 완료로 표시되지 않습니다. 어떤 컬럼도 소유하지 않는 PostgreSQL 시퀀스는
   여기에 포함되지 않습니다: Evaluation이 각각을 나열하고(`PG_STANDALONE_SEQUENCE`), Cut over 단계가
   *Before you repoint: move standalone sequences past the source* 아래에 각각을, 소스에서 실행할
   읽기(`SELECT last_value, is_called FROM <seq>`)와 그 읽기가 돌려준 두 값으로 Aurora DSQL에서 실행할
   문장(`SELECT setval('<seq>', <last_value>, <is_called>)`)과 함께 보여 주므로, 애플리케이션을 전환하기
   전에 실행하세요. 활동 로그의 승인 줄은 직접 처리하도록 남겨진 시퀀스 수를 기록합니다.
   dead-letter 처리된 CDC 변경이 있는 테이블은 자동으로 CHECKSUM으로 비교되며, Cut over는 열리기 전에
   dead-letter queue를 다시 읽습니다([§5.5](05-validation.md#55-cdc-dead-letter와-컷오버) 참고). 재키잉된
   테이블이 있는 PostgreSQL 소스에서는 Cut over가 열리기 전에 그 테이블들의 REPLICA IDENTITY도 다시
   읽습니다([§5.6](05-validation.md#56-재키잉된-postgresql-테이블과-컷오버) 참고). Cut over는 열리기 전에
   소스에서 캡처 중인 테이블의 정의도 다시 읽습니다([§5.7](05-validation.md#57-소스-스키마-변경과-컷오버)
   참고).
5. **외래 키를 적용한 뒤 애플리케이션을 DSQL 엔드포인트로 전환** + 스모크 테스트. Cut over에서 드레인
   확인 체크박스를 선택하고 **Apply foreign keys**를 클릭합니다: 각 키가 바로(`NOT VALID`, 몇 초) 추가되어
   그때부터 모든 신규 쓰기를 강제하고, 이미 적재된 행은 Aurora DSQL이 백그라운드에서 검증하며, 이 단계는
   검증된 키 수와 실패한 키의 상세·키를 깨는 행·**Validate again**을 보여 줍니다. **I've cut over**는
   그 검사가 아니라 키가 자리를 잡을 때까지만 기다립니다 — 다만 MySQL 소스에서 `ON DELETE` / `ON UPDATE`
   동작이 있는 키나 MySQL이 더 느슨하게 맞추는 텍스트 컬럼의 키(대소문자를 구분하지 않는 collation, 또는
   `VARCHAR`의 `utf8mb4_bin` 같은 PAD SPACE collation)처럼 CDC가 고아를 남길 수 있는 키는, 검증될
   때까지 또는 그 검사 없이 cut over하기로 선택할 때까지 Finish가 기다립니다. Finish는 **Apply foreign
   keys**가 추가하는 키만 정확히 기다립니다: 마이그레이션에 포함되지 않은 테이블이 Aurora DSQL에 없으면 그
   테이블의 키는 추가할 것이 없으므로 기다리지 않습니다. Finish는 마지막 검증 이후에 읽은 키 상태만 믿고,
   CDC를 시작하거나 다시 시작하면 마지막 적용 결과가 지워지며(스트림에는 키가 없어야 함) Start CDC의
   **Remove foreign keys**도 그 뒤 CDC를 시작하지 않더라도 마찬가지이므로, 최종 드레인 뒤에 다시
   적용하세요. 자리를 잡지 않은 키와 검증되지 않은 키는 서로 다른 두 보류이고 각각 따로 선택합니다: 강제되는
   외래 키 없이 cut over하기로 한 선택이 자리를 잡은 키의 검증 검사까지 면제하지는 않습니다(그 반대도
   마찬가지입니다).
6. **CDC 파이프라인은 맨 마지막에 제거** — **Start over(우상단)** → *Delete all CDC
   infrastructure* 를 클릭합니다. 복제를 끝내고 MSK / MSK Connect / NAT 비용을 중단하며, 기존 스택을
   정리합니다(나중에 새 Full Load나 CDC를 배포하려면 먼저 제거되어 있어야 하기 때문입니다).

### 롤백 앵커

전환하기 *전에* 롤백 규칙을 정해 두세요. DSQL을 최종 승인(사인오프)하기 전까지는 MySQL 소스를
**삭제하지 말고 동결(읽기 전용)** 상태로 유지합니다. 애플리케이션의 연결 대상을 바꾸기(repoint) 전이라면
롤백은 간단합니다 — 소스는 손대지 않았고 여전히 원본(권위 있는 데이터)이기 때문입니다. 애플리케이션이
DSQL에 쓰기 시작하면 그 새 행은 **DSQL에만** 존재하므로(이 도구는 MySQL → DSQL 단방향 복제이며,
역방향은 아닙니다), 그 시점에 롤백하려면 그 행들을 먼저 직접 조정해야 합니다.

---

## 10.4 도구가 따르는 원칙 (그래서 판정을 신뢰할 수 있음)

- **소스는 항상 읽기 전용.** 도구는 MySQL 소스에 절대 쓰지 않습니다.
- **조용함보다 시끄러움.** 데이터를 조용히 손상시키기를 거부합니다 — 표현할 수 없는 값은 눈에 띄게
  테이블을 멈추고, 완료되지 않은 적재는 절대 성공으로 보고되지 않습니다.
- **멱등적이고 재개 가능.** Full Load 배치 재실행이나 CDC 재적용은 중복을 만들지 않으며, 중단된 적재는
  끝나지 않은 작업만 이어서 재개합니다(**Retry unfinished tables**). 새 복사본은 **Drop & reload**로
  만듭니다: 재실행의 기본값 *Append*는 빠진 행만 채웁니다.
- **건전한 검증.** 증거(행 수, 체크섬, PK 셋)가 뒷받침할 때만 "일치"로 보고합니다.
- **자격증명은 메모리에만 유지.** 디스크·로그·리포트·작업 상태에 절대 기록되지 않습니다.
- **AI는 자문 전용.** 선택적 Bedrock 제안은 검토 전용이며 데이터 경로에 닿지 않습니다.

---

## 10.5 다음 단계

- **자주 묻는 질문:** [11장 — 고객 FAQ](11-customer-faq.md).
- **마이그레이션 실행:** [1장 — 설정](01-setup.md)부터.
- **아키텍처 & AWS 서비스:** [최상위 README](../../../README.md).
- **AWS 배포:** [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md).
- **커스텀 DSQL 싱크 커넥터:** [`connectors/dsql-sink/`](../../../connectors/dsql-sink).

Aurora MySQL에서 Aurora DSQL로의 마이그레이션은 **이종(heterogeneous)** 이동입니다 — 버전 업그레이드가
아니라 규칙이 다른 별개의 엔진으로 옮기는 것입니다. 이 도구의 역할은 그 차이를 처음에 명확히
드러내고(Evaluation), 가능한 곳에서는 결정론적으로 처리하며(Schema Conversion, Full Load, CDC),
결과를 증명하는 것(Validation)입니다 — 그래서 전환이 막연한 믿음에 기댄 도약이 아니라 증거에 기반한
결정이 되도록 말입니다.
