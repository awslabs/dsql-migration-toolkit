# AWS 내부 코드 → GitHub 오픈소스 공개 프로세스 요약

> 실제 DSQL Migration Toolkit 공개 과정에서 정리 (2026-07-14 ~ 진행중)
> 이 문서는 **awslabs** (Type 2: Open Source Accelerator) 경로 기준입니다.

---

## 프로세스 분류 (먼저 결정!)

| Type | 설명 | 경로 | Org |
|------|------|------|-----|
| **Type 2** | AWS 서비스 사용을 돕는 도구/라이브러리 | **ALM Launch** | `awslabs` |
| **Type 3** | 독립적으로 유용한 커뮤니티 프로젝트 | **ALM Launch** | `awslabs` 또는 `aws` |
| **Type 4** | 샘플 코드 (블로그, 워크샵, 데모) | **Self-Service** | `aws-samples` |
| **Type 1** | AWS 공식 지원 제품 (극히 드묾) | ALM + L8+ 승인 | `aws` |

> ⚠️ **샘플 코드(Type 4)**는 [Self-Service](https://console.harmony.a2z.com/open-sourcerer/aws-sample-code)로 가능하지만, "도구/라이브러리"는 해당 안됨.
> ⚠️ **Production-ready 코드**는 PCSR이 아닌 **정식 AppSec Review (Talos ProdSec)** 필요.

---

## 전체 흐름 (Type 2/3 기준)

```
1. ALM Launch 생성
2. 필수 Task 처리 (IP Review + AppSec + 나머지 Opt-Out)
3. Talos Security Engagement 생성 + Survey 완료 + Submit for Review
4. 리뷰 완료 대기
5. GitHub Repo 요청 (Private)
6. AppSec Stage 2 (staging repo 확인)
7. Public 전환 → 완료 🎉
```

---

## Step 1: ALM Launch 생성

**URL**: https://regions.aws.dev/alm-product/products/product-caae12caa85041dcb200f4d138fd7349

> 기존 "Open Source" Product를 사용하면 별도 Product 등록 불필요.
> 새 Product를 만들면 Write Group 권한 문제가 발생할 수 있음.

| 항목 | 값 |
|------|-----|
| Product | "Open Source" (검색 또는 ID: `product-caae12caa85041dcb200f4d138fd7349`) |
| Launch Type | Open Source (⚠️ 드롭다운에 없을 수 있음 — 그냥 폼 작성) |
| Tier | **Tier 2** (변경 시 사유: "Per OSPO guidance, all OS launches select Tier 2") |
| Region | **us-east-1 (IAD)** 만 체크 |
| Customer Disclosure Level | Secret |
| PRFAQ | No |
| Naming/branding | No |
| Pricing | No |

---

## Step 2: ALM Tasks 처리

Launch 생성 후 Tasks 탭에 8개 정도 Task가 자동 생성됨.

### 필수 (Required = Yes) — 진행

| Task | 처리 방법 |
|------|----------|
| **IP Review** | SIM 티켓 생성 → IP 변호사 할당 → Status "In Progress" |
| **Application Security (AppSec) Review** | Talos Engagement 생성 → Status "In Progress" |

### Opt-Out (Required = Yes이지만 해당 없음)

| Task | Opt-Out 사유 |
|------|-------------|
| Configure Pricing & Commerce | `Free open-source project on GitHub. No pricing or commerce configuration needed.` |
| AWS Documentation | `Documentation provided via project README and docs/ directory in GitHub. No AWS docs page required.` |

### Opt-Out (Required = No)

| Task | Opt-Out 사유 |
|------|-------------|
| Compliance (CSAA) | `Open-source community tool, not a managed AWS service. No compliance certification required.` |
| Apply open source positioning | `Type 2 Accelerator project, positioning tenets already considered.` |
| Piper (CX Pre-Mortem) | `Open-source developer tool, not a customer-facing AWS service.` |
| Benchmarking | `Performance data included in repo. No separate benchmarking engagement needed.` |

---

## Step 3: IP Review 티켓

**티켓 생성**: https://t.corp.amazon.com/create/templates/533581d8-4a83-40d5-bae0-ac2fbed41102

**IP 변호사 찾기**: [Pathfinder](https://pathfinder.legal.amazon.dev/#/page/IPOperationsLegal-BusinessPages-MeettheTeam/live?container=IPOperationsLegal-BusinessPages-MeettheTeam-mkc7xsnnmj17)

티켓 작성 후:
- Assignee에 IP 변호사 alias 입력
- Status를 "Pending" → **"Assigned"** 변경 (안 바꾸면 3일 후 자동 종료됨!)

---

## Step 4: Talos Security Engagement (AppSec)

### ⚠️ 중요 — 올바른 경로 선택

| ❌ 잘못된 경로 | ✅ 올바른 경로 |
|---------------|--------------|
| "Open Source Security Review" | **"Application Security Review"** |
| PCSR (RIVER workflow) | **Talos ProdSec** (production-ready 코드) |

> Production-ready 오픈소스 도구는 **PCSR이 아닌 정식 AppSec Review**가 필요함.
> 처음 Talos 생성 시 "Open Source Security Review"를 선택하면 자동 종료됨.

**URL**: https://talos.security.aws.a2z.com/#/talos/create-security-engagement/ProdSec

### Talos 폼 작성 팁

| 필드 | 값 |
|------|-----|
| RIP Service Name | 관련 서비스로 검색 (없으면 가장 가까운 것) |
| Owning Bindle | 팀 Bindle 선택 (Bindle 넣으면 CTI optional) |
| Engagement CTI | Bindle 없으면 필수 — [CTI 생성](https://t.corp.amazon.com/settings/cti-routing) |
| Launch Date | 공개 예정일 |
| Launch Date Confidence | 5 (중간) |
| Launch Date Flexibility | Flexible |

### Intake Survey (32문항) 주요 답변 패턴

| 질문 유형 | 답변 |
|----------|------|
| Development phases complete? | 모두 선택 (Design, Implementation, Testing, Documentation) |
| Purpose for engaging AppSec? | Developing a NEW AWS Service/Product |
| Customers? | External AWS customers |
| Launch type? | GA |
| Special events? | None |
| Networks hosted in? | Customer Network |
| Source code management outside ASBX? | Yes (GitLab) |
| Directly serve external customers? | Yes |

### Surveys 완료 후

1. **"Submission blockers"** 탭 확인
2. **"Actions" → "Submit for review"** 클릭
3. Justification 입력 후 Submit

> "Pen Test Forecasting"과 "CaatSec Confirmation Survey"는 Security Engineer가 작성하는 항목 — 본인은 무시해도 됨.

---

## Step 5: 리뷰 완료 후 — GitHub Repo 요청

IP Review + AppSec 모두 완료 후:

1. **[Approve AWS Repository](https://console.harmony.a2z.com/open-sourcerer/aws-repo)** 접속
2. GitHub 계정 연결: [OpenSourcerer](https://console.harmony.a2z.com/open-sourcerer/connect-account)
3. Org Self-Invite: [Self-Invite](https://console.harmony.a2z.com/open-sourcerer/self-invite) — `awslabs` 선택
4. **Private repo**로 생성됨

---

## Step 6: Public 전환

- AppSec Stage 2 완료
- ALM Launch → "Launched" 마킹
- Repo visibility → Public
- 🎉 완료!

---

## 예상 타임라인

| 단계 | 소요 | 비고 |
|------|------|------|
| ALM Launch + Talos 생성 | 1일 | 폼 작성 |
| IP Review | 1~3주 | 변호사 큐 대기 |
| AppSec Review | 1~3주 | 병렬 진행 가능 |
| Repo 생성 + Stage 2 | 1주 | |
| **총** | **2~4주** | 병렬 진행 기준 |

### 빠르게 하는 팁
- IP + AppSec 동시 제출
- IP 변호사에게 Chime/Slack DM으로 직접 연락
- 코드 정리(저작권 헤더, CONTRIBUTING.md 등) 리뷰 전에 완료 → 피드백 왕복 최소화

---

## 참고 링크 모음

| 리소스 | URL |
|--------|-----|
| ALM Open Source Product | https://regions.aws.dev/alm-product/products/product-caae12caa85041dcb200f4d138fd7349 |
| IP Review 티켓 템플릿 | https://t.corp.amazon.com/create/templates/533581d8-4a83-40d5-bae0-ac2fbed41102 |
| IP Pathfinder | https://pathfinder.legal.amazon.dev/ |
| Talos (AppSec) | https://talos.security.aws.a2z.com/ |
| OpenSourcerer | https://console.harmony.a2z.com/open-sourcerer/ |
| Approve AWS Repo | https://console.harmony.a2z.com/open-sourcerer/aws-repo |
| Self-Invite | https://console.harmony.a2z.com/open-sourcerer/self-invite |
| CTI 생성 | https://t.corp.amazon.com/settings/cti-routing |
| AWS Naming Guidelines | https://w.amazon.com/bin/view/AWS_Portfolio_PMM/Naming/OpenSource |
| Open Source 메인 위키 | https://w.amazon.com/bin/view/Open_Source/ |
| AWS Open Sourcing 위키 | https://w.amazon.com/bin/view/Open_Source/Open_Sourcing/OpenSourcingForAWS |
| OSSM 문의 티켓 | https://t.corp.amazon.com/create/templates/64e7463a-ec3f-4e22-abb1-c82544c73eeb |
| Slack | `#open-source` |

---

## 혼동하기 쉬운 포인트 정리

| 실수 | 올바른 방법 |
|------|------------|
| Talos에서 "Open Source Security Review" 선택 | → **Application Security Review** 선택 |
| PCSR (RIVER workflow)로 진행 | → Production-ready면 **Talos ProdSec** |
| ALM에서 새 Product 만들기 | → 기존 **"Open Source" Product** 사용 |
| IP Review 티켓 Status "Pending" 방치 | → **"Assigned"로 변경** (안하면 3일 auto-resolve) |
| Pen Test / CaatSec Confirmation 직접 작성 시도 | → **Security Engineer가 작성**하는 항목 |
