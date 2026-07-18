# ALM Open Source Launch — 체크리스트 & 링크

**프로젝트**: DSQL Migration Toolkit  
**소스 코드**: https://gitlab.aws.dev/dalyoung/mysql-dsql-migration-tool-public  
**대상 Org**: `awslabs`  
**라이선스**: Apache-2.0  
**작성일**: 2026-07-14

---

## ✅ Step 1: Create a Launch

| 항목 | 값 |
|------|-----|
| ALM Product | [Open Source](https://regions.aws.dev/alm-product/products/product-caae12caa85041dcb200f4d138fd7349) |
| Launch Type | Open Source |
| Launch Name | `DSQL Migration Toolkit` |
| Launch Date | `2026-07-28` (조정 가능) |
| Tier | Tier 2 |
| Region | us-east-1 (IAD) |

---

## ⬜ Step 2: 필수 Tasks (3건 — 병렬 진행)

### 1. IP Review

- **티켓 생성**: https://t.corp.amazon.com/create/templates/533581d8-4a83-40d5-bae0-ac2fbed41102
- **IP 변호사 찾기**: [Pathfinder](https://pathfinder.legal.amazon.dev/#/page/IPOperationsLegal-BusinessPages-MeettheTeam/live?container=IPOperationsLegal-BusinessPages-MeettheTeam-mkc7xsnnmj17)
- 티켓 본문에 포함할 내용:
  - 소스 코드: `https://gitlab.aws.dev/dalyoung/mysql-dsql-migration-tool-public`
  - 라이선스: Apache-2.0
  - 제3자 의존성 (MySQL Connector/J GPL 포함 설명)
  - NOTICE / THIRD-PARTY-NOTICES.md 참조

### 2. Trademark Review

- **Naming Guidelines**: https://w.amazon.com/bin/view/AWS_Portfolio_PMM/Naming/OpenSource
- 프로젝트명 `DSQL Migration Toolkit` 상표 심사
- ALM Task 내에서 진행

### 3. AppSec Review (2-stage)

- **PCSR Process (SMGS Field)**: https://console.harmony.a2z.com/engsec-docs/SA/SMGS-security-reviews/public-content-security-reviews
- **Security Wiki**: https://w.amazon.com/bin/view/Open_Source/Security/#HPublishingNewProjects
- Stage 1: 코드 보안 리뷰 (Repo 생성 전)
- Stage 2: GitHub staging repo 확인 (Repo 생성 후, Public 전환 전)

---

## ⬜ Step 3: GitHub Repo 요청 (Task 1~3 완료 후)

| 항목 | 링크 |
|------|------|
| Repo 생성 요청 | https://console.harmony.a2z.com/open-sourcerer/aws-repo |
| GitHub 계정 연결 | https://console.harmony.a2z.com/open-sourcerer/connect-account |
| Org Self-Invite (awslabs) | https://console.harmony.a2z.com/open-sourcerer/self-invite |

제출 시 필요:
- IP Review 티켓 링크
- Trademark Review 완료 확인
- ALM Launch 페이지 URL

---

## ⬜ Step 4: Public 전환

- [ ] AppSec Stage 2 완료
- [ ] ALM의 모든 Task 해결 확인
- [ ] Repo visibility → Public
- [ ] ALM Launch → "Launched" 마킹

🎉 **공개 완료!**

---

## 📎 참고 링크

| 리소스 | URL |
|--------|-----|
| Open Source 메인 위키 | https://w.amazon.com/bin/view/Open_Source/ |
| AWS Open Sourcing 위키 | https://w.amazon.com/bin/view/Open_Source/Open_Sourcing/OpenSourcingForAWS |
| Releasing Open Source Projects | https://w.amazon.com/bin/view/Open_Source/Open_Sourcing/ |
| Launch Process 교육 | https://learn.a2z.com/app/course/amzn1.c3.v2.7bf69ebf-0036-4285-b6e6-0be1c08eaf1c/home/ |
| Slack 채널 | `#open-source` |
| OSSM 문의 티켓 | https://t.corp.amazon.com/create/templates/64e7463a-ec3f-4e22-abb1-c82544c73eeb |

---

## 📅 예상 타임라인

| 단계 | 소요 시간 | 비고 |
|------|-----------|------|
| ALM Launch 생성 | 당일 | ✅ |
| IP Review | 1~2주 | 변호사 직접 연락 시 단축 가능 |
| Trademark Review | 수일~1주 | |
| AppSec Stage 1 | 1~2주 | |
| Repo 생성 | 수일 | IP+TM+AppSec S1 완료 후 |
| AppSec Stage 2 | 1주 | |
| **총 예상** | **2~4주** | 병렬 진행 기준 |

---

## 💡 빠르게 진행하는 팁

1. 오늘 3건(IP, Trademark, AppSec) 동시 요청
2. IP 변호사에게 Chime/Slack으로 직접 컨텍스트 공유
3. 코드 준비 완벽하게 (저작권 헤더, CONTRIBUTING.md 등 사전 완료)
4. 리뷰 피드백 왕복 최소화
