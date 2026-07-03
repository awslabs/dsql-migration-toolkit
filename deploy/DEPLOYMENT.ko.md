# 배포 가이드 — MySQL → Aurora DSQL 마이그레이션 도구 (app-stack)

_언어: [English](DEPLOYMENT.md) | **한국어** | [日本語](DEPLOYMENT.ja.md)_

이 가이드는 **컨트롤 플레인 앱**을 고객 자신의 AWS 계정과 VPC 안에서(단일 테넌트),
**Application Load Balancer (HTTPS)** 뒤의 단일 태스크 **Amazon ECS Fargate** 서비스로 배포합니다.
이미지는 **Amazon ECR**에서 가져옵니다. 기본적으로 ALB는 **`internal`**(로그인 불필요 — 네트워크가
접근 게이트)이며, **Amazon Cognito (OIDC)** 로그인은 UI를 공개할 때만 필요한 opt-in 추가 기능입니다.
선택적 스트리밍 **CDC 파이프라인**(MSK + Debezium + 싱크)은 별도의 `cdc-stack`이며 여기서 다루지 않습니다.

---

## 빠른 배포 (TL;DR)

급하다면, 정상 경로를 순서대로. 각 단계의 상세 내용은 아래 섹션에 있습니다.

1. 실행 위치 선택 (Fargate 권장).
2. 필수 값 준비.
3. ACM 인증서 준비.
4. CloudFormation 템플릿 업로드.
5. 파라미터 입력.
6. 스택 생성.
7. 도구 URL(`AppUrl`) 열기.
8. (선택) 공개 접속 · Cognito 로그인 · AI 보조 활성화.

---

## 1단계 — 어디서 실행할지 선택

- **로컬** — `uv run mysql-dsql-migrator ui`. UI가 내 머신에서 돌고(브라우저 → `127.0.0.1:8080`),
  **마이그레이션 자체도 거기서 실행**됩니다 — 내 워크스테이션이 소스를 읽고 DSQL에 쓰는 엔진이라,
  모든 데이터가 내 머신과 네트워크를 통과합니다. 따라서 **내 데스크톱이 소스 MySQL과 타깃 Aurora
  DSQL _양쪽_ 모두에 도달**할 수 있어야 합니다 — 프라이빗 소스는 SSM 포트 포워딩 / VPN이 필요하고,
  내 머신은 DSQL 리전으로의 아웃바운드 HTTPS + AWS 자격증명이 있어야 합니다. 인프라 없음 — 평가 /
  소규모 마이그레이션 / 개발에 적합. 호스팅 아키텍처는 아니며, 실제 마이그레이션은 Fargate를 쓰세요.

  > **팁 — 재시작에도 세션(과 편집)을 유지하세요.** 실행 전에
  > `DSQL_MIGRATOR_STORAGE_SECRET`을 고정 랜덤 문자열로 설정하세요. 예:
  > `DSQL_MIGRATOR_STORAGE_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))") uv run mysql-dsql-migrator ui`.
  > 설정하지 않으면 재시작마다 브라우저 세션 id가 바뀌어, 워크플로 진행과 **Schema Conversion
  > 편집(커스터마이즈한 타깃 DDL — 예: `TINYINT(1)`→`smallint` 리매핑)** 이 복원되지 않고, Full Load
  > 재실행이 기본 변환으로 테이블을 재생성합니다. 설정하면 세션이 이어지고 재실행이 적용된 스키마를
  > 재사용합니다. (값은 비밀로 취급 — [`.env.example`](../.env.example) 참고.)
- **ECS Fargate — 권장** — 같은 엔진이 **내 VPC 안의** 단일 태스크 Fargate 서비스 + HTTPS ALB로
  실행되어, 데이터 경로가 내 노트북이 아니라 AWS 안에 머뭅니다. 실제 배포이며 이 가이드의 나머지가
  다룹니다.

> 선택적 대규모 스트리밍 **CDC**(MSK + Debezium + 싱크)는 별도의 `cdc-stack`이며 여기서 다루지 않음.

---

## 2단계 — ECS Fargate에 배포 (권장)

이미지 빌드 불필요 — 이미지가 **ECR Public**에 있어 CloudFormation이 가져온다. 같은
`deploy/cloudformation.yaml`을 배포하는 두 방법:

- **AWS Console — 권장.** 템플릿을 업로드하면 안내형 폼이 값을 받아준다. [2절](#2-app-stack-배포) 참고.
- **AWS CLI.** `aws cloudformation deploy` 한 명령으로 파라미터 전달. 역시 [2절](#2-app-stack-배포).

두 경로가 필요로 하는 값을 먼저 준비한다([1절](#1-사전-요구사항)에 상세). **VPC부터 정하고**(권장:
소스 DB가 있는 VPC), 그 VPC의 ALB·태스크 **서브넷을 그 안에서 고른다**(콘솔에서는 VpcId를 고르면
해당 VPC 서브넷이 드롭다운으로 나온다). 그 외 **ACM 인증서**, **DSQL 클러스터 ARN**. 소스 DB
자격증명은 배포 후 UI(Connect 단계)에서 입력하므로(보통 id/password) 별도 시크릿이 필요 없다.
나머지는 기본값이 처리한다(게시 이미지, `internal` ALB, Cognito off).

**UI 접근 (internal ALB).** 기본이 internal이라 공용 엔드포인트가 없다(SEC05-BP02). `https://<LoadBalancerDns>/`를
**VPC 안에서** 연다 — VPN / Direct Connect / SSM 포트 포워딩. 공개하려면 2절의 오버라이드 노트 참고.

---

## 1. 사전 요구사항

### 접근

- **AWS Console** 접근(권장 경로), **또는** 대상 계정에 인증된 AWS CLI v2
  (`aws sts get-caller-identity`).
- 스택 리소스 생성 권한: IAM 역할, ECS, ELB(ALB), EC2 보안 그룹, CloudWatch Logs,
  Cognito(옵션 — 공개 ALB일 때만).
- 이미지 빌드 불필요 — 이미지는 ECR Public에서 가져온다. (자체 빌드는 제한된 네트워크 전용; 부록 참고.)

### 필수 값

> 🔑 **VPC부터 정하세요 — 나머지는 거기서 따라옵니다.** **소스 RDS/Aurora MySQL이 이미 있는 VPC**를
> 쓰는 게 가장 단순하고 권장됩니다(도구가 소스에 프라이빗하게 도달하고, 소스 보안 그룹을 태스크에만
> 열면 됨). DSQL 타깃과 **같은 리전**이어야 합니다. **아래 두 서브넷 필드는 _이 VPC 안에서_ 고릅니다**
> — 콘솔에서는 VpcId를 고르면 해당 VPC의 서브넷이 드롭다운으로 나와 타이핑 없이 선택합니다. (피어링된
> VPC / Transit Gateway / Direct Connect / VPN도, 라우팅·SG가 태스크를 소스에 닿게 하면 동작.)

| 필수 | 파라미터 | 설명 |
| --- | --- | --- |
| **VPC** | `VpcId` | 위 VPC — 권장: 소스 DB의 VPC, DSQL과 같은 리전. |
| **ALB 서브넷** | `AlbSubnetIds` | **그 VPC의** 서브넷 2개, 다른 AZ — `internal` ALB(권장)면 프라이빗, internet-facing이면 퍼블릭. |
| **태스크 서브넷** | `ServiceSubnetIds` | **그 VPC의** 프라이빗 서브넷 2개, 다른 AZ, **443 egress**(NAT 게이트웨이 또는 VPC 엔드포인트)로 DSQL / Secrets Manager / ECR / CloudWatch 도달. |
| **ACM 인증서** | `CertificateArn` | HTTPS 리스너용 **같은 리전** ACM 인증서의 **ARN**(`arn:aws:acm:<region>:<account>:certificate/<id>`). **운영:** 보유한 도메인으로 ACM 퍼블릭 인증서를 발급해 그 ARN 사용. **빠른 테스트(도메인 없음):** `AWS_REGION=<region> deploy/create_test_cert.sh` 실행 후 출력된 `CertificateArn`을 붙여넣기(self-signed; 브라우저 경고). 기존 인증서는 ACM 콘솔에서 ARN 복사. |
| **DSQL 클러스터 ARN** | `DsqlClusterArn` | 타깃 Aurora DSQL 클러스터. |

> **소스 자격증명**은 배포 **후** UI(Connect 단계)에서 입력합니다 — RDS/Aurora MySQL은 보통
> **id/password** 방식이며, 메모리에만 보관하고 AWS 시크릿이 필요 없습니다. 그래서
> `SourceSecretArn`은 **옵션**입니다(아래 표): 기존 Secrets Manager 시크릿을 **재사용**할 때만 설정하세요.

> **왜 VPC 외에 이것들이 필수인가.** 서브넷과 인증서는 **AWS 자체 요구사항**입니다 — ALB와 Fargate
> 태스크는 반드시 서브넷에 배치돼야 하고 HTTPS 리스너는 인증서가 있어야 하며, CloudFormation이 VPC만
> 보고 자동 선택하지 못합니다. DSQL 클러스터 ARN은 마이그레이션의 **타깃**입니다. 그 외는 모두
> 기본값이 있습니다(아래 표).

### 옵션 값 (없으면 합리적 기본값 사용)

| 옵션 | 파라미터 | 필요한 경우 |
| --- | --- | --- |
| **소스 시크릿 ARN** | `SourceSecretArn` | 기존 Secrets Manager 시크릿을 **재사용**할 때만. 비워두면 UI에서 id/password 입력(일반적인 경우). |
| **소스 DB 도달 경로** | `SourceDbSecurityGroupId`(권장) / `SourceDbCidr` | **둘 중 최소 하나 필수** — 태스크가 소스 MySQL(`SourceDbPort`)로 egress하도록. `SourceDbSecurityGroupId`는 소스 DB SG로 egress를 범위 제한, SG id가 없으면 `SourceDbCidr` 사용. 둘 다 비우면 배포가 거부됩니다(소스로 가는 경로 없음). |
| **커스텀 도메인** | `AppDomainName` | 자체 Route 53 도메인으로 ALB를 front할 때만. |
| **공개 접근 / Cognito** | `AlbScheme`, `AllowedIngressCidr`, `EnableCognitoAuth`, `CognitoDomainPrefix` | UI를 공개할 때만; 기본은 `internal`(로그인 없음). |
| **AI 보조** | `EnableAiAssist`, `BedrockModelId`, `BedrockRegion` | Amazon Bedrock 보조 변환을 켤 때만(모델 선택; IAM 스코프 자동 도출). |
| **커스텀 이미지 / 사이징** | `ContainerImageUri`, `ContainerCpu`, `ContainerMemory` | 프라이빗 ECR 이미지나 기본 외 태스크 크기일 때만. |

---

## 2. app-stack 배포

`deploy/cloudformation.yaml`을 배포하는 두 방법 — 하나를 고른다. 둘 다 같은 스택을 만든다.
파라미터 설명은 3절.

### 권장 — AWS Console (안내형 폼)

먼저 콘솔 우측 상단에서 **올바른 리전**(Aurora DSQL 클러스터와 같은 리전)인지 확인한 뒤:

> **시작 전 준비 — `CertificateArn`을 미리 마련.** 콘솔은 HTTPS 인증서를 대신 만들어 주지 못합니다.
> 보유 도메인의 ACM 인증서가 없다면, 터미널에서 `AWS_REGION=<region> deploy/create_test_cert.sh`를
> 먼저 실행하고 출력된 `arn:aws:acm:…`을 step 3에서 붙여넣을 수 있게 보관하세요(self-signed 테스트
> 인증서 — 브라우저 경고; 운영은 보유 도메인의 실제 ACM 인증서 사용).
>
> **내 데스크톱에서 접속하나요? 공인 IP도 미리 준비.** `AlbScheme=internet-facing`으로 열 거라면,
> 지금 `curl https://checkip.amazonaws.com`로 공인 IP를 확인해 step 3에서
> `AllowedIngressCidr=<그 IP>/32`로 입력하세요(나만 ALB에 접근 가능). 기본값 `10.0.0.0/8`은 내부
> ALB용(VPC/VPN 내부에서 접근)이라 외부 브라우저는 차단됩니다.

**1. Create stack 마법사 열기.** CloudFormation 콘솔로 이동:
<https://console.aws.amazon.com/cloudformation/home> → **Create stack** →
**With new resources (standard)**. (직접 링크, 리전만 교체:
`https://<region>.console.aws.amazon.com/cloudformation/home?region=<region>#/stacks/create`.)

**2. Prerequisite — Prepare template.** **Template is ready** 선택 → **Specify
template**에서 **Upload a template file** → **Choose file** → 이 저장소의
`deploy/cloudformation.yaml` 선택 → **Next**.

**3. Specify stack details.** **Stack name**을 `mysql-dsql-migrator`로 지정한 뒤
파라미터를 채웁니다. 폼은 구획별로 묶여 있고(Network / Migration endpoints / TLS & access /
Authentication / Container image & sizing / AI), 네이티브 선택기라 ID를 타이핑하지 않고
**계정에서 골라** 입력합니다.

**아래 필수 필드를 채웁니다**(나머지는 기본값으로 동작):

| 필드 | 입력값 |
| --- | --- |
| `VpcId` | 드롭다운 — 소스 MySQL이 있는 VPC. |
| `AlbSubnetIds` | 서브넷 멀티선택 — **서로 다른 AZ 2개**(아래 서브넷 박스 참고). |
| `ServiceSubnetIds` | 서브넷 멀티선택 — **서로 다른 AZ의 프라이빗 2개**(프라이빗/NAT 서브넷이 없으면 ALB 서브넷을 그대로 쓰고 `AssignPublicIp=ENABLED` 설정). |
| `CertificateArn` | HTTPS용 ACM 인증서 ARN — **도메인이 없으면 바로 아래 명령 참고.** |
| `DsqlClusterArn` | 타깃 Aurora DSQL 클러스터 ARN. |

> ⚠️ 서브넷 드롭다운은 내 VpcId 것만이 아니라 **리전의 모든 서브넷**을 보여줍니다. 다른 VPC의 서브넷을
> 고르면 배포가 실패하니, 아래 **"어떤 서브넷을 고를까"** 박스를 보고 올바른 것을 선택하세요.

**권장:** `SourceDbSecurityGroupId`(또는 `SourceDbCidr`)를 설정해 태스크가 소스에 도달하게 합니다.
`SourceSecretArn`은 기존 소스 시크릿을 재사용할 때만 입력 — 보통은 비워두고 배포 후 UI에서 소스
host/id/password를 입력합니다.

**ACM 인증서가 아직 없나요?** 한 줄로 self-signed **테스트** 인증서를 만들고, 출력된 ARN을
`CertificateArn`에 붙여넣으면 됩니다(브라우저 경고; 테스트 전용):

```bash
AWS_REGION=<region> deploy/create_test_cert.sh
#  → 출력:  CertificateArn=arn:aws:acm:<region>:<account>:certificate/xxxx
```

**내 데스크톱 브라우저에서 UI에 접속하나요?** 기본값은 `internal` ALB(VPC/VPN 내부에서만 접근)입니다.
내 PC에서 열려면 아래 세 가지를 함께 설정하세요:

| 필드 | 입력값 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **퍼블릭** 서브넷(프라이빗 아님) |
| `AllowedIngressCidr` | 내 데스크톱 공인 IP를 `/32`로 — `curl https://checkip.amazonaws.com`로 확인(예: `203.0.113.5/32`) |

internet-facing ALB인데 `AllowedIngressCidr`를 기본값 `10.0.0.0/8`로 두면 브라우저가 차단됩니다.
`0.0.0.0/0`(전체 인터넷)은 추가로 `EnableCognitoAuth=true`가 필요합니다.

나머지는 기본값 유지(게시 이미지, `internal` ALB, Cognito off). 특히 **`HttpsEgressCidr`는
`0.0.0.0/0` 그대로 두세요** — 태스크가 NAT/IGW로 AWS API(DSQL·Secrets Manager·ECR·CloudWatch)에
나가는 아웃바운드 CIDR입니다. 이 서비스들을 전부 VPC 엔드포인트(PrivateLink)로 둘 때만 좁히고,
그렇지 않은데 좁히면 이미지 pull/DSQL이 막혀 태스크가 기동 실패합니다. → **Next**.

> **어떤 서브넷을 고를까.** 드롭다운은 (내 VpcId 것만이 아니라) **리전의 모든 서브넷**을
> `subnet-id | CIDR | 가용영역(AZ) | Name 태그`로 보여줍니다. **먼저 CIDR 대역으로 내 VpcId의
> 서브넷으로 좁히세요**(예: VPC가 `172.31.0.0/16`이면 → `172.31.x` 서브넷만 고르고, 다른 CIDR은 다른
> VPC 것이니 무시). 그다음 **AZ 컬럼**으로 "다른 AZ 2개"를 맞추고, **Name 태그**로 public/private을
> 구분하세요. 아래 표 기준으로 고릅니다(스택이 미리 flag해 줄 수는 없습니다 — 드롭다운은 AWS가 계정에서
> 채움):
>
> | 필드 | 추천 서브넷 |
> | --- | --- |
> | `AlbSubnetIds` | **서로 다른 2개 AZ의 서브넷 2개.** 기본 `internal` ALB면 **프라이빗**, `internet-facing`이면 **퍼블릭**. |
> | `ServiceSubnetIds` | **서로 다른 2개 AZ의 프라이빗 서브넷 2개**, 각각 아웃바운드 443(NAT 게이트웨이 라우트 또는 VPC 엔드포인트)이 있어 태스크가 DSQL / Secrets Manager / ECR에 도달. |
>
> 어느 게 어느 건지 모르겠다면 **VPC 콘솔 → Subnets**에서 내 VPC로 필터하고 각 서브넷의 AZ와 라우트
> 테이블을 확인하세요(`0.0.0.0/0 → nat-…` 라우트 = egress 있는 프라이빗; `→ igw-…` = 퍼블릭). Name
> 태그 규칙(예: `…-private-a` / `…-public-a`)을 두면 드롭다운이 한눈에 구분됩니다.

**4. Configure stack options.** 기본값으로 충분. 필요하면 태그 추가. → **Next**.

**5. Review and create.** 맨 아래로 스크롤해 **"I acknowledge that AWS CloudFormation might
   create IAM resources with custom names"**(`CAPABILITY_NAMED_IAM`) 체크 → **Submit**.

**6. 대기 + URL 확인.** 스택이 `CREATE_IN_PROGRESS` → `CREATE_COMPLETE`로 진행됩니다(몇 분,
   **Events** 탭에서 관찰). 그다음 **Outputs** 탭에서 **`AppUrl`**을 복사 — 이것이 도구 URL입니다
   (VPC 안에서 접속; 위 "UI 접근" 참고).

**7. 열기 — 도구가 보여야 함.** 브라우저에서 `AppUrl`로 접속(VPC 내부에서)하면
   **MySQL → Aurora DSQL Migration Tool** UI가 뜹니다 — **Connect**로 시작하는 안내형
   워크플로(Connect → Migration plan → Evaluation → Schema Conversion → Data Migration →
   Validation → Cut over). UI가 보이면 배포 완료이며, **Connect**에서 소스 DB 자격증명을
   입력해 시작합니다.

> **▶ 다음: 첫 마이그레이션.** 배포는 여기서 끝 — UI가 떴습니다. 각 단계가 무엇을 하고
> 실제 마이그레이션을 어떻게 진행하는지는 [**사용자 매뉴얼**](../docs/manual/ko/README.md)을
> 따라가세요([설정](../docs/manual/ko/01-setup.md) → Connect에서 시작).

**Prod 프로파일**은 3단계 폼에서 `EnableCognitoAuth=true`, `CognitoDomainPrefix`, `AppDomainName`을
추가 설정(이후 4~5절 진행).

### AWS CLI

환경을 셸 변수로 한 번 설정; 명령 자체는 모든 고객에게 동일. 최소(Dev/Test) 배포:

```bash
# --- 내 환경 (여기만 수정) ----------------------------------------------------
export AWS_REGION=us-east-1
export VPC_ID=vpc-xxxxxxxx                               # 권장: 소스 DB의 VPC
export ALB_SUBNET_IDS=subnet-aaaaaaa,subnet-bbbbbbb      # 서브넷 2개, 다른 AZ
export SERVICE_SUBNET_IDS=subnet-ccccccc,subnet-ddddddd  # 프라이빗 서브넷 2개
# CertificateArn: 아래에 실제 ACM 인증서 ARN을 붙여넣거나, 도메인 없이 self-signed 테스트
# 인증서로 자동 채우려면 스크립트 출력을 한 줄로 캡처:
#   export CERTIFICATE_ARN=$(deploy/create_test_cert.sh | sed -n 's/^CertificateArn=//p')
export CERTIFICATE_ARN=arn:aws:acm:us-east-1:<account>:certificate/xxxx
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:<account>:cluster/xxxx
export SOURCE_DB_SG=sg-source
# -----------------------------------------------------------------------------

aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    AlbSubnetIds="$ALB_SUBNET_IDS" \
    ServiceSubnetIds="$SERVICE_SUBNET_IDS" \
    CertificateArn="$CERTIFICATE_ARN" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    EnableAiAssist=true \
    BedrockRegion="$AWS_REGION"
    # BedrockModelId=us.anthropic.claude-sonnet-4-6   # 옵션 — 기본값 표시; §8 참조
    # SourceSecretArn=...   # 옵션 — 기존 소스 시크릿을 재사용할 때만
```

> **AI 어시스트(권장).** `EnableAiAssist=true` + `BedrockRegion`으로 Schema
> Conversion과 Query Playground의 AI DBA를 켭니다 — 옵트인·자문 전용 기능이며,
> 선택한 모델에 대한 `bedrock:InvokeModel`로만 범위가 제한됩니다. `BedrockModelId`
> (기본 `us.anthropic.claude-sonnet-4-6`)에 대해 해당 리전 Bedrock 콘솔에서 **모델
> 액세스를 활성화**해야 하고, 태스크가 Bedrock 엔드포인트로 egress할 수 있어야 합니다.
> 둘 다 생략하면 AI 없이 배포됩니다(결정론적 경로는 그대로). 자세한 내용·모델 선택은
> §8을 보세요.

**Prod 프로파일**은 추가: `EnableCognitoAuth=true`, `CognitoDomainPrefix=...`,
`AppDomainName=...` (그리고 자체 이미지면 `ContainerImageUri=...`).

> **테스트 지름길 / 오버라이드**
>
> - **ACM 인증서/도메인 없음:** `AWS_REGION=us-east-1 deploy/create_test_cert.sh`가 자체 서명
>   인증서를 import; 그 `CertificateArn` 사용(브라우저 경고; 테스트 전용).
> - **NAT 없음:** `AssignPublicIp=ENABLED` + `ServiceSubnetIds`를 퍼블릭 서브넷에(태스크는 여전히 ALB SG로만 접근).
> - **공개 UI(내 데스크톱에서 접속):** `AlbScheme=internet-facing` **및**
>   `AllowedIngressCidr=<내 공인 IP>/32`(확인: `curl https://checkip.amazonaws.com`). 기본값
>   `10.0.0.0/8`은 내부 전용이라 외부 브라우저를 차단; `0.0.0.0/0`(완전 개방)은 금물(추가로
>   `EnableCognitoAuth=true` 필요).
> - **제한된 네트워크(ECR Public 불가):** `ContainerImageUri`를 자체 프라이빗 ECR 사본으로 오버라이드
>   ([pull-through 캐시](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html)
>   또는 `deploy/Dockerfile`에서 빌드, 부록 참고).

원하면 먼저 템플릿을 검증:

```bash
aws cloudformation validate-template \
  --template-body file://deploy/cloudformation.yaml --region "$AWS_REGION"
```

완료 후 출력 읽기:

```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```

주요 출력: `LoadBalancerDns`, `AppUrl`, `ClusterName`, `ServiceName`,
`TaskRoleArn`, `CognitoHostedUiDomain`.

브라우저에서 **`AppUrl`**로 접속(VPC 내부에서)하면 **MySQL → Aurora DSQL Migration Tool**
UI가 뜹니다 — **Connect**로 시작하는 안내형 워크플로(Connect → Migration plan → Evaluation →
Schema Conversion → Data Migration → Validation → Cut over). UI가 보이면 배포 성공이며,
**Connect**에서 소스 DB 자격증명을 입력해 시작합니다.

---

## 3. 파라미터 레퍼런스

| 파라미터 | 필수 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `VpcId` | yes | — | 소스 DB에 프라이빗하게 도달할 수 있는 VPC. |
| `AlbSubnetIds` | yes | — | ALB용 서브넷 ≥2개(서로 다른 AZ). |
| `ServiceSubnetIds` | yes | — | Fargate 태스크용 프라이빗 서브넷. |
| `AlbScheme` | no | `internal` | `internal` 또는 `internet-facing`. **권장: `internal`**(VPN/Direct Connect/피어링으로 도달); `internet-facing`은 Cognito 켤 때만. |
| `CertificateArn` | yes | — | HTTPS(443) 리스너용 ACM 인증서 ARN. |
| `ContainerImageUri` | no | 게시된 ECR Public 이미지 | ECR Public에 게시된 이미지가 기본 — 빌드 불필요. 제한된 네트워크(자체 프라이빗 ECR 사본 / pull-through 캐시)나 커스텀 빌드에만 오버라이드; immutable 태그 또는 digest 권장. |
| `ContainerCpu` | no | `512` | Fargate 태스크 CPU 단위. |
| `ContainerMemory` | no | `1024` | Fargate 태스크 메모리(MiB), CPU에 유효한 값. |
| `AppPort` | no | `8080` | 컨테이너 수신 포트. |
| `AssignPublicIp` | no | `DISABLED` | NAT 없이 퍼블릭 서브넷에서 태스크 실행하려면 `ENABLED`(테스트); **권장: 프로덕션은 `DISABLED` 유지**(NAT 게이트웨이 또는 VPC 엔드포인트). |
| `AllowedIngressCidr` | no | `10.0.0.0/8` | ALB 443에 도달 허용 CIDR. **권장: 내 네트워크로 범위 제한**, `0.0.0.0/0` 아님. |
| `DsqlClusterArn` | yes | — | 타깃 DSQL 클러스터 ARN(`dsql:DbConnect` 범위 지정). |
| `SourceSecretArn` | no | `""` | **옵션.** 기존 Secrets Manager 시크릿을 **재사용**할 때만 설정(`GetSecretValue` 범위 지정). 비워두면 UI에서 id/password 입력(일반적인 경우). |
| `SourceDbSecurityGroupId` | no* | `""` | 소스 DB SG; raw CIDR보다 **선호(권장) egress 타깃**. *이것/`SourceDbCidr` 중 하나 필수. |
| `SourceDbCidr` | no* | `""` | 소스 DB CIDR(SG id 없을 때 사용). *이것/`SourceDbSecurityGroupId` 중 하나 필수. |
| `SourceDbPort` | no | `3306` | 소스 MySQL 포트. |
| `HttpsEgressCidr` | no | `0.0.0.0/0` | 태스크 아웃바운드 443(AWS API: DSQL 토큰·Secrets Manager·ECR·CloudWatch·Bedrock) + 5432(DSQL)의 **대상** CIDR. **권장: 기본값 `0.0.0.0/0` 그대로** — 태스크가 NAT/IGW로 퍼블릭 AWS 엔드포인트에 도달. 좁히기(예: 내 VPC CIDR)는 위 서비스들을 *전부* 인터페이스 VPC 엔드포인트(PrivateLink)로 둘 때만; 엔드포인트 없이 좁히면 이미지 pull/DSQL이 막혀 태스크가 기동 실패합니다. |
| `EnableCognitoAuth` | no | `false` | ALB가 Cognito(OIDC)로 인증. 기본 `false`: internal ALB(또는 내 CIDR로 범위 제한한 ALB)가 접근 게이트이고 운영자가 이미 IAM/DB 권한을 보유하므로 로그인 불필요. **`AllowedIngressCidr=0.0.0.0/0`일 때만 필수(강제됨).** `true`면 `CognitoDomainPrefix` 필요. |
| `AppDomainName` | Cognito 시 | `""` | ALB 앞단 DNS 이름(인증서와 일치). |
| `CognitoDomainPrefix` | Cognito 시 | `""` | 전역 유니크 Cognito hosted-UI prefix. |
| `EnableAiAssist` | no | `false` | opt-in; 범위 제한된 `bedrock:InvokeModel` 부여. |
| `BedrockModelArns` | no | `""` | **선택적 override**; 비우면 `BedrockModelId`에서 자동 도출. |
| `BedrockRegion` | no | `""` | 앱의 `BEDROCK_REGION`. |
| `BedrockModelId` | no | `us.anthropic.claude-sonnet-4-6` | Anthropic 모델(드롭다운); IAM 스코프 자동 도출. |

---

## 4. DNS를 ALB로 지정 — Optional (커스텀 도메인만)

`AppDomainName`(자체 도메인)을 설정한 경우에만. **기본 설정이면 건너뛰세요** — ALB DNS 이름
(`AppUrl` 출력값)으로 바로 접속합니다.

`AppDomainName`용 Route 53 **alias A 레코드**를 ALB(`LoadBalancerDns`)로 생성. 이름은
`CertificateArn`과 일치해야 함. 예(프라이빗 존의 internal ALB로 alias):

```bash
aws elbv2 describe-load-balancers \
  --names "$(aws cloudformation describe-stack-resource \
    --stack-name mysql-dsql-migrator --logical-resource-id LoadBalancer \
    --query 'StackResourceDetail.PhysicalResourceId' --output text)" \
  --query 'LoadBalancers[0].[DNSName,CanonicalHostedZoneId]' --output text
```

반환된 DNS 이름 + 호스티드 존 id로 alias 레코드를 생성(콘솔 또는
`aws route53 change-resource-record-sets`).

---

## 5. 운영자 사용자 생성 (Cognito) — Optional

Cognito를 켰을 때만(`EnableCognitoAuth=true`, 즉 공개 ALB). 기본 `internal` ALB면 건너뜁니다.
스택의 사용자 풀에 사용자 생성:

```bash
POOL_ID=$(aws cognito-idp list-user-pools --max-results 60 \
  --query "UserPools[?Name=='mysql-dsql-migrator-users'].Id | [0]" --output text)

aws cognito-idp admin-create-user \
  --user-pool-id "$POOL_ID" \
  --username operator@example.com \
  --user-attributes Name=email,Value=operator@example.com Name=email_verified,Value=true
```

사용자는 임시 비밀번호를 받고, ALB가 트리거하는 Cognito hosted UI를 통해 첫 로그인 시 새 비밀번호를
설정하라는 안내를 받습니다.

---

## 6. 검증

```bash
# ECS 서비스가 runningCount = desiredCount (1) 에 도달하고 ACTIVE 여야 함.
aws ecs describe-services --cluster "$(... ClusterName ...)" \
  --services "$(... ServiceName ...)" \
  --query 'services[0].[status,desiredCount,runningCount]' --output text

# 애플리케이션 로그 tail.
aws logs tail /ecs/mysql-dsql-migrator-mysql-dsql-migrator --follow --region "$AWS_REGION"
```

그다음 허용 네트워크(`AllowedIngressCidr`) 안의 호스트에서 `https://AppDomainName/`을 엽니다.
(켜져 있으면) Cognito 로그인으로 리디렉션된 뒤 마이그레이션 워크플로(Connect → Migration plan →
Evaluation → Schema Conversion → Data Migration → Validation → Cut over)로 이동합니다.

### 관측성 & 런타임 진단

배포는 의도적으로 파라미터를 최소화합니다: **로그 레벨과 활동 로그의 CloudWatch 미러링은
CloudFormation 파라미터가 아닙니다** — 앱의 **Diagnostics** 컨트롤(사이드바 푸터)에서 런타임에
조정하세요, 재배포 불필요:

- **로그 레벨** — 문제 해결 중 `INFO`/`DEBUG` 전환(DEBUG는 실패 이벤트에 Python 스택트레이스 추가;
  행 값이나 자격증명은 절대 없음).
- **Send to CloudWatch (stdout)** — 켜면 활동 로그를 stdout으로 스트리밍하고, 컨테이너의 `awslogs`
  드라이버가 이 스택의 CloudWatch 로그 그룹으로 전달(태스크 교체에도 살아남는 내구성 감사 사본).
- **Download activity log** — 같은 푸터에서 전체 UTC, 이벤트당 한 줄 타임라인(연결 / 평가 / 스키마
  적용 / Full Load / CDC)을 받기. 파일은 `/tmp`에서 크기 제한·회전됨.

변경은 앱 전체(단일 태스크)에 적용되고 재시작 시 시작 기본값으로 리셋됩니다. 고급 운영자는
`DSQL_MIGRATOR_LOG_LEVEL` / `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` 환경 변수로 시작 기본값을 설정할 수
있지만, Diagnostics 컨트롤이 의도된 경로입니다.

---

## 7. 새 이미지 버전으로 업데이트

새 태그를 빌드·푸시한 뒤 새 `ContainerImageUri`로 재배포합니다. ECS가 태스크를 롤링 교체합니다:

```bash
export IMAGE_TAG=0.1.1
export IMAGE_URI="$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$IMAGE_TAG"
docker build -f deploy/Dockerfile -t "$ECR_REPO:$IMAGE_TAG" .
docker tag "$ECR_REPO:$IMAGE_TAG" "$IMAGE_URI"
docker push "$IMAGE_URI"

aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator --region "$AWS_REGION" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ContainerImageUri=$IMAGE_URI
  # (나머지 파라미터를 다시 제공하거나 이전 값에 의존)
```

> 컨트롤 플레인은 **단일 태스크**로 실행되므로 교체 중 짧은 중단이 있습니다. 마이그레이션된 데이터,
> DSQL 클러스터, 배포된 cdc-stack은 영향받지 않고 재연결 시 자동 복구됩니다. 진행 중 세션 상태
> (워크플로 진행, 진행 중인 Full Load)는 태스크 임시 디스크에 있어 **살아남지 않습니다** —
> **업데이트 전에 진행 중 작업을 끝내거나 정지**한 뒤, 재연결해 읽기 전용 Evaluation을 다시
> 실행(몇 분)하세요.

---

## 8. AI 보조 변환 활성화 (Optional)

AI 보조는 opt-in이며 **범위 제한된** `bedrock:InvokeModel`을 부여합니다:

```bash
aws cloudformation deploy ... \
  --parameter-overrides \
    EnableAiAssist=true \
    BedrockModelId=us.anthropic.claude-sonnet-4-6 \
    BedrockRegion=$AWS_REGION
```

**AI 보조는 Amazon Bedrock에서만 동작합니다.** Bedrock이 유일한 AI 백엔드이며 —
이 도구에는 Anthropic/OpenAI(또는 그 외) API 키를 직접 입력하는 칸이 없습니다. 따라서
선택할 수 있는 모델은 AWS 자격증명으로 호출하는 Bedrock 파운데이션 모델뿐입니다.
모델은 `BedrockModelId`로 지정합니다(기본값 `us.anthropic.claude-sonnet-4-6`).

**권장 모델 — 최신 Anthropic Claude Opus 또는 Sonnet:**

| 모델 | Bedrock 모델 id (`BedrockModelId`) | 사용 시점 |
|---|---|---|
| Claude Opus 4.8 | `us.anthropic.claude-opus-4-8` | 가장 어려운 `MANUAL` / `UNSUPPORTED` 변환; 최고 품질. |
| Claude Opus 4.6 | `us.anthropic.claude-opus-4-6-v1` | 고품질; 4.8보다 한 단계 아래. |
| Claude Sonnet 4.6 (기본값) | `us.anthropic.claude-sonnet-4-6` | 대부분의 스키마에서 품질·속도·비용의 최적 균형. |

`BedrockModelId`는 위 `us.` cross-region inference profile들의 **드롭다운**이고, 태스크
역할의 `bedrock:InvokeModel` 스코프는 여기서 **자동 도출**됩니다 — 따라서 `BedrockModelArns`는
**설정할 필요가 없습니다**(다른 모델/ARN으로 바꿀 때만 override로 사용). 단, 선택한 모델의
**모델 액세스는 `BedrockRegion`의 Bedrock 콘솔에서 직접 활성화**해야 합니다.

태스크 egress가 Bedrock runtime 엔드포인트에 도달할 수 있는지 확인(NAT 또는 Bedrock VPC 엔드포인트).
UI에서 AI를 켜고, **Verify AI access** 사전 점검으로 도달성을 확인하세요.

---

## 9. Teardown

> **완전 teardown 순서 (모든 리소스/비용 제거).** 마이그레이션은 최대 3개의 스택을 씁니다. 아무것도 —
> 비용도 — 남지 않도록 다음 순서로 제거하세요:
>
> 1. **cdc-stack 먼저 (CDC를 배포한 적이 있다면)** — 비용의 핵심입니다(Amazon MSK / MSK Connect /
>    NAT). **앱이 떠 있는 동안** UI에서 제거하세요: **Start over(우측 상단) → "Delete all CDC
>    infrastructure"** (앱이 `cdc-stack` 삭제를 구동, ~15–25분). 앱이 이미 없으면 수동 삭제:
>    `aws cloudformation delete-stack --stack-name mysql-dsql-cdc-stack --region "$AWS_REGION"`.
>    (CDC는 별도 `cdc-stack` — CDC 문서 참고.)
> 2. **app-stack** — `deploy/teardown.sh` (아래).
> 3. **build-stack** — Option B(CodeBuild)를 썼을 때만 (아래).
> 4. **잔여 확인** — `mysql-dsql-*` CloudFormation 스택이 남지 않았는지
>    (`aws cloudformation list-stacks --query "StackSummaries[?starts_with(StackName,\`mysql-dsql\`) && StackStatus!=\`DELETE_COMPLETE\`].StackName"`),
>    그리고 직접 만든 **Route 53** 레코드와 **CodeBuild 소스 S3 버킷**.

헬퍼 스크립트 사용(스택을 삭제하고 대기; 기본적으로 ECR repo 유지):

```bash
export AWS_REGION=us-east-1
deploy/teardown.sh mysql-dsql-migrator          # 스택만 삭제
DELETE_ECR=true deploy/teardown.sh mysql-dsql-migrator   # ECR repo + 이미지도 제거
```

직접 만든 Route 53 레코드는 수동으로 제거해야 합니다.

**Option B (CodeBuild)**를 썼다면 빌드 스택도 삭제(그 ECR repo는 `EmptyOnDelete`라 이미지가 함께
제거됨):

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-build --region "$AWS_REGION"
```

---

## 10. 문제 해결

| 증상 | 가능 원인 / 조치 |
| --- | --- |
| 서비스가 `runningCount=1`에 도달 못 함 | 이미지 pull 실패(`ContainerImageUri`, execution role, ECR egress/VPC 엔드포인트 확인) — ECS 서비스 이벤트 참고. |
| 이미지 pull에서 멈춤 / egress 없음(프라이빗 서브넷, NAT 없음) | NAT 게이트웨이 또는 VPC 엔드포인트(ecr.api, ecr.dkr, S3 gateway, logs, secretsmanager, sts) 추가, 또는 테스트는 퍼블릭 서브넷의 `ServiceSubnetIds`에 `AssignPublicIp=ENABLED`. |
| 태스크가 `exec format error`로 정지 | 이미지 아키텍처 불일치. `build_and_push.sh`는 태스크 기본 X86_64에 맞춰 `linux/amd64` 빌드; ARM64/Graviton에서 실행 시에만 `IMAGE_PLATFORM=linux/arm64`. |
| 빌드 시 `docker: command not found` | 로컬 컨테이너 런타임 없음. 설치(Option A: `brew install colima docker && colima start`)하거나 Option B(CodeBuild)로 로컬 Docker 없이 클라우드 빌드. |
| 타깃 그룹 unhealthy / 502 | 앱이 `0.0.0.0:AppPort`에서 수신 안 함, 또는 헬스 체크 경로 `/` 실패 — 컨테이너 로그 확인. |
| ALB에서 504 / 타임아웃 | 태스크 SG가 ALB SG 인바운드를 허용 안 함, 또는 태스크가 egress 없는 서브넷에 있음. |
| Cognito 리디렉션 루프 / 401 | `AppDomainName`이 인증서와 Cognito 콜백 `https://AppDomainName/oauth2/idpresponse`와 일치해야 함; 사용자 미생성/미확인. |
| 앱이 소스 DB에 도달 못 함 | 소스 DB SG가 태스크 SG로부터 `SourceDbPort` 인바운드를 허용해야 함; `SourceDbSecurityGroupId`/`SourceDbCidr` 확인. |
| DSQL 인증 에러 | `DsqlClusterArn` 범위, 리전(`DSQL_MIGRATOR_AWS_REGION`), task-role `dsql:DbConnect`. |
| AI 켰을 때 Bedrock 에러 | `BedrockModelArns` 범위, `BedrockRegion`에서 모델 활성화, Bedrock 엔드포인트 egress. |
| 실패 진단에 더 상세히 필요 | 앱 **Diagnostics** 컨트롤(사이드바 푸터)에서 로그 레벨을 `DEBUG`로 설정해 활동 로그 실패 이벤트에 Python 스택트레이스 추가; "Send to CloudWatch (stdout)"를 토글해 내구성 사본. 재배포 불필요. |

---

## 11. 보안 노트

- **최소 권한**: task role은 `dsql:DbConnect` + `dsql:DbConnectAdmin`(클러스터 범위; 앱은 기본적으로
  DSQL `admin` 역할로 연결), 읽기 전용 `dsql:GetCluster` + `dsql:ListTagsForResource`(클러스터
  범위; 개요 다이어그램에 클러스터의 `Name` 태그를 표시하는 데만 사용), `secretsmanager:GetSecretValue`
  (소스 시크릿 범위)만 부여; `bedrock:InvokeModel`은 AI 보조를 켤 때만 추가되며 허용 모델 ARN(들)로
  범위 제한됨. 별도의 execution role이 ECR pull + 로그를 처리하고 컨테이너 시작 시 주입할 자동 생성
  세션 쿠키 시크릿만 읽음(범위 제한된 `secretsmanager:GetSecretValue`).
- **자동 생성 세션 쿠키 시크릿**: 스택이 브라우저 세션 쿠키(`DSQL_MIGRATOR_STORAGE_SECRET`)를 서명하는
  `AWS::SecretsManager::Secret`(운영자 입력 없음)을 생성해, 재연결한 브라우저가 태스크 재시작 간에도
  워크벤치 상태를 재개. 쿠키만 서명 — DB/사용자 자격증명 아님 — 하고 템플릿에 평문으로 절대 없음.
- **감사 추적**: 구조화된 활동 로그(성공 + 실패 타임라인, UI에서 다운로드)는 비밀이 아닌 필드만 기록 —
  행 값, 비밀번호, IAM 토큰은 절대 없음. 태스크 임시 디스크에서 크기 제한·회전됨; 내구성 사본은
  CloudWatch 미러(6절)를 켜기.
- **네트워크**: ALB는 `AllowedIngressCidr`로부터만 443 수락; 태스크는 ALB로부터만 트래픽 수락;
  태스크 egress는 소스 DB(`SourceDbPort`), 아웃바운드 443(AWS 엔드포인트), 5432(Aurora DSQL
  엔드포인트)로 범위 제한. `internal` ALB 선호.
- **자격증명은 절대 저장 안 됨** — 템플릿이나 이미지에. 앱이 런타임에 소스 시크릿을 읽고 단기 IAM
  토큰으로 DSQL에 인증.
- **컨테이너 이미지 CVE (perl).** 앱 이미지를 ECR 스캔하면 base 이미지의 `perl 5.40.1-6`에 대해
  여러 `perl` CVE(예: CVE-2026-12087, CVE-2026-489xx)가 표시됩니다. `perl`은 **`python:3.12-slim`
  (Debian trixie) base의 transitive 패키지**이며 도구가 쓰는 게 아닙니다 — 앱은 **순수 Python이고
  perl을 절대 실행하지 않으므로**, 이 컨테이너에서 취약 코드 경로는 **도달 불가**합니다.
  `Dockerfile` runtime 스테이지는 빌드 시 `apt-get upgrade`를 실행하므로 Debian이 수정판을 내는
  순간 재빌드가 자동으로 채택합니다. 다만 현재 이 CVE들은 **Debian trixie/sid에서 아직 open**
  (업그레이드할 수정 `perl`이 없음)이라, 오늘은 어떤 이미지 재빌드로도 스캔을 깨끗하게 만들 수
  없습니다. 그 전에 clean scan이 컴플라이언스상 필요하면 perl이 없는 base(예: distroless Python
  이미지)로 재빌드하세요 — 단, 이는 별도 검증이 필요한 더 큰 변경입니다.
- 이 스택은 이 저장소에서 배포된 적 **없음** — 프로덕션 사용 전 대상 계정에서 검증하세요.

---


## 부록 — 자체 이미지 빌드 (제한된 네트워크 전용)

> **대부분의 배포는 이 절을 건너뜁니다.** 이미지는 ECR Public에 게시돼 있고 CloudFormation이 기본으로
> 가져오므로 빌드할 게 없습니다. 네트워크가 ECR Public에 도달할 수 없을 때만 자체 이미지를 빌드해
> (`ContainerImageUri`로 전달) 사용하세요. 아래 Option A/B 중 하나 — 둘 다 ECR 저장소를 만들고
> `deploy/Dockerfile`을 `linux/amd64`로 빌드해 ECR에 푸시하고 이미지 URI를 출력합니다.

### Option A — 로컬 빌드 (Docker 호환 런타임 필요)

```bash
export AWS_REGION=us-east-1
deploy/build_and_push.sh            # 태그 기본값은 프로젝트 버전
# 또는 명시적 태그 고정:
deploy/build_and_push.sh 0.1.0
```

<!-- markdownlint-disable-next-line -->
실행 중인 `docker` 데몬 필요(Docker Desktop, 또는 `brew install colima docker && colima start`).

### Option B — AWS CodeBuild 클라우드 빌드 (로컬 Docker 불필요)

빌드 인프라를 한 번 배포(ECR repo + S3 소스 버킷 + CodeBuild 프로젝트)한 뒤, 헬퍼로 소스를 zip해
업로드하고 빌드를 시작합니다:

```bash
export AWS_REGION=us-east-1

# 1회: 빌드 인프라 프로비저닝.
aws cloudformation deploy \
  --template-file deploy/codebuild.yaml \
  --stack-name mysql-dsql-migrator-build \
  --capabilities CAPABILITY_IAM \
  --region "$AWS_REGION"

# 매 빌드: 소스 zip + 업로드, CodeBuild 실행, 대기, 이미지 URI 출력.
deploy/build_in_codebuild.sh            # 태그 기본값은 프로젝트 버전
# 또는 명시적 태그 고정:
deploy/build_in_codebuild.sh 0.1.0
```

CodeBuild는 관리형(권한 있는) 환경에서 Docker를 실행하므로 사용자 머신엔 AWS CLI만 있으면 됩니다.
이미지는 `linux/amd64`로 빌드되어 같은 ECR 저장소에 푸시됩니다.

> 배포 재현성을 위해 릴리스마다 immutable 태그(또는 이미지 digest)를 사용하세요.
