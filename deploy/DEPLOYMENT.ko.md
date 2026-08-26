# 배포 가이드 — MySQL / PostgreSQL → Aurora DSQL 마이그레이션 도구 (app-stack)

_언어: [English](DEPLOYMENT.md) | **한국어** | [日本語](DEPLOYMENT.ja.md)_

이 도구는 **고객 자신의 AWS 계정 안에서(단일 테넌트)** 배포합니다 — 어디서나 같은 도구·같은
UI이며 **어디서 실행하느냐**만 다릅니다. 선택적 스트리밍 **CDC 파이프라인**(MSK + Debezium +
싱크)은 별도의 `cdc-stack`이며 여기서 다루지 않습니다.

---

## 어디서 실행할지 선택

하나를 고르세요 — 각 모드는 아래에 자체 섹션이 있습니다.

- **[로컬에서 실행](#로컬에서-실행)** — `uv run …`, **인프라 없음.** 내 머신이 엔진이므로 소스(MySQL 또는
  PostgreSQL)와 DSQL **양쪽**에 도달할 수 있어야 합니다. 평가·소규모 마이그레이션에 적합. **👉 먼저 이걸
  시도하세요.**
- **[ECS Fargate에 배포](#ecs-fargate에-배포)** — **실제·대규모 마이그레이션에 권장.** 내 VPC 안의
  단일 태스크 **ECS Fargate** 서비스가 **HTTPS ALB** 뒤에서 돌고 이미지는 **ECR**에서 가져오므로,
  데이터 경로가 내 노트북이 아니라 AWS 안에 머뭅니다. ALB는 기본 **`internal`**이며 **Cognito**(OIDC)
  로그인은 공개 노출 시에만 쓰는 opt-in 추가 기능입니다.
- **[단일 EC2 호스트에서 실행](#단일-ec2-호스트에서-실행-소스에서-lambda-free)** — 앱을
  **소스에서**(`git` + `uv` + **systemd** 서비스) 실행하고 **SSM 포트포워드**로 접속합니다(ALB·공인
  IP 없음). 상태는 보존형 EBS 볼륨, CDC는 **인프로세스로** 시드(오프셋 시더 Lambda 없음). 계정이
  **컨테이너/ECR**나 **AWS Lambda**를 쓸 수 없을 때.

---

<br>

## 로컬에서 실행

**ECS Fargate 배포를 결정하기 전에, 먼저 로컬에서 시도해보세요** — **명령어 한 줄이면 UI가 뜹니다.
끝입니다.**

```console
$ uv run mysql-dsql-migrator ui
NiceGUI ready to go on http://127.0.0.1:8080
```

이 URL을 브라우저에서 열면 바로 사용 — **인프라도, 빌드도, 만들 AWS 리소스도 없습니다.** 첫 확인·
평가·소규모 마이그레이션에 좋고, Fargate로 갈지 결정하기 전에 써보기 좋습니다.

<details>
<summary><b>스크린샷</b> — 도구 UI(5단계 가이드 워크플로우)</summary>

<div align="center">
  <a href="../docs/images/demo-ui.png"><img src="../docs/images/demo-ui.png" alt="도구 UI — 5단계 가이드 마이그레이션 워크플로우" width="900"></a>
</div>

</details>

UI가 내 머신에서 돌고(브라우저 → `127.0.0.1:8080`), **마이그레이션 자체도 거기서 실행**됩니다 — 내
워크스테이션이 소스를 읽고 DSQL에 쓰는 엔진이라, 모든 데이터가 내 머신과 네트워크를 통과합니다.
따라서 **내 데스크톱이 소스(MySQL 또는 PostgreSQL)와 타깃 Aurora DSQL _양쪽_ 모두에 도달**할 수 있어야 합니다 —
프라이빗 소스는 SSM 포트 포워딩 / VPN이 필요하고, 내 머신은 DSQL 리전으로의 아웃바운드 HTTPS +
AWS 자격증명이 있어야 합니다. 인프라 없음 — 평가 / 소규모 마이그레이션 / 개발에 적합. 호스팅
아키텍처는 아니며, 실제 마이그레이션은 **[ECS Fargate](#ecs-fargate에-배포)**를 쓰세요.

> [!TIP]
> **재시작에도 세션(과 편집)을 유지하세요.** 브라우저 세션 id(저장된 워크벤치가 보관되는 키)가
> 고정되도록 `DSQL_MIGRATOR_STORAGE_SECRET`을 고정 랜덤 문자열로 지정해 실행하세요:
>
> ```bash
> DSQL_MIGRATOR_STORAGE_SECRET=$(python -c "import secrets; print(secrets.token_urlsafe(32))") \
>   uv run mysql-dsql-migrator ui
> ```
>
> - **설정하지 않으면** — 재시작마다 세션 id가 바뀌어, 워크플로 진행과 **Schema Conversion 편집**
>   (커스터마이즈한 타깃 DDL, 예: `TINYINT(1)`→`smallint`)이 복원되지 않고, Full Load 재실행이 기본
>   변환으로 테이블을 재생성합니다.
> - **설정하면** — 세션이 중단한 지점부터 이어지고, 재실행이 적용된 스키마를 재사용합니다.
>
> 값은 비밀로 취급하세요(— [`.env.example`](../.env.example) 참고).

---

<br>

## ECS Fargate에 배포

> [!TIP]
> **프로덕션 — 실제·대규모 마이그레이션에 권장.** 데이터 경로 전체가 내 노트북이 아니라 AWS
> 안에 머뭅니다(소스 → Fargate → DSQL).

이미지 빌드 없이 세 가지 방법으로 배포합니다 — 이미지는 **ECR Public**에 있고 CloudFormation이
가져옵니다. 같은 `deploy/cloudformation.yaml`을 이렇게 배포하세요:

- **AI 코딩 에이전트(가장 쉬움).** 에이전트(Claude Code / Kiro / Cursor)가 파라미터를 찾아 대신 배포합니다.
- **AWS Console — 권장.** 템플릿을 업로드하면 안내형 폼이 값을 모아줍니다.
- **AWS CLI.** `aws cloudformation deploy` 한 번으로 파라미터를 넘깁니다.

둘 다 [app-stack 배포](#2-app-stack-배포)에 자세히 있고 — 필요한 값은 먼저
[사전 요구사항](#1-사전-요구사항)에서 모으세요.

**UI 접근.** 기본이 `internal` ALB라 `https://<LoadBalancerDns>/`는 VPC 안에서 — VPN,
Direct Connect, 또는 SSM 포트 포워딩으로 — 열어야 합니다. 설계상 공용 엔드포인트가 없습니다 —
Well-Architected SEC05-BP02. 외부에 공개하려면 [app-stack 배포](#2-app-stack-배포)의 오버라이드
노트를 참고하세요.

<details>
<summary><b>아키텍처 다이어그램</b> — 전체 토폴로지(app-stack + 선택적 CDC on MSK Connect)</summary>

<div align="center">
  <a href="../docs/images/architecture-aws.png"><img src="../docs/images/architecture-aws.png" alt="전체 AWS 아키텍처 — 운영자가 HTTPS ALB(선택적 Cognito)를 통해 ECS Fargate 컨트롤 플레인 앱에 접속하고, 앱이 Aurora DSQL로 Full Load를 수행하며, 선택적 CDC 파이프라인에서는 cdc-stack을 배포해 Debezium 소스 + 커스텀 DSQL 싱크 커넥터가 MSK Connect에서(S3의 플러그인 로드) 실행되어 Amazon MSK를 거쳐 Aurora DSQL로 스트리밍하고, gapless 핸드오프를 위한 VPC 내 offset-seeder Lambda가 있음" width="900"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. 사전 요구사항

무엇을 모을지, 그다음 곧바로 [app-stack 배포](#2-app-stack-배포)로 가세요. 자세한 설명은 아래
접이식과 [파라미터 레퍼런스](#파라미터-레퍼런스)에 있습니다.

| 무엇 | 파라미터 | 설명 |
| --- | --- | --- |
| 접근 | — | AWS Console(권장) 또는 AWS CLI v2 — IAM 역할, ECS, ALB, 보안 그룹, CloudWatch Logs를 생성할 수 있어야 함. 이미지 빌드 불필요 — ECR Public에서 가져옵니다. |
| VPC | `VpcId` | 소스 DB의 VPC가 이상적, **DSQL과 같은 리전**. 아래 서브넷은 여기서 고릅니다. |
| ALB 서브넷 2개 + 태스크 서브넷 2개 | `AlbSubnetIds` / `ServiceSubnetIds` | 서로 다른 AZ; 태스크 서브넷은 **443 egress** 필요. |
| ACM 인증서 | `CertificateArn` | 같은 리전. 도메인이 없나요? `AWS_REGION=<region> deploy/create_test_cert.sh`로 self-signed 테스트 인증서를 만드세요. |
| DSQL 클러스터 ARN | `DsqlClusterArn` | 마이그레이션 타깃. |
| 소스 DB 도달성 | `SourceDbSecurityGroupId`(권장) 또는 `SourceDbCidr` | 둘 중 하나. |

소스 DB 자격증명은 배포 **후** UI에서 입력합니다(재사용할 게 아니면 AWS 시크릿 불필요); 그 외
파라미터는 모두 합리적 기본값을 유지합니다.

<details>
<summary><b>전체 파라미터 상세</b> — VPC / 서브넷 / 인증서 안내, 그리고 모든 옵션 값</summary>

#### 접근

- **AWS Console** 접근(권장 경로), **또는** 대상 계정에 인증된 AWS CLI v2
  (`aws sts get-caller-identity`).
- 스택 리소스 생성 권한: IAM 역할, ECS, ELB(ALB), EC2 보안 그룹, CloudWatch Logs,
  Cognito(옵션 — 공개 ALB일 때만).
- 이미지 빌드 불필요 — 이미지는 ECR Public에서 가져온다. (자체 빌드는 제한된 네트워크 전용; 부록 참고.)

#### 폼을 채우기 전에 알아둘 것

> [!IMPORTANT]
> **VPC부터 정하세요.** 소스 RDS/Aurora(MySQL 또는 PostgreSQL)가 이미 있는 VPC를 사용하세요 — DSQL과 같은 리전 —
> 그리고 여기서 고르는 서브넷/인증서는 AWS 자체 요구사항입니다(ALB와 Fargate 태스크는 서브넷에
> 배치돼야 하고, HTTPS 리스너는 인증서가 있어야 함). **이 VPC는 이 계정 소유여야 합니다** —
> RAM 공유(계정 간) VPC는 지원되지 않습니다. CDC 배포 역할의 EC2 권한이 배포하는 계정으로
> 범위 제한돼 있어, 커넥터의 ENI 생성이 `AccessDenied`로 실패하기 때문입니다.

#### 옵션 값 (없으면 합리적 기본값 사용)

| 옵션 | 파라미터 | 필요한 경우 |
| --- | --- | --- |
| **소스 시크릿 ARN** | `SourceSecretArn` | 기존 Secrets Manager 시크릿을 **재사용**할 때만. 비워두면 UI에서 id/password 입력(일반적인 경우). |
| **소스 DB 도달 경로** | `SourceDbSecurityGroupId`(권장) / `SourceDbCidr` | **둘 중 최소 하나 필수** — 태스크가 소스 DB(`SourceDbPort`)로 egress하도록. `SourceDbSecurityGroupId`는 소스 DB SG로 egress를 범위 제한, SG id가 없으면 `SourceDbCidr` 사용. 둘 다 비우면 배포가 거부됩니다(소스로 가는 경로 없음). |
| **커스텀 도메인** | `AppDomainName` | 자체 Route 53 도메인으로 ALB를 front할 때만. |
| **공개 접근 / Cognito** | `AlbScheme`, `AllowedIngressCidr`, `EnableCognitoAuth`, `CognitoDomainPrefix` | UI를 공개할 때만; 기본은 `internal`(로그인 없음). |
| **AI 보조** | `EnableAiAssist`, `BedrockModelId`, `BedrockRegion` | Amazon Bedrock 보조 변환을 켤 때만(모델 선택; IAM 스코프 자동 도출). |
| **커스텀 이미지 / 사이징** | `ContainerImageUri`, `ContainerCpu`, `ContainerMemory` | 프라이빗 ECR 이미지나 기본 외 태스크 크기일 때만. |

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. app-stack 배포

`deploy/cloudformation.yaml`을 배포하는 세 가지 방법 중 하나를 선택하세요. 셋 다 같은 스택을
만듭니다. 파라미터 설명은 **파라미터 레퍼런스** 참고.

| | 옵션 | 적합 |
| --- | --- | --- |
| **A** | **AI 코딩 에이전트** | 가장 쉬움·오류 적음 — Claude Code / Kiro / Cursor 사용 시. |
| **B** | **AWS Console** | 네이티브 피커가 있는 안내형 폼 (권장). |
| **C** | **AWS CLI** | 스크립트/반복 배포. |

#### 옵션 A — 가장 쉬움: AI 코딩 에이전트로 배포

셸 접근이 가능한 AI 코딩 에이전트를 쓴다면 — **Claude Code, Kiro, Cursor, 또는 AWS CLI를
실행할 수 있는 모든 에이전트** — 이 배포 전체를 대신 진행해줄 수 있습니다. 에이전트가 이
가이드를 읽고 **대부분의 파라미터를 내 AWS 계정에서 찾아내고**(리전, VPC, 서브넷, DSQL 클러스터
ARN, 소스 DB 보안 그룹과 포트), 진짜 결정이 필요한 몇 가지만 물은 뒤, CloudFormation 배포를
실행하고 URL을 출력합니다. 이것이 오류가 가장 적은 경로입니다: 수동 배포에서 가장 자주 발목을
잡는 서브넷/AZ 선택, `--s3-bucket` 템플릿 스테이징, 스택 이름 제한, `CAPABILITY_NAMED_IAM`
플래그를 에이전트가 알아서 처리합니다 — **MySQL이든 PostgreSQL이든** 소스에 상관없이.

**준비물:** 에이전트가 가리키도록 클론한 저장소, 그리고 타깃 Aurora DSQL 클러스터가 있는
계정·리전에 대해 **에이전트 셸에서 사용 가능한 AWS 자격증명**(`aws sts get-caller-identity`가
성공해야 함).

**이 프롬프트를 에이전트에 붙여넣으세요**(빈칸 두 개를 채우세요):

```text
deploy/DEPLOYMENT.md("Deploy on ECS Fargate")를 따라 이 저장소의 ECS Fargate app-stack
(deploy/cloudformation.yaml)을 내 AWS 계정에 배포해줘.

타깃 Aurora DSQL 클러스터: <DSQL cluster ARN or endpoint>
소스 데이터베이스: <RDS/Aurora identifier — or "I'll enter it in the UI later">

단계:
1. deploy/DEPLOYMENT.md와 deploy/cloudformation.yaml의 파라미터를 읽어.
2. 가능한 곳은 묻지 말고 DISCOVER(읽기 전용)해: DSQL ARN/엔드포인트에서 리전을 도출하고;
   소스 DB의 VpcId를 찾고; 그 VPC 안에서 서로 다른(DISTINCT) AZ의 서브넷 2개를 AlbSubnetIds와
   ServiceSubnetIds로 선택하고(라우트 테이블로 public/private 분류); 소스 DB의 보안 그룹 id
   (SourceDbSecurityGroupId)와 포트(SourceDbPort: MySQL은 3306, PostgreSQL은 5432)를 찾고;
   DsqlClusterArn을 확인해.
3. ACM 인증서가 없으면 deploy/create_test_cert.sh를 실행하고 출력된 ARN을 사용해(self-signed
   테스트 인증서; 브라우저가 경고하지만 private/internal ALB에는 괜찮음).
4. 해석된 전체 파라미터 세트와 정확한 `aws cloudformation deploy` 명령을 나에게 보여주고,
   무엇이든 만들기 전에 내 승인을 WAIT(대기)해. 정말 추론할 수 없는 것만 물어봐 — 주로:
   internal ALB(VPN/peering으로 접근) vs internet-facing + Cognito 로그인, 그리고 AI 보조
   (Amazon Bedrock) 활성화 여부.
5. 내가 OK하면 배포해 — 스택 이름은 소문자로 28자 이하; --s3-bucket으로 템플릿을 스테이징하고
   (CloudFormation의 51,200바이트 인라인 한도를 초과함); --capabilities CAPABILITY_IAM
   CAPABILITY_NAMED_IAM를 사용해. CREATE_COMPLETE를 기다린 뒤, AppUrl 출력값과 접속 방법을
   출력해(ALB는 기본적으로 internal).

가드레일: 오직 단일 리전(DSQL 클러스터의 리전); 만들기 전에 read/describe; 기존 리소스를 절대
수정하거나 삭제하지 마; 모든 것을 프로덕션으로 취급; 모호하거나 단계가 실패하면 멈추고 물어봐.
```

에이전트가 진짜 결정 몇 가지(internal vs 공개/Cognito, AI 보조 활성화 여부, 비용 승인)를
제시하고 나머지는 모두 채웁니다. **승인하기 전에 에이전트가 해석한 파라미터 세트를 검토하세요** —
에이전트가 내 자격증명으로 실행하는 AWS 작업의 책임은 나에게 있습니다. 나중에 정리하려면
에이전트에게 *"`mysql-dsql-migrator` 스택을 삭제해줘"* 라고 말하면 [Teardown](#teardown) 절차를
따릅니다.

> 이것은 아래 두 수동 경로와 같은 `deploy/cloudformation.yaml`을 사용합니다 — 에이전트는
> 파라미터를 해석하고 배포를 대신 실행할 뿐입니다. 직접 손으로 하고 싶다면(또는 에이전트에 AWS
> 접근이 없다면) Console 또는 CLI 경로를 사용하세요.

#### 옵션 B — 권장: AWS Console (안내형 폼)

![CloudFormation — Create stack → Upload a template file](../docs/images/cfn-create-stack.png)

먼저 콘솔 우측 상단에서 **올바른 리전**(Aurora DSQL 클러스터와 같은 리전)인지 확인한 뒤:

**1. Create stack 마법사 열기.** CloudFormation 콘솔로 이동:
<https://console.aws.amazon.com/cloudformation/home> → **Create stack** →
**With new resources (standard)**. (직접 링크, 리전만 교체:
`https://<region>.console.aws.amazon.com/cloudformation/home?region=<region>#/stacks/create`.)

**2. Prerequisite — Prepare template.** **Template is ready** 선택 → **Specify
template**에서 **Upload a template file** → **Choose file** → 이 저장소의
`deploy/cloudformation.yaml` 선택 → **Next**.

**3. Specify stack details.** **Stack name**을 `mysql-dsql-migrator`로 지정한 뒤
파라미터를 채웁니다. 폼은 네이티브 선택기라 ID를 타이핑하지 않고 **계정에서 골라**
입력합니다.

**아래 필수 필드를 채웁니다**(나머지는 기본값으로 동작):

| 필드 | 입력값 |
| --- | --- |
| `VpcId` | 드롭다운 — 소스 DB가 있는 VPC. |
| `AlbSubnetIds` | 서브넷 멀티선택 — **서로 다른 AZ 2개**(아래 서브넷 박스 참고). |
| `ServiceSubnetIds` | 서브넷 멀티선택 — **서로 다른 AZ의 프라이빗 2개**(프라이빗/NAT 서브넷이 없으면 ALB 서브넷을 그대로 쓰고 `AssignPublicIp=ENABLED` 설정). |
| `CertificateArn` | HTTPS용 ACM 인증서 ARN — **도메인이 없으면 바로 아래 명령 참고.** |
| `DsqlClusterArn` | 타깃 Aurora DSQL 클러스터 ARN. |
| `SourceDbSecurityGroupId`(또는 `SourceDbCidr`) | 둘 중 하나 — 태스크의 소스 DB egress 범위를 지정. 보안 그룹 id를 우선하고, 없을 때만 CIDR 사용. |

> [!WARNING]
> 서브넷 드롭다운은 내 VpcId 것만이 아니라 **리전의 모든 서브넷**을 보여줍니다. 다른 VPC의 서브넷을
> 고르면 배포가 실패하니, 아래 **"어떤 서브넷을 고를까"** 박스를 보고 올바른 것을 선택하세요.

**선택:** `SourceSecretArn`은 기존 소스 시크릿을 재사용할 때만 입력 — 보통은 비워두고 배포 후
UI에서 소스 host/id/password를 입력합니다.

> [!TIP]
> **ACM 인증서가 아직 없나요?** 한 줄로 self-signed **테스트** 인증서를 만들고, 출력된 ARN을
> `CertificateArn`에 붙여넣으면 됩니다(브라우저 경고; 테스트 전용 — 운영에서는 보유 도메인의
> 실제 ACM 인증서를 발급받아 쓰세요):

```bash
AWS_REGION=<region> deploy/create_test_cert.sh
#  → 출력:  CertificateArn=arn:aws:acm:<region>:<account>:certificate/xxxx
```

**내 데스크톱 브라우저에서 UI에 접속하나요?** 기본값은 `internal` ALB(VPC/VPN 내부에서만 접근)입니다.
내 PC에서 열려면 아래 A 또는 B 중 하나를 선택하세요:

**A. 권장 — Cognito 로그인으로 접속**(어디서든, 여러 명이 접속 가능):

| 필드 | 입력값 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **퍼블릭** 서브넷(프라이빗 아님) |
| `EnableCognitoAuth` | `true` — 그리고 `CognitoDomainPrefix`, `CognitoAdminEmail`(아래 노트 참고) |
| `AllowedIngressCidr` | `0.0.0.0/0`이면 충분합니다 — Cognito 로그인이 접근 게이트 역할을 합니다. 사용자들의 네트워크 CIDR을 안다면 그걸로 더 좁혀도 됩니다 |

**B. 대안 — 내 기기 하나만, 로그인 없이:**

| 필드 | 입력값 |
| --- | --- |
| `AlbScheme` | `internet-facing` |
| `AlbSubnetIds` | **퍼블릭** 서브넷(프라이빗 아님) |
| `AllowedIngressCidr` | 내 데스크톱 공인 IP를 `/32`로 — `curl https://checkip.amazonaws.com`로 확인(예: `203.0.113.5/32`) |

나머지 파라미터는 기본값 유지(예: 컨테이너 이미지). 특히 **`HttpsEgressCidr`는
`0.0.0.0/0` 그대로 두세요** — 태스크가 NAT/IGW로 AWS API(DSQL·Secrets Manager·ECR·CloudWatch)에
나가는 아웃바운드 CIDR입니다. 이 서비스들을 전부 VPC 엔드포인트(PrivateLink)로 둘 때만 좁히고,
그렇지 않은데 좁히면 이미지 pull/DSQL이 막혀 태스크가 기동 실패합니다. → **Next**.

> [!TIP]
> **어떤 서브넷을 고를까.** 드롭다운은 (내 VpcId 것만이 아니라) **리전의 모든 서브넷**을
> 보여줍니다. **먼저 CIDR 대역으로 내 VpcId의 서브넷으로 좁히세요**(예: VPC가 `172.31.0.0/16`이면
> → `172.31.x` 서브넷만 고르고, 다른 CIDR은 다른 VPC 것이니 무시), 그다음 **AZ 컬럼**으로 "다른 AZ
> 2개"를 맞추고 **Name 태그**로 public/private을 구분하세요. 어느 게 어느 건지 모르겠다면 **VPC
> 콘솔 → Subnets**에서 내 VPC로 필터하고 각 서브넷의 라우트 테이블을 확인하세요(`0.0.0.0/0 →
> nat-…` = egress 있는 프라이빗; `→ igw-…` = 퍼블릭) — Name 태그 규칙(예: `…-private-a` /
> `…-public-a`)을 두면 이후로는 드롭다운이 한눈에 구분됩니다.

> [!IMPORTANT]
> 위에서 **A**(Cognito)를 선택했다면 3단계 폼에서 `EnableCognitoAuth=true`, `CognitoDomainPrefix`,
> **그리고 `CognitoAdminEmail`**을 함께 설정하세요(이후 4~5단계는 그대로 진행). 그다음
> [DNS를 ALB로 지정](#dns를-alb로-지정--optional-커스텀-도메인만)(커스텀 도메인일 때만)과
> [운영자 사용자 만들기](#운영자-사용자-만들기-cognito--cognito를-켰을-때만)(로그인 및
> 추가 사용자 생성)을 참고하세요. `CognitoAdminEmail`은 선택이 아닙니다 — 유저풀에 self
> sign-up이 없어 이 값 없이 Cognito를 켜면 로그인할 방법이 없는 앱이 되므로 템플릿이
> 거부합니다. `AppDomainName`은 선택이며, 비워 두면 ALB 자체 DNS 이름을 씁니다.

**4. Configure stack options.** 기본값으로 충분. 필요하면 태그 추가. → **Next**.

**5. Review and create.** 맨 아래로 스크롤해 **"I acknowledge that AWS CloudFormation might
   create IAM resources with custom names"**(`CAPABILITY_NAMED_IAM`) 체크 → **Submit**.

**6. 대기 + URL 확인.** 스택이 `CREATE_IN_PROGRESS` → `CREATE_COMPLETE`로 진행됩니다(몇 분,
   **Events** 탭에서 관찰). 그다음 **Outputs** 탭에서 **`AppUrl`**을 복사 — 이것이 도구 URL입니다
   (VPC 안에서 접속; 위 "UI 접근" 참고).

**7. 열기 — 도구가 보여야 함.** 브라우저에서 `AppUrl`로 접속(VPC 내부에서)하면
   **Aurora DSQL Migration Tool** UI가 뜹니다 — **Connect**로 시작하는 안내형
   워크플로(Connect → Evaluation → Schema Conversion → Data Migration →
   Validation → Cut over). UI가 보이면 배포 완료이며, **Connect**에서 소스 DB 자격증명을
   입력해 시작합니다.

> [!NOTE]
> **▶ 다음: 첫 마이그레이션.** 배포는 여기서 끝 — UI가 떴습니다. 각 단계가 무엇을 하고
> 실제 마이그레이션을 어떻게 진행하는지는 [**사용자 매뉴얼**](../docs/manual/ko/README.md)을
> 따라가세요([설정](../docs/manual/ko/01-setup.md) → Connect에서 시작).

<details>
<summary><b>대안 — AWS CLI로 배포</b></summary>

#### 옵션 C — AWS CLI

환경을 셸 변수로 한 번 설정; 명령 자체는 모든 고객에게 동일. 최소(Dev/Test) 배포:

```bash
# --- 내 환경 (여기만 수정) ----------------------------------------------------
export AWS_REGION=us-east-1
# VpcId: 권장 -- 소스 DB의 VPC
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# AlbSubnetIds: 서브넷 2개, 다른 AZ
export ALB_SUBNET_IDS=subnet-0f1e2d3c4b5a69788,subnet-0a9b8c7d6e5f43210
# ServiceSubnetIds: 프라이빗 서브넷 2개
export SERVICE_SUBNET_IDS=subnet-0123456789abcdef0,subnet-0fedcba987654321f
# CertificateArn: 아래에 실제 ACM 인증서 ARN을 붙여넣거나, 도메인 없이 self-signed 테스트
# 인증서로 자동 채우려면 스크립트 출력을 한 줄로 캡처:
#   export CERTIFICATE_ARN=$(deploy/create_test_cert.sh | sed -n 's/^CertificateArn=//p')
export CERTIFICATE_ARN=arn:aws:acm:us-east-1:123456789012:certificate/a1b2c3d4-e5f6-47a8-9b0c-1d2e3f4a5b6c
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------
```

> [!WARNING]
> **스택 이름은 소문자로, 28자 이내로 지정하십시오.** 스택은 ALB를 `<스택이름>-alb`로
> 만들고 ALB 서비스는 이 이름을 32자로 제한합니다. 더 길면 약 2분간 롤백한 뒤
> `The load balancer name '<스택이름>-alb' cannot be longer than '32' characters`로
> 배포가 실패합니다. 소문자도 중요합니다 — ALB의 DNS 이름은 ALB 이름의 대소문자를 그대로
> 물려받고, Cognito 로그인은 이 DNS 이름이 소문자일 때만 동작합니다. ALB가 OAuth
> `redirect_uri`의 호스트를 소문자로 변환해 보내는데 Cognito는 두 문자열을 정확히
> 비교하기 때문입니다.

이 템플릿은 CloudFormation의 인라인 업로드 한도(51,200바이트)를 초과해서, CLI가 이를 스테이징할
S3 버킷이 필요합니다(콘솔은 이 문제를 자동으로 처리하므로 권장 경로인 이유이기도 합니다). 한 번만
만들거나, 이 계정/리전에 이미 있는 버킷을 재사용하세요:

```bash
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export TEMPLATE_BUCKET=mysql-dsql-migrator-templates-$ACCOUNT-$AWS_REGION
aws s3 mb "s3://$TEMPLATE_BUCKET" --region "$AWS_REGION" 2>/dev/null || true
```

```bash
aws cloudformation deploy \
  --template-file deploy/cloudformation.yaml \
  --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" \
  --s3-bucket "$TEMPLATE_BUCKET" \
  --s3-prefix cfn-templates \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    AlbSubnetIds="$ALB_SUBNET_IDS" \
    ServiceSubnetIds="$SERVICE_SUBNET_IDS" \
    CertificateArn="$CERTIFICATE_ARN" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    EnableAiAssist=true \
    BedrockRegion="$AWS_REGION" \
    BedrockModelId=global.anthropic.claude-sonnet-5
    # BedrockModelId 기본값 표시; 다른 모델 선택지는 §8 참조
    # SourceSecretArn=...   # 옵션 — 기존 소스 시크릿을 재사용할 때만
```

> [!TIP]
> **AI 어시스트(권장).** `EnableAiAssist=true` + `BedrockRegion`으로 Schema
> Conversion과 Query Converter의 AI DBA를 켭니다 — 옵트인·자문 전용 기능이며,
> 선택한 모델에 대한 `bedrock:InvokeModel`로만 범위가 제한됩니다. `BedrockModelId`
> (기본 `global.anthropic.claude-sonnet-5`)에 대해 해당 리전 Bedrock 콘솔에서 **모델
> 액세스를 활성화**해야 하고, 태스크가 Bedrock 엔드포인트로 egress할 수 있어야 합니다.
> 둘 다 생략하면 AI 없이 배포됩니다(결정론적 경로는 그대로). 자세한 내용·모델 선택은
> §8을 보세요.

외부 접속 + Cognito 로그인을 쓰려면 `--parameter-overrides`에 다음을 추가하세요:

```bash
    AlbScheme=internet-facing \
    AllowedIngressCidr=0.0.0.0/0 \
    EnableCognitoAuth=true \
    CognitoDomainPrefix=<고유-prefix> \
    CognitoAdminEmail=<이메일>
```

Cognito 필드 3개는 항상 함께 필요합니다 — 템플릿이 강제합니다.

> [!TIP]
> **`--parameter-overrides`에 추가할 수 있는 다른 오버라이드**
>
> - **공개 UI(내 데스크톱에서 접속):** `AlbScheme=internet-facing` **및**
>   `AllowedIngressCidr=<내 공인 IP>/32`(확인: `curl https://checkip.amazonaws.com`). 기본값
>   `10.0.0.0/8`은 내부 전용이라 외부 브라우저를 차단; `0.0.0.0/0`(완전 개방)은 금물(추가로
>   `EnableCognitoAuth=true` 필요).
> - **커스텀 도메인:** `AppDomainName=<내 도메인>`을 추가 — [DNS를 ALB로 지정](#dns를-alb로-지정--optional-커스텀-도메인만) 참고.
> - **커스텀 이미지:** `ContainerImageUri`를 오버라이드 — 제한된 네트워크라면 자체 프라이빗 ECR 사본
>   ([pull-through 캐시](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html)
>   또는 `deploy/Dockerfile`에서 빌드, 부록 참고)으로, 그 외에는 직접 관리하는 다른 이미지로.
> - **적용 전 변경사항 검토(운영):** 위 명령에 `--no-execute-changeset`을 추가하면 배포 대신
>   change-set ARN을 출력합니다. `aws cloudformation describe-change-set --change-set-name <그 ARN>
>   --region "$AWS_REGION" --query 'Changes[].ResourceChange.[Action,LogicalResourceId,ResourceType,Replacement]' --output table`로
>   검토한 뒤, 문제없으면 `aws cloudformation execute-change-set --change-set-name <그 ARN>
>   --region "$AWS_REGION"`으로 적용하세요.

완료 후 출력 읽기:

```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```

주요 출력: `LoadBalancerDns`, `AppUrl`, `ClusterName`, `ServiceName`,
`TaskRoleArn`, `CognitoHostedUiDomain`.

브라우저에서 **`AppUrl`**로 접속(VPC 내부에서)하면 **Aurora DSQL Migration Tool**
UI가 뜹니다 — **Connect**로 시작하는 안내형 워크플로(Connect → Evaluation →
Schema Conversion → Data Migration → Validation → Cut over). UI가 보이면 배포 성공이며,
**Connect**에서 소스 DB 자격증명을 입력해 시작합니다.

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 참고 및 운영

선택적 심화 자료 — 필요한 것만 펼쳐보세요; 첫 배포엔 필수가 아닙니다.

<details>
<summary><b>파라미터 레퍼런스와 태스크 사이징</b> — 모든 파라미터, CPU/메모리 사이징 방법</summary>

### 파라미터 레퍼런스

| 파라미터 | 필수 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `VpcId` | yes | — | 소스 DB에 프라이빗하게 도달할 수 있는 VPC. |
| `AlbSubnetIds` | yes | — | ALB용 서브넷 ≥2개(서로 다른 AZ). |
| `ServiceSubnetIds` | yes | — | Fargate 태스크용 프라이빗 서브넷. |
| `AlbScheme` | no | `internal` | `internal` 또는 `internet-facing`. **권장: `internal`**(VPN/Direct Connect/피어링으로 도달); `internet-facing`은 Cognito 켤 때만. |
| `CertificateArn` | yes | — | HTTPS(443) 리스너용 ACM 인증서 ARN. |
| `ContainerImageUri` | no | 게시된 ECR Public 이미지 | ECR Public에 게시된 이미지가 기본 — 빌드 불필요. 제한된 네트워크(자체 프라이빗 ECR 사본 / pull-through 캐시)나 커스텀 빌드에만 오버라이드; immutable 태그 또는 digest 권장. |
| `ContainerCpu` | no | `512` | Fargate 태스크 CPU 단위. **Full Load는 CPU-bound**(소스 리더가 행마다 Python 타입 변환 수행)이므로 대규모 마이그레이션에서는 올려야 함 — 동일 데이터의 payments+orders 로드 실측에서 **512(기본)보다 4096(4 vCPU)에서 약 3.8배 빨랐음**. 평가용은 기본 `512`로 충분, 실제 대용량 Full Load에는 **4096 이상** 권장. [매뉴얼 §7.2](../docs/manual/ko/07-performance-and-tuning.md#72-병렬수-튜닝) 참고. |
| `ContainerMemory` | no | `1024` | Fargate 태스크 메모리(MiB) — **hard limit**: 초과하면 커널이 태스크를 OOM-kill(graceful shutdown 없음, CloudWatch 급등 + ELB 타임아웃만 남음). 메모리는 테이블 크기가 아니라 버퍼링된 파이프라인 — 대략 `table_parallelism × (prefetch + batch_parallelism) × 배치당 바이트`를 **워커 프로세스에 걸쳐 합산** — 으로 제한되며, **넓은/대형 LOB 행이 배치당 바이트를 키우므로** 동시 소스 쓰기가 있는 실제 로드에선 `1024` 기본이 빠듯할 수 있음. **사이징:** 평가/소형은 `1024`로 충분, 실제 Full Load엔 **≥ 2048**, 큰 `TEXT`/`BLOB`이나 병렬도 상향 시 **≥ 4096**(이땐 `ContainerCpu` ≥ 2048 필요). CPU에 유효해야 함(아래 사이징 표 참고). 앱이 메모리 high-water와 ~80% 압박 경고를 로그(+활동 로그)에 남겨 OOM 접근을 kill 전에 볼 수 있음. |
| `AppPort` | no | `8080` | 컨테이너 수신 포트. |
| `AssignPublicIp` | no | `DISABLED` | NAT 없이 퍼블릭 서브넷에서 태스크 실행하려면 `ENABLED`(테스트); **권장: 프로덕션은 `DISABLED` 유지**(NAT 게이트웨이 또는 VPC 엔드포인트). |
| `AllowedIngressCidr` | no | `10.0.0.0/8` | ALB 443에 도달 허용 CIDR. **권장: 내 네트워크로 범위 제한**, `0.0.0.0/0` 아님. |
| `DsqlClusterArn` | yes | — | 타깃 DSQL 클러스터 ARN(`dsql:DbConnect` 범위 지정). |
| `SourceSecretArn` | no | `""` | **옵션.** 기존 Secrets Manager 시크릿을 **재사용**할 때만 설정(`GetSecretValue` 범위 지정). 비워두면 UI에서 id/password 입력(일반적인 경우). |
| `SourceDbSecurityGroupId` | no* | `""` | 소스 DB SG; raw CIDR보다 **선호(권장) egress 타깃**. *이것/`SourceDbCidr` 중 하나 필수. |
| `SourceDbCidr` | no* | `""` | 소스 DB CIDR(SG id 없을 때 사용). *이것/`SourceDbSecurityGroupId` 중 하나 필수. |
| `SourceDbPort` | no | `3306` | 소스 DB 포트 — **`3306`은 MySQL, `5432`는 PostgreSQL** (PostgreSQL 소스는 기본값을 재정의). |
| `HttpsEgressCidr` | no | `0.0.0.0/0` | 태스크 아웃바운드 443(AWS API: DSQL 토큰·Secrets Manager·ECR·CloudWatch·Bedrock) + 5432(DSQL)의 **대상** CIDR. **권장: 기본값 `0.0.0.0/0` 그대로** — 태스크가 NAT/IGW로 퍼블릭 AWS 엔드포인트에 도달. 좁히기(예: 내 VPC CIDR)는 위 서비스들을 *전부* 인터페이스 VPC 엔드포인트(PrivateLink)로 둘 때만; 엔드포인트 없이 좁히면 이미지 pull/DSQL이 막혀 태스크가 기동 실패합니다. |
| `EnableCognitoAuth` | no | `false` | ALB가 Cognito(OIDC)로 인증. 기본 `false`: internal ALB(또는 내 CIDR로 범위 제한한 ALB)가 접근 게이트이고 운영자가 이미 IAM/DB 권한을 보유하므로 로그인 불필요. **`AllowedIngressCidr=0.0.0.0/0`일 때만 필수(강제됨).** `true`면 `CognitoDomainPrefix`와 `CognitoAdminEmail` **둘 다** 필요. |
| `AppDomainName` | no | `""` | ALB 앞단 DNS 이름(인증서와 일치해야 함). **비워 두면** ALB 자체 DNS 이름을 Cognito 콜백 호스트로 사용 — 커스텀 도메인이나 Route 53 레코드가 필요 없습니다. |
| `CognitoDomainPrefix` | Cognito 시 | `""` | 전역 유니크 Cognito hosted-UI prefix (`https://<prefix>.auth.<region>.amazoncognito.com`). |
| `CognitoAdminEmail` | Cognito 시 | `""` | 스택이 생성하는 **첫 로그인 사용자**의 이메일. Cognito가 임시 비밀번호를 이 주소로 발송하고, hosted UI가 첫 로그인 시 새 비밀번호를 요구합니다. **Cognito를 켜면 필수** — 유저풀이 self sign-up을 막아 두므로, 이 값이 없으면 배포는 성공하지만 아무도 로그인할 수 없는 앱이 됩니다(템플릿이 그 조합을 거부). 사용자 추가는 [§운영자 사용자 만들기](#운영자-사용자-만들기-cognito--cognito를-켰을-때만) 참고. |
| `EnableAiAssist` | no | `false` | opt-in; 범위 제한된 `bedrock:InvokeModel` 부여. |
| `BedrockModelArns` | no | `""` | **선택적 override**; 비우면 `BedrockModelId`에서 자동 도출. |
| `BedrockRegion` | no | `""` | 앱의 `BEDROCK_REGION`. |
| `BedrockModelId` | no | `global.anthropic.claude-sonnet-5` | Anthropic 모델(드롭다운); IAM 스코프 자동 도출. |

### 태스크 사이징 — `ContainerCpu` / `ContainerMemory`

Fargate는 CPU와 메모리를 **독립적으로 못 고릅니다**: CPU 값마다 허용 메모리 범위가 고정이고,
메모리는 **hard limit**(초과 시 OOM kill, 앱 종료 로그 없음). 유효한 조합에서 고르세요:

| `ContainerCpu` (vCPU) | 허용 `ContainerMemory` | 증분 |
| --- | --- | --- |
| `256` (0.25) | 512, 1024, 2048 MiB | 고정 |
| `512` (0.5) | 1–4 GB | 1 GB |
| `1024` (1) | 2–8 GB | 1 GB |
| `2048` (2) | 4–16 GB | 1 GB |
| `4096` (4) | 8–30 GB | 1 GB |
| `8192` (8) | 16–60 GB | 4 GB |
| `16384` (16) | 32–120 GB | 8 GB |

**워크로드별 권장:**

- **평가 / 소형 테이블:** `512` / `1024` MiB 기본으로 충분.
- **실제 Full Load:** **`1024` CPU / `2048` MiB** 이상. Full Load는 CPU-bound(행별 타입 변환)이고
  메모리는 워커 프로세스에 걸쳐 `table_parallelism × batch_parallelism`로 증가 — 동시 소스 쓰기가
  있는 로드에서 `512`/`1024` 기본이 OOM-kill된 사례가 있음.
- **큰 `TEXT`/`BLOB` 테이블이나 병렬도 상향 시:** **`4096` CPU / `8192`+ MiB** — 넓은 행이 배치를
  키움. (메모리 4 GB 초과는 CPU도 올려야: 4 GB는 CPU ≥ `1024`, 8 GB는 CPU ≥ `2048`.)

> [!TIP]
> 메모리 상향은 **재배포**(스택이 태스크를 in-place 업데이트) — Fargate는 태스크 메모리를 자동
> 스케일하지 않고, 단일 태스크 컨트롤 플레인이라 수평 스케일도 안 됨. 확신이 없으면 넉넉히: 과다
> 프로비저닝은 비용이 조금 더 들 뿐이지만, 부족하면 마이그레이션 도중 OOM-kill됩니다. 앱이 메모리
> high-water와 ~80% 압박 경고를 로그(+활동 로그)에 남기므로 근거를 보고 적정 크기를 잡을 수 있음.

</details>

<details>
<summary><b>커스텀 도메인과 Cognito 로그인</b> — 선택 사항; 기본 internal ALB면 건너뛰기</summary>

### DNS를 ALB로 지정 — Optional (커스텀 도메인만)

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

### 운영자 사용자 만들기 (Cognito) — Cognito를 켰을 때만

Cognito를 켰을 때만(`EnableCognitoAuth=true`). 기본 `internal` ALB + Cognito 미사용이면 건너뜁니다.

**첫 사용자는 이미 존재합니다** — 스택이 `CognitoAdminEmail`로 생성했고, Cognito가 그 주소로 임시
비밀번호를 발송했습니다. 어느 주소로 갔는지는 스택 출력 **`CognitoFirstUser`** 로 확인하세요.
로그인은 `AppUrl`을 열고 그 이메일 + 임시 비밀번호로 로그인한 뒤, 안내에 따라 새 비밀번호를 설정합니다.

**추가** 사용자를 만들려면 스택 출력 `CognitoUserPoolId`를 사용합니다:

```bash
POOL_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator \
  --query "Stacks[0].Outputs[?OutputKey=='CognitoUserPoolId'].OutputValue | [0]" \
  --output text)

aws cognito-idp admin-create-user \
  --user-pool-id "$POOL_ID" \
  --username operator@example.com \
  --user-attributes Name=email,Value=operator@example.com Name=email_verified,Value=true \
  --desired-delivery-mediums EMAIL
```

각 사용자는 임시 비밀번호를 받고, ALB가 트리거하는 Cognito hosted UI를 통해 첫 로그인 시 새 비밀번호를
설정하라는 안내를 받습니다. 유저풀은 **self sign-up이 비활성**이므로 모든 사용자를 이 방식으로 만들어야
합니다.

</details>

<details>
<summary><b>검증, 업데이트, AI 보조</b> — 배포 후 점검, 새 이미지 롤아웃, Bedrock 활성화</summary>

### 검증

```bash
# ECS 서비스가 runningCount = desiredCount (1) 에 도달하고 ACTIVE 여야 함.
aws ecs describe-services --cluster "$(... ClusterName ...)" \
  --services "$(... ServiceName ...)" \
  --query 'services[0].[status,desiredCount,runningCount]' --output text

# 애플리케이션 로그 tail.
aws logs tail /ecs/mysql-dsql-migrator-mysql-dsql-migrator --follow --region "$AWS_REGION"
```

그다음 허용 네트워크(`AllowedIngressCidr`) 안의 호스트에서 `https://AppDomainName/`을 엽니다.
(켜져 있으면) Cognito 로그인으로 리디렉션된 뒤 마이그레이션 워크플로(Connect →
Evaluation → Schema Conversion → Data Migration → Validation → Cut over)로 이동합니다.

#### 관측성 & 런타임 진단

배포는 의도적으로 파라미터를 최소화합니다: **로그 레벨과 활동 로그의 CloudWatch 미러링은
CloudFormation 파라미터가 아닙니다** — 앱의 **Settings → Diagnostics**(사이드바 푸터의 톱니)에서 런타임에
조정하세요, 재배포 불필요:

- **로그 레벨** — 문제 해결 중 `INFO`/`DEBUG` 전환(DEBUG는 실패 이벤트에 Python 스택트레이스 추가;
  행 값이나 자격증명은 절대 없음).
- **Send to CloudWatch (stdout)** — 켜면 활동 로그를 stdout으로 스트리밍하고, 컨테이너의 `awslogs`
  드라이버가 이 스택의 CloudWatch 로그 그룹으로 전달(태스크 교체에도 살아남는 내구성 감사 사본).
- **Download activity log** — 같은 다이얼로그의 **Activity log** 탭에서 전체 UTC, 이벤트당 한 줄
  타임라인(연결 / 평가 / 스키마 적용 / Full Load / CDC)을 받기. 파일은 `/tmp`에서 크기 제한·회전됨.

변경은 앱 전체(단일 태스크)에 적용되고 재시작 시 시작 기본값으로 리셋됩니다. 고급 운영자는
`DSQL_MIGRATOR_LOG_LEVEL` / `DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT` 환경 변수로 시작 기본값을 설정할 수
있지만, Settings 다이얼로그가 의도된 경로입니다.

### 새 이미지 버전으로 업데이트

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
  --s3-bucket "$TEMPLATE_BUCKET" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides ContainerImageUri=$IMAGE_URI
  # (나머지 파라미터를 다시 제공하거나 이전 값에 의존)
  # $TEMPLATE_BUCKET: 위 AWS CLI 배포 섹션에서 만든 스테이징 버킷
  # (이 템플릿은 CloudFormation 인라인 업로드 한도 51,200바이트를 초과합니다)
```

> [!WARNING]
> 컨트롤 플레인은 **단일 태스크**로 실행되므로 교체 중 짧은 중단이 있습니다. 마이그레이션된 데이터,
> DSQL 클러스터, 배포된 cdc-stack은 영향받지 않고 재연결 시 자동 복구됩니다. 진행 중 세션 상태
> (워크플로 진행, 진행 중인 Full Load)는 태스크 임시 디스크에 있어 **살아남지 않습니다** —
> **업데이트 전에 진행 중 작업을 끝내거나 정지**한 뒤, 재연결해 읽기 전용 Evaluation을 다시
> 실행(몇 분)하세요.

### AI 보조 변환 활성화 (Optional)

AI 보조는 opt-in이며 **범위 제한된** `bedrock:InvokeModel`을 부여합니다:

```bash
aws cloudformation deploy ... \
  --parameter-overrides \
    EnableAiAssist=true \
    BedrockModelId=global.anthropic.claude-sonnet-5 \
    BedrockRegion=$AWS_REGION
```

**AI 보조는 Amazon Bedrock에서만 동작합니다.** Bedrock이 유일한 AI 백엔드이며 —
이 도구에는 Anthropic/OpenAI(또는 그 외) API 키를 직접 입력하는 칸이 없습니다. 따라서
선택할 수 있는 모델은 AWS 자격증명으로 호출하는 Bedrock 파운데이션 모델뿐입니다.
모델은 `BedrockModelId`로 지정합니다(기본값 `global.anthropic.claude-sonnet-5`).

**권장 모델 — 최신 Anthropic Claude Opus 또는 Sonnet:**

| 모델 | Bedrock 모델 id (`BedrockModelId`) | 사용 시점 |
|---|---|---|
| Claude Sonnet 5 (기본값) | `global.anthropic.claude-sonnet-5` | 대부분의 스키마에서 품질·속도·비용의 최적 균형. |
| Claude Opus 5 | `global.anthropic.claude-opus-5` | 가장 어려운 `MANUAL` / `UNSUPPORTED` 변환; 최고 품질. |
| Claude Opus 4.8 | `global.anthropic.claude-opus-4-8` | 고품질; Opus 5보다 한 단계 아래. |
| Claude Sonnet 4.6 | `global.anthropic.claude-sonnet-4-6` | 이전 세대 Sonnet. |

`BedrockModelId`는 위 `global.` cross-region inference profile들의 **드롭다운**이고(모든
상용 리전에서 해석되므로 하나의 목록으로 어떤 배포든 커버), 태스크 역할의 `bedrock:InvokeModel`
스코프는 여기서 **자동 도출**됩니다 — 따라서 `BedrockModelArns`는
**설정할 필요가 없습니다**(다른 모델/ARN으로 바꿀 때만 override로 사용). 단, 선택한 모델의
**모델 액세스는 `BedrockRegion`의 Bedrock 콘솔에서 직접 활성화**해야 합니다.

태스크 egress가 Bedrock runtime 엔드포인트에 도달할 수 있는지 확인(NAT 또는 Bedrock VPC 엔드포인트).
UI에서 AI를 켜고, **Verify AI access** 사전 점검으로 도달성을 확인하세요.

</details>

<details>
<summary><b>Teardown, 문제 해결, 보안</b> — 전부 제거, 흔한 문제, 보안 노트</summary>

### Teardown

> [!WARNING]
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

### 문제 해결

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
| 실패 진단에 더 상세히 필요 | **Settings → Diagnostics**(사이드바 푸터의 톱니)에서 로그 레벨을 `DEBUG`로 설정해 활동 로그 실패 이벤트에 Python 스택트레이스 추가; "Send to CloudWatch (stdout)"를 토글해 내구성 사본. 재배포 불필요. |

### 보안 노트

- **최소 권한**: task role은 `dsql:DbConnect` + `dsql:DbConnectAdmin`(클러스터 범위; 앱은 기본적으로
  DSQL `admin` 역할로 연결), 읽기 전용 `dsql:GetCluster` + `dsql:ListTagsForResource`(클러스터
  범위; 개요 다이어그램에 클러스터의 `Name` 태그를 표시하는 데만 사용), `secretsmanager:GetSecretValue`
  (소스 시크릿 범위)만 부여; `bedrock:InvokeModel`은 AI 보조를 켤 때만 추가되며 허용 모델 ARN(들)로
  범위 제한됨. 별도의 execution role이 ECR pull + 로그를 처리하고 컨테이너 시작 시 주입할 자동 생성
  세션 쿠키 시크릿만 읽음(범위 제한된 `secretsmanager:GetSecretValue`).
- **자동 생성 세션 쿠키 시크릿**: 스택이 브라우저 세션 쿠키(`DSQL_MIGRATOR_STORAGE_SECRET`)를 서명하는
  `AWS::SecretsManager::Secret`(운영자 입력 없음)을 생성해, 재시작 간에도 브라우저 세션 id가 안정적으로
  유지됨 — 아래의 durable 스냅샷을 찾는 키. 쿠키만 서명 — DB/사용자 자격증명 아님 — 하고 템플릿에 평문으로 절대 없음.
- **Durable 세션 재개**: 각 세션의 비밀 아닌 워크벤치 스냅샷(워크플로 진행·평가 결과·스키마 선택·CDC 시작점)을
  툴의 관리형 플러그인 버킷(`mysql-dsql-migrator-plugins-<account>-<region>`, 자동 프로비저닝 — 운영자 입력 없음)의
  `sessions/` 프리픽스에 기록해, 인프로세스 재시작뿐 아니라 Fargate **태스크 교체**(재배포)까지 견딤. 위의 안정적
  쿠키 시크릿과 함께, 재연결한 브라우저가 Step 1(Evaluation) 재실행 없이 워크벤치를 재개. 비밀 아님만(Property 7) —
  소스 DB 비밀번호는 Connect 화면에서 다시 입력.
- **감사 추적**: 구조화된 활동 로그(성공 + 실패 타임라인, UI에서 다운로드)는 비밀이 아닌 필드만 기록 —
  행 값, 비밀번호, IAM 토큰은 절대 없음. 태스크 임시 디스크에서 크기 제한·회전됨; 내구성 사본은
  CloudWatch 미러(**검증** 섹션)를 켜기.
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

</details>

---

<br>

## 단일 EC2 호스트에서 실행 (소스에서, Lambda-free)

**컨테이너/ECR나 AWS Lambda를 쓸 수 없는 계정**을 위한 방식입니다. 같은 컨트롤 플레인 앱이 VPC
안의 **EC2 호스트 하나에서 소스 그대로**, **systemd 서비스**로 실행됩니다 — 이미지 빌드도 ECR도
ALB도 없습니다. UI에는 **SSM 포트포워드**로 접속합니다(호스트에 공인 IP도 인바운드 규칙도 없음).
상태(Full Load 작업 / 세션)는 **보존형 EBS 볼륨**에 있어 재부팅 후에도 유지되며 **S3 버킷이 필요
없습니다.** CDC는 Kafka를 **인프로세스로** 시드하므로, Fargate와 달리 **오프셋 시더 Lambda를
만들지 않습니다**(`SeedMode=External`).

템플릿: **`deploy/cloudformation-ec2.yaml`**.

<details>
<summary><b>아키텍처 다이어그램</b> — 단일 EC2 호스트, 인프로세스 CDC 시드(SeedMode=External)</summary>

<div align="center">
  <a href="../docs/images/architecture-aws-ec2.png"><img src="../docs/images/architecture-aws-ec2.png" alt="단일 EC2 호스트 아키텍처 — 마이그레이션 도구가 VPC 내 EC2 호스트 1대에서 소스 그대로 실행되고 SSM 포트포워드로 접속(ALB 없음), Aurora DSQL로 Full Load를 수행하고 MSK로 CDC를 인프로세스 시드하며, Debezium 소스 + 커스텀 DSQL 싱크 커넥터가 MSK Connect에서 S3의 플러그인을 로드" width="820"></a>
</div>

</details>

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 언제 쓰나

- ✅ 계정/정책이 **컨테이너 실행이나 ECR pull을 금지**하거나 **AWS Lambda를 금지**할 때.
- ✅ 그래도 Fargate처럼 **VPC 안의 프라이빗 데이터 경로**(소스 → 호스트 → DSQL)를 원하고, 데이터를
  노트북으로 우회시키고 싶지 않을 때.
- ❌ 그 외에는 **[ECS Fargate](#ecs-fargate에-배포)**가 낫습니다 — 관리형·로드밸런싱 경로이며
  패치할 호스트가 없습니다.

> [!WARNING]
> **단일 호스트 = 단일 장애점(SPOF).** ALB도, Auto Scaling도, 두 번째 태스크도 없습니다. 상태는
> 인스턴스 재부팅/교체에도 보존형 EBS 볼륨에서 살아남지만, 컨트롤 플레인 자체는 한 대입니다 —
> 직접 진행하는 마이그레이션에는 적합하지만, 장기 상시 HA 서비스용은 아닙니다.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 1. 사전 요구사항

- **인터넷 egress가 되는 프라이빗 서브넷(NAT 게이트웨이).** 호스트는 **공인 IP가 없고**,
  **첫 부팅 때 `uv`·CPython·Python 휠을 받고 저장소를 클론하는 것을 모두 공개 인터넷**
  (astral.sh · PyPI · GitHub)에서 하며 — 이후 소스 DB·DSQL·AWS API 접근도 같은 egress로
  처리합니다. VPC 엔드포인트만으로는 안 됩니다(이 공개 소스들은 PrivateLink로 받을 수 없음).
  **소스 DB(MySQL 또는 PostgreSQL)와 같은 VPC**에 두세요(CDC를 쓸 거면 MSK와도 같은 VPC).
- **AWS CLI + Session Manager 플러그인**(내 컴퓨터에) — UI를 여는 방법입니다(ALB나 공용
  엔드포인트가 없어 SSM으로 포트포워드).
  [설치 안내](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
- **CDC를 쓸 때만 — 호스트 서브넷의 CIDR.** 이 값을 cdc-stack에 넘기면 MSK가 인프로세스
  시드를 위해 호스트를 9098 포트로 허용합니다(아래
  [호스트를 MSK 9098에 허용](#5-호스트를-msk-9098에-허용-cdc만) 참고).
- **호스트가 앱 소스를 가져오는 방법.** 기본값(`SourceMode=git`)은 공개 저장소를 HTTPS로
  클론합니다 — 자격증명 불필요. 호스트 네트워크가 저장소에 닿지 못하면, 체크아웃 tarball을
  S3에 올려 `SourceMode=s3`(`SourceS3Uri`)를 사용하세요.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 2. 필수 / 핵심 파라미터

| 파라미터 | 필수 | 기본값 | 무엇인가 |
| --- | --- | --- | --- |
| `VpcId` | 예 | — | 소스 DB / MSK의 VPC(DSQL과 동일 리전). |
| `HostSubnetId` | 예 | — | `VpcId`의 **NAT-egress 프라이빗 서브넷**, MSK와 같은 위치. |
| `DsqlClusterArn` | 예 | — | 타깃 DSQL 클러스터(`dsql:DbConnect` 범위). |
| `SourceDbSecurityGroupId` / `SourceDbCidr` | 하나 필수 | `""` | 소스 DB로의 호스트 egress를 개방(원시 CIDR보다 SG 선호). |
| `SourceMode` | 아니오 | `git` | `git`(공개 HTTPS로 `SourceRepoUrl@SourceRepoRef` 클론) 또는 `s3`(`SourceS3Uri` tarball). |
| `SourceS3Uri` | `s3`면 필수 | `""` | 저장소 루트의 `s3://…/source.tar.gz` — 임시 "내 로컬 복사본 실행" 경로. |
| `MskEgressCidr` | 아니오 | `0.0.0.0/0` | 인프로세스 시드를 위해 호스트가 MSK 9098에 도달할 CIDR; 최소 권한을 위해 커넥터 서브넷 CIDR로 좁히기. |
| `InstanceType` | 아니오 | `t3.large` | 컨트롤 플레인 호스트 크기. |
| `StateVolumeSizeGiB` | 아니오 | `20` | 보존형 EBS 상태 볼륨; 대형 테이블 Full Load의 로컬 CSV 스필오버는 크게. |
| `SourceSecretArn` | 아니오 | `""` | 기존 소스 자격증명 시크릿 재사용 시에만(그 외에는 UI에서 사용자/비밀번호 입력). |
| `EnableAiAssist` / `BedrockModelId` / `BedrockRegion` | 아니오 | off / `global.anthropic.claude-sonnet-5` | Fargate와 동일한 opt-in Bedrock AI 보조(IAM 범위는 모델에서 자동 도출). |
| `KeyName` | 아니오 | `""` | 선택적 SSH 키; SSM이 기본 접속 경로라 보통 비워둠(호스트에 인바운드 규칙 자체가 없음). |

> [!WARNING]
> 스택 이름은 **`mysql-dsql-cdc-`로 시작하면 안 됩니다**(그 접두사는 CDC 배포 역할 범위에 들어감).
> `mysql-dsql-migrator-ec2`가 적절합니다.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 3. 배포

이 스택(`deploy/cloudformation-ec2.yaml`)을 배포하는 세 가지 방법 — **하나를 선택**(셋 다 같은
호스트를 만들며, 파라미터는 위 표 참조):

| | 옵션 | 적합 |
| --- | --- | --- |
| **A** | **AI 코딩 에이전트** | 가장 쉬움·오류 적음 — Claude Code / Kiro / Cursor 사용 시. |
| **B** | **AWS Console** | 네이티브 피커가 있는 안내형 폼 (권장). |
| **C** | **AWS CLI** | 스크립트/반복 배포. |

#### 옵션 A — 가장 쉬움: AI 코딩 에이전트로 배포

셸 접근이 가능한 에이전트(**Claude Code, Kiro, Cursor, 또는 AWS CLI를 실행할 수 있는 모든
에이전트**)가 내 계정에서 파라미터를 찾아내고 이 배포를 대신 실행해줄 수 있습니다 —
[Fargate의 옵션 A](#2-app-stack-배포)와 같은 개념이되, EC2-소스 스택을 대상으로 합니다(UI는 SSM
포트포워드로 접속; ALB/Cognito 없음). 클론한 저장소와 **에이전트 셸에서 사용 가능한 AWS 자격증명**
(`aws sts get-caller-identity`가 성공해야 함)이 필요합니다. 이 프롬프트를 붙여넣으세요(빈칸 두 개를
채우세요):

```text
deploy/DEPLOYMENT.md("Run on a single EC2 host")를 따라 이 저장소의 단일 EC2 호스트 app-stack
(deploy/cloudformation-ec2.yaml)을 내 AWS 계정에 배포해줘.

타깃 Aurora DSQL 클러스터: <DSQL cluster ARN or endpoint>
소스 데이터베이스: <RDS/Aurora identifier — or "I'll enter it in the UI later">

단계:
1. deploy/DEPLOYMENT.md("Run on a single EC2 host")와 deploy/cloudformation-ec2.yaml의
   파라미터를 읽어.
2. 가능한 곳은 묻지 말고 DISCOVER(읽기 전용)해: DSQL ARN/엔드포인트에서 리전을 도출하고;
   소스 DB의 VpcId를 찾고; HostSubnetId = 그 VPC의 NAT-egress PRIVATE 서브넷(라우트 테이블에
   0.0.0.0/0 -> NAT 게이트웨이가 있음)을 고르되, CDC를 쓸 거면 MSK와 같은 위치로; 그 서브넷의
   CIDR을 MskEgressCidr로 읽고; 소스 DB의 SourceDbSecurityGroupId와 포트(SourceDbPort: MySQL은
   3306, PostgreSQL은 5432)를 찾고; DsqlClusterArn을 확인해.
3. 해석된 전체 파라미터 세트와 정확한 `aws cloudformation deploy` 명령을 나에게 보여주고,
   무엇이든 만들기 전에 내 승인을 WAIT(대기)해. 정말 추론할 수 없는 것만 물어봐 — 주로: CDC를
   쓸지 여부(서브넷 선택과 MskEgressCidr에 영향), SourceMode git(기본; 공개 저장소를 클론) vs
   s3, 그리고 AI 보조(Amazon Bedrock) 활성화 여부.
4. 내가 OK하면 배포해 — --s3-bucket으로 템플릿을 스테이징하고(CloudFormation의 51,200바이트
   인라인 한도를 초과함); --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM를 사용해; 스택
   이름은 mysql-dsql-migrator-ec2로(mysql-dsql-cdc-로 시작하면 안 됨). CREATE_COMPLETE를 기다린
   뒤, UI에 SSM 포트포워드로 접속하는 방법을 알려줘.

가드레일: 오직 단일 리전(DSQL 클러스터의 리전); 만들기 전에 read/describe; 기존 리소스를 절대
수정하거나 삭제하지 마; 모든 것을 프로덕션으로 취급; 모호하거나 단계가 실패하면 멈추고 물어봐.
```

**승인하기 전에 해석된 파라미터를 검토하세요** — 에이전트가 내 자격증명으로 실행하는 AWS 작업의
책임은 나에게 있습니다. 나중에 정리하려면 에이전트에게 *"`mysql-dsql-migrator-ec2` 스택을
삭제해줘"* 라고 말하면 [Teardown](#6-teardown) 절차를 따릅니다.

#### 옵션 B — 권장: AWS Console (안내형 폼)

템플릿을 업로드하고 안내형 폼을 채웁니다(`VpcId` / `HostSubnetId`는 네이티브 선택기; Console이
템플릿을 대신 스테이징하므로 S3 버킷 불필요). 절차는 [옵션 B — AWS Console](#2-app-stack-배포)와
동일합니다 — 이 템플릿을 고르고, 위의 EC2 파라미터를 입력하고, 스택 이름을
`mysql-dsql-migrator-ec2`로 지정하면 됩니다.

#### 옵션 C — AWS CLI

`aws cloudformation deploy` 한 번:

```bash
# --- 내 환경 (여기를 수정) ---------------------------------------------------
export AWS_REGION=us-east-1
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=vpc-0a1b2c3d4e5f6a7b8
# HostSubnetId: VPC_ID 안의 NAT-egress 프라이빗 서브넷, MSK와 같은 위치
export HOST_SUBNET_ID=subnet-0123456789abcdef0
export DSQL_CLUSTER_ARN=arn:aws:dsql:us-east-1:123456789012:cluster/f0a1b2c3d4e5f6a7b8c9d0e1f2
export SOURCE_DB_SG=sg-0a1b2c3d4e5f6a7b8
# -----------------------------------------------------------------------------

# 호스트 서브넷 CIDR — 호스트의 MSK egress 범위 지정(아래 MskEgressCidr)과,
# CDC의 경우 호스트를 9098로 허용하는 데 사용("호스트를 MSK 9098에 허용" 참고):
export HOST_SUBNET_CIDR=$(aws ec2 describe-subnets --subnet-ids "$HOST_SUBNET_ID" \
  --region "$AWS_REGION" --query 'Subnets[0].CidrBlock' --output text)

# 이 템플릿은 CloudFormation 인라인 업로드 한도(51,200바이트)를 초과해서, CLI가
# 스테이징할 S3 버킷이 필요합니다. 한 번만 만들거나, 이미 있는 버킷을 재사용하세요:
export TEMPLATE_BUCKET=mysql-dsql-migrator-templates-$ACCOUNT-$AWS_REGION
aws s3 mb "s3://$TEMPLATE_BUCKET" --region "$AWS_REGION" 2>/dev/null || true

aws cloudformation deploy \
  --template-file deploy/cloudformation-ec2.yaml \
  --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" \
  --s3-bucket "$TEMPLATE_BUCKET" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    HostSubnetId="$HOST_SUBNET_ID" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    MskEgressCidr="$HOST_SUBNET_CIDR"
    # 기본 SourceMode=git은 공개 저장소를 클론합니다(자격증명 불필요). 호스트가 저장소에
    # 닿지 못하면 추가:  SourceMode=s3 SourceS3Uri=s3://$TEMPLATE_BUCKET/dsql-src.tar.gz
```

첫 부팅은 약 3~4분: 호스트가 443으로 Python 3.12 + wheel을 설치하고,
`uv sync --extra cdc-external`(인프로세스 시드에 필요한 `kafka-python` + MSK IAM 서명기 포함)을
실행한 뒤 서비스를 시작합니다. 진행 상황은 호스트의 `/var/log/dsql-migrator-userdata.log`에 있습니다.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 4. UI 접속 (SSM 포트포워드)

스택은 `HostInstanceId`와 바로 실행 가능한 `SsmPortForwardCommand`를 출력합니다:

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region "$AWS_REGION"
```

`http://localhost:8080`을 열면 도구 UI가 뜹니다(같은 가이드 워크플로우). SSM Run Command로
`systemctl is-active dsql-migrator.service`와 `journalctl -u dsql-migrator`로 서비스 상태를
확인하세요.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 5. 호스트를 MSK 9098에 허용 (CDC만)

CDC에서는 호스트가 Kafka를 인프로세스로 시드하므로 MSK Serverless 9098에 도달해야 합니다 —
cdc-stack의 `HostSubnetCidr` 파라미터가 호스트를 허용합니다(커넥터 SG 인그레스 추가). **이 EC2
호스트에선 자동입니다:** 호스트가 부팅 때 자기 서브넷 CIDR을 도출하고, UI에서 **CDC 인프라 배포**를
누르면 도구가 그 값을 넣어줍니다 — 따로 설정할 게 없습니다. (cdc-stack을 도구 밖에서 직접 배포할
때만 `HostSubnetCidr`을 수동으로 넘깁니다.) 어느 경우든 호스트가 MSK에 도달할 수 없으면 **Start
CDC가 커넥터를 만들기 전에 명확히 실패**(`CdcDeployError`)합니다 — 조용한 갭은 없습니다.

<hr style="border: none; height: 1px; background-color: #d0d7de; margin: 1.5em 0;">

### 6. Teardown

CDC를 배포했다면 **호스트가 살아 있는 동안 먼저** 제거하세요 — **Data Migration** 단계에서
**Delete all CDC infrastructure**를 사용합니다. 이 삭제는 호스트의 앱에서 실행되므로, 호스트를 먼저
지우면 cdc-stack을 손으로(`aws cloudformation delete-stack`) 지워야 합니다. 그다음 EC2 호스트를
정리하세요:

```bash
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
```

> [!WARNING]
> 상태 EBS 볼륨은 설계상 **`DeletionPolicy: Retain`**이라 **스택 삭제 후에도 남습니다** — 보관을
> 원치 않으면 수동으로 삭제하세요(`aws:cloudformation:stack-name` 태그로 찾음).

---

<br>

## 부록 — 자체 이미지 빌드 (ECS Fargate 전용; 제한된 네트워크만)

> [!NOTE]
> **이 절은 ECS Fargate 배포에만 해당합니다** — 단일 EC2 호스트 모드는 소스에서 실행되어 컨테이너
> 이미지를 쓰지 않으므로 이 절이 전혀 필요 없습니다.
>
> **그리고 Fargate 배포도 대부분 이 절을 건너뜁니다.** 이미지는 ECR Public에 게시돼 있고
> CloudFormation이 기본으로 가져오므로 빌드할 게 없습니다. 네트워크가 ECR Public에 도달할 수 없을
> 때만 자체 이미지를 빌드해(`ContainerImageUri`로 전달) 사용하세요. 아래 Option A/B 중 하나 — 둘 다 ECR 저장소를 만들고
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

> [!TIP]
> 배포 재현성을 위해 릴리스마다 immutable 태그(또는 이미지 digest)를 사용하세요.
