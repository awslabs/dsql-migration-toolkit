# 테스트 절차 — Lambda-free "EC2 + MSK only" 모드 (SeedMode=External)

_언어: [English](TEST_EC2_MSK_ONLY.md) | **한국어**_

옵션 ③을 직접 검증하는 절차입니다. 컨트롤 플레인 전체를 **VPC 내부의 단일 EC2 호스트**
(`deploy/cloudformation-ec2.yaml`)에서 실행하고, SSM 포트포워드로 접속한 뒤, CDC 시작 시
Kafka 준비를 **인프로세스로**(오프셋 시더 Lambda 없이) 수행하는지 확인합니다.

이 문서는 **직접 실행**하는 용도입니다. 안전 → 위험 순서로 진행합니다: 먼저 기존 배포가
그대로임을 (읽기 전용으로) 증명하고, 그다음 EC2 호스트를 띄워 실제 External CDC를 돌립니다.

> 사전 준비: AWS CLI v2(**Session Manager 플러그인** 포함), 대상 계정 자격증명, `jq`,
> 그리고 저장소 루트에서 연 셸. 한 번만 설정:
> ```bash
> export AWS_REGION=us-east-1          # 사용 리전
> export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
> ```

---

## 0. 자동 테스트 (AWS 불필요) — 베이스라인

```bash
# 저장소 루트에서 실행 (worktree는 editable venv라 PYTHONPATH 설정 필요):
PYTHONPATH="$PWD/src" .venv/bin/python -m pytest -q
```
기대: 전부 통과(2900개 이상). 순수 로직, 인프로세스 seed, SeedMode 게이팅, EC2 템플릿 구조,
host-is-mode 설정 배선을 모두 seam 주입으로 커버 — 실제 Kafka/AWS는 건드리지 않습니다.

원하면 부분만 확인:
```bash
PYTHONPATH="$PWD/src" .venv/bin/python -m pytest -q \
  tests/test_ec2_appstack.py tests/test_cdc_stack_seedmode.py \
  tests/test_cdc_kafka_seed.py tests/test_config.py
```

---

## 1. 기본(Fargate + Lambda) 경로가 그대로임을 증명 — 읽기 전용

### 1a. 템플릿 린트 통과
```bash
aws cloudformation validate-template \
  --template-body file://deploy/cloudformation-ec2.yaml --region "$AWS_REGION" >/dev/null && echo "EC2 template OK"
aws cloudformation validate-template \
  --template-body file://deploy/cdc-stack/cdc-stack.yaml --region "$AWS_REGION" >/dev/null && echo "cdc-stack OK"
```
> cdc-stack은 인라인 51,200바이트 한도를 넘기 때문에 일부 CLI에서 `validate-template`이
> 크기로 실패할 수 있습니다. 그 경우 change-set(1b)으로 넘어가세요 — S3 업로드 방식이라 괜찮습니다.

### 1b. 실제 Lambda-모드 cdc-stack에 change-set 드라이런 → 변경 0 기대
핵심 증거입니다: 기존 cdc-stack을 새 템플릿으로 **현재 파라미터** 그대로 re-template
(SeedMode 기본 `Lambda`, HostSubnetCidr 기본 `""`)했을 때 **리소스 변경이 없어야** 합니다.

```bash
CDC_STACK=mysql-dsql-cdc-<접미사>     # 기존 cdc-stack 이름

# (초과 크기) 템플릿을 관리형 플러그인 버킷에 올린 뒤 TemplateURL로 change-set 지정.
BUCKET=mysql-dsql-migrator-plugins-$ACCOUNT-$AWS_REGION
aws s3 cp deploy/cdc-stack/cdc-stack.yaml "s3://$BUCKET/cdc-plugins/cdc-stack-test.yaml" --region "$AWS_REGION"

# 주의: SeedMode는 배포된 스택에 없는 NEW 파라미터라 명시 값(Lambda = 기본/불변 모드)을
# 줘야 합니다 — UsePreviousValue 불가. HostSubnetCidr도 신규지만 기본값("")이 있어 생략 가능.
# `--use-previous-template`는 값 없는 플래그라 `false`를 붙이면 에러; --template-url을 줄 때는
# 아예 넣지 마세요.
aws cloudformation create-change-set \
  --stack-name "$CDC_STACK" \
  --change-set-name seedmode-nochange-$(date +%s) \
  --template-url "https://$BUCKET.s3.$AWS_REGION.amazonaws.com/cdc-plugins/cdc-stack-test.yaml" \
  --parameters ParameterKey=SeedMode,ParameterValue=Lambda \
               $(aws cloudformation describe-stacks --stack-name "$CDC_STACK" --region "$AWS_REGION" \
                 --query "Stacks[0].Parameters[?ParameterKey!='SeedMode'].ParameterKey" --output text \
                 | tr '\t' '\n' | sed 's/^/ParameterKey=/;s/$/,UsePreviousValue=true/') \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --region "$AWS_REGION"

# 확인: Changes 배열이 비어 있어야 함(있어도 무해한 Metadata 정도).
aws cloudformation describe-change-set --stack-name "$CDC_STACK" \
  --change-set-name <위에서-쓴-이름> --region "$AWS_REGION" \
  --query 'Changes[].ResourceChange.{Action:Action,Type:ResourceType,Id:LogicalResourceId}' --output table

# 실행하지 않고 change-set 삭제(아무것도 바뀌지 않았음):
aws cloudformation delete-change-set --stack-name "$CDC_STACK" \
  --change-set-name <위에서-쓴-이름> --region "$AWS_REGION"
```
✅ 통과 = Changes 비어 있음. SeedMode/HostSubnetCidr 추가가 기존 Lambda-모드 스택에 무해함을
증명합니다. ❌ 어떤 리소스든 Modify/Remove로 나오면 중단하고 보고.

> Fargate app-stack(`deploy/cloudformation.yaml`)은 옵션 ③에서 **수정하지 않으므로**
> change-set을 만들 대상이 없습니다.

---

## 2. 앱 소스 스테이징 (Docker/ECR 없음)

이 모드는 앱을 **소스에서 직접 실행**합니다(`git clone` + `uv sync` + systemd) — 컨테이너도
레지스트리도 없습니다. 호스트는 `SourceMode`에 따라 소스를 취득합니다:

- **`git`**(기본): `SourceRepoUrl@SourceRepoRef` clone. 공개 HTTPS는 인증 불필요; 임시 AWS
  GitLab SSH 경로는 배포키(`DeployKeySsmParam`)를 씁니다.
- **`s3`**: `SourceS3Uri`에서 소스 tarball을 받아 전개 — repo가 공개 GitHub로 가기 전에
  **지금 로컬 작업본을 실행하는** 가장 간단한 방법.

이 테스트에서는 **`s3` + 로컬 체크아웃**을 씁니다. repo 루트를 tar로 묶어 관리형 플러그인
버킷(이미 CDC 아티팩트용으로 쓰는 버킷이라 신규 버킷 불필요)에 업로드합니다:

```bash
export BUCKET=mysql-dsql-migrator-plugins-$ACCOUNT-$AWS_REGION
# repo 루트(이 워크트리)를 tar. --exclude로 tarball을 작게 유지; 앱엔 src/, pyproject.toml,
# uv.lock, connectors/, deploy/ 만 있으면 됩니다.
tar -czf /tmp/dsql-src.tar.gz \
  --exclude='.git' --exclude='.venv' --exclude='node_modules' --exclude='.claude' \
  -C "$PWD" .
export SOURCE_S3_URI="s3://$BUCKET/source/dsql-src.tar.gz"
aws s3 cp /tmp/dsql-src.tar.gz "$SOURCE_S3_URI" --region "$AWS_REGION"
echo "SOURCE_S3_URI=$SOURCE_S3_URI"
```
> 호스트는 `--strip-components=1`로 전개하므로, tarball은 repo 루트로 풀려야 합니다. 위
> `tar -C "$PWD" .`는 파일을 아카이브 루트에 두므로 호스트가 선행 `./`를 벗겨냅니다.
>
> 호스트는 `uv sync` 시 `--extra cdc-external`을 설치하므로 `kafka-python` + MSK IAM 서명자
> (`SeedMode=External`에 필요)가 `uv.lock`에서 함께 옵니다 — 이미지 빌드도, `docker`도 없음.

---

## 3. EC2 app-stack 배포 (VPC 내부 단일 호스트)

(a) **MSK/cdc-stack과 같은 VPC**이고 (b) **NAT/PrivateLink egress 경로**가 있는 서브넷을
고르세요(호스트에 퍼블릭 IP 없음). 그 서브넷의 CIDR을 확보 — 4단계에서 필요합니다.

```bash
export VPC_ID=vpc-xxxxxxxx
export HOST_SUBNET_ID=subnet-xxxxxxxx     # VPC_ID 내, NAT-egress, MSK와 동일 배치
export DSQL_CLUSTER_ARN=arn:aws:dsql:$AWS_REGION:$ACCOUNT:cluster/xxxx
export SOURCE_DB_SG=sg-source             # 또는 SourceDbCidr

# 호스트 서브넷 CIDR (4단계에서 호스트를 MSK 9098에 허용할 때 사용):
export HOST_SUBNET_CIDR=$(aws ec2 describe-subnets --subnet-ids "$HOST_SUBNET_ID" \
  --region "$AWS_REGION" --query 'Subnets[0].CidrBlock' --output text)
echo "HOST_SUBNET_CIDR=$HOST_SUBNET_CIDR"

aws cloudformation deploy \
  --template-file deploy/cloudformation-ec2.yaml \
  --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    VpcId="$VPC_ID" \
    HostSubnetId="$HOST_SUBNET_ID" \
    DsqlClusterArn="$DSQL_CLUSTER_ARN" \
    SourceDbSecurityGroupId="$SOURCE_DB_SG" \
    SourceMode=s3 \
    SourceS3Uri="$SOURCE_S3_URI" \
    MskEgressCidr="$HOST_SUBNET_CIDR"      # 또는 커넥터 서브넷 CIDR; 9098 egress를 좁힘
```
> `SourceMode=s3` + `SourceS3Uri`는 업로드한 로컬 복사본을 실행합니다. 최종 공개 GitHub
> 상태에서는 둘 다 생략하면 됩니다(기본 `SourceMode=git`으로 공개 repo clone).
> 스택 이름은 `mysql-dsql-cdc-`로 **시작하면 안 됩니다**(그 접두사는 CdcDeployRole 스코프에
> 포함됨). `mysql-dsql-migrator-ec2`면 됩니다.

출력 읽기:
```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```
`HostInstanceId`와 `SsmPortForwardCommand`를 기록해 두세요.

### 3a. 호스트가 서비스를 부팅했는지 확인
```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

# user-data(git/uv 설치 + uv sync)가 돌 시간(~3-4분) 후 SSM Run Command로 확인:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["systemctl is-active dsql-migrator.service","tail -n 30 /var/log/dsql-migrator-userdata.log","mount | grep dsql-migrator"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# 이어서: aws ssm get-command-invocation --command-id <id> --instance-id $INSTANCE_ID --region $AWS_REGION --query StandardOutputContent --output text
```
✅ 통과 = `active`(systemd 서비스 실행 중), `/var/lib/dsql-migrator`에 EBS 마운트, user-data 로그에 오류 없음.

### 3b. SSM 포트포워드로 UI 접속
출력의 `SsmPortForwardCommand`를 실행(또는):
```bash
aws ssm start-session --target "$INSTANCE_ID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}' \
  --region "$AWS_REGION"
```
`http://localhost:8080` 열기 → 마이그레이션 도구 UI 로드. 이 세션은 열어 둡니다.

---

## 4. 호스트를 MSK 9098에 허용 (도달성 부분)

앱은 아직 `HostSubnetCidr`을 cdc-stack에 자동 주입하지 않으므로(연기됨), ingress를 한 번
추가해야 합니다 — cdc-stack을 파라미터와 함께 재배포하거나, (테스트에는 가장 간단하게)
UI의 CDC 인프라 배포에서 파라미터를 넘겨서. 템플릿-파라미터 방식:

```bash
# cdc-stack을 직접 배포/채택한다면 파라미터에 HostSubnetCidr 추가:
#   ... --parameter-overrides ... HostSubnetCidr="$HOST_SUBNET_CIDR"
# 이러면 ConnectorHostDiagnosticsIngress(호스트 서브넷 CIDR에서 9098)가 생성됩니다.
```
커넥터 SG에 규칙이 생겼는지 확인:
```bash
CDC_STACK=mysql-dsql-cdc-<접미사>
SG=$(aws cloudformation describe-stack-resources --stack-name "$CDC_STACK" --region "$AWS_REGION" \
  --query "StackResources[?LogicalResourceId=='ConnectorSecurityGroup'].PhysicalResourceId" --output text)
aws ec2 describe-security-groups --group-ids "$SG" --region "$AWS_REGION" \
  --query "SecurityGroups[0].IpPermissions[?FromPort==\`9098\`].IpRanges[].CidrIp" --output text
```
✅ 통과 = `$HOST_SUBNET_CIDR`(그리고 bastion `172.31.0.0/20`)이 보임.

---

## 5. End-to-end: EC2 호스트에서 External CDC

UI(포트포워드 경유)에서 정상 여정을 진행: **Connect → Evaluation → Schema Conversion →
Data Migration(Full Load) → Start CDC**. 호스트 서비스에
`DSQL_MIGRATOR_CDC_SEED_MODE=external`이 설정돼 있어 Start CDC가 seed를 인프로세스로 실행합니다.

모드가 실제로 적용됐는지 확인(서비스 env + 배포 로그):
```bash
# 서비스 env 파일이 external을 보여야 함:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["grep DSQL_MIGRATOR_CDC_SEED_MODE /etc/dsql-migrator.env"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# 기대: DSQL_MIGRATOR_CDC_SEED_MODE=external
```
CDC 배포 로그(UI 또는 호스트의 `journalctl -u dsql-migrator`)에 인프로세스 준비 라인이 보여야
합니다: `SeedMode=External: preparing CDC topics + offset in-process …` 이어서
`In-process CDC prep complete (offset seed: true|skipped)`, 그리고 커넥터가 RUNNING 도달.

External 모드에서 **오프셋 시더 Lambda가 생성되지 않았는지** 확인:
```bash
aws cloudformation describe-stack-resources --stack-name "$CDC_STACK" --region "$AWS_REGION" \
  --query "StackResources[?ResourceType=='AWS::Lambda::Function']" --output table
# 기대: 비어 있음 (cdc-stack을 SeedMode=External로 배포했을 때 OffsetSeederFunction 없음)
```

데이터 복제(소스 → DSQL) 확인:
```bash
# 저장소의 compare-rows 헬퍼(또는 /compare-rows 스킬) 사용:
.venv/bin/python scripts/compare_rows.py <table> ...
```
✅ 통과 = 행 수가 수렴; CDC 변경이 DSQL에 반영됨.

---

## 6. 네거티브 + 복원력 체크

- **도달 불가 시 loud 실패(조용한 갭 없음):** EC2 호스트는 배포하되 9098 ingress는 추가하지
  않고(4단계 생략) Start CDC. 배포가 `CdcDeployError: In-process CDC seed
  (SeedMode=External) failed before creating the connectors …`로 실패하고 **커넥터가 생성되지
  않아야** 합니다 — 조용한 성공은 절대 없음.
- **EBS resume:** 인스턴스 재부팅
  (`aws ec2 reboot-instances --instance-ids $INSTANCE_ID`) 후 포트포워드 재연결,
  `http://localhost:8080` 재접속 → 진행 중이던 job/session 상태가 보존형 EBS 볼륨에서
  복원됨(SQLite가 재시작을 견딤).
- **Fargate/로컬은 여전히 Lambda:** `DSQL_MIGRATOR_CDC_SEED_MODE`가 설정되지 않은 Fargate
  배포(또는 로컬 `uv run mysql-dsql-migrator ui`)에서 Start CDC는 여전히 VPC 내부
  OffsetSeederFunction(Lambda 모드)을 생성해야 함 — host-is-mode가 새지 않았음을 증명.

---

## 7. 정리

```bash
# CDC 인프라(테스트용 cdc-stack을 배포했다면) — UI의 Delete CDC infra, 또는:
aws cloudformation delete-stack --stack-name "$CDC_STACK" --region "$AWS_REGION"

# EC2 app-stack. 주의: 상태 EBS 볼륨은 DeletionPolicy: Retain이라 설계상 스택 삭제 후에도
# 남습니다 — 보관이 불필요하면 수동 삭제하세요.
aws cloudformation delete-stack --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"
aws cloudformation wait stack-delete-complete --stack-name mysql-dsql-migrator-ec2 --region "$AWS_REGION"

# 남은 상태 볼륨을 찾아 삭제(원할 경우):
aws ec2 describe-volumes --region "$AWS_REGION" \
  --filters "Name=tag:aws:cloudformation:stack-name,Values=mysql-dsql-migrator-ec2" \
  --query 'Volumes[].VolumeId' --output text
# aws ec2 delete-volume --volume-id vol-xxxx --region "$AWS_REGION"
```

---

## 테스트 중 마주칠 알려진 갭 (전부 의도적 연기)
- **`HostSubnetCidr`는 수동 파라미터** — 앱이 아직 cdc-stack에 자동 도출·주입하지 않으므로
  4단계는 수동 1회 추가입니다.
- **소스는 S3 경유 로컬 복사본(임시)** — repo가 공개 GitHub로 가기 전엔 `SourceMode=s3` +
  `SourceS3Uri`(2단계)를 씁니다. 공개되면 기본 `SourceMode=git`(공개 HTTPS clone, S3·인증
  불필요)으로 전환하세요. AWS GitLab SSH 경로(`DeployKeySsmParam`)도 대안이지만 out-of-band
  배포키가 필요합니다(아래 참조); S3 경로는 그걸 완전히 피합니다.
- **`uv sync`가 부팅 시 의존성 다운로드** — 호스트가 user-data에서 Python 3.12 + wheel을
  443으로 설치하므로 첫 부팅이 ~3-4분 걸리고 NAT/egress가 필요합니다; 실패는
  `/var/log/dsql-migrator-userdata.log`에 남습니다.
- **대용량 테이블 Full Load 스테이징** — `staging_bucket`은 S3 전용; 아주 큰 테이블은
  `DSQL_MIGRATOR_STAGING_BUCKET`을 설정하거나 EBS 볼륨을 그에 맞게 키우세요.

### (선택) S3 대신 임시 AWS GitLab SSH clone
S3 tarball 대신 지금 AWS GitLab에서 `git clone` 하려면:
1. `ssh-keygen -t ecdsa -f deploy-key`(패스프레이즈 없이); `deploy-key.pub`을 GitLab
   프로젝트의 **읽기전용 Deploy Key**로 등록.
2. `aws ssm put-parameter --name mysql-dsql-migrator/deploy-key --type SecureString
   --value "$(cat deploy-key)" --region "$AWS_REGION"`(이름에 선행 `/` 없이).
3. `SourceMode=git SourceRepoUrl=git@ssh.gitlab.aws.dev:dalyoung/mysql-dsql-migration-tool-public.git
   DeployKeySsmParam=mysql-dsql-migrator/deploy-key`로 배포 — read-deploy-key IAM 권한과
   22번 포트 egress가 자동으로 켜집니다. repo가 공개 GitHub로 가면 키와 SSM 파라미터를 삭제하세요.
