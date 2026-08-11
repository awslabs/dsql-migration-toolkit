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

aws cloudformation create-change-set \
  --stack-name "$CDC_STACK" \
  --change-set-name seedmode-nochange-$(date +%s) \
  --template-url "https://$BUCKET.s3.$AWS_REGION.amazonaws.com/cdc-plugins/cdc-stack-test.yaml" \
  --use-previous-template false \
  --parameters ParameterKey=SeedMode,UsePreviousValue=true \
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

## 2. `cdc-external` extra가 포함된 이미지 퍼블리시

`SeedMode=External`은 `kafka-python` + MSK IAM SASL 서명자가 이미지에 baked 되어 있어야
합니다. 기본 퍼블리시 이미지(`:0.1.303`)는 `--extra cdc-external` Dockerfile 변경 이전이라
**새 이미지를 빌드·푸시**해야 합니다(로컬 Docker가 없으면 CodeBuild 사용):

```bash
# 로컬 Docker → ECR Public (전체 스크립트는 deploy/build_and_push.sh 참조):
deploy/build_and_push.sh                 # deploy/Dockerfile로 빌드 후 태그 푸시
# 푸시된 이미지 URI 기록, 예: public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.308
export IMAGE_URI=public.ecr.aws/z0q0i9j0/mysql-dsql-migrator:0.1.308
```
extra가 이미지에 들어갔는지 확인:
```bash
docker run --rm --entrypoint python "$IMAGE_URI" -c "import kafka, aws_msk_iam_sasl_signer; print('cdc-external OK')"
```
✅ 통과 = `cdc-external OK` 출력. ❌ `ModuleNotFoundError` = extra 누락;
`deploy/Dockerfile`에 `uv sync ... --extra cdc-external`가 있는지 확인 후 재빌드.

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
    ContainerImageUri="$IMAGE_URI" \
    MskEgressCidr="$HOST_SUBNET_CIDR"      # 또는 커넥터 서브넷 CIDR; 9098 egress를 좁힘
```
> 스택 이름은 `mysql-dsql-cdc-`로 **시작하면 안 됩니다**(그 접두사는 CdcDeployRole 스코프에
> 포함됨). `mysql-dsql-migrator-ec2`면 됩니다.

출력 읽기:
```bash
aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query 'Stacks[0].Outputs' --output table
```
`HostInstanceId`와 `SsmPortForwardCommand`를 기록해 두세요.

### 3a. 호스트가 컨테이너를 부팅했는지 확인
```bash
INSTANCE_ID=$(aws cloudformation describe-stacks --stack-name mysql-dsql-migrator-ec2 \
  --region "$AWS_REGION" --query "Stacks[0].Outputs[?OutputKey=='HostInstanceId'].OutputValue" --output text)

# user-data가 돌 시간(~2-3분) 후 SSM Run Command로 확인:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["docker ps --format {{.Names}}\\ {{.Status}}","tail -n 20 /var/log/dsql-migrator-userdata.log","mount | grep dsql-migrator"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# 이어서: aws ssm get-command-invocation --command-id <id> --instance-id $INSTANCE_ID --region $AWS_REGION --query StandardOutputContent --output text
```
✅ 통과 = `dsql-migrator  Up ...`, `/var/lib/dsql-migrator`에 EBS 마운트, user-data 로그에 오류 없음.

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
Data Migration(Full Load) → Start CDC**. 호스트 컨테이너에
`DSQL_MIGRATOR_CDC_SEED_MODE=external`이 설정돼 있어 Start CDC가 seed를 인프로세스로 실행합니다.

모드가 실제로 적용됐는지 확인(호스트 env + 배포 로그):
```bash
# 컨테이너 env가 external을 보여야 함:
aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters 'commands=["docker exec dsql-migrator printenv DSQL_MIGRATOR_CDC_SEED_MODE"]' \
  --region "$AWS_REGION" --query Command.CommandId --output text
# 기대: external
```
CDC 배포 로그(UI 또는 `docker logs dsql-migrator`)에 인프로세스 준비 라인이 보여야 합니다:
`SeedMode=External: preparing CDC topics + offset in-process …` 이어서
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
- **이미지 재퍼블리시 필요** — 퍼블리시 기본값 `:0.1.303`에는 `cdc-external` extra가 없습니다;
  2단계에서 `:0.1.308`을 빌드합니다. `ContainerImageUri`를 그 이미지로 지정하세요.
- **`ContainerImageUri` 기본값 drift** — 템플릿 기본값이 아직 옛 태그를 가리킵니다; 항상
  방금 빌드한 이미지를 넘기세요.
- **대용량 테이블 Full Load 스테이징** — `staging_bucket`은 S3 전용; 아주 큰 테이블은
  `DSQL_MIGRATOR_STAGING_BUCKET`을 설정하거나 EBS 볼륨을 그에 맞게 키우세요.
