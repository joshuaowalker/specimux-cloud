"""The specimux-cloud stack (docs/DESIGN.md "System topology"), with a
near-zero idle bill: instances exist only while a job runs.

- A VPC with public subnets only: no NAT gateway. The run API task and
  Batch instances get public IPs and reach AWS services directly.
- One S3 bucket with the layout from the design; archives/ moves to
  Deep Archive after 30 days.
- One DynamoDB table (pay per request) for the control plane.
- One EFS file system shared by the run API and the engine jobs, mounted
  at /mnt/runs in both: the mirrors of runs in progress.
- AWS Batch on EC2 (on demand, C/M families, 0 to 64 vCPUs) with the
  engine job definition, and a GPU environment (g6, g5 or g6e xlarge) with
  the dorado job definition for POD5 runs; the run API admits two runs per
  stage at a time.
- The run API on ECS Fargate (0.5 vCPU, 1 GB), reached by engine jobs
  through a Cloud Map name inside the VPC and from the internet only
  through an application load balancer with an ACM certificate at
  runs.<domain> (-c domain=; without one it is reachable only inside the
  VPC).
- ECR repositories for the three images; the session secret in Secrets
  Manager.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_certificatemanager as acm,
    aws_elasticloadbalancingv2 as elbv2,
    aws_route53 as route53,
    aws_route53_targets as targets,
    aws_batch as batch,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_efs as efs,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secrets,
    aws_servicediscovery as sd,
)
from constructs import Construct

WORK_ROOT = "/mnt/runs"
RUNAPI_PORT = 8090
# The public name, -c domain=example.org (required): a hosted zone for the
# apex must already exist in the account (Route 53 creates it with the
# registration). -c domain= (empty) for none.
RUNAPI_HOST = "runs"


class SpecimuxCloudStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # --- network ---
        vpc = ec2.Vpc(self, "Vpc", max_azs=2, nat_gateways=0,
                      subnet_configuration=[ec2.SubnetConfiguration(
                          name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24)])
        public = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)
        # A third zone for the GPU compute environment only (the ALB and the
        # service stay on the two above). On Sep 22 2026 g5, g6 and g6e were
        # all out of capacity in 2a and 2b while AWS pointed at 2c for each.
        gpu_subnet_c = ec2.PublicSubnet(self, "GpuSubnetC", vpc_id=vpc.vpc_id,
                                        availability_zone=f"{self.region}c", cidr_block="10.0.2.0/24",
                                        map_public_ip_on_launch=True)
        gpu_subnet_c.add_default_internet_route(vpc.internet_gateway_id, vpc.internet_connectivity_established)
        gpu_subnets = ec2.SubnetSelection(subnets=[*vpc.public_subnets, gpu_subnet_c])

        # --- storage ---
        bucket = s3.Bucket(
            self, "Bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(prefix="archives/", transitions=[s3.Transition(
                    storage_class=s3.StorageClass.DEEP_ARCHIVE, transition_after=cdk.Duration.days(30))]),
                s3.LifecycleRule(abort_incomplete_multipart_upload_after=cdk.Duration.days(7)),
            ],
            cors=[s3.CorsRule(allowed_methods=[s3.HttpMethods.PUT, s3.HttpMethods.GET],
                              allowed_origins=["*"], allowed_headers=["*"], exposed_headers=["ETag"])],
        )
        table = dynamodb.Table(
            self, "Table",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
        )
        fs_sg = ec2.SecurityGroup(self, "EfsSg", vpc=vpc, description="EFS mount targets")
        file_system = efs.FileSystem(
            self, "Runs", vpc=vpc, vpc_subnets=public, security_group=fs_sg,
            lifecycle_policy=efs.LifecyclePolicy.AFTER_30_DAYS,
            throughput_mode=efs.ThroughputMode.ELASTIC,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        access_point = file_system.add_access_point(
            "RunsAp", path="/runs", create_acl=efs.Acl(owner_gid="0", owner_uid="0", permissions="755"),
            posix_user=efs.PosixUser(gid="0", uid="0"))

        # --- images and secrets ---
        engine_repo = ecr.Repository(self, "EngineRepo", repository_name="specimux-cloud/engine",
                                     removal_policy=cdk.RemovalPolicy.RETAIN)
        runapi_repo = ecr.Repository(self, "RunApiRepo", repository_name="specimux-cloud/runapi",
                                     removal_policy=cdk.RemovalPolicy.RETAIN)
        dorado_repo = ecr.Repository(self, "DoradoRepo", repository_name="specimux-cloud/dorado",
                                     removal_policy=cdk.RemovalPolicy.RETAIN)
        # Signs run tokens and session cookies; hosts' service keys live in
        # the table (`specimux-cloud hosts add`), not here
        session_secret = secrets.Secret(self, "SessionSecret", description="specimux-cloud session secret",
                                        generate_secret_string=secrets.SecretStringGenerator(
                                            exclude_punctuation=True, password_length=64))

        # --- Batch: the engine stage ---
        batch_sg = ec2.SecurityGroup(self, "BatchSg", vpc=vpc, description="Batch compute instances")
        fs_sg.add_ingress_rule(batch_sg, ec2.Port.tcp(2049), "NFS from Batch instances")
        # The engine works on the instance's own disk (demux appends and the
        # consensus debug files are ~100x slower on EFS; Sep 22 2026) and
        # mirrors only what the dashboard reads to EFS. A million reads used
        # ~5 GB; the root volume leaves room for much larger runs.
        engine_lt = ec2.LaunchTemplate(self, "EngineLaunchTemplate", block_devices=[
            ec2.BlockDevice(device_name="/dev/xvda", volume=ec2.BlockDeviceVolume.ebs(
                100, volume_type=ec2.EbsDeviceVolumeType.GP3, encrypted=True, delete_on_termination=True))])
        compute = batch.ManagedEc2EcsComputeEnvironment(
            self, "EngineCompute", vpc=vpc, vpc_subnets=public, security_groups=[batch_sg],
            instance_classes=[ec2.InstanceClass.C6I, ec2.InstanceClass.M6I, ec2.InstanceClass.C5, ec2.InstanceClass.M5],
            minv_cpus=0, maxv_cpus=64, use_optimal_instance_classes=False,
            spot=False, launch_template=engine_lt,
        )
        engine_queue = batch.JobQueue(self, "EngineQueue", compute_environments=[
            batch.OrderedComputeEnvironment(compute_environment=compute, order=1)])
        engine_role = iam.Role(self, "EngineJobRole", assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
                               description="The engine container's role; inputs arrive over presigned URLs")
        engine_container = batch.EcsEc2ContainerDefinition(
            self, "EngineContainer",
            image=ecs.ContainerImage.from_ecr_repository(engine_repo, "latest"),
            cpu=16, memory=cdk.Size.gibibytes(30),   # the default; a run's spec may override
            job_role=engine_role,
            logging=ecs.LogDrivers.aws_logs(stream_prefix="engine",
                                            log_retention=logs.RetentionDays.ONE_MONTH),
            volumes=[batch.EcsVolume.efs(name="runs", file_system=file_system, container_path=WORK_ROOT,
                                         access_point_id=access_point.access_point_id,
                                         enable_transit_encryption=True, use_job_role=False),
                     batch.EcsVolume.host(name="scratch", host_path="/scratch", container_path="/scratch")],
            environment={"SPECIMUX_SCRATCH": "/scratch"},
        )
        # A reclaimed or failed host retries (same generation, same work
        # dir); anything else exits. The second rule matters: Batch retries
        # every exit that no rule matches, which turned one wrapper failure
        # into a second attempt.
        retry_rules = [
            batch.RetryStrategy.of(batch.Action.RETRY, batch.Reason.custom(on_status_reason="Host EC2*")),
            batch.RetryStrategy.of(batch.Action.EXIT, batch.Reason.custom(on_exit_code="*")),
        ]
        engine_jobdef = batch.EcsJobDefinition(
            self, "EngineJobDef", container=engine_container,
            timeout=cdk.Duration.hours(12), retry_attempts=2,
            retry_strategies=retry_rules,
        )

        # --- Batch: the dorado stage (GPU) ---
        # Zero-minimum: an instance exists only while a POD5 run is being
        # basecalled. xlarge sizes: the same GPU as the 2xlarge (dorado is
        # GPU-bound) with 4 vCPUs and 16 GiB, so two fit the 8-vCPU G quota.
        # Chosen by cost per vCPU: g6 (L4) $0.81/h, g5 (A10G) $1.01/h, g6e
        # (L40S, about twice as fast) $1.86/h. BEST_FIT_PROGRESSIVE takes the
        # cheapest fit and does not fall back when it runs out (Sep 22 2026:
        # every 2xlarge was out in 2a/2b/2c for an hour). The NVIDIA ECS AMI
        # brings the driver; dorado brings its CUDA runtime.
        gpu_compute = batch.ManagedEc2EcsComputeEnvironment(
            self, "DoradoCompute", vpc=vpc, vpc_subnets=gpu_subnets, security_groups=[batch_sg],
            instance_types=[ec2.InstanceType("g6.xlarge"), ec2.InstanceType("g5.xlarge"),
                            ec2.InstanceType("g6e.xlarge")],
            images=[batch.EcsMachineImage(image_type=batch.EcsMachineImageType.ECS_AL2023_NVIDIA)],
            allocation_strategy=batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
            minv_cpus=0, maxv_cpus=8, use_optimal_instance_classes=False,
            spot=False,
        )
        dorado_queue = batch.JobQueue(self, "DoradoQueue", compute_environments=[
            batch.OrderedComputeEnvironment(compute_environment=gpu_compute, order=1)])
        dorado_role = iam.Role(self, "DoradoJobRole", assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
                               description="The dorado container's role; inputs and outputs go over presigned URLs")
        dorado_container = batch.EcsEc2ContainerDefinition(
            self, "DoradoContainer",
            image=ecs.ContainerImage.from_ecr_repository(dorado_repo, "latest"),
            cpu=4, memory=cdk.Size.gibibytes(14), gpu=1,   # an xlarge minus the agent's share
            job_role=dorado_role,
            logging=ecs.LogDrivers.aws_logs(stream_prefix="dorado",
                                            log_retention=logs.RetentionDays.ONE_MONTH),
            environment={"SPECIMUX_DORADO_DEVICE": "cuda:all", "SPECIMUX_SCRATCH": "/scratch"},
        )
        dorado_jobdef = batch.EcsJobDefinition(
            self, "DoradoJobDef", container=dorado_container,
            timeout=cdk.Duration.hours(8), retry_attempts=2, retry_strategies=retry_rules,
        )
        # The model complexes the dorado image bakes (docker/dorado.Dockerfile
        # DORADO_MODELS); the first is the default a POD5 run gets
        dorado_models = "sup@v5.0.0,sup@v5.2.0,hac@v6.0.0"

        # --- the run API on Fargate ---
        cluster = ecs.Cluster(self, "Cluster", vpc=vpc, container_insights_v2=ecs.ContainerInsights.DISABLED)
        namespace = sd.PrivateDnsNamespace(self, "Namespace", name="specimux.local", vpc=vpc)
        task_role = iam.Role(self, "RunApiTaskRole", assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"))
        bucket.grant_read_write(task_role)
        table.grant_read_write_data(task_role)
        task_role.add_to_policy(iam.PolicyStatement(
            actions=["sqs:CreateQueue", "sqs:GetQueueUrl", "sqs:SendMessage", "sqs:ReceiveMessage",
                     "sqs:DeleteMessage", "sqs:DeleteQueue", "sqs:GetQueueAttributes"],
            resources=[f"arn:aws:sqs:{self.region}:{self.account}:specimux-cloud-*"]))
        task_role.add_to_policy(iam.PolicyStatement(
            actions=["batch:SubmitJob", "batch:DescribeJobs", "batch:ListJobs", "batch:TerminateJob",
                     "batch:TagResource"],   # SubmitJob with tags needs TagResource
            resources=["*"]))
        task_def = ecs.FargateTaskDefinition(self, "RunApiTask", cpu=512, memory_limit_mib=1024,
                                             task_role=task_role)
        task_def.add_volume(name="runs", efs_volume_configuration=ecs.EfsVolumeConfiguration(
            file_system_id=file_system.file_system_id, transit_encryption="ENABLED",
            authorization_config=ecs.AuthorizationConfig(access_point_id=access_point.access_point_id)))
        container = task_def.add_container(
            "runapi",
            image=ecs.ContainerImage.from_ecr_repository(runapi_repo, "latest"),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="runapi", log_retention=logs.RetentionDays.ONE_MONTH),
            environment={
                "AWS_REGION": self.region,
                "SPECIMUX_BUCKET": bucket.bucket_name,
                "SPECIMUX_TABLE": table.table_name,
                "SPECIMUX_QUEUE_PREFIX": "specimux-cloud",
                "SPECIMUX_BATCH_QUEUE_ENGINE": engine_queue.job_queue_arn,
                # by name, not ARN: Batch resolves the latest revision, so a
                # job-definition change never replaces the run API task
                "SPECIMUX_BATCH_JOBDEF_ENGINE": engine_jobdef.job_definition_name,
                "SPECIMUX_BATCH_QUEUE_DORADO": dorado_queue.job_queue_arn,
                "SPECIMUX_BATCH_JOBDEF_DORADO": dorado_jobdef.job_definition_name,
                "SPECIMUX_DORADO_MODELS": dorado_models,
                "SPECIMUX_WORK_ROOT": WORK_ROOT,
                "SPECIMUX_ENGINE_API_URL": f"http://runapi.specimux.local:{RUNAPI_PORT}",
            },
            secrets={"SPECIMUX_SESSION_SECRET": ecs.Secret.from_secrets_manager(session_secret)},
            port_mappings=[ecs.PortMapping(container_port=RUNAPI_PORT)],
        )
        container.add_mount_points(ecs.MountPoint(container_path=WORK_ROOT, source_volume="runs", read_only=False))
        domain = self.node.try_get_context("domain")
        if domain is None:
            raise ValueError("-c domain=<your domain> is required (-c domain= for no public name)")
        api_sg = ec2.SecurityGroup(self, "RunApiSg", vpc=vpc, description="run API task")
        fs_sg.add_ingress_rule(api_sg, ec2.Port.tcp(2049), "NFS from the run API")
        # Engine jobs reach the task directly inside the VPC. Nothing else
        # reaches port 8090: the internet comes through the load balancer
        # (TLS) or not at all, since service keys and session cookies must
        # never cross it in plain HTTP. Without a domain the run API is
        # reachable only inside the VPC (the test client, a tunnel).
        api_sg.add_ingress_rule(batch_sg, ec2.Port.tcp(RUNAPI_PORT), "engine jobs to the run API")
        if not domain:
            api_sg.add_ingress_rule(ec2.Peer.ipv4(vpc.vpc_cidr_block), ec2.Port.tcp(RUNAPI_PORT),
                                    "the run API inside the VPC")
        if domain:
            container.add_environment("SPECIMUX_BASE_URL", f"https://{RUNAPI_HOST}.{domain}")
        # First deployment: the images are not in ECR yet, so the service
        # starts at zero tasks (`-c runapiDesired=1` once they are pushed)
        desired = int(self.node.try_get_context("runapiDesired") or 0)
        service = ecs.FargateService(
            self, "RunApiService", cluster=cluster, task_definition=task_def, desired_count=desired,
            circuit_breaker=ecs.DeploymentCircuitBreaker(enable=True, rollback=True),
            assign_public_ip=True, vpc_subnets=public, security_groups=[api_sg],
            cloud_map_options=ecs.CloudMapOptions(name="runapi", cloud_map_namespace=namespace,
                                                  dns_record_type=sd.DnsRecordType.A),
            min_healthy_percent=0, max_healthy_percent=100,   # one task, replaced in place
        )
        file_system.connections.allow_default_port_from(service)

        # --- TLS and a name: an ALB with an ACM certificate in front of the task ---
        if domain:
            zone = route53.HostedZone.from_lookup(self, "Zone", domain_name=domain)
            fqdn = f"{RUNAPI_HOST}.{domain}"
            cert = acm.Certificate(self, "Cert", domain_name=fqdn,
                                   validation=acm.CertificateValidation.from_dns(zone))
            alb = elbv2.ApplicationLoadBalancer(self, "Alb", vpc=vpc, internet_facing=True,
                                                vpc_subnets=public,
                                                idle_timeout=cdk.Duration.seconds(120))
            alb.add_redirect()  # 80 → 443
            listener = alb.add_listener("Https", port=443, certificates=[cert],
                                        ssl_policy=elbv2.SslPolicy.RECOMMENDED_TLS)
            listener.add_targets("RunApi", port=RUNAPI_PORT, protocol=elbv2.ApplicationProtocol.HTTP,
                                 targets=[service],
                                 health_check=elbv2.HealthCheck(path="/v1/version",
                                                                interval=cdk.Duration.seconds(30)),
                                 deregistration_delay=cdk.Duration.seconds(10))
            route53.ARecord(self, "RunApiRecord", zone=zone, record_name=RUNAPI_HOST,
                            target=route53.RecordTarget.from_alias(targets.LoadBalancerTarget(alb)))
            cdk.CfnOutput(self, "RunApiUrl", value=f"https://{fqdn}")

        # --- an optional test client inside the region (-c testClient=1) ---
        # A small instance for driving runs with large inputs: Google Drive
        # to EC2 and EC2 to S3 are fast and free, and the uploader runs on
        # it exactly as it would in a lab. Access via SSM (no key pair, no
        # open port). The repo is private, so the uploader arrives as a wheel
        # staged under tools/ in the bucket, with any config files a test
        # needs (aws s3 cp dist/*.whl s3://<bucket>/tools/). Stop it or drop
        # it between tests.
        if self.node.try_get_context("testClient"):
            client_role = iam.Role(self, "TestClientRole", assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
                                   managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                                       "AmazonSSMManagedInstanceCore")])
            bucket.grant_read(client_role, "tools/*")
            # engine experiments: the saved reads of past runs and the engine image
            bucket.grant_read(client_role, "runs/*")
            engine_repo.grant_pull(client_role)
            user_data = ec2.UserData.for_linux()
            user_data.add_commands(
                "set -x",
                "dnf install -y python3.12 python3.12-pip unzip",
                "curl -fsSL -o /tmp/rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip"
                " && unzip -q -o /tmp/rclone.zip -d /tmp && install -m 755 /tmp/rclone-*-linux-amd64/rclone"
                " /usr/local/bin/rclone",
                "mkdir -p /data/tools /data/pod5 && chown -R ec2-user:ec2-user /data",
                f"aws s3 cp --recursive s3://{bucket.bucket_name}/tools/ /data/tools/",
                "python3.12 -m pip install --quiet gdown /data/tools/*.whl",
                "chown -R ec2-user:ec2-user /data",
            )
            test_client = ec2.Instance(
                self, "TestClient", vpc=vpc, vpc_subnets=public, role=client_role,
                # -c testClientType=c6i.4xlarge sizes it like an engine job for experiments
                instance_type=ec2.InstanceType(self.node.try_get_context("testClientType") or "t3.medium"),
                machine_image=ec2.MachineImage.latest_amazon_linux2023(),
                block_devices=[ec2.BlockDevice(device_name="/dev/xvda",
                                               volume=ec2.BlockDeviceVolume.ebs(60, encrypted=True))],
                user_data=user_data, associate_public_ip_address=True,
            )
            cdk.CfnOutput(self, "TestClientInstanceId", value=test_client.instance_id)

        # --- outputs ---
        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)
        cdk.CfnOutput(self, "TableName", value=table.table_name)
        cdk.CfnOutput(self, "EngineRepoUri", value=engine_repo.repository_uri)
        cdk.CfnOutput(self, "RunApiRepoUri", value=runapi_repo.repository_uri)
        cdk.CfnOutput(self, "DoradoRepoUri", value=dorado_repo.repository_uri)
        cdk.CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        cdk.CfnOutput(self, "ServiceName", value=service.service_name)
        cdk.CfnOutput(self, "EngineJobQueue", value=engine_queue.job_queue_arn)
        cdk.CfnOutput(self, "DoradoJobQueue", value=dorado_queue.job_queue_arn)
