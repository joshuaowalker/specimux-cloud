"""AWS backends: S3, SQS, Batch and DynamoDB behind the same interfaces as
the local ones, so the run API's logic does not change.

Conventions:

- One S3 bucket, keys exactly as the local DirectoryStorage lays them out.
  Presigned URLs are S3's own; ETags are S3's (MD5 for single-part PUTs,
  which is what the uploader does today).
- One SQS standard queue per run, named after the run id, created on
  first use; at-least-once delivery with a visibility timeout, like
  MemoryQueue.
- Batch jobs named ``<run>-<kind>-<generation>``; ``find_by_name`` lists
  the queue's jobs by that name, which is how a lost submission is
  adopted.
- One DynamoDB table with a composite key (``pk``, ``sk``): runs
  (``RUN#<id>`` / ``RUN``), a client-token index row, archives, intents,
  commands (``RUN#<id>`` / ``CMD#<id>``) and stage slots
  (``STAGE#<name>`` / ``SLOT#<n>``), all written with condition
  expressions.
"""

import json
import logging
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

from .base import ConflictError, JobHandle, JobSpec, JobStatus, Message, ObjectInfo

logger = logging.getLogger(__name__)


def _boto3():
    try:
        import boto3
        return boto3
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("AWS backends need boto3: pip install 'specimux-cloud[aws]'") from e


# --- Storage ---

class S3Storage:
    def __init__(self, bucket: str, region: Optional[str] = None, client=None):
        self.bucket = bucket
        if client is None:
            # SigV4 with a regional endpoint: the legacy signer produces
            # global-endpoint URLs that S3 answers with a 307 the uploader
            # must not follow (the redirect drops the body on some clients)
            from botocore.config import Config
            client = _boto3().client(
                "s3", region_name=region,
                endpoint_url=f"https://s3.{region}.amazonaws.com" if region else None,
                config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))
        self.client = client

    @staticmethod
    def _etag(raw: str) -> str:
        return (raw or "").strip('"')

    def put(self, key: str, data: bytes) -> ObjectInfo:
        r = self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return ObjectInfo(key, len(data), self._etag(r.get("ETag", "")))

    def put_file(self, key: str, path: Path) -> ObjectInfo:
        self.client.upload_file(str(path), self.bucket, key)
        return self.head(key)

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def download(self, key: str, dest: Path) -> ObjectInfo:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, key, str(dest))
        return self.head(key)

    def head(self, key: str) -> Optional[ObjectInfo]:
        try:
            r = self.client.head_object(Bucket=self.bucket, Key=key)
        except self.client.exceptions.ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return ObjectInfo(key, int(r["ContentLength"]), self._etag(r.get("ETag", "")))

    def list(self, prefix: str) -> list[ObjectInfo]:
        out = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                out.append(ObjectInfo(o["Key"], int(o["Size"]), self._etag(o.get("ETag", ""))))
        return out

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def delete_prefix(self, prefix: str) -> int:
        objs = self.list(prefix)
        for i in range(0, len(objs), 1000):
            chunk = objs[i:i + 1000]
            self.client.delete_objects(Bucket=self.bucket,
                                       Delete={"Objects": [{"Key": o.key} for o in chunk], "Quiet": True})
        return len(objs)

    def presign_put(self, key: str, expires_s: int = 3600) -> str:
        return self.client.generate_presigned_url("put_object", Params={"Bucket": self.bucket, "Key": key},
                                                  ExpiresIn=expires_s, HttpMethod="PUT")

    def presign_get(self, key: str, expires_s: int = 3600, filename: Optional[str] = None) -> str:
        params = {"Bucket": self.bucket, "Key": key}
        if filename:
            params["ResponseContentDisposition"] = f'attachment; filename="{filename}"'
        return self.client.generate_presigned_url("get_object", Params=params, ExpiresIn=expires_s)


# --- Command queue ---

class SqsQueue:
    def __init__(self, prefix: str = "specimux-cloud", region: Optional[str] = None,
                 visibility_s: int = 30, client=None):
        self.prefix = prefix
        self.visibility_s = visibility_s
        self.client = client or _boto3().client("sqs", region_name=region)
        self._urls: dict[str, str] = {}

    def _url(self, run_id: str, create: bool = True) -> Optional[str]:
        url = self._urls.get(run_id)
        if url:
            return url
        name = f"{self.prefix}-{run_id}"
        try:
            url = self.client.get_queue_url(QueueName=name)["QueueUrl"]
        except self.client.exceptions.QueueDoesNotExist:
            if not create:
                return None
            url = self.client.create_queue(QueueName=name, Attributes={
                "VisibilityTimeout": str(self.visibility_s),
                "MessageRetentionPeriod": str(4 * 24 * 3600),
            })["QueueUrl"]
        self._urls[run_id] = url
        return url

    def send(self, run_id: str, body: dict) -> str:
        r = self.client.send_message(QueueUrl=self._url(run_id), MessageBody=json.dumps(body))
        return r["MessageId"]

    def receive(self, run_id: str, wait_s: float = 0.0, max_messages: int = 10) -> list[Message]:
        url = self._url(run_id, create=False)
        if url is None:
            return []
        r = self.client.receive_message(QueueUrl=url, MaxNumberOfMessages=max(1, min(10, max_messages)),
                                        WaitTimeSeconds=int(min(20, max(0, wait_s))))
        out = []
        for m in r.get("Messages", []):
            try:
                body = json.loads(m["Body"])
            except ValueError:
                body = {"raw": m["Body"]}
            # the receipt handle is what deletes the message: it is the id we hand out
            out.append(Message(m["ReceiptHandle"], body))
        return out

    def ack(self, run_id: str, message_id: str) -> None:
        url = self._url(run_id, create=False)
        if url:
            self.client.delete_message(QueueUrl=url, ReceiptHandle=message_id)

    def purge(self, run_id: str) -> None:
        url = self._url(run_id, create=False)
        if url:
            self.client.delete_queue(QueueUrl=url)
            self._urls.pop(run_id, None)


# --- Launcher ---

class BatchLauncher:
    """AWS Batch. ``job_queues`` and ``job_definitions`` map a job kind
    (``engine``, ``dorado``) to the queue and definition to submit to."""

    _STATES = {
        "SUBMITTED": "pending", "PENDING": "pending", "RUNNABLE": "pending",
        "STARTING": "pending", "RUNNING": "running",
        "SUCCEEDED": "succeeded", "FAILED": "failed",
    }

    def __init__(self, job_queues: dict, job_definitions: dict, region: Optional[str] = None, client=None):
        self.job_queues = dict(job_queues)
        self.job_definitions = dict(job_definitions)
        self.client = client or _boto3().client("batch", region_name=region)

    def submit(self, spec: JobSpec) -> JobHandle:
        r = self.client.submit_job(
            jobName=spec.name,
            jobQueue=self.job_queues[spec.kind],
            jobDefinition=self.job_definitions[spec.kind],
            containerOverrides={
                "environment": [{"name": k, "value": str(v)} for k, v in spec.env.items()],
                **({"command": list(spec.args)} if spec.args else {}),
                **({"resourceRequirements": [
                    {"type": "VCPU", "value": str(spec.vcpus)},
                    {"type": "MEMORY", "value": str(spec.memory_mib or spec.vcpus * 1900)},
                ]} if spec.vcpus else {}),
            },
            tags={"run_id": spec.run_id, "generation": str(spec.generation), "kind": spec.kind},
            propagateTags=True,
            **({"timeout": {"attemptDurationSeconds": int(spec.timeout_s)}} if spec.timeout_s else {}),
        )
        return JobHandle(r["jobId"], spec.name)

    def describe(self, job_id: str) -> JobStatus:
        jobs = self.client.describe_jobs(jobs=[job_id]).get("jobs", [])
        if not jobs:
            return JobStatus("unknown", reason="no such job")
        j = jobs[0]
        state = self._STATES.get(j.get("status"), "unknown")
        container = j.get("container") or {}
        code = container.get("exitCode")
        reason = j.get("statusReason") or container.get("reason") or ""
        return JobStatus(state, exit_code=int(code) if code is not None else None, reason=reason)

    def find_by_name(self, name: str) -> Optional[JobHandle]:
        for queue in set(self.job_queues.values()):
            r = self.client.list_jobs(jobQueue=queue, filters=[{"name": "JOB_NAME", "values": [name]}])
            summaries = r.get("jobSummaryList", [])
            if summaries:
                # the most recent submission of that name
                latest = max(summaries, key=lambda s: s.get("createdAt", 0))
                return JobHandle(latest["jobId"], name)
        return None

    def cancel(self, job_id: str, reason: str = "") -> None:
        self.client.terminate_job(jobId=job_id, reason=reason or "cancelled by run API")


# --- Store ---

def _to_ddb(value):
    """JSON → DynamoDB-safe (floats become Decimal)."""
    return json.loads(json.dumps(value), parse_float=Decimal)


def _from_ddb(value):
    if isinstance(value, list):
        return [_from_ddb(v) for v in value]
    if isinstance(value, dict):
        return {k: _from_ddb(v) for k, v in value.items()}
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


class DynamoStore:
    def __init__(self, table: str, region: Optional[str] = None, resource=None):
        self.table_name = table
        self.resource = resource or _boto3().resource("dynamodb", region_name=region)
        self.table = self.resource.Table(table)

    @staticmethod
    def create_table(table: str, region: Optional[str] = None, client=None) -> None:
        """Create the table (pay per request); for CDK-less setups and tests."""
        client = client or _boto3().client("dynamodb", region_name=region)
        client.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                  {"AttributeName": "sk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            BillingMode="PAY_PER_REQUEST",
        )
        client.get_waiter("table_exists").wait(TableName=table)

    def _cond_failed(self, e) -> bool:
        return e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"

    # runs
    def create_run(self, run: dict, client_token: Optional[str] = None) -> dict:
        if client_token:
            existing = self.table.get_item(Key={"pk": f"TOKEN#{client_token}", "sk": "TOKEN"}).get("Item")
            if existing:
                return self.get_run(existing["run_id"])
        now = time.time()
        run = {**run, "created": run.get("created", now), "updated": now}
        self.table.put_item(Item={"pk": f"RUN#{run['id']}", "sk": "RUN", "kind": "run",
                                  "user_id": run.get("user_id"), "host": run.get("host"),
                                  "state": run.get("state"),
                                  "created": _to_ddb(run["created"]), "doc": _to_ddb(run)},
                            ConditionExpression="attribute_not_exists(pk)")
        if client_token:
            try:
                self.table.put_item(Item={"pk": f"TOKEN#{client_token}", "sk": "TOKEN", "run_id": run["id"]},
                                    ConditionExpression="attribute_not_exists(pk)")
            except self.table.meta.client.exceptions.ConditionalCheckFailedException:
                # lost a race with an identical create: hand back the winner
                self.table.delete_item(Key={"pk": f"RUN#{run['id']}", "sk": "RUN"})
                return self.create_run(run, client_token)
        return run

    def get_run(self, run_id: str) -> Optional[dict]:
        item = self.table.get_item(Key={"pk": f"RUN#{run_id}", "sk": "RUN"}).get("Item")
        return _from_ddb(item["doc"]) if item else None

    def update_run(self, run_id: str, updates: dict, expected_state: Optional[Iterable[str]] = None) -> dict:
        for attempt in range(5):
            run = self.get_run(run_id)
            if run is None:
                raise ConflictError(f"no run {run_id}")
            if expected_state is not None and run.get("state") not in set(expected_state):
                raise ConflictError(f"run {run_id} is {run.get('state')}, expected {list(expected_state)}")
            new = {**run, **(updates(dict(run)) if callable(updates) else updates), "updated": time.time()}
            try:
                # optimistic: the stored doc must still be the one we read
                self.table.put_item(
                    Item={"pk": f"RUN#{run_id}", "sk": "RUN", "kind": "run", "user_id": new.get("user_id"),
                          "host": new.get("host"), "state": new.get("state"),
                          "created": _to_ddb(new.get("created")), "doc": _to_ddb(new)},
                    ConditionExpression="attribute_exists(pk) AND #d.updated = :u",
                    ExpressionAttributeNames={"#d": "doc"},
                    ExpressionAttributeValues={":u": _to_ddb(run.get("updated"))},
                )
                return new
            except self.table.meta.client.exceptions.ConditionalCheckFailedException:
                continue  # someone else wrote first: re-read and retry
        raise ConflictError(f"run {run_id}: too much contention")

    def list_runs(self, user_id: Optional[str] = None, states: Optional[Iterable[str]] = None,
                  host: Optional[str] = None) -> list[dict]:
        from boto3.dynamodb.conditions import Attr
        expr = Attr("kind").eq("run")
        if user_id is not None:
            expr = expr & Attr("user_id").eq(user_id)
        if host is not None:
            expr = expr & Attr("host").eq(host)
        items = []
        kwargs = {"FilterExpression": expr}
        while True:
            r = self.table.scan(**kwargs)
            items.extend(r.get("Items", []))
            if "LastEvaluatedKey" not in r:
                break
            kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        runs = sorted((_from_ddb(i["doc"]) for i in items), key=lambda r: r.get("created", 0))
        if states is not None:
            wanted = set(states)
            runs = [r for r in runs if r.get("state") in wanted]
        return runs

    def delete_run(self, run_id: str) -> None:
        from boto3.dynamodb.conditions import Key
        r = self.table.query(KeyConditionExpression=Key("pk").eq(f"RUN#{run_id}"))
        with self.table.batch_writer() as batch:
            for item in r.get("Items", []):
                batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    # hosts
    def put_host(self, host: dict) -> dict:
        self.table.put_item(Item={"pk": f"HOST#{host['id']}", "sk": "HOST", "kind": "host",
                                  "doc": _to_ddb(host)})
        return host

    def get_host(self, host_id: str) -> Optional[dict]:
        item = self.table.get_item(Key={"pk": f"HOST#{host_id}", "sk": "HOST"}).get("Item")
        return _from_ddb(item["doc"]) if item else None

    def list_hosts(self) -> list[dict]:
        from boto3.dynamodb.conditions import Attr
        items, kwargs = [], {"FilterExpression": Attr("kind").eq("host")}
        while True:
            r = self.table.scan(**kwargs)
            items.extend(r.get("Items", []))
            if "LastEvaluatedKey" not in r:
                break
            kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        return sorted((_from_ddb(i["doc"]) for i in items), key=lambda h: h["id"])

    # archives
    def put_archive(self, archive: dict) -> dict:
        self.table.put_item(Item={"pk": f"ARCHIVE#{archive['id']}", "sk": "ARCHIVE", "kind": "archive",
                                  "user_id": archive.get("user_id"), "doc": _to_ddb(archive)})
        return archive

    def get_archive(self, archive_id: str) -> Optional[dict]:
        item = self.table.get_item(Key={"pk": f"ARCHIVE#{archive_id}", "sk": "ARCHIVE"}).get("Item")
        return _from_ddb(item["doc"]) if item else None

    # intents
    def open_intent(self, run_id: str, kind: str, payload: dict) -> str:
        iid = uuid.uuid4().hex
        self.table.put_item(Item={"pk": "INTENTS", "sk": f"OPEN#{iid}", "kind": "intent", "id": iid,
                                  "run_id": run_id, "intent_kind": kind, "payload": _to_ddb(payload),
                                  "opened": _to_ddb(time.time())})
        return iid

    def resolve_intent(self, intent_id: str, result: dict) -> None:
        item = self.table.get_item(Key={"pk": "INTENTS", "sk": f"OPEN#{intent_id}"}).get("Item")
        if not item:
            return
        self.table.put_item(Item={**item, "pk": f"RUN#{item['run_id']}", "sk": f"INTENT#{intent_id}",
                                  "result": _to_ddb(result), "resolved": _to_ddb(time.time())})
        self.table.delete_item(Key={"pk": "INTENTS", "sk": f"OPEN#{intent_id}"})

    def list_open_intents(self, run_id: Optional[str] = None) -> list[dict]:
        from boto3.dynamodb.conditions import Key
        r = self.table.query(KeyConditionExpression=Key("pk").eq("INTENTS"))
        out = []
        for item in sorted(r.get("Items", []), key=lambda i: i.get("opened", 0)):
            if run_id is not None and item.get("run_id") != run_id:
                continue
            out.append({"id": item["id"], "run_id": item["run_id"], "kind": item["intent_kind"],
                        "payload": _from_ddb(item.get("payload", {})), "opened": _from_ddb(item.get("opened"))})
        return out

    # commands
    def put_command(self, run_id: str, command: dict) -> dict:
        now = time.time()
        doc = {**command, "run_id": run_id, "outcome": "pending", "created": now}
        try:
            self.table.put_item(Item={"pk": f"RUN#{run_id}", "sk": f"CMD#{command['id']}", "kind": "command",
                                      "outcome": "pending", "created": _to_ddb(now), "doc": _to_ddb(doc)},
                                ConditionExpression="attribute_not_exists(pk)")
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            pass
        return doc

    def get_command(self, run_id: str, command_id: str) -> Optional[dict]:
        item = self.table.get_item(Key={"pk": f"RUN#{run_id}", "sk": f"CMD#{command_id}"}).get("Item")
        if not item:
            return None
        return {**_from_ddb(item["doc"]), "outcome": item.get("outcome"), "reason": item.get("reason")}

    def mark_command(self, run_id: str, command_id: str, outcome: str, reason: Optional[str] = None) -> None:
        try:
            self.table.update_item(Key={"pk": f"RUN#{run_id}", "sk": f"CMD#{command_id}"},
                                   UpdateExpression="SET outcome = :o, reason = :r, updated = :u",
                                   ConditionExpression="attribute_exists(pk)",
                                   ExpressionAttributeValues={":o": outcome, ":r": reason, ":u": _to_ddb(time.time())})
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            pass

    def list_commands(self, run_id: str, pending_only: bool = False) -> list[dict]:
        from boto3.dynamodb.conditions import Key
        r = self.table.query(KeyConditionExpression=Key("pk").eq(f"RUN#{run_id}") & Key("sk").begins_with("CMD#"))
        cmds = [{**_from_ddb(i["doc"]), "outcome": i.get("outcome"), "reason": i.get("reason")}
                for i in sorted(r.get("Items", []), key=lambda i: i.get("created", 0))]
        if pending_only:
            cmds = [c for c in cmds if c["outcome"] == "pending"]
        return cmds

    # reservations: one item per taken slot, SLOT#0 .. SLOT#<slots-1>
    def _slot_items(self, stage: str) -> list[dict]:
        from boto3.dynamodb.conditions import Key
        r = self.table.query(KeyConditionExpression=Key("pk").eq(f"STAGE#{stage}") & Key("sk").begins_with("SLOT#"),
                             ConsistentRead=True)
        return sorted(r.get("Items", []), key=lambda i: int(i["sk"][5:]))

    def reserve_stage(self, stage: str, run_id: str, slots: int = 1) -> bool:
        errors = self.table.meta.client.exceptions
        for _ in range(3):
            items = self._slot_items(stage)
            if any(i.get("run_id") == run_id for i in items):
                return True
            taken = {int(i["sk"][5:]) for i in items}
            free = [n for n in range(slots) if n not in taken]
            if not free:
                return False
            try:
                self.table.put_item(Item={"pk": f"STAGE#{stage}", "sk": f"SLOT#{free[0]}", "run_id": run_id,
                                          "since": _to_ddb(time.time())},
                                    ConditionExpression="attribute_not_exists(pk)")
                return True
            except errors.ConditionalCheckFailedException:
                continue  # another run took that slot first; look again
        return False

    def release_stage(self, stage: str, run_id: str) -> None:
        for item in self._slot_items(stage):
            if item.get("run_id") != run_id:
                continue
            try:
                self.table.delete_item(Key={"pk": item["pk"], "sk": item["sk"]},
                                       ConditionExpression="run_id = :r",
                                       ExpressionAttributeValues={":r": run_id})
            except self.table.meta.client.exceptions.ConditionalCheckFailedException:
                pass

    def stage_holders(self, stage: str) -> list[str]:
        return [i["run_id"] for i in self._slot_items(stage)]
