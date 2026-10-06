# Operating a deployment

For whoever runs a live deployment: releasing, rolling, testing and
cleaning up. Setting one up is in the README ("Deploying on AWS"); the
design is in [DESIGN.md](DESIGN.md). Nothing here names an account or a
resource: every name comes from `infra/cdk-outputs.json`, which `cdk
deploy --outputs-file` writes and git ignores.

## Shell setup

```bash
export AWS_PROFILE=<profile> AWS_DEFAULT_REGION=<region>
# CDK also needs these (the stack looks up the domain's hosted zone)
export CDK_DEFAULT_ACCOUNT=<account id> CDK_DEFAULT_REGION=$AWS_DEFAULT_REGION
out() { jq -r ".\"specimux-cloud\".$1" infra/cdk-outputs.json; }   # from the repo root
out TableName; out ClusterName; out ServiceName; out EngineJobQueue; out DoradoJobQueue
```

Log groups are not outputs; list them with `aws logs describe-log-groups
--log-group-name-prefix specimux-cloud --query 'logGroups[].logGroupName'`.
The run API's, the engine's, dorado's and the fetch job's groups are
named after their constructs (`RunApiTask…`, `Engine…`, `Dorado…`,
`Fetch…`). In zsh, quote `--query` values, and write `${r}:latest`, not
`$r:latest` (`:l` is a zsh modifier).

## Before touching anything live

Users run real work on the service, and a roll or a broken release costs
them hours. Before a deploy, a roll or a cleanup:

- `GET /v1/load` (any service key, or the console's header) shows runs
  busy, waiting and queued across all hosts. An upload in progress shows
  there but not in Batch.
- Batch: `aws batch list-jobs --job-queue $(out EngineJobQueue)
  --job-status RUNNING` (and `RUNNABLE`, `STARTING`; the same for
  `DoradoJobQueue`). The fetch job runs on the engine queue.

Roll only when nothing is uploading or running. A running job survives a
roll, because its reports retry, but an uploader sees errors for a minute
or two.

## Releasing

Release a change in this order, verifying each step before the next:

1. **The suite first, if the change needs one.** The images pin
   specimux-suite from PyPI (`ARG SUITE_SPEC` in both Dockerfiles), and
   so does `pyproject.toml`. Bump them after the suite is on PyPI. To try
   an unreleased suite, use `SUITE_REF=<branch> docker/build-push.sh all`,
   which resolves the branch to its commit so Docker's cache can't keep
   a stale install.
2. **CI must be green.** It runs Python 3.11 to 3.13; 3.11 rejects nested
   same-quote f-strings, so run `python3.11 -m py_compile` on changed
   files before pushing.
3. **Bump the version** in `pyproject.toml` and
   `src/specimux_cloud/__init__.py`, commit "Bump version to X.Y.Z",
   push, and wait for CI.
4. **Create the GitHub release** (`gh release create vX.Y.Z --target main
   ...`). It runs `.github/workflows/workflow.yml`, which publishes to
   PyPI by trusted publishing; don't rename the workflow file. Check
   `https://pypi.org/pypi/specimux-cloud/json`. PyPI comes before the
   roll because `GET /v1/version` offers the run API's own version to
   uploaders as the latest.
5. **Build and push the images** (below).
6. **Deploy the stack** (`cdk deploy`), if `infra/` changed or the
   environment needs to. A task definition change rolls the service by
   itself.
7. **Roll the run API** (below), unless step 6 already did.
8. **Check** that `GET /v1/version` reports the new version, and run a
   smoke test.

## Images

```bash
docker/build-push.sh all       # engine + run API: about 10 + 2 minutes
docker/build-push.sh dorado    # only when dorado or its models change
```

- **The engine image takes effect on the next job,** because Batch pulls
  `:latest` per job. The run API image needs a roll.
- **Build from a committed tree.** The second tag is `HEAD`'s commit, so
  a build of uncommitted changes carries the previous commit's tag.
- **Every image is tagged `:latest` and with the commit.** `--provenance=false`
  keeps one manifest per image, so ECR's lifecycle rule (the ten newest
  tagged images per repository) expires old builds whole.
- **The engine image ends with `pip check`.** A cached suite layer once
  shipped an old specimux under `--no-deps`; the check fails the build
  instead.
- **The dorado image is about 6 GB,** and pushing it from a slow uplink
  takes about an hour. Its build cache lives in its ECR repository under
  `:buildcache`, so a pruned local cache doesn't force a full re-push. Its
  Ubuntu base is pinned by digest: a moved tag invalidates every layer
  above it, so change the digest deliberately.
- **Dorado models are listed in two places, kept in step:**
  `DORADO_MODELS` in `docker/dorado.Dockerfile` (full names) and the
  model list in `infra/stack.py` (`SPECIMUX_DORADO_MODELS`, the names
  offered). Changing the latter means a `cdk deploy`.

## Deploying the stack

```bash
cd infra
npx --yes aws-cdk@2 diff   -c domain=<domain> -c runapiDesired=1
npx --yes aws-cdk@2 deploy -c domain=<domain> -c runapiDesired=1 \
    --require-approval never --outputs-file cdk-outputs.json
```

Pass the same `-c` options to every `diff` and `deploy`. A missing one
plans to remove whatever it adds (`runapiDesired` scales the run API to
zero). Read the diff before deploying.

CloudFormation fails ("Internal Failure") when it replaces the run API's
task definition while a job definition changes. That is why job
definitions are referenced by name, never by ARN; keep it so.

## Rolling the run API

```bash
aws ecs update-service --cluster $(out ClusterName) --service $(out ServiceName) --force-new-deployment
aws ecs wait services-stable --cluster $(out ClusterName) --services $(out ServiceName)
```

The service runs one task with minimum healthy 0%. A roll stops the old
task before the new one starts, so expect a minute or two of 503s. Its
background loops (reconcile, launch queue, clean-up) are not built to
run twice at once, so don't overlap tasks.

**Fargate retirement notices:** AWS retires old tasks in a window it
picks, possibly mid-run. When a notice arrives, roll at a quiet moment
before the window; a task started after the cutoff isn't retired.

## Hosts and keys

```bash
specimux-cloud hosts --backend aws --table $(out TableName) list
specimux-cloud hosts --backend aws --table $(out TableName) key <host> <label>     # printed once
specimux-cloud hosts --backend aws --table $(out TableName) revoke <host> <label>
```

A key is printed once and never written anywhere else: not in notes, not
in shell history files you keep, not in this repo. A key for a test is a
temporary label: write it to a file readable only by you, outside the
repo, use it, then revoke the label and delete the file. An expired SSO
login stops revoking too, so revoke before signing off.

## Smoke tests

After a release, pick the cheapest test that exercises the change:

- **Engine only:** rerun a sealed run from its basecalled reads, which
  uses no GPU: `specimux-cloud submit --rerun-of <run> --start engine
  --wait ...`. About 25 minutes for a million reads.
- **Basecalling:** `--rerun-of <run> --start basecall` re-runs dorado on
  the stored POD5 (both GPUs if there are files enough). Reruns refuse
  archives older than 28 days, which are in Deep Archive.
- **The whole path from outside:** `specimux-cloud submit --drive-folder
  <public folder link> --primers ... --specimens ... --reference ...
  --wait`.
- **A small FASTQ:** `head -100000 reads.fastq` gives a 25k-read subset,
  about 10 minutes end to end.
- **The host API:** `SPECIMUX_CONTRACT_RUN_API=<url>
  SPECIMUX_CONTRACT_KEY=<key> pytest tests/test_contract.py`.

Rough timings for a 10.5 GB, five-file POD5 set (1.07 M reads):

| Step | Time |
|---|---|
| Copy from Drive | 7 min |
| Waiting for a GPU | 4–10 min |
| Basecalling | about 50 min on one GPU, 34 on two |
| Engine | about 25 min |
| Packaging and seal | 2 min |

## Watching runs

- **Logs:** `aws logs tail <group> --since 30m --format short`.
  - The run API's group carries its decisions (launches, claims, exits).
  - Dorado writes one log stream per worker; its own stderr comes back in
    the run's exit report (`log_tail`).
  - The engine's group has the pipeline's output.
- **Run documents:** DynamoDB, `pk = RUN#<id>`, `sk = RUN`, field `doc`.
  Read them with a boto3 resource. `DynamoStore.update_run` (conditional,
  with a callable) is the manual-correction tool, and the last resort.
- **EFS without an instance:** run a one-off Fargate task from the run
  API's task definition with a command override (`sh -c "du -sh ..."`).

**GPU capacity:** on-demand G capacity in a region comes and goes. Batch
tries the listed types (g6, g5, g6e xlarge) and may wait several minutes
for one. An "InsufficientInstanceCapacity" message that suggests other
zones lists every zone but the failed one, so it says nothing about
capacity. Each GPU worker takes 4 vCPUs of the account's "Running
On-Demand G and VT instances" quota, so two workers need 8.

## Cleaning up

- **ECR** expires all but the ten newest tagged images per repository by
  itself.
- **S3:** archives move to Deep Archive after 30 days. The run API's
  clean-up loop removes finished runs' EFS directories an hour after
  they end, and deletes cancelled and incomplete runs' archives after
  7 days.
- **Local Docker:** the build machine may be shared with other projects.
  - Remove only this project's images and its own buildx builder's cache
    (`docker buildx du --builder specimux`, then `docker buildx prune
    --builder specimux ...`).
  - Never run a machine-wide prune.
  - Check what a prune option keeps before using it: `--keep-storage`
    once kept old entries and dropped the newest dorado layers.
  - List what would go, and have it agreed, before deleting.

## Known behaviour worth remembering

- **Batch retries any exit that no rule matches,** so the job definitions
  carry an explicit EXIT rule.
- **Batch retries share the generation.** The engine wrapper's
  `engine-exit.json` short-circuits a generation that already finished.
- **Never block the run API's event loop:** long polls and backend calls
  run in the threadpool, and the seal is a thread.
- **There is no `/healthz`;** a 404 there is normal.
- **The engine works on local disk and mirrors to EFS** (`--mirror-dir`).
  On EFS directly the demux ran about 200 times slower, because every
  read costs metadata operations.
