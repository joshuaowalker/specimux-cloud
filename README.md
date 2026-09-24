# specimux-cloud

The hosted form of [specimux-suite](https://github.com/joshuaowalker/specimux-suite).
A lab sends a nanopore sequencing run to the cloud (raw POD5, which the
service basecalls on a GPU, or FASTQ already basecalled by MinKNOW) and
gets the same live dashboard and results the suite produces on a laptop:
demultiplexing, consensus sequences per specimen, and identification
against a reference database.

- **[Uploading a run](#uploading-a-run):** you have a job code and a run
  folder. Start here.
- **[Creating runs](#creating-runs):** the console, sharing a dashboard
  publicly, and the command line.
- **[Running a deployment](#running-a-deployment):** for operators and
  developers: the AWS stack, hosts and keys, the local stack, and how the
  pieces fit together.

## Uploading a run

Every run has a **job code** (it looks like `r1a2b3c4d.<secret>`), which
you get from whoever created the run: a website such as mycomap.org, or
the service's console. The job code is all the uploader needs; there is no
account or login. Treat it like a password until the upload is done.

### Install

On the computer that has the reads (the sequencing laptop, a lab server,
anything with Python 3.11 or later and an internet connection):

```bash
pip install specimux-cloud
```

### A finished run folder

When sequencing is over, or the reads are already on disk:

```bash
specimux-cloud upload --run-api https://runs.specimux.com --job-code <code> --once <folder>
```

The folder can be a MinKNOW run folder or any folder of `.pod5` or
`.fastq` / `.fastq.gz` files; subfolders are included. `--once` uploads
what is there and tells the service the upload is complete, and
processing starts. The run's type (POD5 or FASTQ) was chosen when the run
was created, so upload the matching files.

### While sequencing

Start the uploader on the MinKNOW run folder while MinKNOW is still
writing it, without `--once`:

```bash
specimux-cloud upload --run-api https://runs.specimux.com --job-code <code> <MinKNOW run folder>
```

It uploads each file once it has stopped growing (30 seconds unchanged;
`--settle` changes that) and finishes by itself when MinKNOW writes its
`final_summary_*.txt` at the end of the run. For a **live** run the
service processes files as they arrive, so the dashboard fills while you
sequence; for a batch run, uploading during sequencing just means the
upload is done when sequencing is.

Reads under MinKNOW's `fastq_fail` and `pod5_fail` folders are left out;
`--include-failed` sends them too.

### When something goes wrong

- **The upload was interrupted** (network drop, laptop asleep, Ctrl+C):
  run the same command again. Files already uploaded are checked and
  skipped; nothing is sent twice. Network errors are retried on their own.
- **You forgot `--once`** and sequencing is over: the uploader keeps
  waiting for MinKNOW's final summary. After ten quiet minutes it prints a
  reminder. Press Ctrl+C and run the same command with `--once`, or click
  **Upload is complete** on the run page; a running uploader notices
  within a minute and exits.
- **"stopped taking uploads" or "no longer taking uploads"**: the upload
  was already completed (from the run page, say), the run was cancelled,
  or it expired. A run with no upload activity for 24 hours (or none at
  all within 7 days of creation) is closed; what it had received is
  deleted a week later. Upload the folder again to a new run.
- **Lost the job code**, or it may have leaked: the run page's **New job
  code** issues another while the run is still taking uploads, and the
  old one stops working at once.
- **A bad job code** fails at once with the server's reason; check that
  the whole code was copied.

### After the upload

Processing starts on its own: basecalling first for POD5, then the
pipeline. The run page shows the stage and progress and links to the
live **dashboard**. For scale: a full MinION run of about a million reads
basecalled with dorado's SUP model in about 47 minutes on one GPU, and
the pipeline then took about 25 minutes. When a run is done its page
offers three downloads: `results.zip` (the summary package and the run's
event log), `output.zip` (the full output) and `reads.zip`
(demultiplexed reads per specimen).

## Creating runs

A deployment is one URL, the **run API** (for example
`https://runs.specimux.com`). Runs are created by a **host**: a website
that knows its users, or the built-in **console** at `<run API>/console/`,
which you log into with a **service key** from the operator. A service
key creates runs and sees every run of its host; keep it out of email,
chat and git repositories.

### In the console

1. Open `<run API>/console/` and log in with your service key.
2. **New run:** upload the primers FASTA and the specimens file
   (Index.txt), optionally a reference FASTA (`name="..."` headers;
   without one there is no identification; references used before are
   offered again), and choose the input:
   - **FASTQ**, uploaded after or during sequencing;
   - **Live** FASTQ, processed as it arrives (see
     [While sequencing](#while-sequencing));
   - **POD5**, basecalled by the service. The defaults are dorado's
     `sup@v5.0.0` model and reads 400–2000 bases long, the full ITS
     amplicon.
3. The console shows the job code once, with the upload command to run
   where the reads are.
4. The run page follows the run from there: stage, basecalling
   progress, the dashboard link and, at the end, the downloads. It also
   has **Upload is complete**, **Cancel** (stops a run at whatever stage
   it is in) and **Retry** (runs a failed run's failed stage again;
   basecalling skips the files it already delivered).

The runs list shows the whole service's load (runs busy and queued per
stage, and any waiting for a machine), which explains a wait before
basecalling or the pipeline starts. Each stage runs two runs at a time.

### Sharing a dashboard publicly

**Share publicly** on the run page makes a link that opens the run's
dashboard for anyone who has it, with no login: for example an audience
watching a live run. The dashboard shows the link as its QR code. Public
viewers can watch and star specimens (starring only raises a specimen's
processing priority; the run page can block it) but cannot finalize,
correct, or download unless you allow downloads. **Stop sharing** or
**New link** ends every public session at once.

### From the command line

With a service key, one command creates the run, uploads, waits and
downloads `results.zip`:

```bash
specimux-cloud submit --run-api https://runs.specimux.com --service-key <key> \
    --primers primers.fasta --specimens Index.txt [--reference refs.fasta] \
    --wait <folder or files>
```

POD5 input is detected from the files; `--model`, `--min-length`,
`--max-length` and `--min-qscore` set basecalling, and `--profile` and
`--min-reads` the pipeline. `--live` makes a live run of a folder the
uploader keeps watching. The service keeps each reference database once,
by its SHA-256, so `submit` sends a reference only the first time your
host uses it (another host's copy is never lent: each host sends a
reference once itself). One run
at a time:

```bash
specimux-cloud run status|cancel|retry <run id> --run-api URL --service-key KEY [--reason TEXT]
```

`submit` and `run` also take the key from `SPECIMUX_SERVICE_KEY`, which
keeps it out of your shell history.

## Running a deployment

The rest of this README is for whoever runs a deployment or works on the
code. The design and the reasoning behind it are in
[docs/DESIGN.md](docs/DESIGN.md).

### For a website that wants to be a host

A host creates runs with its service key over a small HTTP API, shows its
users the job code, and has one **authorize route** that vouches for a
logged-in user when they open a run's dashboard: the run API mints a
short-lived token at the host's request, so the host never signs
anything. The console (`src/specimux_cloud/console/app.py`) is a complete
host built only on that API, and `tests/test_contract.py` checks a host's
side of it against any deployment. The API is under "The run API" in
[docs/DESIGN.md](docs/DESIGN.md).

### Deploying on AWS

`infra/` is a CDK app (Python) for one AWS account and region:

- a VPC with public subnets only (no NAT gateway);
- an S3 bucket for uploads, results and finished runs, a DynamoDB table
  for runs, hosts and keys, and EFS for the mirrors of runs in progress,
  which the dashboard reads;
- AWS Batch: a CPU environment for the pipeline (c6i, m6i, c5 or m5, up
  to 64 vCPUs) and a GPU environment for dorado (g6, g5 or g6e xlarge),
  both at zero instances when idle, two runs at a time on each;
- the run API on Fargate behind an application load balancer at
  `https://runs.<domain>`, with an ACM certificate;
- ECR repositories for the three images, and the session secret in
  Secrets Manager.

Idle cost is the Fargate task, the load balancer and storage, about $1.50
a day; instances exist only while a run executes.

Before deploying you need a Route 53 hosted zone for your domain in the
account, and a quota of at least 4 vCPUs for "Running On-Demand G and VT
instances" for basecalling. Then:

```bash
export AWS_PROFILE=<your profile>
cd infra
npx aws-cdk@2 bootstrap                                   # once per account and region
npx aws-cdk@2 deploy -c domain=example.org --outputs-file cdk-outputs.json
cd ..
docker/build-push.sh all                                  # engine and run API images
docker/build-push.sh dorado                               # dorado plus its models, several GB
cd infra
npx aws-cdk@2 deploy -c domain=example.org -c runapiDesired=1 --outputs-file cdk-outputs.json
```

The first deploy creates everything with the run API at zero tasks; the
second starts it once its image exists. `-c domain` is required on every
deploy; `-c domain=` with no value
deploys without the load balancer and certificate (the run API is then
reachable only inside the VPC). The region is the profile's, or
`us-west-2`. `build-push.sh` reads the repository URIs from
`cdk-outputs.json`. A new engine image takes effect on the next run
(Batch pulls `:latest` per job); a new run API image needs the ECS
service rolled (`aws ecs update-service --force-new-deployment`).

Dorado models are baked into the dorado image (`DORADO_MODELS` in
`docker/dorado.Dockerfile`), and the model names the run API offers are
`SPECIMUX_DORADO_MODELS` in `infra/stack.py`; keep the two in step.

Then register the hosts that may use the deployment. A key is printed
once:

```bash
TABLE=$(jq -r '."specimux-cloud".TableName' infra/cdk-outputs.json)
specimux-cloud hosts --backend aws --table $TABLE add lab --name "Our lab" --label lab-staff \
    --console --base-url https://runs.example.org
specimux-cloud hosts --backend aws --table $TABLE add partner --name "Partner site" --label server \
    --authorize-url https://partner.example.org/specimux/authorize
```

`--console` makes the console the host's authorize route, for a host with
no website. Hosts have labelled keys (`hosts key`, `hosts rotate` with a
day's grace for the old key, `hosts revoke`, `hosts disable`, `hosts
list`, `hosts set`) and see only their own runs.

## Development

```bash
pip install -e '.[dev]'
pytest
```

The suite comes from PyPI (0.3.3 or later); to work against an
unreleased suite, install the sibling checkout editable first
(`pip install -e ../specimux-suite`).

The package has four parts:

- `specimux_cloud.runapi`: the always-on service the browser, the
  uploader and hosts talk to: event store and fan-out, a dashboard per
  run, job control, uploads, results, run sessions.
- `specimux_cloud.console`: the built-in host, mounted at `/console/`
  beside the run API. It talks to the run API only over HTTP with the key,
  exactly as a host's website does.
- `specimux_cloud.engine` and `specimux_cloud.dorado`: what runs inside
  the compute jobs: a wrapper around `specimux-suite` plus the cloud
  plugin that forwards events to the run API and applies commands from
  it, and the basecalling job around `dorado`.
- `specimux_cloud.backends`: storage, command queue, job launcher and
  control-plane store behind small interfaces. Local implementations (a
  directory, memory, subprocesses, SQLite) run the whole system on a
  laptop and in CI; AWS implementations (S3, SQS, Batch, DynamoDB) are a
  backend swap.

### The local stack

```bash
specimux-cloud runapi --data-dir data --port 8090
```

starts the run API over the local backends: storage under `data/storage`,
control-plane state in `data/control-plane.sqlite`, engine work dirs under
`data/work`, engine logs under `data/logs`, and the console at
`http://127.0.0.1:8090/console/`. A host `dev` exists with the key
`dev.dev-service-key` (`--dev-key` changes it, `--dev-key ''` removes it).
Everything under [Uploading a run](#uploading-a-run) and [Creating runs](#creating-runs) works against it with
`--run-api http://127.0.0.1:8090`. The run API launches the engine as a
subprocess (`specimux-cloud engine`, which runs `specimux-suite batch`
with the `cloud` plugin), and a POD5 run's dorado job the same way; that
runs whatever `dorado` is on `PATH` on the device
`SPECIMUX_DORADO_DEVICE` (default `cuda:all`; `cpu` works, slowly).

`tests/test_local_stack.py` does whole runs end to end with stand-in
bioinformatics tools (`tests/fake_tools/`); `tests/test_contract.py` is
the job API as a host sees it, and runs against a deployment with
`SPECIMUX_CONTRACT_RUN_API` and `SPECIMUX_CONTRACT_KEY` set.

## How it works

### Run lifecycle

`created` → `uploading` → `input_complete` → (`basecalling` for POD5) →
`running` → `finalizing` → `sealing` → `sealed`, or `failed`; an
abandoned upload ends `incomplete` (no upload request for 24 hours, or
nothing uploaded within 7 days of creation; the uploader's status checks
don't count as activity), which closes its job code. The upload of a
cancelled or abandoned run is deleted a week after it ended; a finished
run's EFS directory an hour after, once its `view.zip` is in S3, and its
dashboard is served from that (see docs/DESIGN.md, "Storage"). Each stage
runs at most two runs at once (`SPECIMUX_STAGE_SLOTS`, default
`engine=2,dorado=2`; two dorado slots are what an 8-vCPU G-instance quota
allows): a run whose stage is full waits in `input_complete` and takes
the next free slot, oldest first. Concurrent runs share nothing: each has
its own EFS directory, SQS queue and scratch directory (per run and
generation). `GET /v1/load` (any service key) reports the whole
service's load in counts, cached for 15 seconds, and the console shows it
above the runs list. The run API reconciles every two minutes
(`SPECIMUX_RECONCILE_S`), so a job that dies without reporting fails its
run within minutes.

A live run takes uploads while its engine runs: the engine launches with
the first upload request (or the next free slot), the wrapper polls
`POST /v1/runs/{id}/inputs` for new files and renames each into the
engine's watch directory on local disk, and once the upload is complete
and every file has been demultiplexed (its `specimux.completed`, seen
through ingest) it sends the engine SIGINT, the suite's "finalize and
exit". A live engine job may run 96 hours (batch: 12). A relaunch
replays every uploaded file into a fresh engine.

A run can be shared publicly (`POST /v1/runs/{id}/public`, the owner's
service key): the link is the dashboard URL with a share token in the
fragment (`#token=s1.<run>.<secret>`), which the page exchanges at
`POST /v1/session` like a host's run token, for a `public`-scope session
of seven days (a public viewer has no host to return to). Every request
of a public session checks, through a five-second cache, that sharing is
still on under the same link; public sessions may send `watch`/`unwatch`
unless the owner blocked starring, nothing else, and download only if
allowed. The run's viewer carries the link in `/api/state`'s `share`, so
the dashboard's QR code shows it; event streams are capped at 200 per
run.

Each stage has its own job identity on the run (`stages.<stage>`:
generation, job secret, active), so two stages' jobs can run at once; a
job's secret opens only its own stage's routes, and a finished job's
only its exit report.

### POD5 input: the dorado stage

A run created with `"input": "pod5"` is basecalled by the service before
the engine runs. Its spec carries a `basecall` object, defaults filled in
from the published protocol:

```json
{"model": "sup@v5.0.0", "min_length": 400, "max_length": 2000, "min_qscore": null}
```

`model` is a dorado model complex from `GET /v1/options` (`dorado_models`,
the ones the dorado image bakes); the length window keeps the full ITS
amplicon (100 to 700 for ITS2 alone); `min_qscore` is dorado's own floor,
off by default. On `complete` the run enters `basecalling`: the dorado
job (`specimux-cloud dorado`, `dorado basecaller <model> <file>
--emit-fastq --no-trim`) takes each POD5 file of the manifest, writes the
reads inside the window to `runs/<user>/<run>/fastq/<name>.fastq` and
reports it; when the job exits with every file present the engine
launches over those FASTQs. Progress (`basecalling`: files done, reads
called and kept) is on the run record and the console's run page.

### Hosts, keys and dashboard sessions

The run API knows hosts, not users: a host creates runs and vouches for
its users. Each host has labelled service keys (`<host>.<random>`, stored
hashed) and an authorize URL, and sees only its own runs. A browser gets
at a run's dashboard through its host: the page, served by the run API,
sends the browser to the host's authorize URL, the host checks its own
session and asks the run API for a run token (`POST
/v1/runs/{id}/tokens`, service key), and redirects back with the token in
the URL fragment; the page exchanges it at `POST /v1/session` for a
cookie scoped to the run. The console is a host whose authorize route
works the same way.

### How the engine talks to the run API

Events go out through the suite's HTTP event forwarder to
`POST /v1/runs/{id}/ingest`, batched, in order, with the job secret and
the run's generation; the log in the work dir is the buffer. Commands
come in by long-polling `GET /v1/runs/{id}/commands/next`: the run API
fronts the queue backend (memory locally, SQS in AWS), the plugin applies
each command through the suite's commands facade with the id the run API
assigned, acknowledges it, and the outcome event travels back through
ingest. Fronting the queue with the run API keeps one engine code path
over both backends and one credential (the job secret) per job.

The engine works on the job's local disk (`SPECIMUX_SCRATCH`) and
mirrors what the dashboard reads (the event log, consensus and summary
FASTAs, photos) into the run's EFS directory with the suite's
`--mirror-dir`; after the engine exits the wrapper builds the three
downloads locally and uploads them (`POST /v1/runs/{id}/package-uploads`).
EFS made the demux about 200 times slower; see docs/DESIGN.md, "Engine
storage". On SIGTERM (a cancel, a timeout) both wrappers stop their child
and report exit 143 within Batch's 30-second window.

## License

specimux-cloud's own code is under the BSD 3-Clause License (`LICENSE`).
That license covers this repository only, not the software the Docker
images download when you build them:

- **Dorado** and its basecalling models (`docker/dorado.Dockerfile`) come
  from Oxford Nanopore Technologies under their own licence, the Oxford
  Nanopore Technologies PLC. Public License (Version 1.0 at dorado 2.1.2),
  which limits use to research purposes and sets conditions on
  redistribution. This repository does not include or redistribute Dorado;
  whoever builds and runs the dorado image accepts Oxford Nanopore's terms,
  and should read them (the `LICENCE.txt` in the dorado release) before
  distributing the image or offering basecalling to others.
- **The engine image** (`docker/engine.Dockerfile`) installs specimux-suite
  and its tools (specimux, speconsense) from PyPI and spoa, MCL and vsearch
  from Bioconda, each under its own license.
