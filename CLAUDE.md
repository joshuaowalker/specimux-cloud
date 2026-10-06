# CLAUDE.md

Guidance for Claude Code in this repository.

## Quick reference

- **Install:** `pip install -e '.[dev]'` (add `pip install -e ../specimux-suite` to work against an unreleased suite)
- **Test:** `pytest` (CI runs Python 3.11, 3.12 and 3.13; run `python3.11 -m py_compile` on changed files before pushing, since 3.11 rejects nested same-quote f-strings)
- **Local stack:** `specimux-cloud runapi --data-dir $PWD/data --port 8090` (console at `/console/`, host `dev`, key `dev.dev-service-key`)
- **Operating the AWS deployment** (release, images, deploy, roll, keys, smoke tests, logs, clean-up): [docs/OPS.md](docs/OPS.md)

## Where things are explained

- [README.md](README.md): users first (uploading, creating runs), then
  running a deployment, development, and "How it works".
- [docs/DESIGN.md](docs/DESIGN.md): the design and the reasons for it.
  Read it rather than re-deriving a decision ("Rejected alternatives" and
  "Open questions" are at the end).
- Package map: `runapi` (the always-on service), `console` (the built-in
  host), `engine` and `dorado` (inside the compute jobs), `fetch.py` and
  `drive.py` (Google Drive input), and `backends` (local and AWS
  implementations of storage, queue, launcher and store).

## Rules for this repository

- **The repository is public.** Never commit any of these:
  - account ids, resource names or hosted zone ids
    (`infra/cdk-outputs.json` and `infra/cdk.context.json` are ignored
    for that reason);
  - keys or secrets;
  - personal names or contact details. Use role labels such as
    `lab-staff`, `user-1` or "the operator" in docs, examples and tests.

  Local-only files carry what must stay private: `docs/reference/` and
  `docs/MYCOMAP_INTEGRATION.md` are excluded.
- **Keep `/v1` additive.** Uploaders in the field are older than the run
  API ("Contracts and versioning" in DESIGN.md).
- **Specimux-suite is a separate repository and release.** The images
  and `pyproject.toml` pin it from PyPI. A change the pipeline itself
  needs goes to the suite first.
- **The specimux-cloud release order** is in docs/OPS.md: PyPI before
  the run API roll.
- Comments and docs explain why, in plain words; match the surrounding
  code's density.
