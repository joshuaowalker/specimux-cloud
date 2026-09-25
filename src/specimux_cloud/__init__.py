"""specimux-cloud: the hosted form of specimux-suite.

Four parts, matching docs/DESIGN.md:

- ``runapi``: the always-on service the browser, the uploader and hosts
  (mycomap.org, the console) talk to — event store and fan-out, per-run
  dashboard, job control, uploads, results, run sessions.
- ``console``: a built-in host — login with a service key, job page,
  dashboard links, downloads; talks to the run API over HTTP only.
- ``engine``: what runs inside a compute job — a wrapper around
  ``specimux-suite`` plus the cloud plugin that forwards events and
  applies commands.
- ``backends``: storage, command queue, job launcher and control-plane
  store behind small interfaces, with local implementations (a
  directory, memory, subprocesses, SQLite) and AWS ones (S3, SQS,
  Batch, DynamoDB). The run API's logic is the same over both.
"""

__version__ = "0.1.2"
