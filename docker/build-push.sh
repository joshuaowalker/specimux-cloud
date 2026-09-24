#!/usr/bin/env bash
# Build the images for linux/amd64 (the Batch and Fargate hosts) and push
# them to the stack's ECR repositories, tagged :latest and with the git sha.
#
#   AWS_PROFILE=specimux-cloud docker/build-push.sh [engine|runapi|dorado|all]
#
# `all` is the engine and the run API; the dorado image (large: the dorado
# tarball plus its models) is built only when named.
#
# Reads the repository URIs from infra/cdk-outputs.json (written by
# `cdk deploy --outputs-file`). SUITE_SPEC pins the specimux-suite version
# (default: the Dockerfiles' PyPI pin); a git ref such as `cloud` installs
# that branch instead, for testing unreleased suite changes.
set -euo pipefail
cd "$(dirname "$0")/.."
REGION=${AWS_DEFAULT_REGION:-us-west-2}
OUTPUTS=infra/cdk-outputs.json
SUITE_SPEC=${SUITE_SPEC:-}
if [[ -n ${SUITE_REF:-} ]]; then
  # A branch name resolves to its commit so the pip-install layer is
  # rebuilt when the branch moves (Docker caches by the literal argument)
  resolved=$(git ls-remote https://github.com/joshuaowalker/specimux-suite.git "refs/heads/$SUITE_REF" "refs/tags/$SUITE_REF" | head -1 | cut -f1)
  SUITE_SPEC="git+https://github.com/joshuaowalker/specimux-suite.git@${resolved:-$SUITE_REF}"
fi
SHA=$(git rev-parse --short HEAD)
which=${1:-all}

repo_uri() { python3 -c "import json,sys; print(json.load(open('$OUTPUTS'))['specimux-cloud']['$1'])"; }
ENGINE_URI=$(repo_uri EngineRepoUri)
RUNAPI_URI=$(repo_uri RunApiRepoUri)
DORADO_URI=$(repo_uri DoradoRepoUri 2>/dev/null || true)
REGISTRY=${ENGINE_URI%%/*}

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
docker buildx inspect specimux >/dev/null 2>&1 || docker buildx create --name specimux --use >/dev/null
docker buildx use specimux

build() {  # name dockerfile uri
  echo "== building $1 (suite: ${SUITE_SPEC:-Dockerfile default} @ $SHA) for linux/amd64"
  docker buildx build --platform linux/amd64 -f "$2" ${SUITE_SPEC:+--build-arg SUITE_SPEC="$SUITE_SPEC"} \
    -t "$3:latest" -t "$3:$SHA" --push .
}
[[ $which == all || $which == engine ]] && build engine docker/engine.Dockerfile "$ENGINE_URI"
[[ $which == all || $which == runapi ]] && build runapi docker/runapi.Dockerfile "$RUNAPI_URI"
[[ $which == dorado ]] && build dorado docker/dorado.Dockerfile "${DORADO_URI:?deploy the stack first: no DoradoRepoUri in $OUTPUTS}"
echo "pushed: $which"
