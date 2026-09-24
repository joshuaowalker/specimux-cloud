# The engine image: specimux-suite with its tools, plus the cloud wrapper
# and plugin. One image serves every run; the run API passes the run
# identity in the environment (see engine/wrapper.py).
#
# Build from the repository root:
#   docker build -f docker/engine.Dockerfile -t specimux-cloud/engine .
# SUITE_SPEC pins the specimux-suite version: a PyPI requirement, or a
# git URL (git+https://github.com/joshuaowalker/specimux-suite.git@<ref>)
# to test an unreleased branch.
FROM mambaorg/micromamba:2.0-ubuntu22.04
ARG SUITE_SPEC="specimux-suite==0.3.5"
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# The bioinformatics binaries the suite invokes: spoa and mcl for
# speconsense, vsearch for identification and speconsense's scalability mode
RUN micromamba install -y -n base -c conda-forge -c bioconda \
        python=3.12 pip spoa mcl vsearch \
    && micromamba clean -a -y
ENV PATH=/opt/conda/bin:$PATH
RUN pip install --no-cache-dir "${SUITE_SPEC}"
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN pip install --no-cache-dir --no-deps /src && pip install --no-cache-dir httpx cryptography python-multipart
# EFS is mounted here by the job definition; the run API passes
# SPECIMUX_WORK_DIR=/mnt/runs/<run id>
RUN mkdir -p /mnt/runs
WORKDIR /mnt/runs
ENTRYPOINT ["specimux-cloud", "engine"]
