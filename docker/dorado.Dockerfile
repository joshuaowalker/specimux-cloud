# The dorado image: ONT's dorado tarball (it bundles its CUDA runtime; the
# NVIDIA driver comes from the Batch GPU host), the basecalling models the
# service offers, and the dorado wrapper (dorado/wrapper.py, standard
# library only). One image serves every POD5 run; the run API passes the
# run identity in the environment.
#
# Build from the repository root:
#   docker build -f docker/dorado.Dockerfile -t specimux-cloud/dorado .
# DORADO_MODELS lists the models to bake (full names; the CDN serves the
# ones in dorado's models list, e.g. there is no sup@v6.0.0); the run API
# offers the matching model complexes (SPECIMUX_DORADO_MODELS in
# infra/stack.py), so the two lists change together.
FROM ubuntu:24.04
ARG DORADO_VERSION=2.1.2
ARG DORADO_MODELS="dna_r10.4.1_e8.2_400bps_sup@v5.0.0 dna_r10.4.1_e8.2_400bps_sup@v5.2.0 dna_r10.4.1_e8.2_400bps_hac@v6.0.0"
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL "https://cdn.oxfordnanoportal.com/software/analysis/dorado-${DORADO_VERSION}-linux-x64.tar.gz" \
        | tar -xz -C /opt \
    && ln -s "/opt/dorado-${DORADO_VERSION}-linux-x64" /opt/dorado
ENV PATH=/opt/dorado/bin:$PATH
# The models, fetched from the same CDN `dorado download` uses, into the
# directory dorado searches when given a model complex such as sup@v5.0.0
ENV DORADO_MODELS_DIRECTORY=/opt/models
RUN mkdir -p /opt/models && for m in ${DORADO_MODELS}; do \
        curl -fsSL "https://cdn.oxfordnanoportal.com/software/analysis/dorado/${m}.zip" -o "/tmp/${m}.zip" \
        && python3 -c "import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "/tmp/${m}.zip" "/opt/models/${m}" \
        && rm "/tmp/${m}.zip" \
        && if [ -f "/opt/models/${m}/${m}/config.toml" ]; then mv "/opt/models/${m}/${m}"/* "/opt/models/${m}/" && rmdir "/opt/models/${m}/${m}"; fi \
        && test -f "/opt/models/${m}/config.toml" || exit 1; \
    done && du -sh /opt/models/*
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir --no-deps /src
ENV PATH=/opt/venv/bin:$PATH
ENV SPECIMUX_SCRATCH=/scratch SPECIMUX_DORADO_DEVICE=cuda:all
RUN mkdir -p /scratch
WORKDIR /scratch
ENTRYPOINT ["python3", "-m", "specimux_cloud.dorado.wrapper"]
