# The run API image. Needs the suite (viewer app factory, pages) but none
# of the bioinformatics tools.
FROM python:3.12-slim
ARG SUITE_SPEC="specimux-suite==0.3.6"
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "${SUITE_SPEC}"
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN pip install --no-cache-dir "/src[aws]"
RUN mkdir -p /mnt/runs /data
EXPOSE 8090
CMD ["specimux-cloud", "runapi", "--backend", "aws", "--host", "0.0.0.0", "--port", "8090"]
