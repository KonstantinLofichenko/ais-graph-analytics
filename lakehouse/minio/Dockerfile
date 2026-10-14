# Upstream community images were removed; build pinned official source instead.
FROM golang:1.24.8-bookworm AS build
ENV CGO_ENABLED=0
WORKDIR /src
RUN curl -fsSL https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-10-15T17-29-55Z | tar -xz --strip-components=1 \
    && go build -trimpath -o /out/minio .
WORKDIR /src-mc
RUN curl -fsSL https://codeload.github.com/minio/mc/tar.gz/refs/tags/RELEASE.2025-08-13T08-35-41Z | tar -xz --strip-components=1 \
    && go build -trimpath -o /out/mc .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/minio /out/mc /usr/local/bin/
COPY --from=build /src/LICENSE /usr/share/doc/minio/LICENSE
COPY --from=build /src-mc/LICENSE /usr/share/doc/mc/LICENSE
ENTRYPOINT ["minio"]
