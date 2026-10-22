# Latest stable PostgreSQL / pgvector verified on 2026-10-22.
ARG POSTGRES_IMAGE=ghcr.io/cloudnative-pg/postgresql:18.6-system-trixie@sha256:f7e00138159bb45d70b624ba25e446924cb3ed1bf366d13e936c3f81b914064e
FROM ${POSTGRES_IMAGE} AS builder
USER root
ARG PGVECTOR_VERSION=0.8.7
ARG PGVECTOR_COMMIT=f37c13f68b57d2c3472b2214fbcff699d6d34876
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git ca-certificates postgresql-server-dev-18 \
    && git clone --depth 1 --branch v${PGVECTOR_VERSION} https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector && test "$(git rev-parse HEAD)" = "${PGVECTOR_COMMIT}" \
    && make OPTFLAGS="" PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config \
    && make install PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
FROM ${POSTGRES_IMAGE}
COPY --from=builder /usr/lib/postgresql/18/lib/vector.so /usr/lib/postgresql/18/lib/vector.so
COPY --from=builder /usr/share/postgresql/18/extension/vector* /usr/share/postgresql/18/extension/
USER 26
