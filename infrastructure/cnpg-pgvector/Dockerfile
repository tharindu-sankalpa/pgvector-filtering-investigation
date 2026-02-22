# PostgreSQL 18 with pgVector extension for CNPG
# Based on Nexus platform's PostgreSQL 18 TimescaleDB Dockerfile pattern

FROM ghcr.io/cloudnative-pg/postgresql:18.1-standard-trixie@sha256:7a72106d396a9c6d06ca25be191700c86031e0a06ccdeaf1a31da14f198cbbc4

USER root

# Install pgVector extension
RUN apt update \
      && apt install -y gnupg postgresql-common apt-transport-https lsb-release wget build-essential git \
      && /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
      && apt update \
      && apt install -y postgresql-18-pgvector \
      && apt-get autoremove -y \
      && rm -rf /var/lib/apt/lists/* /var/cache/* /var/log/*

# Switch back to CNPG user
USER 26
