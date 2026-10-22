#!/usr/bin/env bash
set -euo pipefail
export AWS_DEFAULT_REGION=ap-south-1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
TAG=${TAG:-eks-20261022.2}
aws ecr get-login-password | docker login --username AWS --password-stdin "$ECR_REGISTRY"
docker buildx build --platform linux/amd64 --pull --push \
  -t "$ECR_REGISTRY/pgvector-investigation/postgresql:18.6-pgvector0.8.7" \
  -f infrastructure/cnpg-pgvector/Dockerfile .
docker buildx build --platform linux/amd64 --pull --push \
  -t "$ECR_REGISTRY/pgvector-investigation/benchmark-engine:$TAG" \
  -f infrastructure/docker/Dockerfile .
