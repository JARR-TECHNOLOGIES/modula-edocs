#!/usr/bin/env bash
set -euo pipefail

# Simulate Cloud Run Job execution locally for a given customer_id.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-modula-edocs-job:local}"
CUSTOMER_ID="${1:-}"

if [[ -z "${CUSTOMER_ID}" ]]; then
  echo "Usage: CUSTOMER_ID is required" >&2
  echo "Example: ./run.sh stg-modula-12345" >&2
  exit 1
fi

required_envs=(MONGO_USERNAME MONGO_PASSWORD MONGO_CLUSTER)
missing=()
for var in "${required_envs[@]}"; do
  [[ -n "${!var:-}" ]] || missing+=("${var}")
done

if (( ${#missing[@]} )); then
  echo "Missing required environment variables: ${missing[*]}" >&2
  exit 1
fi

# Host dirs to mirror Cloud Run volumes
BUCKET_HOST_DIR="${BUCKET_HOST_DIR:-${REPO_ROOT}/.local/gcp-bucket}"
STAGE_HOST_DIR="${STAGE_HOST_DIR:-${REPO_ROOT}/.local/staged-files}"
mkdir -p "${BUCKET_HOST_DIR}" "${STAGE_HOST_DIR}"

echo "Building image '${IMAGE_NAME}'..."
docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"

echo "Running job for customer_id='${CUSTOMER_ID}'..."
docker run --rm \
  -e MONGO_USERNAME \
  -e MONGO_PASSWORD \
  -e MONGO_CLUSTER \
  -e LOG_LEVEL="${LOG_LEVEL:-DEBUG}" \
  -e TZ="${TZ:-America/Costa_Rica}" \
  -e STAGE_FILES_ROOT="/tmp/staged-files" \
  -e FILES_ROOT="/gcp-bucket" \
  -e TAR_TMP_ROOT="/tmp/tar-tmp" \
  -v "${BUCKET_HOST_DIR}:/gcp-bucket" \
  -v "${STAGE_HOST_DIR}:/tmp/staged-files" \
  "${IMAGE_NAME}" \
  --customer-id "${CUSTOMER_ID}"
