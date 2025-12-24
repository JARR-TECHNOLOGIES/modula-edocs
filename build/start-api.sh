#!/usr/bin/env sh
set -e

REQUIRED_ENVS="
FILES_API_KEY
FILES_API_SECRET
"

fail_envs() {
  echo "Missing required environment variables: $1" >&2
  kill -TERM 1 2>/dev/null || true
  exit 1
}

fail_invalid() {
  echo "Invalid environment variable: $1 ($2)" >&2
  kill -TERM 1 2>/dev/null || true
  exit 1
}

missing=""
for var in $REQUIRED_ENVS; do
  if [ -z "$(eval echo "\$$var")" ]; then
    if [ -z "$missing" ]; then
      missing="$var"
    else
      missing="$missing, $var"
    fi
  fi
done

[ -n "$missing" ] && fail_envs "$missing"

# Defaults for Gunicorn tunables
WORKERS="${GUNICORN_WORKERS:-1}"
THREADS="${GUNICORN_THREADS:-6}"
TIMEOUT="${GUNICORN_TIMEOUT:-120}"
KEEPALIVE="${GUNICORN_KEEPALIVE:-5}"
MAX_REQ="${GUNICORN_MAX_REQUESTS:-1000}"
MAX_REQ_JITTER="${GUNICORN_MAX_REQUESTS_JITTER:-200}"
EXTRA_ARGS="${GUNICORN_EXTRA_ARGS:-}"
HOST="${API_HOST:-0.0.0.0}"
PORT="${API_PORT:-8000}"

cd /app/api
exec gunicorn app:app \
  -b "${HOST}:${PORT}" \
  --workers="${WORKERS}" \
  --threads="${THREADS}" \
  --timeout="${TIMEOUT}" \
  --keep-alive="${KEEPALIVE}" \
  --max-requests="${MAX_REQ}" \
  --max-requests-jitter="${MAX_REQ_JITTER}" \
  --worker-class=gthread \
  ${EXTRA_ARGS} \
  --access-logfile - \
  --error-logfile -
