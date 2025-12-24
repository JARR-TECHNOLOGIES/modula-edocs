#!/usr/bin/env bash
set -euo pipefail

RED="\033[0;31m"
GREEN="\033[0;32m"
NC="\033[0m"

# Check required environment variables
REQUIRED_ENVS=(
  "FILES_API_KEY"
  "FILES_API_SECRET"
)

echo -e "${GREEN}[ENTRYPOINT] Validating required environment variables...${NC}"

# Check required environment variables
for VAR in "${REQUIRED_ENVS[@]}"; do
    VALUE="${!VAR:-}"

    if [[ -z "$VALUE" ]]; then
        echo -e "${RED}[ERROR] Environment variable '$VAR' is REQUIRED but not set.${NC}"
        exit 1
    fi
done

echo -e "${GREEN}[OK] All required variables are set.${NC}"

# Start supervisord
echo -e "${GREEN}[ENTRYPOINT] Starting supervisor...${NC}"

exec supervisord -n