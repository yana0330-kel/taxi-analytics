#!/usr/bin/env bash
set -Eeuo pipefail

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname Superset \
  --lastname Admin \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init
