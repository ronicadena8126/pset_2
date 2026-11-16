#!/usr/bin/env bash
# Kestra OSS lee secretos desde variables SECRET_<NOMBRE> codificadas en base64.
# Genera .env_encoded a partir de las variables SNOWFLAKE_* de .env.
set -euo pipefail
cd "$(dirname "$0")/.."
: > .env_encoded
grep -E '^SNOWFLAKE_[A-Z_]+=' .env | while IFS='=' read -r key value; do
  value="${value%%#*}"; value="$(echo -n "$value" | xargs)"
  echo "SECRET_${key}=$(echo -n "$value" | base64 | tr -d '\n')" >> .env_encoded
done
echo "Generado .env_encoded con $(wc -l < .env_encoded) secretos"
