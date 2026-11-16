#!/usr/bin/env bash
# Kestra OSS lee secretos desde variables SECRET_<NOMBRE> codificadas en base64.
# Genera .env_encoded a partir de .env (SNOWFLAKE_*) y agrega la llave privada key-pair.
set -euo pipefail
cd "$(dirname "$0")/.."
: > .env_encoded
grep -E '^SNOWFLAKE_[A-Z_]+=' .env | while IFS='=' read -r key value; do
  value="${value%%#*}"; value="$(echo -n "$value" | xargs)"
  echo "SECRET_${key}=$(echo -n "$value" | base64 | tr -d '\n')" >> .env_encoded
done
KEY_PATH="$(grep -E '^SNOWFLAKE_PRIVATE_KEY_PATH=' .env | cut -d= -f2- | xargs)"
KEY_PATH="${KEY_PATH/#\~/$HOME}"
if [ -n "$KEY_PATH" ] && [ -f "$KEY_PATH" ]; then
  echo "SECRET_SNOWFLAKE_PRIVATE_KEY=$(base64 < "$KEY_PATH" | tr -d '\n')" >> .env_encoded
fi
echo "Generado .env_encoded con $(wc -l < .env_encoded | xargs) secretos (no se versiona)"
