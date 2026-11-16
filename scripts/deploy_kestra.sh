#!/usr/bin/env bash
# Publica en Kestra los namespace files (proyecto dbt, job Spark) y los flujos.
# Mismo patrón que subir_a_kestra.sh del laboratorio (Semana 4): si el flow existe, lo actualiza.
# Uso, desde la raíz de pset_2/:  bash scripts/deploy_kestra.sh
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && { set -a; source .env; set +a; }
KESTRA=${KESTRA:-http://localhost:8080/api/v1/main}
USUARIO="${KESTRA_USER:-admin@kestra.io}:${KESTRA_PASSWORD:-Admin1234}"
NS="usfq.pset2"

upload() {  # $1 = ruta local, $2 = ruta en el namespace
  curl -sf -u "$USUARIO" -X POST "$KESTRA/namespaces/$NS/files?path=/$2" -F "fileContent=@$1" > /dev/null
  echo "  namespace file: $2"
}

echo "Subiendo namespace files..."
upload kestra/scripts/ingest_uci.py scripts/ingest_uci.py
upload kestra/scripts/ingest_uci.py kestra/scripts/ingest_uci.py
upload spark/build_obt.py spark/build_obt.py
find dbt -type f \( -name '*.sql' -o -name '*.yml' \) -not -path 'dbt/target/*' -not -path 'dbt/logs/*' |
  while read -r f; do upload "$f" "$f"; done

echo "Publicando flujos..."
for flow in kestra/flows/*.yml; do
  id=$(grep -E '^id:' "$flow" | awk '{print $2}')
  if curl -sf -u "$USUARIO" "$KESTRA/flows/$NS/$id" > /dev/null; then
    curl -sf -u "$USUARIO" -X PUT -H "Content-Type: application/x-yaml" --data-binary @"$flow" "$KESTRA/flows/$NS/$id" > /dev/null
    echo "  flow $NS.$id actualizado"
  else
    curl -sf -u "$USUARIO" -X POST -H "Content-Type: application/x-yaml" --data-binary @"$flow" "$KESTRA/flows" > /dev/null
    echo "  flow $NS.$id creado"
  fi
done
echo "Listo. UI: http://localhost:8080 (en Codespaces: pestaña Ports, puerto 8080)"
