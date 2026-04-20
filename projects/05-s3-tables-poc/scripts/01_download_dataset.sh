#!/usr/bin/env bash
# ============================================================
# Download NYC Taxi Yellow Trip Data (10 meses)
# ============================================================
set -euo pipefail

DATA_DIR="./data"
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR="2024"
MONTHS=$(seq -w 1 10)

mkdir -p "$DATA_DIR"

echo "📥 Baixando NYC Taxi Yellow Trip Data — ${YEAR}"
echo "================================================"

for MONTH in $MONTHS; do
    FILE="yellow_tripdata_${YEAR}-${MONTH}.parquet"
    URL="${BASE_URL}/${FILE}"
    DEST="${DATA_DIR}/${FILE}"

    if [ -f "$DEST" ]; then
        echo "  ✅ ${FILE} já existe, pulando..."
        continue
    fi

    echo "  ⬇️  Baixando ${FILE}..."
    curl -sL -o "$DEST" "$URL"
    SIZE=$(du -h "$DEST" | cut -f1)
    echo "     → ${SIZE}"
done

echo ""
echo "✅ Download completo. Arquivos em ${DATA_DIR}/"
ls -lh "$DATA_DIR"/*.parquet
