#!/bin/sh
set -e

DATA_DIR="${DATA_DIR:-/app}"
DB_PATH="$DATA_DIR/sales.db"
DB_URL="https://github.com/ryu-haru/sales-crm/releases/download/v1.0/sales.db.gz"

if [ ! -f "$DB_PATH" ]; then
  echo "[startup] Downloading database..."
  mkdir -p "$DATA_DIR"
  curl -L --progress-bar "$DB_URL" | gunzip > "$DB_PATH"
  echo "[startup] Database ready: $(du -h "$DB_PATH" | cut -f1)"
else
  echo "[startup] Database already exists, skipping download."
fi

exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
