#!/bin/bash
# ETL Entrypoint Script
# Runs migrations first, then starts the ETL pipeline

set -e

echo "============================================"
echo "ETL Pipeline Startup"
echo "============================================"

# Wait a bit for databases to be fully ready
echo "Waiting for databases to be ready..."
sleep 5

# Run Alembic migrations
echo "Running database migrations..."
if alembic -c alembic.ini upgrade head; then
    echo "✅ Migrations completed successfully"
else
    echo "❌ Migration failed!"
    exit 1
fi

echo ""
echo "Starting ETL pipeline..."
echo "============================================"

# Run the main ETL application
exec python app.py
