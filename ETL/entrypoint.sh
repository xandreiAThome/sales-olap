#!/bin/bash
# ETL Entrypoint Script
# Installs dependencies, runs migrations, then starts the ETL pipeline

set -e

echo "============================================"
echo "ETL Pipeline Startup"
echo "============================================"

# Ensure we're in the correct directory
cd /app

# Wait a bit for databases to be fully ready
echo "Waiting for databases to be ready..."
sleep 8

# Debug: Check if alembic.ini exists
if [ ! -f "alembic.ini" ]; then
    echo "❌ ERROR: alembic.ini not found in /app"
    ls -la
    exit 1
fi

# Run Alembic migrations
echo "Running database migrations..."
echo "Current migration status:"
alembic -c alembic.ini current

echo ""
echo "Applying migrations..."
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
