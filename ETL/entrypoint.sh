#!/bin/bash
# ETL Entrypoint Script
# Runs migrations first, then starts the ETL pipeline

set -e

echo "============================================"
echo "ETL Pipeline Startup"
echo "============================================"

# Ensure we're in the correct directory
cd /app

# Wait a bit for databases to be fully ready
echo "Waiting for databases to be ready..."
sleep 5

# Debug: Check if alembic.ini exists
if [ ! -f "alembic.ini" ]; then
    echo "❌ ERROR: alembic.ini not found in /app"
    ls -la
    exit 1
fi

# Run Alembic migrations
echo "Running database migrations..."
if alembic -c alembic.ini upgrade head; then
    echo "✅ Migrations completed successfully"
else
    echo "❌ Migration failed!"
    echo "Current directory: $(pwd)"
    echo "Files in directory:"
    ls -la
    exit 1
fi

echo ""
echo "Starting ETL pipeline..."
echo "============================================"

# Run the main ETL application
exec python app.py
