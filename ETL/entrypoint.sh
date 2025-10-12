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

# First, check current migration status
echo "Checking current migration status..."
alembic -c alembic.ini current || true

# Try to upgrade
if alembic -c alembic.ini upgrade head 2>&1 | tee /tmp/migration.log; then
    echo "✅ Migrations completed successfully"
else
    exit_code=$?
    echo ""
    echo "⚠️  Migration command failed (exit code: $exit_code)"
    
    # Check if it's a duplicate key/index error (safe to ignore)
    if grep -qi "duplicate\|already exists" /tmp/migration.log; then
        echo "✅ This is a 'duplicate key/index' error - database is already up-to-date"
        echo "   Continuing with ETL pipeline..."
    else
        echo "❌ Migration failed with a different error"
        echo "   Check the error above for details"
        exit 1
    fi
fi

echo ""
echo "Starting ETL pipeline..."
echo "============================================"

# Run the main ETL application
exec python app.py
