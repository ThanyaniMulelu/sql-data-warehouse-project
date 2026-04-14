#!/usr/bin/env bash
# =============================================================================
# Start the Data Warehouse Airflow Stack
# =============================================================================
# Usage:  ./start.sh
# =============================================================================
set -euo pipefail

echo "============================================="
echo " Data Warehouse — Airflow + Docker Stack"
echo "============================================="

# Ensure required directories exist
mkdir -p dags logs plugins

# Build and start all services
echo ""
echo ">> Building and starting containers..."
docker compose up -d --build

echo ""
echo ">> Waiting for services to become healthy..."
echo "   (this may take 30-60 seconds on first run)"
echo ""

# Wait for airflow-webserver health
MAX_WAIT=120
WAITED=0
until docker compose ps airflow-webserver 2>/dev/null | grep -q "healthy"; do
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "WARNING: Timed out waiting for webserver. Check: docker compose logs airflow-webserver"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    printf "   waiting... (%ds)\n" $WAITED
done

echo ""
echo "============================================="
echo " Stack is up!"
echo "============================================="
echo ""
echo " Airflow UI:   http://localhost:8080"
echo "   Username:   airflow"
echo "   Password:   airflow"
echo ""
echo " SQL Server:   localhost:1433"
echo "   Username:   sa"
echo "   Password:   (see .env file)"
echo ""
echo " Next steps:"
echo "   1. Open Airflow UI at http://localhost:8080"
echo "   2. Trigger the 'init_warehouse' DAG (one-time setup)"
echo "   3. Once init completes, trigger 'daily_etl' to test"
echo "   4. The daily_etl DAG will then run automatically each day"
echo ""
echo " To stop:  docker compose down"
echo " To stop + remove data:  docker compose down -v"
echo "============================================="
