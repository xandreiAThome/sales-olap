#!/bin/bash
# ETL Docker Management Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Function to wait for databases to be healthy
wait_for_databases() {
    log_info "Waiting for databases to be healthy..."
    
    timeout=60
    elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        src_health=$(docker inspect --format='{{.State.Health.Status}}' mysql_src_db 2>/dev/null || echo "starting")
        wh_health=$(docker inspect --format='{{.State.Health.Status}}' mysql_warehouse_db 2>/dev/null || echo "starting")
        
        if [ "$src_health" = "healthy" ] && [ "$wh_health" = "healthy" ]; then
            log_info "Both databases are healthy!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    log_error "Databases did not become healthy in time."
    return 1
}

# Command functions
cmd_start() {
    log_info "Starting MySQL databases..."
    docker-compose up -d mysql_src_db mysql_warehouse_db
    wait_for_databases
}

cmd_stop() {
    log_info "Stopping all services..."
    docker-compose down
}

cmd_clean() {
    log_warn "This will remove all containers and volumes (data will be lost)!"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        log_info "Cleaning up..."
        docker-compose down -v
        log_info "Cleanup complete."
    else
        log_info "Cleanup cancelled."
    fi
}

cmd_build() {
    log_info "Building ETL image..."
    docker-compose build etl
}

cmd_run_etl() {
    log_info "Running ETL pipeline..."
    docker-compose up etl
}

cmd_logs() {
    service=${1:-etl}
    log_info "Showing logs for $service..."
    docker-compose logs -f "$service"
}

cmd_shell() {
    log_info "Opening shell in ETL container..."
    docker-compose run --rm etl bash
}

cmd_db_source() {
    log_info "Connecting to source database..."
    docker exec -it mysql_src_db mysql -u devuser1 -pdevpass1 db_src
}

cmd_db_warehouse() {
    log_info "Connecting to warehouse database..."
    docker exec -it mysql_warehouse_db mysql -u devuser2 -pdevpass2 db_warehouse
}

cmd_migrate() {
    log_info "Running Alembic migrations..."
    docker-compose run --rm etl alembic upgrade head
}

cmd_status() {
    log_info "Service status:"
    docker-compose ps
    echo ""
    log_info "Volume usage:"
    docker volume ls | grep etl || echo "No volumes found"
}

cmd_help() {
    cat << EOF
ETL Docker Management Script

Usage: ./etl.sh [command]

Commands:
    start           Start the MySQL databases
    stop            Stop all services
    clean           Stop services and remove volumes (data loss!)
    build           Build the ETL Docker image
    run             Run the ETL pipeline
    logs [service]  Show logs (default: etl)
    shell           Open bash shell in ETL container
    db-src          Connect to source database
    db-wh           Connect to warehouse database
    migrate         Run Alembic migrations
    status          Show service and volume status
    help            Show this help message

Examples:
    ./etl.sh start              # Start databases
    ./etl.sh run                # Run ETL pipeline
    ./etl.sh logs mysql_src_db  # View source DB logs
    ./etl.sh db-wh              # Connect to warehouse DB
    ./etl.sh migrate            # Run database migrations

EOF
}

# Main script
main() {
    check_docker
    
    case "${1:-help}" in
        start)
            cmd_start
            ;;
        stop)
            cmd_stop
            ;;
        clean)
            cmd_clean
            ;;
        build)
            cmd_build
            ;;
        run)
            cmd_run_etl
            ;;
        logs)
            cmd_logs "$2"
            ;;
        shell)
            cmd_shell
            ;;
        db-src)
            cmd_db_source
            ;;
        db-wh)
            cmd_db_warehouse
            ;;
        migrate)
            cmd_migrate
            ;;
        status)
            cmd_status
            ;;
        help|--help|-h)
            cmd_help
            ;;
        *)
            log_error "Unknown command: $1"
            cmd_help
            exit 1
            ;;
    esac
}

main "$@"
