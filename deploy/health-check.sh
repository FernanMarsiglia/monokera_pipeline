#!/bin/bash

###############################################################################
# Health check script - Validate pipeline is running correctly
# Usage: bash deploy/health-check.sh
###############################################################################

set -e

echo "🏥 Running health checks for monokera-pipeline..."
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

compose_cmd() {
    if docker compose version &> /dev/null; then
        echo "docker compose"
    elif command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    else
        echo ""
    fi
}

COMPOSE_CMD=$(compose_cmd)

check_docker() {
    echo -n "Checking Docker daemon... "
    if docker ps &> /dev/null; then
        echo -e "${GREEN}✅${NC}"
    else
        echo -e "${RED}❌${NC}"
        return 1
    fi
}

check_containers() {
    echo -n "Checking Docker Compose containers... "
    if [ -z "$COMPOSE_CMD" ]; then
        echo -e "${RED}❌${NC} (docker compose not installed)"
        return 1
    fi
    local count=$($COMPOSE_CMD ps --services --filter "status=running" | wc -l)
    local expected=3  # airflow-scheduler, webserver, postgres
    
    if [ "$count" -ge "$expected" ]; then
        echo -e "${GREEN}✅${NC} ($count running)"
    else
        echo -e "${RED}❌${NC} (only $count running, expected $expected)"
        echo "Container status:"
        $COMPOSE_CMD ps
        return 1
    fi
}

check_airflow() {
    echo -n "Checking Airflow... "
    if [ -z "$COMPOSE_CMD" ]; then
        echo -e "${RED}❌${NC} (docker compose not installed)"
        return 1
    fi
    if $COMPOSE_CMD exec -T airflow-scheduler airflow version &> /dev/null; then
        echo -e "${GREEN}✅${NC}"
    else
        echo -e "${RED}❌${NC}"
        return 1
    fi
}

check_dag() {
    echo -n "Checking DAG 'spacenews_pipeline_local'... "
    if [ -z "$COMPOSE_CMD" ]; then
        echo -e "${RED}❌${NC} (docker compose not installed)"
        return 1
    fi
    if $COMPOSE_CMD exec -T airflow-scheduler airflow dags list | grep -q "spacenews_pipeline_local"; then
        echo -e "${GREEN}✅${NC}"
    else
        echo -e "${RED}❌${NC}"
        return 1
    fi
}

check_s3() {
    echo -n "Checking S3 access... "
    if [ -z "$COMPOSE_CMD" ]; then
        echo -e "${RED}❌${NC} (docker compose not installed)"
        return 1
    fi
    if $COMPOSE_CMD exec -T airflow-scheduler python3 << 'EOF' &> /dev/null
import boto3
s3 = boto3.client('s3', region_name='us-east-1')
s3.head_bucket(Bucket='monokera-bucket')
print("S3 accessible")
EOF
    then
        echo -e "${GREEN}✅${NC}"
    else
        echo -e "${YELLOW}⚠️${NC} (check AWS credentials in .env)"
        return 1
    fi
}

check_disk() {
    echo -n "Checking disk space... "
    local usage=$(df / | awk 'NR==2 {print int($5)}')
    
    if [ "$usage" -lt 80 ]; then
        echo -e "${GREEN}✅${NC} (${usage}% used)"
    elif [ "$usage" -lt 90 ]; then
        echo -e "${YELLOW}⚠️${NC} (${usage}% used, monitor)"
        return 1
    else
        echo -e "${RED}❌${NC} (${usage}% used - CRITICAL)"
        return 1
    fi
}

check_memory() {
    echo -n "Checking memory... "
    if command -v free &> /dev/null; then
        local usage=$(free | awk 'NR==2 {printf "%.0f", $3/$2 * 100}')
        
        if [ "$usage" -lt 80 ]; then
            echo -e "${GREEN}✅${NC} (${usage}% used)"
        else
            echo -e "${YELLOW}⚠️${NC} (${usage}% used)"
            return 1
        fi
    else
        echo -e "${YELLOW}⚠️${NC} (could not check)"
    fi
}

check_git() {
    echo -n "Checking Git status... "
    local branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    local status=$(git status --short 2>/dev/null | wc -l)
    
    if [ "$status" -eq 0 ]; then
        echo -e "${GREEN}✅${NC} (branch: $branch, clean)"
    else
        echo -e "${YELLOW}⚠️${NC} (branch: $branch, $status changes)"
    fi
}

# Run all checks
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SYSTEM CHECKS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
check_docker || true
check_disk || true
check_memory || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "DOCKER & SERVICES"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
check_containers || true
check_airflow || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PIPELINE & DATA"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
check_dag || true
check_s3 || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "REPOSITORY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
check_git || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Health check complete!"
echo ""
echo "🔗 Access Airflow: http://$(hostname -I | awk '{print $1}'):8080"
echo "📊 Monitor: docker compose logs -f airflow-scheduler"
echo "📝 DAG Status: docker compose exec -T airflow-scheduler airflow dags list"
