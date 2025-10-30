#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
PID_FILE="${PROJECT_ROOT}/api.pid"
LOG_FILE="${PROJECT_ROOT}/logs/api.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

if [[ ! -f "$PID_FILE" ]]; then
    warning "API PID file not found, nothing to stop"
    exit 0
fi

PID=$(cat "$PID_FILE")
if ! kill -0 "$PID" 2>/dev/null; then
    warning "API process $PID not running, removing stale PID file"
    rm -f "$PID_FILE"
    exit 0
fi

log "Stopping API service (PID: $PID)..."
kill -TERM "$PID"

for ((i=1; i<=30; i++)); do
    if ! kill -0 "$PID" 2>/dev/null; then
        success "API service stopped"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
    log "Waiting for graceful shutdown ($i/30)"
done

warning "API service did not stop gracefully, sending SIGKILL"
kill -KILL "$PID" 2>/dev/null || true
sleep 2

if kill -0 "$PID" 2>/dev/null; then
    error "Unable to kill API process"
    exit 1
fi

rm -f "$PID_FILE"
success "API service force-stopped"

if [[ -f "$LOG_FILE" ]]; then
    log "Recent API log entries:"
    tail -n 10 "$LOG_FILE"
fi
