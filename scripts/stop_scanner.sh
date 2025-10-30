#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
PID_FILE="${PROJECT_ROOT}/scanner.pid"
LOG_FILE="${PROJECT_ROOT}/logs/scanner.log"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

if [[ ! -f "$PID_FILE" ]]; then
    warning "Scanner PID file not found ($PID_FILE)"
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    warning "Scanner process $PID not running"
    rm -f "$PID_FILE"
    exit 0
fi

log "Stopping scanner (PID: $PID)..."
kill -TERM "$PID"

for ((i=1; i<=30; i++)); do
    if ! kill -0 "$PID" 2>/dev/null; then
        success "Scanner stopped"
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
    log "Waiting for graceful shutdown ($i/30)"
done

warning "Scanner did not stop gracefully, sending SIGKILL"
kill -KILL "$PID" 2>/dev/null || true
sleep 2

if kill -0 "$PID" 2>/dev/null; then
    error "Unable to kill scanner process"
    exit 1
fi

rm -f "$PID_FILE"
success "Scanner force-stopped"

if [[ -f "$LOG_FILE" ]]; then
    log "Recent scanner logs:"
    tail -n 10 "$LOG_FILE"
fi
