#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BINARY_PATH="${PROJECT_ROOT}/target/release/api_main"
CONFIG_FILE="${PROJECT_ROOT}/config.yaml"
LOG_FILE="${PROJECT_ROOT}/logs/api.log"
PID_FILE="${PROJECT_ROOT}/api.pid"

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

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

if [[ ! -f "$BINARY_PATH" ]]; then
    error "API binary not found at: $BINARY_PATH"
    error "Please build the project (e.g. cargo build --release)"
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    error "Config file not found: $CONFIG_FILE"
    exit 1
fi

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        warning "API service already running with PID: $PID"
        exit 0
    else
        warning "Removing stale API PID file"
        rm -f "$PID_FILE"
    fi
fi

mkdir -p "$(dirname "$LOG_FILE")"
chmod +x "$BINARY_PATH"

log "Starting API service..."
log "Config: $CONFIG_FILE"
log "Log file: $LOG_FILE"

nohup "$BINARY_PATH" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
SERVICE_PID=$!

echo "$SERVICE_PID" > "$PID_FILE"
sleep 2

if kill -0 "$SERVICE_PID" 2>/dev/null; then
    success "API service started (PID: $SERVICE_PID)"
    log "Tail logs with: tail -f $LOG_FILE"
else
    error "Failed to start API service, see log: $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
