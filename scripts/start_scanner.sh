#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
BINARY_PATH="${PROJECT_ROOT}/target/release/rs-block-data-scanner"
CONFIG_FILE="${PROJECT_ROOT}/config.yaml"
LOG_FILE="${PROJECT_ROOT}/logs/scanner.log"
PID_FILE="${PROJECT_ROOT}/scanner.pid"

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
    error "Binary not found at: $BINARY_PATH"
    error "Please run 'cargo build --release' first"
    exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    error "Config file not found: $CONFIG_FILE"
    exit 1
fi

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        warning "Scanner service already running with PID: $PID"
        exit 0
    else
        warning "Removing stale scanner PID file"
        rm -f "$PID_FILE"
    fi
fi

mkdir -p "$(dirname "$LOG_FILE")"
chmod +x "$BINARY_PATH"

log "Starting block scanner..."
nohup "$BINARY_PATH" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
SERVICE_PID=$!

echo "$SERVICE_PID" > "$PID_FILE"
sleep 2

if kill -0 "$SERVICE_PID" 2>/dev/null; then
    success "Scanner started (PID: $SERVICE_PID)"
    log "Logs: $LOG_FILE"
else
    error "Failed to start scanner, see log: $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
