#!/bin/bash

# ============================
# Block Data Scanner Stop Script
# ============================
# This script stops the rs-block-data-scanner service gracefully
# and verifies that it has been completely shut down.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/scanner.pid"
LOG_FILE="${SCRIPT_DIR}/app.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
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

# Check if PID file exists
if [[ ! -f "$PID_FILE" ]]; then
    warning "PID file not found: $PID_FILE"
    warning "Service may not be running"
    exit 0
fi

# Read PID from file
PID=$(cat "$PID_FILE")

# Check if process is running
if ! kill -0 "$PID" 2>/dev/null; then
    warning "Process with PID $PID is not running"
    warning "Removing stale PID file..."
    rm -f "$PID_FILE"
    exit 0
fi

log "Stopping rs-block-data-scanner service (PID: $PID)..."

# Send SIGTERM for graceful shutdown
log "Sending SIGTERM signal for graceful shutdown..."
kill -TERM "$PID"

# Wait for graceful shutdown (up to 30 seconds)
TIMEOUT=30
ELAPSED=0
INTERVAL=1

while [[ $ELAPSED -lt $TIMEOUT ]]; do
    if ! kill -0 "$PID" 2>/dev/null; then
        success "Service stopped gracefully"
        rm -f "$PID_FILE"
        exit 0
    fi
    
    log "Waiting for graceful shutdown... (${ELAPSED}s/${TIMEOUT}s)"
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

# If still running after timeout, force kill
warning "Service did not stop gracefully within ${TIMEOUT} seconds"
log "Sending SIGKILL signal to force stop..."
kill -KILL "$PID" 2>/dev/null || true

# Wait a moment and verify
sleep 2
if kill -0 "$PID" 2>/dev/null; then
    error "Failed to stop service even with SIGKILL"
    error "Manual intervention may be required"
    exit 1
else
    success "Service force-stopped successfully"
    rm -f "$PID_FILE"
fi

# Verify no related processes are still running
log "Verifying no related processes are running..."
REMAINING_PIDS=$(pgrep -f "rs-block-data-scanner" || true)

if [[ -n "$REMAINING_PIDS" ]]; then
    warning "Found remaining rs-block-data-scanner processes:"
    echo "$REMAINING_PIDS" | while read -r remaining_pid; do
        echo "  PID: $remaining_pid"
        # Show process info
        if command -v ps >/dev/null 2>&1; then
            ps -p "$remaining_pid" -o pid,ppid,cmd 2>/dev/null || true
        fi
    done
    warning "You may need to manually stop these processes"
else
    success "No remaining rs-block-data-scanner processes found"
fi

# Show final log entries
if [[ -f "$LOG_FILE" ]]; then
    log "Final log entries:"
    tail -n 10 "$LOG_FILE" | sed 's/^/  /'
fi

success "Service stop completed"
