#!/bin/bash

# ============================
# Block Data Scanner Start Script
# ============================
# This script starts the rs-block-data-scanner service in the background
# and outputs the process ID to a PID file for management.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="${SCRIPT_DIR}/target/release/rs-block-data-scanner"
CONFIG_FILE="${SCRIPT_DIR}/config.bsc.yaml"
LOG_FILE="${SCRIPT_DIR}/app.log"
PID_FILE="${SCRIPT_DIR}/scanner.pid"

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

# Check if binary exists
if [[ ! -f "$BINARY_PATH" ]]; then
    error "Binary not found at: $BINARY_PATH"
    error "Please run rebuild.sh first to build the application"
    exit 1
fi

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    error "Config file not found: $CONFIG_FILE"
    error "Please ensure the config file exists"
    exit 1
fi

# Check if service is already running
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        warning "Service is already running with PID: $PID"
        warning "Use stop.sh to stop the service first"
        exit 1
    else
        warning "Stale PID file found, removing..."
        rm -f "$PID_FILE"
    fi
fi

# Make binary executable
chmod +x "$BINARY_PATH"

# Start the service
log "Starting rs-block-data-scanner service..."
log "Config: $CONFIG_FILE"
log "Log file: $LOG_FILE"
log "PID file: $PID_FILE"

# Start service in background and capture PID
nohup "$BINARY_PATH" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
SERVICE_PID=$!

# Save PID to file
echo "$SERVICE_PID" > "$PID_FILE"

# Wait a moment to check if the process started successfully
sleep 2

# Verify the process is still running
if kill -0 "$SERVICE_PID" 2>/dev/null; then
    success "Service started successfully!"
    success "PID: $SERVICE_PID"
    success "Log file: $LOG_FILE"
    success "PID file: $PID_FILE"
    
    # Show recent log entries
    log "Recent log entries:"
    if [[ -f "$LOG_FILE" ]]; then
        tail -n 5 "$LOG_FILE" | sed 's/^/  /'
    fi
    
    log "Use 'tail -f $LOG_FILE' to monitor logs"
    log "Use './stop.sh' to stop the service"
else
    error "Failed to start service"
    error "Check the log file for details: $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
