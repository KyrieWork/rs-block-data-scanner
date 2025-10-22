#!/bin/bash

# ============================
# Block Data Scanner Rebuild Script
# ============================
# This script pulls the latest code from git and rebuilds the application
# in release mode for production deployment.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/target"
BINARY_PATH="${SCRIPT_DIR}/target/release/rs-block-data-scanner"

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

# Check if we're in a git repository
if [[ ! -d "${SCRIPT_DIR}/.git" ]]; then
    error "Not in a git repository"
    exit 1
fi

# Check if git is available
if ! command -v git >/dev/null 2>&1; then
    error "Git is not installed or not in PATH"
    exit 1
fi

# Check if cargo is available
if ! command -v cargo >/dev/null 2>&1; then
    error "Cargo is not installed or not in PATH"
    error "Please install Rust toolchain first"
    exit 1
fi

log "Starting rebuild process..."

# Step 1: Pull latest code from git
log "Step 1: Pulling latest code from git..."
cd "$SCRIPT_DIR"

# Check current branch
CURRENT_BRANCH=$(git branch --show-current)
log "Current branch: $CURRENT_BRANCH"

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    warning "You have uncommitted changes:"
    git status --short
    warning "These changes will be preserved during the pull"
fi

# Pull latest changes
if git pull origin "$CURRENT_BRANCH"; then
    success "Git pull completed successfully"
else
    error "Git pull failed"
    exit 1
fi

# Step 2: Clean target directory
log "Step 2: Cleaning target directory..."
if [[ -d "$TARGET_DIR" ]]; then
    log "Removing target directory: $TARGET_DIR"
    rm -rf "$TARGET_DIR"
    success "Target directory cleaned"
else
    log "Target directory does not exist, skipping cleanup"
fi

# Step 3: Build in release mode
log "Step 3: Building application in release mode..."
log "This may take several minutes..."

# Show cargo version for debugging
log "Cargo version: $(cargo --version)"

# Build with release profile
if cargo build --release; then
    success "Build completed successfully"
else
    error "Build failed"
    exit 1
fi

# Step 4: Verify binary was created
if [[ -f "$BINARY_PATH" ]]; then
    success "Binary created successfully: $BINARY_PATH"
    
    # Show binary information
    BINARY_SIZE=$(du -h "$BINARY_PATH" | cut -f1)
    log "Binary size: $BINARY_SIZE"
    
    # Make binary executable
    chmod +x "$BINARY_PATH"
    success "Binary is now executable"
    
    # Show binary version if available
    if "$BINARY_PATH" --help >/dev/null 2>&1; then
        log "Binary is functional and ready for deployment"
    else
        warning "Binary was created but may have issues (help command failed)"
    fi
else
    error "Binary was not created at expected location: $BINARY_PATH"
    exit 1
fi

# Step 5: Show build summary
log "Build summary:"
log "  - Git branch: $CURRENT_BRANCH"
log "  - Binary path: $BINARY_PATH"
log "  - Binary size: $BINARY_SIZE"
log "  - Build mode: release"

success "Rebuild completed successfully!"
log "You can now use ./start.sh to start the service"
