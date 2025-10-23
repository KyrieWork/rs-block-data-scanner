#!/bin/bash

# ============================
# RocksDB Storage Analysis Script
# ============================
# This script analyzes the storage usage of different chains in the RocksDB data directory.
# It provides both chain-level and file-level storage information.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data/rocksdb"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Logging functions
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

info() {
    echo -e "${CYAN}[INFO]${NC} $1"
}

# Function to format bytes to human readable format
format_bytes() {
    local bytes=$1
    if [[ $bytes -ge 1073741824 ]]; then
        echo "$(echo "scale=2; $bytes/1073741824" | bc)GB"
    elif [[ $bytes -ge 1048576 ]]; then
        echo "$(echo "scale=2; $bytes/1048576" | bc)MB"
    elif [[ $bytes -ge 1024 ]]; then
        echo "$(echo "scale=2; $bytes/1024" | bc)KB"
    else
        echo "${bytes}B"
    fi
}

# Function to get file size in bytes
get_file_size() {
    local file_path="$1"
    if [[ -f "$file_path" ]]; then
        stat -f%z "$file_path" 2>/dev/null || stat -c%s "$file_path" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Function to analyze a single chain directory
analyze_chain() {
    local chain_dir="$1"
    local chain_name=$(basename "$chain_dir")
    
    if [[ ! -d "$chain_dir" ]]; then
        warning "Chain directory does not exist: $chain_dir"
        return 1
    fi
    
    info "Analyzing chain: $chain_name"
    
    # Get total size of chain directory
    local total_size_bytes=0
    while IFS= read -r -d '' file; do
        if [[ -f "$file" ]]; then
            local file_size=$(get_file_size "$file")
            total_size_bytes=$((total_size_bytes + file_size))
        fi
    done < <(find "$chain_dir" -type f -print0 2>/dev/null)
    
    local total_size_formatted=$(format_bytes "$total_size_bytes")
    
    echo -e "\n${MAGENTA}=== Chain: $chain_name ===${NC}"
    echo -e "${GREEN}Total Size:${NC} $total_size_formatted ($total_size_bytes bytes)"
    
    # Analyze .sst files specifically
    local sst_files=()
    local sst_total_size=0
    local sst_count=0
    
    while IFS= read -r -d '' file; do
        if [[ -f "$file" ]]; then
            sst_files+=("$file")
            local file_size=$(get_file_size "$file")
            sst_total_size=$((sst_total_size + file_size))
            sst_count=$((sst_count + 1))
        fi
    done < <(find "$chain_dir" -name "*.sst" -type f -print0 2>/dev/null)
    
    if [[ $sst_count -gt 0 ]]; then
        local sst_total_formatted=$(format_bytes "$sst_total_size")
        echo -e "${GREEN}SST Files:${NC} $sst_count files, total size: $sst_total_formatted"
        echo -e "${GREEN}SST Percentage:${NC} $(echo "scale=1; $sst_total_size * 100 / $total_size_bytes" | bc)%"
        
        echo -e "\n${CYAN}Individual SST Files:${NC}"
        printf "%-20s %-12s %-10s\n" "Filename" "Size" "Percentage"
        printf "%-20s %-12s %-10s\n" "--------" "----" "----------"
        
        for sst_file in "${sst_files[@]}"; do
            local filename=$(basename "$sst_file")
            local file_size=$(get_file_size "$sst_file")
            local file_size_formatted=$(format_bytes "$file_size")
            local percentage=$(echo "scale=1; $file_size * 100 / $total_size_bytes" | bc)
            printf "%-20s %-12s %-10s%%\n" "$filename" "$file_size_formatted" "$percentage"
        done
    else
        echo -e "${YELLOW}No SST files found in $chain_name${NC}"
    fi
    
    # Show other file types
    echo -e "\n${CYAN}Other Files:${NC}"
    local other_files=()
    while IFS= read -r -d '' file; do
        if [[ -f "$file" && ! "$file" =~ \.sst$ ]]; then
            other_files+=("$file")
        fi
    done < <(find "$chain_dir" -type f -print0 2>/dev/null)
    
    if [[ ${#other_files[@]} -gt 0 ]]; then
        printf "%-25s %-12s\n" "Filename" "Size"
        printf "%-25s %-12s\n" "--------" "----"
        for other_file in "${other_files[@]}"; do
            local filename=$(basename "$other_file")
            local file_size=$(get_file_size "$other_file")
            local file_size_formatted=$(format_bytes "$file_size")
            printf "%-25s %-12s\n" "$filename" "$file_size_formatted"
        done
    else
        echo "No other files found"
    fi
    
    return 0
}

# Function to show summary of all chains
show_summary() {
    local total_all_chains=0
    local chain_count=0
    
    echo -e "\n${MAGENTA}=== SUMMARY ===${NC}"
    printf "%-15s %-12s %-8s\n" "Chain" "Total Size" "SST Files"
    printf "%-15s %-12s %-8s\n" "-----" "----------" "---------"
    
    for chain_dir in "$DATA_DIR"/*; do
        if [[ -d "$chain_dir" ]]; then
            local chain_name=$(basename "$chain_dir")
            local chain_size_bytes=0
            while IFS= read -r -d '' file; do
                if [[ -f "$file" ]]; then
                    local file_size=$(get_file_size "$file")
                    chain_size_bytes=$((chain_size_bytes + file_size))
                fi
            done < <(find "$chain_dir" -type f -print0 2>/dev/null)
            
            local chain_size_formatted=$(format_bytes "$chain_size_bytes")
            local sst_count=$(find "$chain_dir" -name "*.sst" -type f 2>/dev/null | wc -l)
            
            printf "%-15s %-12s %-8s\n" "$chain_name" "$chain_size_formatted" "$sst_count"
            
            total_all_chains=$((total_all_chains + chain_size_bytes))
            chain_count=$((chain_count + 1))
        fi
    done
    
    if [[ $chain_count -gt 0 ]]; then
        local total_formatted=$(format_bytes "$total_all_chains")
        echo -e "\n${GREEN}Total across all chains:${NC} $total_formatted ($total_all_chains bytes)"
        echo -e "${GREEN}Number of chains:${NC} $chain_count"
    else
        echo -e "\n${YELLOW}No chain directories found${NC}"
    fi
}

# Main function
main() {
    log "Starting RocksDB storage analysis..."
    
    # Check if data directory exists
    if [[ ! -d "$DATA_DIR" ]]; then
        error "Data directory does not exist: $DATA_DIR"
        exit 1
    fi
    
    # Check if bc is available for calculations
    if ! command -v bc >/dev/null 2>&1; then
        warning "bc command not found. Some calculations may not work properly."
        warning "Please install bc: brew install bc (macOS) or apt-get install bc (Ubuntu)"
    fi
    
    info "Analyzing storage in: $DATA_DIR"
    
    # Find all chain directories
    local chain_dirs=()
    for chain_dir in "$DATA_DIR"/*; do
        if [[ -d "$chain_dir" ]]; then
            chain_dirs+=("$chain_dir")
        fi
    done
    
    if [[ ${#chain_dirs[@]} -eq 0 ]]; then
        warning "No chain directories found in $DATA_DIR"
        echo -e "\n${CYAN}Expected structure:${NC}"
        echo "  $DATA_DIR/"
        echo "  ├── chain1/"
        echo "  │   ├── *.sst files"
        echo "  │   └── other RocksDB files"
        echo "  ├── chain2/"
        echo "  │   ├── *.sst files"
        echo "  │   └── other RocksDB files"
        echo "  └── ..."
        exit 0
    fi
    
    # Analyze each chain
    for chain_dir in "${chain_dirs[@]}"; do
        analyze_chain "$chain_dir"
    done
    
    # Show summary
    show_summary
    
    success "Storage analysis completed!"
}

# Handle command line arguments
case "${1:-}" in
    -h|--help)
        echo "Usage: $0 [options]"
        echo ""
        echo "Analyze RocksDB storage usage for different blockchain chains."
        echo ""
        echo "Options:"
        echo "  -h, --help     Show this help message"
        echo "  -s, --summary  Show only summary information"
        echo ""
        echo "This script analyzes the storage in: $DATA_DIR"
        echo ""
        echo "Output includes:"
        echo "  - Total size per chain"
        echo "  - Individual .sst file sizes and percentages"
        echo "  - Other RocksDB files"
        echo "  - Summary across all chains"
        exit 0
        ;;
    -s|--summary)
        # Show only summary
        if [[ ! -d "$DATA_DIR" ]]; then
            error "Data directory does not exist: $DATA_DIR"
            exit 1
        fi
        show_summary
        exit 0
        ;;
    "")
        # Default: full analysis
        main
        ;;
    *)
        error "Unknown option: $1"
        echo "Use -h or --help for usage information"
        exit 1
        ;;
esac
