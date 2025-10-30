#!/bin/bash

SET_COLOR=true

if [[ -n "${NO_COLOR:-}" ]]; then
    SET_COLOR=false
fi

if [ "$SET_COLOR" = true ]; then
    BOLD='\033[1m'
    NC='\033[0m'
else
    BOLD=''
    NC=''
fi

echo -e "${BOLD}Available scripts:${NC}"
cat <<'USAGE'
  scripts/start_scanner.sh   - Start the block scanner service
  scripts/stop_scanner.sh    - Stop the block scanner service
  scripts/start_api.sh       - Start the API service
  scripts/stop_api.sh        - Stop the API service
  scripts/help.sh            - Show this help message
USAGE
