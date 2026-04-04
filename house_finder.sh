#!/bin/bash
# house_finder.sh — Simple wrapper to execute house_finder

set -e  # exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Load environment variables (RAPIDAPI_KEY, ANTHROPIC_API_KEY)
source "$HOME/.zshrc" 2>/dev/null || true

# Run the script
python3 "$SCRIPT_DIR/house_finder.py"
