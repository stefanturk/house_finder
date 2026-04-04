#!/bin/bash
# house_finder.sh — Simple wrapper to execute house_finder with cleanup

set -e  # exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DB_FILE="$SCRIPT_DIR/house_finder.db"

# Load environment variables (RAPIDAPI_KEY, ANTHROPIC_API_KEY)
source "$HOME/.zshrc" 2>/dev/null || true

# Clean up old database (fresh start each run)
if [ -f "$DB_FILE" ]; then
    rm "$DB_FILE"
    echo "Cleared house_finder.db"
fi

# Run the script
echo ""
python3 "$SCRIPT_DIR/house_finder.py"
