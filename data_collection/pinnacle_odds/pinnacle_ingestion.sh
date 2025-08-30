#!/usr/bin/env bash
#
# Cron wrapper for running pinnacle_to_json.py every 2 hours
#

# Configuration
REPO_DIR="/home/ubuntu/Documents/projects/tennis_model"
SCRIPT_DIR="$REPO_DIR/data_collection/pinnacle_odds"
PYTHON_SCRIPT="pinnacle_to_json.py"
LOG_FILE="$SCRIPT_DIR/cron.log"
LOCK_FILE="$SCRIPT_DIR/ingestion.lock"

# Ensure repository directory exists
mkdir -p "$SCRIPT_DIR"

# Logging function
log_message() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Prevent overlapping runs
if [ -f "$LOCK_FILE" ]; then
  log_message "WARNING: Previous run still in progress. Skipping."
  exit 1
fi
echo $$ > "$LOCK_FILE"

# Cleanup function
cleanup() {
  rm -f "$LOCK_FILE"
  log_message "Cleaned up lock file"
}
trap cleanup EXIT INT TERM

log_message "=== Starting Tennis Odds Ingestion ==="

# Change to script directory
cd "$SCRIPT_DIR" || {
  log_message "ERROR: Cannot cd to $SCRIPT_DIR"
  exit 1
}

# Run the ingestion script
if python3 "$PYTHON_SCRIPT"; then
  log_message "SUCCESS: $PYTHON_SCRIPT completed"
  exit 0
else
  EXIT_CODE=$?
  log_message "ERROR: $PYTHON_SCRIPT failed with exit code $EXIT_CODE"
  exit $EXIT_CODE
fi
