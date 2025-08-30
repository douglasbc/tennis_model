#!/usr/bin/env bash
#
# Cron wrapper for running pinnacle_to_json.py every 2 hours
#

# Configuration
REPO_DIR="/home/ubuntu/Documents/projects/tennis_model"
VENV_DIR="$REPO_DIR/venv"
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
  # Deactivate virtual environment if it was activated
  if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
    log_message "Deactivated virtual environment"
  fi
  log_message "Cleaned up lock file"
}
trap cleanup EXIT INT TERM

log_message "=== Starting Tennis Odds Ingestion ==="

# Activate virtual environment
if [ -f "$VENV_DIR/bin/activate" ]; then
  source "$VENV_DIR/bin/activate"
  log_message "Activated virtual environment: $VIRTUAL_ENV"
else
  log_message "ERROR: Virtual environment not found at $VENV_DIR"
  exit 1
fi

# Change to script directory
cd "$SCRIPT_DIR" || {
  log_message "ERROR: Cannot cd to $SCRIPT_DIR"
  exit 1
}

# Run the ingestion script (use python from venv)
if python "$PYTHON_SCRIPT"; then
  log_message "SUCCESS: $PYTHON_SCRIPT completed"
  exit 0
else
  EXIT_CODE=$?
  log_message "ERROR: $PYTHON_SCRIPT failed with exit code $EXIT_CODE"
  exit $EXIT_CODE
fi
