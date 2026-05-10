#!/bin/bash -e

# Enable xtrace when DEBUG is set, e.g. `DEBUG=1 ./fex_daily.sh 4`.
# Default is quiet output; flip the switch only when you need to debug.
# `-e` stays on the shebang so any failed command still aborts the run.
[[ -n "$DEBUG" ]] && set -x

# ---- Paths ----
# Base directories used by this script. Change these if the repos move.
SCRIPT_DIR="$HOME/git/taifex_daily"
WEB_DIR="$HOME/git/taifex_web"
WEB_JSON_DIR="$WEB_DIR/web_json"
FUT_RPT_DIR="$SCRIPT_DIR/fut_rpt"
OPT_RPT_DIR="$SCRIPT_DIR/opt_rpt"

# ---- Retention (days) ----
# Files older than these thresholds will be removed at the end of each run.
KEEP_DAYS_RPT=15   # raw fut_rpt/opt_rpt download files
KEEP_DAYS_JSON=0   # generated *TX_* JSON in web_json (kept only for today)

# ---- Help ----
show_help() {
    cat <<'EOF'
Usage: fex_daily.sh [DAYS_AGO | YYYYMMDD | -h]

Fetch TAIFEX TX/MTX daily reports from a given date up to today,
then clean up old report/JSON files.

Arguments:
  (none)        Fetch today's data only
  DAYS_AGO      Number of days ago (1-365), e.g. 4
  YYYYMMDD      Specific start date, e.g. 20231215
  -h, --help    Show this help message

Examples:
  fex_daily.sh             # today
  fex_daily.sh 4           # from 4 days ago to today
  fex_daily.sh 20231215    # from 2023-12-15 to today

Output:
  ~/git/taifex_web/web_json/   (JSON for the web project)
  ~/git/taifex_daily/fut_rpt/  (futures raw reports)
  ~/git/taifex_daily/opt_rpt/  (options raw reports)

Cleanup (per run):
  fut_rpt/, opt_rpt/  : files older than KEEP_DAYS_RPT days
  web_json/*TX_*      : files older than KEEP_DAYS_JSON days

Environment:
  DEBUG=1       Enable bash xtrace (verbose command tracing) for debugging
                Example: DEBUG=1 fex_daily.sh 4
EOF
}

# Show help when explicitly requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

today=$(date +%Y%m%d)

# Check if parameter is empty
if [ ! $1 ]; then
    echo "you have not input a parameter!"
    date_age=$today
else
    # Check if parameter is in YYYYMMDD date format (8 digits)
    if [[ "$1" =~ ^[0-9]{8}$ ]]; then
        echo "the date you input is $1"
        # Validate if the date is valid
        if date -d "$1" >/dev/null 2>&1; then
            date_age=$1
        else
            echo "Invalid date format: $1"
            echo "Please use YYYYMMDD format (e.g., 20231215)"
            exit 1
        fi
    # Check if parameter is a number (days ago, 1-365)
    elif [[ "$1" =~ ^[1-9]$|^[1-9][0-9]$|^[1-2][0-9][0-9]$|^3[0-5][0-9]$|^36[0-5]$ ]]; then
        echo "the days you input is $1"
        date_age=$(date +%Y%m%d -d "${1} days ago")
    else
        echo "Invalid parameter: $1"
        echo "Please input either:"
        echo "  - Number of days ago (e.g., 4)"
        echo "  - Date in YYYYMMDD format (e.g., 20231215)"
        echo "Run with -h or --help for full usage."
        exit 1
    fi
fi

echo "get $date_age~$today fex"
# cd ~/public_html
mkdir -p "$WEB_JSON_DIR"
cd "$WEB_JSON_DIR"
# Contracts to fetch each run. Add more codes here when needed (e.g. TE, TF).
CONTRACTS=(TX MTX)
# The trailing `1` is the K-line interval code consumed by mining_rpt.py:
#   1   = 1-min K-line
#   300 = 5-min K-line
#   ...and so on. Bump if a different bar size is needed.
for contract in "${CONTRACTS[@]}"; do
    "$SCRIPT_DIR/mining_rpt.py" -e "$contract" 1 -d "$date_age"
done

# Wait briefly so mining_rpt.py finishes flushing files to disk
# before the cleanup pass below runs find against the same dirs.
sleep 3

# Prune old raw reports and outdated web JSON output.
# Use -delete instead of -exec rm: it's faster (no per-file fork) and
# safe here because we only match regular files (-type f).
find "$FUT_RPT_DIR/" -mtime +"$KEEP_DAYS_RPT"  -type f -delete
find "$OPT_RPT_DIR/" -mtime +"$KEEP_DAYS_RPT"  -type f -delete
find "$WEB_JSON_DIR" -mtime +"$KEEP_DAYS_JSON" -type f -name "*TX_*" -delete
