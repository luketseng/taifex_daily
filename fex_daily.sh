#!/bin/bash -xe

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
        exit 1
    fi
fi

echo "get $date_age~$today fex"
# cd ~/public_html
mkdir -p ~/git/taifex_web/web_json
cd ~/git/taifex_web/web_json
~/git/taifex_daily/mining_rpt.py -e TX 1 -d $date_age
~/git/taifex_daily/mining_rpt.py -e MTX 1 -d $date_age
sleep 3
find ~/git/taifex_daily/fut_rpt/ -mtime +15 -type f -name '*' -exec rm -rf {} \;
find ~/git/taifex_daily/opt_rpt/ -mtime +15 -type f -name '*' -exec rm -rf {} \;
find ~/git/taifex_web/web_json -mtime +0 -type f -name "*TX_*" -exec rm -rf {} \;
