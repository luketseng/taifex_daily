#!/bin/bash -xe

today=$(date +%Y%m%d)
#date_age=$(date +%Y%m%d -d "${1:-4} days ago")

#if [ ! -n "$1" ]; then
#if [ "$1" = "" ]; then
if [ ! $1 ]; then
    echo "you have not input a word!"
    date_age=$today
else
    echo "the word you input is $1"
    date_age=$(date +%Y%m%d -d "${1} days ago")
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

