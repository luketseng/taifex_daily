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

# For crontab list
# m h  dom mon dow   command
# @reboot cd ~/git/docker_mgmt && docker compose up -d;
# 05 15,19 * * * ~/git/taifex_daily/mining_rpt.py --upload-recover > ~/log.txt 2>&1
# 45 16 * * 1-5 cd ~/git/taifex_web/web_json && ~/git/taifex_daily/get_data.py >> ~/log2.txt 2>&1
# 30 15,20 * * 1-4 ~/git/taifex_daily/fex_daily.sh
# 30 15,20 * * 5 ~/git/taifex_daily/fex_daily.sh 4

# create a hard link to file
# ln file hard-file
# luke@raspberrypi:~/git/workspace(master 1h13m)$ ln -v /opt/backup/FCT_DB.db
# ./FCT_DB.db' => '/opt/backup/FCT_DB.db
