#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This is a mining taifex history script.
1. mining taifex rpt every day.
2. update to google drive.
"""

import sys, os, time
import zipfile, wget, argparse, re
import sqlite3
from datetime import datetime, timedelta
from pathutils import sys_path_append

# append module
#sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'device'))
sys_path_append('./device')
from gdrive import gdrive

class mining_rpt():
    path=os.path.dirname(__file__)
    date=None
    fex_info=None
    filename=None
    url=None
    gdrive_dir_id=None
    rptdirpath=None

    def __init__(self, item='fut_rpt', date=None):
        self.date=datetime.today().strftime('%Y_%m_%d') if date==None else date
        self.fex_info={
            'fut_rpt':{
                'filename':"Daily_%s.zip" %self.date,
                'url':'https://www.taifex.com.tw/DailyDownload/DailyDownload',
                'gdrive_id': gdrive().__class__.__dict__.get('fut_rpt_id', None)
            },
            'opt_rpt':{
                'filename':"OptionsDaily_%s.zip" %self.date,
                'url':"http://www.taifex.com.tw/DailyDownload/OptionsDailyDownload",
                'gdrive_id': gdrive().__class__.__dict__.get('opt_rpt_id', None)
            }
        }
        try:
            self.filename=self.fex_info[item]['filename']
            self.url=self.fex_info[item]['url']
            self.gdrive_dir_id=self.fex_info[item]['gdrive_id']
            self.rptdirpath=os.path.join(self.path, item)
        except:
            print('fex_info not found item')

    def download_rpt(self):

        try:
            os.mkdir(self.rptdirpath)
        except:
            pass

        def checkZipFile(path):
            try:
                zip_file=zipfile.ZipFile(path)
                zip_file.testzip()
                #os.system('unzip %s -d %s/tmp' %(path, os.path.dirname(__file__)))
                zip_file.close()
                print('\n%s: Download completed' %path)
            except zipfile.BadZipfile:
                os.remove(path)
                print('\nBadZipfile: remove %s' %path)

        storepath=os.path.join(self.rptdirpath, self.filename)
        if args.recover==False and os.path.exists(storepath):
            print('Exist: %s' %storepath)
        else:
            file_url=os.path.join(self.url, self.filename)
            os.system('wget -O %s %s' %(storepath, file_url))
            #wget.download(file_url, savefile_path)
            checkZipFile(storepath)

    def unzip_all2rptdir(self):
        print('Exteacting...unzip')

        try:
            os.mkdir(os.path.join(self.rptdirpath, 'tmp'))
        except:
            pass

        if os.path.isdir(self.rptdirpath):
            print('Exist: %s' %self.rptdirpath)
            for dirname, dirnames, filenames in os.walk(self.rptdirpath):
                #print(dirname, dirnames, filenames)
                if dirname == self.rptdirpath:
                    for filename in filenames:
                        file_abspath=os.path.abspath(os.path.join(dirname, filename))
                        print(file_abspath, self.gdrive_dir_id)
                        try:
                            zip_file=zipfile.ZipFile(file_abspath)
                            zip_file.testzip()
                            zip_file.close()
                        except zipfile.BadZipfile:
                            continue

                        with zipfile.ZipFile(file_abspath, 'r') as zf:
                            for rptname in zf.namelist():
                                zf.extract(rptname, os.path.join(dirname, 'tmp'))
                                print(os.path.join(dirname, 'tmp', rptname)+' done')
                                #print('upzip %s in %s/tmp' %(rptfile, ldir))
            print('all %s file done in %s/tmp\n' %(self.rptdirpath, self.rptdirpath))
        else:
            print('not found dir\n')

    def unzip_file(self, local, exdir):

        try:
            zip_file=zipfile.ZipFile(local)
            zip_file.testzip()
            zip_file.close()
            with zipfile.ZipFile(local, 'r') as zf:
                for rptname in zf.namelist():
                    zf.extract(rptname, exdir)
            print('unzip %s: done' %local)
        except zipfile.BadZipfile:
            os.remove(path)
            print('\nBadZipfile: %s not exist' %local)
            sys.exit()

    def upload_gdrive(self, daily_info):

        file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, daily_info.filename))

        if os.path.exists(file_abspath):
            gdrive().UploadFile(file_abspath, daily_info.gdrive_dir_id)
        else:
            print('upload file path is not exist')

    def parser_rpt_to_DB(self, daily_info):
        rpt_file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, 'tmp', daily_info.filename)).replace('.zip', '.rpt')
        ## split rpt_file_abspath to list ex. ['Daily', '2018', '05', '08', 'zip']
        fname_list=re.split('_|\.', daily_info.filename)

        ## check rpt file is exist on tmp, zip is exist?, gdrive is exist? or return None
        if not os.path.isfile(rpt_file_abspath):
            zip_file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, daily_info.filename))
            if not os.path.isfile(zip_file_abspath):
                oid=gdrive().getIdByName(daily_info.filename, daily_info.gdrive_dir_id)
                if oid==None:
                    return None
                gdrive().GetContentFile(zip_file_abspath, oid)
            self.unzip_file(zip_file_abspath, os.path.join(daily_info.rptdirpath, 'tmp'))

        ## Confirmation item(TX, mouth), grep next month if not found this month
        if fname_list[0]=='Daily':
            futc=''.join(fname_list[1:3])
            get_fut=os.popen("cat %s | grep ,TX | grep -P '%s\s+'" %(rpt_file_abspath, futc)).read().strip()
            if get_fut=='':
                futc=(datetime.strptime(futc, '%Y%m')+timedelta(days=31)).strftime('%Y%m')
                get_fut=os.popen("cat %s | grep ,TX | grep -P '%s\s+'" %(rpt_file_abspath, futc)).read().strip()
        textlist=get_fut.split('\n')

        ## Mining 150000-050000, 084500-134500
        req=[]
        tmp=len(textlist)
        for i in range(tmp):
            (_date, _fut, _futc, _time, _price, _volume)=map(str.strip, textlist[i].split(',')[:-3])
            _price=int(_price)
            _volume=int(_volume)
            _ptrtime=datetime.strptime(_date+_time, '%Y%m%d%H%M%S')

            ## 150000-235900, (跨日判斷)000000-050000(多筆), 084500-134500(多筆)
            if i==0:
                (Open, High, Low, Close, Volume)=(_price, _price, _price, _price, _volume)
                open_time=datetime.strptime(_date+_time[:-2], '%Y%m%d%H%M')
                step_time=open_time+timedelta(minutes=1)
                (date, fut, futc, endtime)=map(str.strip, textlist[-1].split(',')[:4])
                close_time=datetime.strptime(date+endtime[:-2], '%Y%m%d%H%M')+timedelta(minutes=1)
                next
                #print(open_time, step_time, close_time)
            if _ptrtime<step_time or _ptrtime.strftime('%H%M%S')=='050000' or (_ptrtime.strftime('%H%M%S')=='134500'):
                High=_price if _price>High else High
                Low=_price if _price<Low else Low
                Close=_price
                Volume+=_volume
            else:
                ele=(step_time.strftime('%Y/%m/%d'), str(step_time.time()), Open, High, Low, Close, Volume/2)
                if i!=tmp-1:
                    req.append(ele)
                    #print(ele, len(req))
                ## diff time to append
                diff_second=(_ptrtime-step_time).seconds
                if diff_second>59 and step_time.strftime('%H%M%S')!='050000':
                    for i in range(1, diff_second//60+1, 1):
                        diff_ele=list(ele)
                        diff_ele[1]=str((step_time+timedelta(minutes=i)).time())
                        diff_ele[6]=0
                        req.append(tuple(diff_ele))
                        #print(diff_ele, len(req))
                (Open, High, Low, Close, Volume)=(_price, _price, _price, _price, _volume)
                step_time=datetime.strptime(_date+_time[:-2], '%Y%m%d%H%M')+timedelta(minutes=1)
                next
            if i==tmp-1:
                ele=(step_time.strftime('%Y/%m/%d'), str(step_time.time()), Open, High, Low, Close, Volume/2)
                req.append(ele)
                #print(ele, len(req))
                break
            ## use progressbar
            k=float(i+1)/tmp*100
            step=tmp//32
            _str='='*(i//step)+'>'+' '*(32-(i//step))
            sys.stdout.write('\r[%s][%.1f%%]' %(_str, k))
            sys.stdout.flush()
        sys.stdout.write('\n')

        ## query to DB
        conn=sqlite3.connect(os.path.abspath(daily_info.path)+'/FCT_DB.db')
        cursor=conn.cursor()
        SQL="INSERT INTO tw%s VALUES (?,?,?,?,?,?,?);" %"TX"

        for i in range(len(req)):
            cursor.execute(SQL, req[i])
        conn.commit()
        conn.close()
        print(tmp, len(req))

## library of gdrive
#oid=gdrive().getIdByName('Daily_2018_08_15.zip', gdrive().fut_rpt_id)
#gdrive().GetContentFile('/home/luke/fex_daily/trash/aa.zip', oid)
#gdrive().UploadFile('/home/luke/fex_daily/trash/aa.zip', gdrive().fut_rpt_id)

if __name__ == '__main__':
    #before_date=int(sys.argv[1]) if len(sys.argv)>1 else 1
    parser=argparse.ArgumentParser()
    parser.add_argument("-d", "--date", type=str, default=datetime.today().strftime('%Y%m%d'), \
                        help="download rpt $DATE~today, tpye=str ex:20180101.")
    parser.add_argument("-r", "--recover", type=bool, default=False, help="flag with $RECOVER, tpye=bool.")
    args=parser.parse_args()

    items=('fut_rpt', 'opt_rpt')

    today=datetime.today()
    date=datetime.strptime(args.date, "%Y%m%d")
    date=date if args.date!=None and date<today else today+timedelta(days=-1)
    diff_days=(date-today).days+1

    # every daily
    for i in range(diff_days, 1, 1):
        date=(today+timedelta(days=i)).strftime('%Y_%m_%d')

        for j in items[:2]:
            daily_mining=mining_rpt(j, date=date)

            daily_mining.download_rpt()
            daily_mining.upload_gdrive(daily_mining)
            if j=='fut_rpt':
                daily_mining.parser_rpt_to_DB(daily_mining)
                #sys.exit()
            #daily_mining.get_$timestep_data_$datebydate_from_DB(daily_mining, $timestep, $day1, $day2)

    # unzip_all2rptdir for once
    for i in items[:0]:
        daily_mining=mining_rpt(i)
        daily_mining.unzip_all2rptdir()
        pass