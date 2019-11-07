#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This is a mining taifex history script.
1. mining taifex rpt every day.
2. update to google drive.
"""

import sys, os
import zipfile, wget, argparse, re
import sqlite3
import logging
from datetime import datetime, timedelta
from devices.gdrive import gdrive

class mining_rpt():
    path=os.path.dirname(__file__)
    date=None
    fex_info=None
    filename=None
    url=None
    gdrive_dir_id=None
    rptdirpath=None
    item=None

    def __init__(self, date, item='fut_rpt'):
        global logger
        self.date=today.strftime('%Y_%m_%d') if date==None else date
        logger.info('Mining: {}, {}'.format(self.date, item))

        '''get gdrive() device'''
        self.fex_info={
            'fut_rpt':{
                'filename':"Daily_{}.zip".format(self.date),
                'url':'https://www.taifex.com.tw/DailyDownload/DailyDownload',
                'gdrive_id': _gdrive.__class__.__dict__['item_obj'].get('fut_rpt', None)['id']
            },
            'opt_rpt':{
                'filename':"OptionsDaily_{}.zip".format(self.date),
                'url':"http://www.taifex.com.tw/DailyDownload/OptionsDailyDownload",
                'gdrive_id': _gdrive.__class__.__dict__['item_obj'].get('opt_rpt', None)['id']
            }
        }
        try:
            self.filename=self.fex_info[item]['filename']
            self.url=self.fex_info[item]['url']
            self.gdrive_dir_id=self.fex_info[item]['gdrive_id']
            self.rptdirpath=os.path.join(self.path, item)
            self.item=item
            logger.info('self.gdrive_dir_id: {}'.format(self.gdrive_dir_id))
            logger.info('ready to download {} to {} via url: {}'.format(self.filename, self.rptdirpath, self.url))
        except:
            logger.error('fex_info not found item')

    def download_rpt(self):

        def checkZipFile(path):
            try:
                zip_file=zipfile.ZipFile(path)
                zip_file.testzip()
                zip_file.close()
                logger.info('Download completed for {}'.format(path))
            except zipfile.BadZipfile:
                os.remove(path)
                logger.warning('BadZipfile: remove {}'.format(path))

        if not os.path.exists(self.rptdirpath):
            os.mkdir(self.rptdirpath)

        storepath=os.path.join(self.rptdirpath, self.filename)
        if args.recover==False and os.path.exists(storepath):
            logger.info('%s is exist' %storepath)
        else:
            file_url=os.path.join(self.url, self.filename)
            logger.info('wget {} via {}'.format(storepath, file_url))
            os.system('wget -O {} {}'.format(storepath, file_url))
            ## check zip is not empty
            checkZipFile(storepath)

    def unzip_all2rptdir(self):
        logger.info('Exteacting for all unzip...')

        tmp_path=os.path.join(self.rptdirpath, 'tmp')
        if not os.path.exists(tmp_path):
            os.mkdir(tmp_path)

        if os.path.isdir(self.rptdirpath):
            logger.info('Exist on local: {}'.format(self.rptdirpath))
            for dirname, dirnames, filenames in os.walk(self.rptdirpath):
                if dirname == self.rptdirpath:
                    for filename in filenames:
                        file_abspath=os.path.abspath(os.path.join(dirname, filename))
                        logger.debug(file_abspath, self.gdrive_dir_id)
                        try:
                            zip_file=zipfile.ZipFile(file_abspath)
                            zip_file.testzip()
                            zip_file.close()
                        except zipfile.BadZipfile:
                            continue

                        with zipfile.ZipFile(file_abspath, 'r') as zf:
                            for rptname in zf.namelist():
                                zf.extract(rptname, os.path.join(dirname, 'tmp'))
                                logger.debug(os.path.join(dirname, 'tmp', rptname)+' done')
            logger.info('all {path} file done in {}/tmp\n'.format(path=self.rptdirpath))
        else:
            logger.info('not found dir\n')

    def unzip_file(self, local, exdir):

        try:
            with zipfile.ZipFile(local, 'r') as zf:
                for rptname in zf.namelist():
                    zf.extract(rptname, exdir)
            logger.info("unzip file '{}' to '{}' done".format(local, exdir))
        except :
            assert False, 'except for unzip file to {}'.format(local)

    def upload_gdrive(self, daily_info):

        file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, daily_info.filename))

        if os.path.exists(file_abspath):
            _gdrive.UploadFile(file_abspath, daily_info.item, recover=args.recover)
        else:
            logger.warning('Warning: file path is not exist')

    def parser_rpt_to_DB(self, daily_info, fut='TX'):
        zip_file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, daily_info.filename))
        rpt_file_abspath=os.path.abspath(os.path.join(daily_info.rptdirpath, 'tmp', daily_info.filename)).replace('.zip', '.rpt')
        ## split rpt_file_abspath to list ex. ['Daily', '2018', '05', '08', 'zip']
        fname_list=re.split('_|\.', daily_info.filename)

        ## check rpt file is exist on tmp, zip is exist?, gdrive is exist? or return None
        if not os.path.isfile(rpt_file_abspath) or args.recover:
            if not os.path.isfile(zip_file_abspath):
                oid=_gdrive.getObjByName(daily_info.filename, daily_info.gdrive_dir_id)['id']
                if oid==None:
                    logger.warning('warning to get oid: _gdrive.getObjByName({}, {})'.format(daily_info.filename, daily_info.gdrive_dir_id))
                _gdrive.GetContentFile(daily_info.item, zip_file_abspath)
            self.unzip_file(zip_file_abspath, os.path.join(daily_info.rptdirpath, 'tmp'))

        ## Confirmation item(TX, mouth), grep next month if not found this month
        if fname_list[0]=='Daily':
            futc=''.join(fname_list[1:3])
            get_fut=os.popen("cat %s | grep ,%s | grep -P '%s\s+'" %(rpt_file_abspath, fut, futc)).read().strip()
            if get_fut=='':
                futc=(datetime.strptime(futc, '%Y%m')+timedelta(days=31)).strftime('%Y%m')
                get_fut=os.popen("cat %s | grep ,%s | grep -P '%s\s+'" %(rpt_file_abspath, fut, futc)).read().strip()
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
                (Open, High, Low, Close, Volume)=(_price, _price, _price, _price, 0)
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
        #print(repr(req[0])+'\n'+repr(req[839])+'\n'+repr(req[-1]))
        SQL="INSERT INTO tw%s VALUES (?,?,?,?,?,?,?);" %fut
        SQL_Detete="DELETE FROM tw%s WHERE Date=\'%s\' and Time<=\'%s\';" %(fut, req[-1][0],req[-1][1])

        ## delete old data
        cursor.execute(SQL_Detete)
        if req[0][1]=='15:01:00':
            SQL_Detete1="DELETE FROM tw%s WHERE Date=\'%s\' and Time>=\'%s\';" %(fut, req[0][0],req[0][1])
            SQL_Detete2="DELETE FROM tw%s WHERE Date=\'%s\' and Time<=\'%s\';" %(fut, req[839][0],req[839][1])
            cursor.execute(SQL_Detete1)
            cursor.execute(SQL_Detete2)
        conn.commit()

        ## insert new data
        for i in range(len(req)):
            cursor.execute(SQL, req[i])
        conn.commit()
        conn.close()
        print(tmp, len(req))

    def export_sql_to_txt(self, date_list):

        date=datetime.strptime(date_list[0], "%Y%m%d").strftime('%Y/%m/%d')
        #print(date_list.__len__())
        fut=date_list[2] if len(date_list)>2 else 'TX'
        size=int(date_list[3]) if len(date_list)>3 else 300
        conn=sqlite3.connect(os.path.abspath(os.path.dirname(__file__))+'/FCT_DB.db')
        cursor=conn.cursor()
        #SQL1="SELECT Date, MAX(High), MIN(Low), SUM(Volume) FROM tw%s WHERE Date=\'%s\' and Time>\'08:45:00\' and Time<=\'13:45:00\' ORDER BY Date, Time;" %("TX", date)

        export_str='Date,Time,Open,High,Low,Close,Volume\n'
        while True:
            SQL="SELECT * FROM tw%s WHERE Date=\'%s\' and Time>\'08:45:00\' and Time<=\'13:45:00\' ORDER BY Date, Time;" %(fut, date)
            cursor.execute(SQL)
            if size==1:
                req=cursor.fetchall()
                if req:
                    for i in req:
                        export_str+=(str(map(str, i)).strip("\[").strip("\]").replace("\'", "").replace(" ", ""))+'\n'
            else:
                while True:
                    req=cursor.fetchmany(size)
                    if not req:
                        break
                    else:
                        #print(req)
                        (High, Low, Vol)=(req[0][3], req[0][3], 0)
                        for i in req:
                            High=i[3] if i[3]>High else High
                            Low=i[4] if i[4]<Low else Low
                            Vol+=i[6]
                        Close_time=req[-1][1]
                        if req[-1][1]=='13:30:00': Close_time='13:45:00'
                        export_str+='%s,%s,%s,%s,%s,%s,%s\n' %(req[-1][0], Close_time, req[0][2], High, Low, req[-1][5], Vol)

            date=(datetime.strptime(date, "%Y/%m/%d")+timedelta(days=1)).strftime('%Y/%m/%d')
            if datetime.strptime(date, "%Y/%m/%d")>datetime.strptime(date_list[1], "%Y%m%d"):
                print(export_str)
                break

def get_logging_moduel():
    global logger
    logger=logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    console=logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s | %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)

def valid_date(date_text):
    global logger
    date_interval=date_text.split('-')
    today=datetime.today()

    try:
        start_date=datetime.strptime(date_interval[0], '%Y%m%d')
        ## set end_date if not setting
        end_date=today.replace(minute=0, hour=0, second=0, microsecond=0)
        if len(date_interval)==2:
            end_date=datetime.strptime(date_interval[1], '%Y%m%d')

    except ValueError:
        logger.error('valid date format fail: "--date {!s}"'.format(date_text))
        assert False, 'valid date format fail: "--date {!s}"'.format(date_text)

    ## valid end_date after start_date
    if start_date > end_date or start_date > today:
        logger.error('start date({}) after end_date({} or today({}))'.format(start_date, end_date, today))
        assert False, 'start date({}) after end_date({} or today({}))'.format(start_date, end_date, today)

    logger.info('(start_date, end_date) = ({}, {})'.format(start_date, end_date))
    return (start_date, end_date)

if __name__ == '__main__':
    '''init config'''
    today=datetime.today()
    items=('fut_rpt', 'opt_rpt')
    _gdrive=gdrive()

    '''set logging moduel for debug'''
    get_logging_moduel()

    '''set args for mining_rpt control'''
    parser=argparse.ArgumentParser()
    parser.add_argument('-d', '--date', type=str, default=today.strftime('%Y%m%d'), help='download rpt from $DATE~today, tpye=str ex: 20180101-20180102')
    parser.add_argument("-e", "--export", nargs='+', type=str, default=None, help="Date1 Date2 Future(TX) Interval(300), tpye=str ex: -e 20180101 20180102 TX 300")
    parser.add_argument('--upload-recover', dest='recover', default=False, action='store_true', help='switch for new rpt instead of gdrive exist.')
    args=parser.parse_args()

    (start_D, end_D)=valid_date(args.date)
    logger.info('{!s}'.format(args))

    if args.export==None:
        pass
    elif args.export!=None and len(args.export)>1 and len(args.export)<5:
        mining_rpt(today.strftime('%Y_%m_%d')).export_sql_to_txt(args.export)
        sys.exit()
    else:
        logger.error('error arg: '+repr(args.export)+', ex:-e 20180101 20180102 TX 300')
        sys.exit()

    ## every daily
    i=start_D
    while i <= end_D:
        date=i.strftime('%Y_%m_%d')
        logger.info('Start mining for {}'.format(date))

        for j in items:
            daily_mining=mining_rpt(date, j)

            daily_mining.download_rpt()
            daily_mining.upload_gdrive(daily_mining)
            if j=='fut_rpt':
                daily_mining.parser_rpt_to_DB(daily_mining, 'TX')
                daily_mining.parser_rpt_to_DB(daily_mining, 'MTX')
        i+=timedelta(days=1)

    '''
    # unzip_all2rptdir for once
    for i in items[:0]:
        daily_mining=mining_rpt(i)
        daily_mining.unzip_all2rptdir()
        pass
    '''
