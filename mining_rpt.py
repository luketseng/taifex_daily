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
import json
import numpy as np
import time
from datetime import datetime, timedelta
from devices.gdrive import gdrive

class mining_rpt():
    path = os.path.dirname(__file__)
    db_name='FCT_DB.db'
    date = None
    item = None
    fex_dict_item = None

    def __init__(self, *args, **kwargs):
        config_file = '{}/config.json'.format(self.path)
        with open(config_file, 'r') as f:
            fex_dicts = json.load(f, encoding='utf-8')
        ## ready for default date and fex item
        self.date = kwargs.get('date', today.strftime('%Y_%m_%d'))
        self.item = kwargs.get('item', 'fut_rpt')
        logger.info("Mining info: date='{}', item='{}'".format(self.date, self.item))
        try:
            self.fex_dict_item = fex_dicts[self.item]
            if self.item == 'fut_rpt':
                self.fex_dict_item['filename'] = "Daily_{}.zip".format(self.date)
            elif self.item == 'opt_rpt':
                self.fex_dict_item['filename'] = "OptionsDaily_{}.zip".format(self.date)
            self.fex_dict_item['rptdirpath'] = os.path.join(self.path, self.item)
            logger.info(
                'ready to download {filename} to {rptdirpath} via url: {url}'.format(**self.fex_dict_item))
        except:
            logger.error('fex_info not found item')
        ## get gdrive() device
        if 'gdevice' not in globals():
            global gdevice
            gdevice = gdrive()

    def download_rpt(self):

        def checkZipFile(path):
            try:
                zip_file = zipfile.ZipFile(path)
                zip_file.testzip()
                zip_file.close()
                logger.info('Download completed : {} and check done'.format(path))
            except zipfile.BadZipfile:
                logger.warning('BadZipfile: remove {}'.format(path))
                os.remove(path)

        fex_info = self.fex_dict_item
        if not os.path.exists(fex_info['rptdirpath']):
            os.mkdir(fex_info['rptdirpath'])

        storepath = os.path.join(fex_info['rptdirpath'], fex_info['filename'])
        if not args.recover and os.path.exists(storepath):
            logger.info('{} is exist, args.recover = {}'.format(storepath, args.recover))
        else:
            file_url = os.path.join(fex_info['url'], fex_info['filename'])
            logger.info('wget {} via {}'.format(storepath, file_url))
            os.system('wget -O {} {}'.format(storepath, file_url))
            ## check zip is not empty
            checkZipFile(storepath)

    def unzip_all2rptdir(self):
        logger.info('Exteacting for all unzip...')

        fex_info = self.fex_dict_item
        tmp_path = os.path.join(fex_info['rptdirpath'], 'tmp')
        if not os.path.exists(tmp_path):
            os.mkdir(tmp_path)

        if os.path.isdir(fex_info['rptdirpath']):
            logger.info('Exist on local: {rptdirpath}'.format(**fex_info))
            for dirname, dirnames, filenames in os.walk(fex_info['rptdirpath']):
                if dirname == fex_info['rptdirpath']:
                    for filename in filenames:
                        file_abspath = os.path.abspath(os.path.join(dirname, filename))
                        try:
                            zip_file = zipfile.ZipFile(file_abspath)
                            zip_file.testzip()
                            zip_file.close()
                        except zipfile.BadZipfile:
                            continue

                        with zipfile.ZipFile(file_abspath, 'r') as zf:
                            for rptname in zf.namelist():
                                zf.extract(rptname, os.path.join(dirname, 'tmp'))
                                logger.debug(os.path.join(dirname, 'tmp', rptname) + ' done')
            logger.info('all {rptdirpath} file unzip to {rptdirpath}/tmp'.format(**fex_info))
        else:
            logger.warning('Warning: {} dir not exist'.format(fex_info['rptdirpath']))

    def unzip_file(self, local, exdir):

        try:
            with zipfile.ZipFile(local, 'r') as zf:
                for rptname in zf.namelist():
                    zf.extract(rptname, exdir)
            logger.info("Unzip '{}' to '{}'".format(local, exdir))
        except:
            assert False, 'except for unzip file to {}'.format(local)

    def upload_gdrive(self):

        fex_info = self.fex_dict_item
        file_abspath = os.path.abspath(os.path.join(fex_info['rptdirpath'], fex_info['filename']))

        if os.path.exists(file_abspath):
            gdevice.UploadFile(file_abspath, self.item, recover=args.recover)
        else:
            logger.warning('Warning: file path is not exist')

    def parser_rpt_to_DB(self, fut='TX'):

        fex_info = self.fex_dict_item
        zip_file_relpath = os.path.join(fex_info['rptdirpath'], fex_info['filename'])
        rpt_file_relpath = os.path.join(fex_info['rptdirpath'], 'tmp', fex_info['filename']).replace(
            '.zip', '.rpt')

        ## check rpt file is exist on tmp, zip is exist?, gdrive is exist? or return None
        if not os.path.isfile(rpt_file_relpath) or args.recover:
            if not os.path.isfile(zip_file_relpath):
                logger.info(
                    'not found {} from "www.taifex.com.tw" and get zip via gdrive'.format(zip_file_relpath))
                gdevice.GetContentFile(fex_info['filename'], zip_file_relpath)
            self.unzip_file(zip_file_relpath, os.path.dirname(rpt_file_relpath))

        ## Confirmation get_fut(flie, fut, fut_mouth), grep next month if close on this month
        date = datetime.strptime(self.date, '%Y_%m_%d')
        if 'Daily' in fex_info['filename']:
            grep_info = (rpt_file_relpath, fut, date.strftime('%Y%m'))
            tick_result = os.popen("cat {} | grep ,{} | grep -P '{}\s+'".format(*grep_info)).read()
            if tick_result == '':
                futc = date + timedelta(weeks=4)
                grep_info = (rpt_file_relpath, fut, futc.strftime('%Y%m'))
                tick_result = os.popen("cat {} | grep ,{} | grep -P '{}\s+'".format(*grep_info)).read()
        tick_result = tick_result.strip().replace(',', ' ').replace('*', ' ')

        ## tick_result to np.array.reshape
        raw_data = tick_result.split()
        num_tick = len(tick_result.splitlines())
        tick_len = len(tick_result.splitlines()[0].split())
        logger.info('reshape check, num of tick: {}'.format(num_tick))
        logger.info('reshape check, tick row_data[:{}]: {}'.format(tick_len, raw_data[:tick_len]))
        assert len(raw_data) / tick_len == num_tick, 'reshape check np.array.reshape(2-dim, -1) fail'
        tick_array = np.array(raw_data).reshape(num_tick, -1)

        ## found first tick time: 150000-050000, 084500-134500
        logger.debug('first tick:  {}'.format(tick_array[0]))
        if datetime.strptime(tick_array[0, 3], '%H%M%S').hour == 15:
            stime = datetime.strptime(tick_array[0, 0] + '150000', '%Y%m%d%H%M%S') + timedelta(minutes=1)
        else:
            stime = datetime.strptime(tick_array[0, 0] + '084500', '%Y%m%d%H%M%S') + timedelta(minutes=1)

        req = list()
        tmp = list()
        tick_len = len(tick_array)
        for i, tick in enumerate(tick_array, 1):
            ## push tick to list
            t = datetime.strptime(tick[0] + tick[3], '%Y%m%d%H%M%S')
            if t >= stime + timedelta(minutes=-1) and t < stime or t == t.replace(
                    hour=5, minute=0, second=0, microsecond=0) or t == t.replace(
                        hour=13, minute=45, second=0, microsecond=0):
                logger.debug('append {} to tmp list'.format(tick))
                tmp.append(tuple(tick))
                if i < tick_len:
                    continue
            ## cal one min result
            if not tmp:
                tmp.append(tuple(tick))
                stime = datetime.strptime(tick[0] + '{}00'.format(tick[3][:4]),
                                          '%Y%m%d%H%M%S') + timedelta(minutes=1)
                continue
            req_array = np.array(tmp)
            logger.debug(req_array)
            Date = stime.strftime('%Y/%m/%d')
            Time = stime.strftime('%H:%M:%S')
            Open = req_array[:, 4][0].astype('int')
            High = req_array[:, 4].astype('int').max(axis=0)
            Low = req_array[:, 4].astype('int').min(axis=0)
            Close = req_array[:, 4][-1].astype('int')
            Vol = req_array[:, 5].astype('int').sum(axis=0) / 2
            out = (Date, Time, Open, High, Low, Close, Vol)
            logger.debug(out)  # change to info
            req.append(tuple(out))
            ## init tmp list
            if i < tick_len:
                tmp = list()
                tmp.append(tick)
                stime += timedelta(minutes=1)
                if t == datetime.strptime(tick[0] + '084500', '%Y%m%d%H%M%S'):
                    stime = datetime.strptime(tick[0] + '084500', '%Y%m%d%H%M%S') + timedelta(minutes=1)
                logger.debug('next time step: {}'.format(stime))

            ## use progressbar
            k = float(i + 1) / tick_len * 100
            step = tick_len // 32
            _str = '=' * (i // step) + '>' + ' ' * (32 - (i // step))
            sys.stdout.write('\r[%s][%.1f%%]' % (_str, k))
            sys.stdout.flush()
        sys.stdout.write('\n')
        logger.info('num of sql data: {}'.format(len(req)))

        ## query to DB
        conn = sqlite3.connect(os.path.join(os.path.abspath(self.path), self.db_name))
        cursor = conn.cursor()
        fut = 'TX' if fut not in fex_info['symbol'] else fut

        ## delete old data
        SQL_Detete = "DELETE FROM tw{} WHERE Date=\'{}\' and Time<=\'{}\';".format(fut, *req[-1][:2])
        cursor.execute(SQL_Detete)
        if req[0][1] == '15:01:00':
            SQL_Detete1 = "DELETE FROM tw{} WHERE Date=\'{}\' and Time>=\'{}\';".format(fut, *req[0][:2])
            SQL_Detete2 = "DELETE FROM tw{} WHERE Date=\'{}\' and Time<=\'{}\';".format(fut, *req[839][:2])
            cursor.execute(SQL_Detete1)
            cursor.execute(SQL_Detete2)
        conn.commit()

        ## insert new data
        SQL = "INSERT INTO tw{} VALUES (?,?,?,?,?,?,?);".format(fut)
        for i in range(len(req)):
            logger.debug(req[i])
            cursor.execute(SQL, req[i])
        conn.commit()
        conn.close()

    def export_sql_to_txt(self):

        fex_info = self.fex_dict_item
        ## vaild args input
        logger.debug("-e args input '{}'".format(args.export))
        if len(args.export) != 2:
            assert False, "error -e args input '{}'".format(args.export)
        fut = 'TX' if args.export[0] not in fex_info['symbol'] else args.export[0]
        interval = 300 if args.export[1] not in ['1', '5', '15', '30', '60', '300'] else int(args.export[1])
        logger.info("(fut, interval, date) = ('{}', {}, {})".format(fut, interval, start_D))
        ## read DB via sqlite3
        conn = sqlite3.connect(os.path.join(os.path.abspath(self.path), self.db_name))
        cursor = conn.cursor()

        def loop_for_oneday(date):
            content = ''
            SQL = "SELECT * FROM tw{!s} WHERE Date=\'{!s}\' and Time>\'08:45:00\' and Time<=\'13:45:00\' ORDER BY Date, Time;".format(
                fut, date)
            cursor.execute(SQL)
            if interval == 1:
                req = cursor.fetchall()
                for i in req:
                    content += '{},{},{},{},{},{},{}\n'.format(*i)
                logger.debug(export_str)
            else:
                while True:
                    req = cursor.fetchmany(interval)
                    if not req:
                        break
                    else:
                        req_array = np.array(req)
                        logger.debug(req_array)
                        Date = req_array[:, 0][-1]
                        Time = req_array[:, 1][-1]
                        Open = req_array[:, 2][0].astype('int')
                        High = req_array[:, 3].astype('int').max(axis=0)
                        Low = req_array[:, 4].astype('int').min(axis=0)
                        Close = req_array[:, 5][-1].astype('int')
                        Vol = req_array[:, 6].astype('int').sum(axis=0)
                        out = (Date, Time, Open, High, Low, Close, Vol)
                        content += '{},{},{},{},{},{},{}\n'.format(*out)
            logger.debug(content)
            return content

        d = start_D
        export_str = 'Date,Time,Open,High,Low,Close,Volume'
        date_string = start_D.strftime('%Y%m%d') + "-" + end_D.strftime(
            '%Y%m%d') if start_D != end_D else start_D.strftime('%Y%m%d')
        os.system('echo "{}" > {}_{}'.format(export_str, fut, date_string))
        while not d > end_D:
            #export_str += loop_for_oneday(d.strftime('%Y/%m/%d'))
            tmp = loop_for_oneday(d.strftime('%Y/%m/%d'))
            if bool(tmp.strip()):
                print(d)
                os.system('echo "{}" >> {}_{}'.format(tmp.strip(), fut, date_string))
            d += timedelta(days=1)
        #date_string = start_D.strftime('%Y%m%d') + "-" + end_D.strftime(
        #    '%Y%m%d') if start_D != end_D else start_D.strftime('%Y%m%d')
        #os.system('echo "{}" > {}_{}'.format(export_str, fut, date_string))
        logger.info('out file: {}_{}'.format(fut, date_string))
        ''' output 1 year json file '''
        logger.info('start output 1.5 year json file: {} and {}'.format(fut, end_D.strftime('%Y%m%d')))
        interval = 300
        data = list()
        if not os.path.isfile('FUT_{}.json'.format(fut)):
            #d = end_D + timedelta(weeks=-80)
            d = datetime.strptime('2020/01/01', '%Y/%m/%d')
            while not d > end_D:
                result = loop_for_oneday(d.strftime('%Y/%m/%d')).strip()
                logger.info('{}'.format(d))
                if result:
                    i = result.split(',')
                    t = datetime.strptime(i[0], '%Y/%m/%d') + timedelta(hours=23)
                    t_mk = int(time.mktime(t.timetuple())) * 1000
                    data.append([t_mk] + list(map(int, i[2:])))
                d += timedelta(days=1)
        else:
            with open('FUT_{}.json'.format(fut), 'r') as f:
                data = json.load(f, encoding='utf-8')
            result = loop_for_oneday(end_D.strftime('%Y/%m/%d')).strip()
            last_ts = data[-1][0]
            if result:
                #data.pop(0)
                i = result.split(',')
                t = datetime.strptime(i[0], '%Y/%m/%d') + timedelta(hours=23)
                t_mk = int(time.mktime(t.timetuple())) * 1000
                append_flag = bool(str(t_mk) not in str(data))
                # check result: end date data not in FUT_{}.json
                if str(t_mk) not in str(data):
                   data.append([t_mk] + list(map(int, i[2:])))
        with open('FUT_{}.json'.format(fut), 'w') as f:
            json.dump(data, f, indent=4)

def get_logging_moduel():
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)

def valid_date(date_text):
    date_interval = date_text.split('-')

    try:
        start_date = datetime.strptime(date_interval[0], '%Y%m%d')
        ## set end_date if not setting
        end_date = today
        if len(date_interval) == 2:
            end_date = datetime.strptime(date_interval[1], '%Y%m%d')

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
    ## init config
    today = datetime.today().replace(minute=0, hour=0, second=0, microsecond=0)
    items = ('fut_rpt', 'opt_rpt')
    ## set logging moduel for debug
    get_logging_moduel()
    ## set args for mining_rpt control
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d',
        '--date',
        type=str,
        default=today.strftime('%Y%m%d'),
        help='download rpt from $DATE~today, tpye=str ex: 20180101-20180102')
    parser.add_argument(
        "-e",
        "--export",
        nargs='+',
        type=str,
        default=None,
        help="Future symbol(TX) Interval(300), tpye=str ex: -e TX 300, use -d Date1-Date2")
    parser.add_argument(
        '--upload-recover',
        dest='recover',
        default=False,
        action='store_true',
        help='switch for new rpt instead of gdrive exist.')
    args = parser.parse_args()

    (start_D, end_D) = valid_date(args.date)
    logger.info('{!s}'.format(args))

    if args.export != None:
        mining_rpt().export_sql_to_txt()
        sys.exit()

    ## every daily
    i = start_D
    while not i > end_D:
        date = i.strftime('%Y_%m_%d')
        logger.info('Start mining for {}'.format(date))

        for j in items:
            daily_mining = mining_rpt(date=date, item=j)

            daily_mining.download_rpt()
            daily_mining.upload_gdrive()
            if j == 'fut_rpt':
                daily_mining.parser_rpt_to_DB('TX')
                daily_mining.parser_rpt_to_DB('MTX')
        i += timedelta(days=1)
    '''
    # unzip_all2rptdir for once
    for i in items[:0]:
        daily_mining=mining_rpt(i)
        daily_mining.unzip_all2rptdir()
        pass
    '''
