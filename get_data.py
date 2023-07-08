#!/usr/bin/python3
# -*- coding: utf-8 -*-

from selenium.webdriver import Chrome
from selenium.webdriver import ChromeOptions as Options
#from selenium.webdriver import Firefox
#from selenium.webdriver.firefox.options import Options
import sys, os
import re
import sqlite3
import chardet
import json
import numpy as np
import time
from datetime import datetime, timedelta, date

class PraserDataTools():
    lines_data = list()
    date = ''
    cht_maps = {
        '臺股期貨': 'TX',
        '電子期貨': 'TE',
        '金融期貨': 'TF',
        '小型臺指期貨': 'MTX',
        '臺指選擇權': 'TXO',
        '買權': 'CALL',
        '賣權': 'PUT',
        '外資及陸資(不含外資自營商)': 'FOR',
        '外資及陸資': 'FOR',
        '外資': 'FOR',
        '外資自營商': 'FOR_D',
        '投信': 'INV',
        '自營商(自行買賣)': 'DEA',
        '自營商(避險)': 'DEA_H',
        '自營商': 'DEA'}

    def insert_data_from_csv(self, item=None):
        '''item [Fut|OP]'''
        '''check sys default enconding'''
        #print(sys.getdefaultencoding())

        #with open('2years_data.csv', 'rb') as f:
        #    a=f.readline()
        #    print(chardet.detect(a), type(a))
        #    print(a.decode('big5'))
        #with open('2years_data.csv', 'r', encoding='big5') as f:
        with open('{}.csv'.format(item), 'r', encoding='big5') as f:
            str_line = f.read()
            cht_maps = self.cht_maps
            for k, v in cht_maps.items():
                str_line = str_line.replace(k, v)

            with sqlite3.connect('II_DB.db') as conn:
                cursor = conn.cursor()
                lines_str = ''
                for line in str_line.split():
                    temp = line.split(',')
                    if temp[1] in set(cht_maps.values()):
                        title = repr(temp[:-12]).replace(' ', '').replace('[', '').replace(']', '')
                        value = ','.join(temp[-12:])
                        line_str = '({},{}),'.format(title, value)
                        #print(line_str)
                        lines_str += line_str
                SQL = "INSERT INTO II_{} VALUES {};".format(item, lines_str.strip(','))
                cursor.execute(SQL)
                conn.commit()

    def insert_data_from_url(self, item=None, date=''):
        '''item [Fut|SPOT|OP]'''
        self.item = item
        if date != '':  self.date = date
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-features=VizDisplayCompositor')
        #options.add_argument('--disable-dev-shm-usage')
        '''open driver via different browser
            with Firefox(firefox_options=options) as driver:'''
        with Chrome(options=options) as driver:
            self.lines_data = list()
            if item == 'SPOT':
                driver.get(
                    #'https://www.twse.com.tw/fund/BFI82U?response=html&dayDate={}&weekDate=&monthDate=&type=day'
                    'https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={}&response=html'
                    .format(date.replace('/', '')))
                e = driver.find_elements_by_tag_name('tr')
                for i in e:
                    #print(i.text.split())
                    self.lines_data.append(i.text.split())

            if item == 'Fut' or item == 'OP':
                if item == 'Fut':
                    driver.get('https://www.taifex.com.tw/cht/3/futContractsDateExcel')
                    col_num = list(range(3)) + list(range(3, 15))
                    split_size = [0, 2, 6, 9, 11, 15, 17, 21, 23, 27, 29, 33, 35, 39, 41, 45]
                if item == 'OP':
                    driver.get('https://www.taifex.com.tw/cht/3/callsAndPutsDateExcel')
                    col_num = list(range(3)) + list(range(3, 9))
                    split_size = [0, 2, 6, 8, 11, 15, 17, 21, 23, 27, 29, 33, 35, 39, 41, 45]

                e = driver.find_element_by_xpath('//*[@id="printhere"]/div[2]/table/tbody/tr[1]/td/p/span[2]')
                date_flag = '{}\t{}'.format(driver.title, e.text)
                #print(repr(date_flag.split()))
                self.lines_data.append(date_flag.split())
                e = driver.find_elements_by_tag_name('tbody')[1]
                for i in col_num:
                    row = e.find_elements_by_tag_name('tr')[i]
                    if i != 2:
                        line = row.text.split()
                    else:
                        line_str = row.text.replace('\n', '')
                        line = [line_str[split_size[i]:ele] for i, ele in enumerate(split_size[1:], 0)]
                    #print(line)
                    self.lines_data.append(line)
            '''use data_line_list parser'''
            insert_data = self.pre_insert_db(item)
            #print(insert_data)
            if insert_data:
                self.insert_daily_data(*insert_data)

    def pre_insert_db(self, item=None):
        if not self.lines_data:
            print('self.lines_data is None')
            sys.exit()

        cht_maps = self.cht_maps
        lines_str = ''
        if item == 'Fut' or item == 'OP':
            '''check date data is today date'''
            if self.date not in self.lines_data[0][1]:
                print('Not get {} {} line data'.format(self.date, item))
                sys.exit()
            (date, COM, II, PC) = ('', '', '', '')
            for line_data in self.lines_data[:1] + self.lines_data[4:]:
                if len(line_data) == 2:
                    date = line_data[1][2:]
                    self.date = date
                else:
                    line_str = ''
                    if len(line_data[:-12]) == 3:
                        COM = cht_maps[line_data[1]]
                        II = cht_maps[line_data[2]]
                    elif len(line_data[:-12]) == 4:
                        COM = cht_maps[line_data[1]]
                        PC = cht_maps[line_data[2]]
                        II = cht_maps[line_data[3]]
                    elif len(line_data[:-12]) == 2:
                        PC = cht_maps[line_data[0]]
                        II = cht_maps[line_data[1]]
                    elif len(line_data[:-12]) == 1:
                        II = cht_maps[line_data[0]]

                    value = ','.join([i.replace(',', '') for i in line_data[-12:]])
                    if item == 'Fut':
                        line_str = '({},{},{},{}),'.format(repr(date), repr(COM), repr(II), value)
                    elif item == 'OP':
                        line_str = '({},{},{},{},{}),'.format(repr(date), repr(COM), repr(PC), repr(II), value)
                    #print(line_str)
                    lines_str += line_str
        elif item == 'SPOT':
            date = ''
            for line_data in self.lines_data[:1] + self.lines_data[2:-1]:
                if len(line_data) == 2:
                    match = re.match(r'(\d+)年(\d+)月(\d+)日', line_data[0])
                    if match:
                        date = '{}/{}/{}'.format(int(match.group(1)) + 1911, match.group(2), match.group(3))
                        '''check date data is today date'''
                        if self.date not in date:
                            print('Not get {} {} line data'.format(self.date, item))
                            sys.exit()
                else:
                    II = cht_maps[line_data[0]]
                    value = ','.join([i.replace(',', '') for i in line_data[-3:]])
                    line_str = '({},{},{}),'.format(repr(date), repr(II), value)
                    #print(line_str)
                    lines_str += line_str
        return (date, lines_str.strip(','))

    def insert_daily_data(self, date, data):
        path = os.path.join(os.path.dirname(__file__), 'II_DB.db')
        table_name = 'II_{}'.format(self.item)
        with sqlite3.connect(path) as conn:
            cursor = conn.cursor()
            SQL = "DELETE FROM {} WHERE Date='{}';".format(table_name, date)
            cursor.execute(SQL)
            conn.commit()
            SQL = "INSERT INTO {} VALUES {};".format(table_name, data)
            cursor.execute(SQL)
            conn.commit()

    def sqlite3_fetchall(self, db_path, query):
        '''get req of query on db'''
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            requests = cursor.fetchall()
        return requests

    def strategy_out_put(self, date=None):
        # SELECT Date, OI_Net_Contract FROM II_Fut WHERE Fut='TX' and Institutional='FOR';
        # SELECT * from twTX WHERE Date='2020/03/20'
        # SELECT Date, OI_B_Contract, OI_S_Contract, OI_B_Amount, OI_S_Amount FROM II_OP WHERE Institutional='FOR';
        # SELECT Date, TR_Net_Amount FROM II_SPOT WHERE Date='2014/06/09';
        # export my strategy
        #date='2020/03/20'
        date = self.date if not date else date
        #date_age = (datetime.strptime(date, '%Y/%m/%d') + timedelta(weeks=-80)).strftime('%Y/%m/%d')
        date_age = datetime.strptime('2020/01/01', '%Y/%m/%d').strftime('%Y/%m/%d')
        path_fut = os.path.join(os.path.dirname(__file__), 'FCT_DB.db')
        path = os.path.join(os.path.dirname(__file__), 'II_DB.db')
        '''get TX fut close'''
        data_table = dict()
        qurey = 'SELECT Date, Time, Close FROM twTX WHERE Date>="{!s}" and (Time="13:30:00" or Time="13:45:00") ORDER BY Date, Time;'.format(
            date_age)
        req = self.sqlite3_fetchall(path_fut, qurey)
        for (k, *v) in req:
            data_table[k] = v[1:]
        '''get TX'''
        qurey = "SELECT Date, OI_Net_Contract FROM II_Fut WHERE Fut='TX' and Date>='{}' and Institutional='FOR';".format(
            date_age)
        req = self.sqlite3_fetchall(path, qurey)
        tmp = 0
        for i, v in enumerate(req):
            if i > 0:
                diff = v[1] - tmp
                total_v = diff * data_table[v[0]][0] * 200
                total_s = float('{:.2f}'.format(total_v / 100000000))
            else:
                diff = None
                total_s = None
            data_table[v[0]].extend([v[1], diff, total_s])
            tmp = v[1]
        '''get SPOT'''
        #qurey = "SELECT Date, TR_Net_Amount FROM II_SPOT WHERE Date>='{}' and Institutional='FOR';".format(
        qurey = "SELECT Date, Sum(TR_Net_Amount) FROM II_SPOT WHERE Date>='{}' and Institutional like 'FOR%' GROUP BY Date;".format(
            date_age)
        req = self.sqlite3_fetchall(path, qurey)
        for v in req:
            value = float('{:.2f}'.format(v[1] / 100000000))
            data_table[v[0]].append(value)
        with sqlite3.connect(path) as conn:
            cursor = conn.cursor()
            '''get OP'''
            SQL = "SELECT Date, OI_B_Contract, OI_S_Contract, OI_B_Amount, OI_S_Amount FROM II_OP WHERE Institutional='FOR' and  Date>='{}';".format(
                date_age)
            cursor.execute(SQL)
            while 1:
                req = cursor.fetchmany(2)
                if not req:
                    break
                value = float('{:.2f}'.format(
                    ((req[-2][3] + req[-1][4]) - (req[-2][4] + req[-1][3])) / 100000))
                data_table[req[-2][0]].append(value)
            output_data = list()
            for k, v in data_table.items():
                t = datetime.strptime(k, '%Y/%m/%d') + timedelta(hours=23)
                t_mk = int(time.mktime(t.timetuple())) * 1000
                if v[3]:
                    output_data.append([t_mk] + v[3:6] + [v[1]])
            with open('data.json', 'w') as f:
                json.dump(output_data, f, indent=4)
            '''Get MTX strategy'''
            SQL = "SELECT Date, OI_Net_Contract FROM II_Fut where Date>='{}' and Fut='MTX'".format(date_age)
            # cursor.fetchall() will fetch all data
            cursor.execute(SQL)
            export_info = list()
            i = 1
            while 1:
                req = cursor.fetchmany(3)
                if not req:
                    break
                else:
                    req_array = np.array(req)
                    II_contract = req_array[:, 1]
                    if req_array[:, 0][0] == req_array[:, 0][1] and req_array[:, 0][0] == req_array[:, 0][-1]:
                        Date = req_array[:, 0][-1]
                        Sum = req_array[:, 1].astype('int').sum(axis=0)
                    if i > 4:
                        req_output = np.array(export_info)
                        tmp = list(req_output[:, 4][-4:].astype('int'))
                        Avg = np.average(tmp + [int(Sum)])
                    else:
                        Avg = 0
                    if i > 5:
                        req_output = np.array(export_info)
                        pre_Avg = float(req_output[:, 5][-1])
                        BS = round(Sum - pre_Avg, 1)
                    else:
                        BS = 0

                    dow = datetime.strptime(Date, '%Y/%m/%d').isoweekday()
                    export_info.append([Date] + list(II_contract) + [Sum, Avg, BS, dow])
                    i += 1
            output_data = list()
            for i in export_info:
                t = datetime.strptime(i[0], '%Y/%m/%d') + timedelta(hours=23)
                t_mk = int(time.mktime(t.timetuple())) * 1000
                output_data.append([t_mk] + i[1:4] + i[5:6])
            with open('data_MTX.json', 'w') as f:
                json.dump(output_data, f, indent=4)

if __name__ == '__main__':
    tools = PraserDataTools()
    '''insert data from csv file(Fut, OP)'''
    #tools.insert_data_from_csv(item='Fut')
    #tools.insert_data_from_csv(item='OP')
    #sys.exit()
    '''insert data from url(SPOT)'''
    #date=date(2020, 3, 23)
    #while date < date.today():
    #    date_str=date.strftime('%Y/%m/%d')
    #    print(date_str)
    #    tools.insert_data_from_url(item='SPOT', date=date_str)
    #    date+=timedelta(days=1)
    #sys.exit()
    '''daily to do'''
    '''insert daily data to DB'''
    tools.date = date.today().strftime('%Y/%m/%d')
    #tools.date = date(2023, 3, 20).strftime('%Y/%m/%d')
    tools.insert_data_from_url(item='Fut')
    tools.insert_data_from_url(item='OP')
    tools.insert_data_from_url(item='SPOT')
    tools.strategy_out_put()
