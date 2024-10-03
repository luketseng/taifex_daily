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

    def parser_data(self, item, element):
        if item == "Fut" or item == "OP":
            printhere_element = element
           # # Get all the child elements with class "section"
           # section_elements = printhere_element.find_elements_by_class_name('section')

           # # Access the third "section" element (index starts from 0)
           # third_section_element = section_elements[-1]
           # #print(third_section_element)

            # Use XPath to find the specific p element (fourth one)
            try:
                fourth_p_element = printhere_element.find_element_by_xpath('//*[@id="printhere"]/div[4]/p[1]')
            except:
                print(f"The date not find the table and sys.exit")
                sys.exit()

            # Locate the span element within the p element
            span_element = fourth_p_element.find_element_by_class_name('right')

            # Extract text content from the span
            span_text_date = span_element.get_attribute('textContent')[2:]
            print("print date: ", span_text_date)
            data = [f"{span_text_date}"]

           # ### get title region
           # title_element = printhere_element.find_element_by_xpath('//*[@id="printhere"]/div[4]/div[2]/table/thead')
           # # Extract table header rows
           # header_rows = title_element.find_elements_by_xpath('tr')

           # # Initialize empty data structure for JSON
           # data = []

           # # Iterate through header rows
           # for row in header_rows:
           #     # Extract header cells (th elements)
           #     header_cells = row.find_elements_by_tag_name('th')

           #     # Initialize empty row data
           #     row_data = []

           #     # Iterate through header cells
           #     for cell in header_cells:
           #         # Extract cell content (text within <div> if present)
           #         #cell_content = cell.find_element_by_tag_name('div').text if cell.find_element_by_tag_name('div') else cell.text
           #         try:
           #             cell_content = cell.find_element_by_tag_name('div').text
           #         except:
           #             cell_content = cell.text  # Use cell text if no div found

           #         # Remove any leading/trailing whitespace and newlines
           #         cell_content = cell_content.strip().replace("\n", "")

           #         # Append cell content to row data
           #         row_data.append(cell_content)

           #     # Append row data to main data structure
           #     data.append(row_data)

           # # Convert data to JSON with Unicode encoding (default UTF-8)
           # json_data = json.dumps(data, indent=4, ensure_ascii=False)
           # print(json_data)

            ### get content region
            content_element = printhere_element.find_element_by_xpath('//*[@id="printhere"]/div[4]/div[2]/table/tbody')
            # Extract table content rows
            content_rows = content_element.find_elements_by_xpath('tr')

            # Initialize empty data structure for JSON
           # data = []

            # Iterate through content rows
            for row in content_rows:
                # Extract content cells (th elements)
                content_cells = row.find_elements_by_tag_name('td')

                # Initialize empty row data
                row_data = []

                # Iterate through content cells
                for cell in content_cells:
                    # Extract cell content (text within <div> if present)
                    #cell_content = cell.find_element_by_tag_name('div').text if cell.find_element_by_tag_name('div') else cell.text
                    divs = []
                    try:
                        #cell_content = cell.find_element_by_tag_name('div').text
                        #print(cell_content)

                        # Find all <div> elements within the cell
                        divs = cell.find_elements_by_tag_name('div')
                        #print(divs)

                        # If there are any divs, extract the text from the last one
                        if divs:
                            if item == "Fut":
                                cell_content = divs[0].text  # Access the last element using -1
                            if item == "OP":
                                cell_content = divs[-1].text  # Access the last element using -1
                        else:
                            assert False
                    except:
                        cell_content = cell.text  # Use cell text if no div found

                    # Remove any leading/trailing whitespace and newlines
                    cell_content = cell_content.strip().replace("\n", "")

                    # Append cell content to row data
                    row_data.append(cell_content)

                # Append row data to main data structure
                data.append(row_data)

            # Convert data to JSON with Unicode encoding (default UTF-8)
            json_data = json.dumps(data, indent=4, ensure_ascii=False)
            print(json_data)

            # Return the JSON string
            return data


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
        print("item", item)
        with Chrome(options=options) as driver:
            self.lines_data = list()
            if item == 'SPOT':
                driver.get(
                    #'https://www.twse.com.tw/fund/BFI82U?response=html&dayDate={}&weekDate=&monthDate=&type=day'
                    'https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate={}&response=html'
                    .format(date.replace('/', '')))
                e = driver.find_elements_by_tag_name('tr')
                for i in e:
                    print(i.text.split())
                    self.lines_data.append(i.text.split())

            if item == 'Fut' or item == 'OP':
                if item == 'Fut':
                    #driver.get('https://www.taifex.com.tw/cht/3/futContractsDateExcel')
                    # access to get fut web
                    taifex_fut_contracts_date_url = 'https://www.taifex.com.tw/cht/3/futContractsDate'
                    driver.get(taifex_fut_contracts_date_url)

                if item == 'OP':
                    #driver.get('https://www.taifex.com.tw/cht/3/callsAndPutsDateExcel')
                    taifex_fut_contracts_date_url = 'https://www.taifex.com.tw/cht/3/callsAndPutsDate'
                    driver.get(taifex_fut_contracts_date_url)

                # Locate the element with id="queryDate"
                query_date_element = driver.find_element_by_id('queryDate')

                # Get the current value of the element
                current_date = query_date_element.get_attribute('value')
                print('Current date:', current_date)

                # Modify the value of the element
                mod_date = date if date else current_date
                query_date_element.clear()
                query_date_element.send_keys(mod_date)

                # Get the current value of the element again
                current_date = query_date_element.get_attribute('value')
                print('Current date after modify:', current_date)

                # Locate the element with id="button"
                button_element = driver.find_element_by_id('button')

                # Click the button
                button_element.click()

                # Locate the parent element with id="printhere"
                printhere_element = driver.find_element_by_id('printhere')

                self.lines_data = self.parser_data(item, printhere_element)

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
            if self.date not in self.lines_data:
                print('Not get {} {} line data'.format(self.date, item))
                sys.exit()
            (date, COM, II, PC) = ('', '', '', '')

            date = self.lines_data[0]
            # loop table content rows
            print("loop table content rows")
            part_lines_data = self.lines_data[1:13] if item == 'Fut' else self.lines_data[1:7]
            for line_data in part_lines_data:
                # 16 cell mean have COM info
                if len(line_data) == 15:
                    COM = cht_maps.get(line_data[1])
                    if not COM:
                        print(f"Not support {line_data[1]}")
                        continue
                    II = cht_maps[line_data[2]]
                elif len(line_data) == 13:
                    II = cht_maps[line_data[0]]
                elif len(line_data) == 16:
                    COM = cht_maps.get(line_data[1])
                    if not COM:
                        print(f"Not support {line_data[1]}")
                        continue
                    PC = cht_maps.get(line_data[2])
                    II = cht_maps[line_data[3]]
                elif len(line_data) == 14:
                    PC = cht_maps.get(line_data[0])
                    II = cht_maps[line_data[1]]
                else:
                    continue
                value = ','.join([i.replace(',', '') for i in line_data[-12:]])

                if item == 'Fut':
                    line_str = '({},{},{},{}),'.format(repr(date), repr(COM), repr(II), value)
                elif item == 'OP':
                    line_str = '({},{},{},{},{}),'.format(repr(date), repr(COM), repr(PC), repr(II), value)
                print(line_str)
                lines_str += line_str
            #assert False

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
            #print(json.dumps(data_table))
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
    #tools.date = date(2024, 4, 16).strftime('%Y/%m/%d')
    tools.insert_data_from_url(item='Fut', date=tools.date)
    tools.insert_data_from_url(item='OP', date=tools.date)
    tools.insert_data_from_url(item='SPOT', date=tools.date)
    tools.strategy_out_put()
