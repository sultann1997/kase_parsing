import requests
from bs4 import BeautifulSoup
import csv
from urllib.request import urlopen
import urllib
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import os
import pathlib
import requests
import ssl

#script with cx_Oracle with written username/password and dsn
import ora_connect
from datetime import datetime

# function to donwload and unzip files
def download_unzip(url, extract_to='.'):
    try:
        http_response = urlopen(url)
        zipfile = ZipFile(BytesIO(http_response.read()))
        zipfile.extractall(path=extract_to)
    except:
        print('HTTPError')  # Website contains empty links
        pass


def connecting_excel_sheets(excel_file):
    if str(excel_file).endswith('.xlsx'):
        excel_engine = 'openpyxl'
    else:
        excel_engine = 'xlrd'
    my_excel = pd.ExcelFile(excel_file)
    new_df = pd.DataFrame()
    for sheet_name in my_excel.sheet_names:
        temp_df = pd.read_excel(my_excel, sheet_name, engine=excel_engine)
        if temp_df.empty:
            pass
        else:
            columns_index = temp_df[temp_df.columns[0]].ne('Р’в„– Р“Р‡/Р“Р‡').idxmin() #find row with column names
            headers = temp_df.iloc[columns_index]
            if columns_index != 0:
                non_na_index = 3
            else:
                non_na_index = 2
            df_index = temp_df[temp_df.columns[0]].notna(
            ).cumsum().eq(non_na_index).idxmax()
            temp_df = temp_df.iloc[df_index:]
            temp_df.columns = headers
            temp_df[temp_df.columns[-1]] = headers[-1]
            temp_df = temp_df.loc[:, temp_df.columns.notnull()]
            for column in temp_df.columns:
                try:
                    if column.startswith('Р“С’Р“В»Р“В­Р“В®Р“В·Р“В­Р“В Р“С— Р“В¶Р“ТђР“В­Р“В '):
                        temp_list = column.split(',')
                        temp_df.rename({column: "Р“С’Р“В Р“В±Р“В·Р“ТђР“Р†Р“В­Р“В Р“С— Р“В¶Р“ТђР“В­Р“В "},
                                    axis=1, inplace=True)
                        if 'Р“вЂ¦Р“В¤Р“РЃР“В­Р“РЃР“В¶Р“В  Р“РЃР“В§Р“В¬Р“ТђР“В°Р“ТђР“В­Р“РЃР“С— Р“В¶Р“ТђР“В­Р“В»' in temp_df.columns:
                            pass
                        else:
                            temp_df['Р“вЂ¦Р“В¤Р“РЃР“В­Р“РЃР“В¶Р“В  Р“РЃР“В§Р“В¬Р“ТђР“В°Р“ТђР“В­Р“РЃР“С— Р“В¶Р“ТђР“В­Р“В»'] = temp_list[1]
                except AttributeError:
                    print(column)
            if 'Р“вЂљР“РЃР“В¤ Р“В¶Р“ТђР“В­Р“В­Р“В®Р“В© Р“РЋР“С–Р“В¬Р“В Р“Р€Р“РЃ' in temp_df.columns:
                pass
            else:
                temp_df['Р“вЂљР“РЃР“В¤ Р“В¶Р“ТђР“В­Р“В­Р“В®Р“В© Р“РЋР“С–Р“В¬Р“В Р“Р€Р“РЃ'] = sheet_name
            needed_columns = ['Р“вЂ™Р“В®Р“В°Р“Р€Р“В®Р“СћР“В»Р“В© Р“Р„Р“В®Р“В¤', 'ISIN', 'Р“РЊР“в‚¬Р“РЊ', 'Р“вЂљР“РЃР“В¤ Р“В¶Р“ТђР“В­Р“В­Р“В®Р“В© Р“РЋР“С–Р“В¬Р“В Р“Р€Р“РЃ',  "Р“Р‰Р“В°Р“В Р“Р†Р“Р„Р“В®Р“Тђ Р“В­Р“В Р“РЃР“В¬Р“ТђР“В­Р“В®Р“СћР“В Р“В­Р“РЃР“Тђ Р“Р…Р“В¬Р“РЃР“Р†Р“ТђР“В­Р“Р†Р“В ",
                            "Р“С’Р“В Р“В±Р“В·Р“ТђР“Р†Р“В­Р“В Р“С— Р“В¶Р“ТђР“В­Р“В ", "Р“вЂ¦Р“В¤Р“РЃР“В­Р“РЃР“В¶Р“В  Р“РЃР“В§Р“В¬Р“ТђР“В°Р“ТђР“В­Р“РЃР“С— Р“В¶Р“ТђР“В­Р“В»"]
            for column in needed_columns:
                if column in temp_df.columns:
                    pass
                else:
                    temp_df[str(column)] = " "
            temp_df = temp_df[needed_columns]
            temp_df['ISIN'] = temp_df.apply(
                lambda x: x['Р“РЊР“в‚¬Р“РЊ'] if x['ISIN'] == '-' else x['ISIN'], axis=1)
            temp_df.drop(['Р“РЊР“в‚¬Р“РЊ'], axis=1, inplace=True)
            new_df = new_df.append(temp_df)
            new_df.columns = ['TRADE_CODE', 'ISIN', 'TYPE_VALUE', 'SHORT_ISSUER_NAME', 'CALC_PRICE', 'PRICE_UNIT']
    return new_df


def parse(URL='https://kase.kz/ru/documents/marketvaluation/'):
    ssl._create_default_https_context = ssl._create_unverified_context

    df_kase = pd.DataFrame()  # creating Empty DataFrame
    script_path = pathlib.Path(pathlib.Path.cwd(), 'files')

    if requests.get(URL, verify = False).status_code == 200:
        print('Connected')
    else:
        raise ValueError('Could not connect to server')

    soup = BeautifulSoup(requests.get(URL, verify = False).text, features='html.parser')
    items = soup.select('div[id*="a202"]')
    hrefs = [i.find_all('a') for i in items]

    file_list = ora_connect.OracleTable().returnDataframe('select distinct file_name from dwh.kase_parsed_v2')
    file_list = file_list['FILE_NAME'].to_list()

    months_dict = {
        'Р“С—Р“В­Р“СћР“В Р“В°Р“С—': "01",
        "Р“Т‘Р“ТђР“СћР“В°Р“В Р“В«Р“С—": "02",
        "Р“В¬Р“В Р“В°Р“Р†Р“В ": "03",
        "Р“В Р“Р‡Р“В°Р“ТђР“В«Р“С—": "04",
        "Р“В¬Р“В Р“С—": "05",
        "Р“РЃР“С•Р“В­Р“С—": "06",
        "Р“РЃР“С•Р“В«Р“С—": "07",
        "Р“В Р“СћР“Р€Р“С–Р“В±Р“Р†Р“В ": "08",
        "Р“В±Р“ТђР“В­Р“Р†Р“С—Р“РЋР“В°Р“С—": "09",
        "Р“В®Р“Р„Р“Р†Р“С—Р“РЋР“В°Р“С—": "10",
        "Р“В­Р“В®Р“С—Р“РЋР“В°Р“С—": "11",
        "Р“В¤Р“ТђР“Р„Р“В Р“РЋР“В°Р“С—": "12",
    }

    for year in hrefs:
        for a in year:
            if len(str(a['href'])) > 30 and len(a.text)>10:  # Some links are broken and are too short
                if a.text.find('.') == -1:
                    dt_str = a.text[a.text.rfind(' Р“В­Р“В  ')+4:][:-5].split(' ') #take the string between value 'Р“В­Р“В ' and 'Р“Р€Р“В®Р“В¤Р“В ' to get date
                    dt_str[1] = months_dict[dt_str[1]]
                    date = ''.join(dt_str)
                    date = datetime.strptime(date, '%d%m%Y').strftime('%d-%m-%Y')
                else:
                    dt_str = a.text[a.text.rfind(' Р“В­Р“В  ')+4:]
                    date = datetime.strptime(dt_str, '%d.%m.%Y').strftime('%d-%m-%Y')
                file_name = a['href'].rsplit('/', 1)[-1]
                if file_name in file_list: #os.listdir(script_path):
                    pass
                else:
                    url_zip = 'https://kase.kz/' + a['href']
                    current_dir = pathlib.Path(script_path, file_name)
                    if file_name.endswith('.zip'):
                        # create directories for each zipfile to be extracted in
                        try:
                            os.makedirs(current_dir)
                        except FileExistsError:
                            pass
                        download_unzip(url_zip, current_dir)
                        try:
                            excel_file_name = [i for i in os.listdir(
                                current_dir) if i.endswith('.xls') or i.endswith('.xlsx')][0]
                            excel_dir = pathlib.Path(
                                current_dir, excel_file_name)
                        except:
                            pass
                    else:
                        try:
                            excel_file_name = file_name
                            urllib.request.urlretrieve(url_zip, current_dir)
                            excel_dir = current_dir
                        except:
                            pass
                    try:
                        try:
                            temp_df = connecting_excel_sheets(excel_dir)
                        except:
                            pass
                        temp_df['FILE_NAME'] = file_name
                        temp_df['EXCEL_FILE'] = excel_file_name
                        temp_df['DT'] = date
                        df_kase = df_kase.append(temp_df)
                        df_kase.reset_index(drop=True, inplace=True)
                    except:
                        pass
            else:
                pass

    for col in df_kase.columns:
        df_kase[col] = df_kase[col].apply(str)
    if not df_kase.empty:
        df_kase.drop_duplicates(inplace=True)
        ora_connect.OracleTable().pushDatatoOracle(df_kase, 'dwh.kase_parsed')
        print("{0} rows inserted".format(df_kase.shape))
    else:
        print("Nothing to insert.")

if __name__ == '__main__':
    parse()

 
