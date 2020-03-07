import pandas as pd
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import datetime
from os.path import dirname as dirname

files = os.listdir("{}/data/cleaned/snapshot/".format(dirname(dirname(dirname(__file__)))))
europe_file = sorted([file for file in files if "europe" in file])[-1]
sec_file = sorted([file for file in files if "sec" in file])[-1]

# Load most recent EUROPE & SEC data
europe_data = pd.read_csv("{}/data/cleaned/snapshot/{}".format(dirname(dirname(dirname(__file__))),europe_file),index_col=0)
sec_data = pd.read_csv("{}/data/cleaned/snapshot/{}".format(dirname(dirname(dirname(__file__))),sec_file),index_col=0)

# Connect to Google Sheets
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    "{}/credentials/sheets_oauth.json".format(dirname(dirname(dirname(__file__)))), scope)
gc = gspread.authorize(credentials)

# Upload raw dataframes to spreadsheet
spreadsheet_key = '138rcq32-IuGKptSmijwFMmIMYcB3qDu18Ld9wyP2Ttk'
spreadsheet = gc.open_by_key(spreadsheet_key)

sec_worksheet = spreadsheet.worksheet('raw_SEC')
sec_worksheet.clear()
sec_data.insert(0,'symbol',sec_data['primarysymbol'])
set_with_dataframe(sec_worksheet, sec_data)

europe_worksheet = spreadsheet.worksheet('raw_EUROPE')
europe_worksheet.clear()
set_with_dataframe(europe_worksheet, europe_data)

info_worksheet = spreadsheet.worksheet('raw_info')
info_worksheet.clear()
set_with_dataframe(info_worksheet, pd.DataFrame({'last_updated':[datetime.datetime.today().date(),]}))



