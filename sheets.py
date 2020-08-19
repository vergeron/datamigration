from __future__ import print_function
import httplib2
import oauth2client
import os
import googleapiclient
import openpyxl

import pandas as pd
import pyodbc
import json
from json import dumps
from datetime import date, datetime
from googleapiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from openpyxl import Workbook
from pandas import DataFrame, ExcelWriter
import google.protobuf

""" This is the code to get raw data from a specific Google Sheet"""
try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secrets.json'
APPLICATION_NAME = 'Google Sheets API Python'


def get_credentials():

    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:
            credentials = tools.run_flow(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def main():

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = googleapiclient.discovery.build(
        'sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)


    spreadsheetId = '1NNi9PGYbd-TwNEplBe4AiMdhSPh2dTFIcSjHIbBuXKE'
    rangeName = 'Sheet1'

    # TODO: Add desired entries to the request body if needed
    clear_values_request_body = {}


    request = service.spreadsheets().values().clear(spreadsheetId=spreadsheetId,
                                                    range=rangeName, body=clear_values_request_body)
    response = request.execute()


    responseText = '\n'.join(
        [str(response), 'The Google Sheet has been cleared!'])
    print(responseText)


    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=RONY;"
                          "Database=testdb;"
                          "Trusted_Connection=yes;")


    sql = 'select * from employees'
    cursor = cnxn.cursor()
    cursor.execute(sql)
    list(cursor.fetchall())


    sqlData = pd.read_sql_query(sql, cnxn)
    df = DataFrame(sqlData)

    df['start_date'] = df['start_date'].astype(str)


    lc = df.values.tolist()
    headers = df.columns.values.tolist()
    dfHeadersArray = [headers]

    print(headers)
    print(lc)


    value_input_option = 'USER_ENTERED'  # TODO: Update placeholder value.


    insert_data_option = 'OVERWRITE'  # TODO: Update placeholder value.

    value_range_body = {
        "majorDimension": "ROWS",
        "values": dfHeadersArray + lc
    }

    request = service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=rangeName,
                                                     valueInputOption=value_input_option,
                                                     insertDataOption=insert_data_option, body=value_range_body)
    response = request.execute()


if __name__ == '__main__':
    main()