import traceback
import sys
import pyodbc
import requests
import os
import logging
import datetime
import json
import sqlalchemy
from pandas import json_normalize


class ApiLoader:
    def __init__(self, url, server, db, schema, table):
        self.url = url
        self.server = server
        self.db = db
        self.schema = schema
        self.table = table

        """setting logger"""
        self.log_name = f'{os.getcwd()}\\logs\\log_{datetime.datetime.now().strftime("%d%m%Y")}.log'
        self.log = logging.getLogger(__name__)
        if not os.path.exists(f'{os.getcwd()}\\logs'):
            os.makedirs(f'{os.getcwd()}\\logs')
        self.handler = logging.FileHandler(self.log_name)
        self.handler.setLevel(logging.DEBUG)
        self.handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.log.addHandler(self.handler)

    def get_report(self, url):
        """
        Getting report from request to url
        :param url: url of source
        :return report:
        """
        try:
            report = requests.get(url)
        except requests.exceptions.ConnectionError:
            self.log.error(f'Got connection error: {traceback.format_exc()}')
            sys.exit(1)
        except Exception:
            self.log.error(f'Got error: {traceback.format_exc()}')
            sys.exit(1)
        else:
            if report.status_code != 200:
                self.log.error(f'Got error, status code {report.status_code} - {report.text}')
                sys.exit(1)
            return report

    def parse_report(self, report):
        """
        Parse json from report to df
        :param report: report from request to url
        :return:
        """
        try:
            report_json = json.loads(report.text)
        except json.decoder.JSONDecodeError:
            self.log.error(f'Decode json error: {traceback.format_exc()}')
            sys.exit(1)
        else:
            self.log.info('json parsed successfully')
        if len(report_json) > 0:
            df = json_normalize(report_json)
            return df
        self.log.info('json is empty')

    def insert_df(self, df):
        """
        Insert df in table
        :param df: df with data
        :return:
        """
        connect_pyodbc = sqlalchemy.create_engine(f'mssql+pyodbc://@{self.server}/{self.db}?'
                                                  f'driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
        try:
            df.to_sql(self.table, connect_pyodbc, schema=self.schema, if_exists='append', index=False)
        except pyodbc.OperationalError:
            self.log.error(f'Error in connection: {traceback.format_exc()}')
        except pyodbc.ProgrammingError:
            self.log.error(f'Error in insert: {traceback.format_exc()}')
        except Exception:
            self.log.error(f'Error occurs: {traceback.format_exc()}')
        else:
            self.log.info('df was inserted')

    def main(self):
        report = self.get_report(self.url)
        df = self.parse_report(report)
        self.insert_df(df)
        self.log.info('loading was completed')


loader = ApiLoader(url='https://random-data-api.com/api/cannabis/random_cannabis?size=10',
                   server='localhost',
                   db='test_db',
                   schema='dbo',
                   table='test_table'
                   )
# loader.main()