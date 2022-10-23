import pandas as pd
import pyodbc
import numpy as np
import datetime
from sqlalchemy import create_engine



driver = 'ODBC Driver 17 for SQL Server'
server = '127.0.0.1'
port = 1433
database = 'TestDB' 
uid = 'SA'
pwd = 'reallyStrongPwd123'
engine = create_engine('mssql+pyodbc://SA:reallyStrongPwd123@127.0.0.1:1433/TestDB?driver=ODBC+Driver+17+for+SQL+Server',fast_executemany = True)



df = pd.read_sql("select count(*) from tb_impartx_fsm_tickets_test_mukul_Sep30",engine)
df.to_excel("output.xlsx",sheet_name='Sheet_1')  