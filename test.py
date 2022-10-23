import pandas as pd
import pyodbc
import numpy as np
import datetime

#src sql
driver = 'ODBC Driver 17 for SQL Server'
server = '127.0.0.1'
port = 1433
database = 'TestDB' 
uid = 'SA'
pwd = 'reallyStrongPwd123'
con_string = 'DRIVER={};SERVER={};port={};DATABASE={};uid={};pwd={}'.format(driver,server,port,database,uid,pwd)
src_cnxn = pyodbc.connect(con_string)
src_cursor = src_cnxn.cursor()
src_cursor.fast_executemany = True

#dest sql
driver = 'ODBC Driver 17 for SQL Server'
server = '127.0.0.1'
port = 1433
database = 'TestDB' 
uid = 'SA'
pwd = 'reallyStrongPwd123'
con_string = 'DRIVER={};SERVER={};port={};DATABASE={};uid={};pwd={}'.format(driver,server,port,database,uid,pwd)
dest_cnxn = pyodbc.connect(con_string)
dest_cursor = dest_cnxn.cursor()
dest_cursor.fast_executemany = True




src_cursor.execute("Select * from {}".format("tb_impartx_fsm_tickets_test_mukul_Sep30"))
params = list(src_cursor.fetchall())
dest_cursor.executemany("INSERT INTO {} VALUES (?".format("tb_impartx_fsm_tickets_test_mukul_Sep30")+ "".join(",?"*(len(params[0])-1))+ ")", params)
src_cnxn.close()
dest_cnxn.commit()
dest_cnxn.close()