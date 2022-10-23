#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Name: Python Automation Script for import to Ivalua_projects
# Purpose:Data Import to Ivalua_projects
# Author: eragmuk
# Created:     05/08/2020


# In[2]:


import pandas as pd
import numpy as np
import pyodbc
import sqlite3
import time
import logging,datetime


# In[3]:


# logger config
handlers = [
    logging.FileHandler("PACMAN_Ivalua_Projects_Auto_Import_log[{}].log".format(datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))),
    logging.StreamHandler()]
logging.basicConfig(
    format='%(asctime)s - [%(levelname)s] : %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    handlers=handlers,
    level=logging.DEBUG)

logging.info("Name: Python Automation Script for import to Ivalua_projects & Purpose:Data Import to Ivalua_projects & Author:eragmuk")
start_time = time.time()
started = "Started Operation: "+str(datetime.datetime.now().time())
logging.info(started)

#checking for the drivers
for driver in pyodbc.drivers():
    print(driver)


# In[4]:


# define the server and the database
driver = 'ODBC Driver 17 for SQL Server'
server = '10.125.215.133'
port = 1433
database = 'SPONGEBOB'
uid = 'seteam'
pwd = 'SEsGetDegrees*'


# In[5]:


# define the connection string

try:
    con_string = 'DRIVER={};SERVER={};port={};DATABASE={};uid={};pwd={}'.format(driver,server,port,database,uid,pwd)
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    logging.info("Connected to server {}@{}:{}".format(uid,server,port))
except Exception as e:
    logging.error(str(e))


# In[6]:


mapping = {
   "tb_ivalua_savings_test_mukul_3" : "query.xlsx",
}


# In[7]:



for table_name in mapping.keys():
    try:
        logging.info("Parameters- Destination Table: {} , Source Sheet: {}".format(table_name,mapping[table_name]))
        #import a the excel file
        cursor.execute("SELECT TOP(1) * FROM dbo.{}".format(table_name))
        c = cursor.description
        columns_type = {}
        datetime_columns = {}
        for i in c:
            columns_type[i[0].replace("#",".")] = i[1]
        for k,v in list(columns_type.items()):
            if v == datetime.datetime or v==datetime.time or v==datetime.date:
                del columns_type[k]
                datetime_columns[k] = v


        df = pd.read_excel ('//usmmisgroup01001.mm.us.am.ericsson.se/group13ia2_new/resources/Sourcing_Excellence/10. Sourcing Automation & Technology/03. Import Data Files/14. Ivalua/Test/mukul/SSS_savings/{}'.format(mapping[table_name]),converters=columns_type).fillna(value=np.nan)

        for k,v in list(datetime_columns.items()):
            if v==datetime.date:
                df[k] = pd.to_datetime(df[k]).dt.date
            if v==datetime.time:
                df[k] = pd.to_datetime(df[k]).dt.time

        # Getting number of columns for table and sheet.
        cursor.execute("SELECT COUNT(COLUMN_NAME) as Number FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name))
        no_table_col = cursor.fetchall()[0][0]
        no_sheet_col = len(df.columns)
        logging.info("test1")

        # If columns in sheet are less than 1 i.e. sheet is empty then the program exits.
        if no_sheet_col < 1:
            logging.error("No.of Columns in sheet is less than 1.")
            exit()
        logging.info("test2")
        # Checking number of Columns in Excel data match with SQL table.
        # If there is a mismatch then program exits else continues.
        cursor.execute("SELECT * FROM dbo.{}".format(table_name))
        last_column = [[column[0],column[1]] for column in cursor.description][-1]
        table_type = 0
        logging.info("test3")
        if no_table_col == no_sheet_col and last_column[0] != 'Data Upload Date':
            pass
        elif no_table_col == no_sheet_col+1 and last_column[0] == 'Data Upload Date' and last_column[1] == datetime.datetime:
            table_type = 1
            pass
        else:
            logging.error ("No.of Columns from source and destination don't match.  Sheet : {} and SQL table : {}".format(no_sheet_col,no_table_col))
            logging.error ("SQL table name: {} ,  Sheet name:{} ".format(table_name,mapping[table_name]))
            continue
        logging.info("test4")
        # Truncating and adding new data into the SQL table.
        try:
            logging.info ("Inserting into table.")
            df1 = df
            df1 = df1.replace({pd.NaT:None}) # Replacing NaNs and NaTs in the data with None
            params = [tuple(r) for r in df1.to_numpy()] # Generating a list of tuples from the panda dataframe e.g. [(1,'ABC'),(2,'XYZ')]
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [Forecast Begin Date] date".format(table_name))
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [Forecast End Date] date".format(table_name))
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [Creation Date] date".format(table_name))
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [Modification Date] date".format(table_name))
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [Begin Date Actual] date".format(table_name))
            cursor.execute("ALTER TABLE dbo.{} ALTER COLUMN [End Date Actual] date".format(table_name))
            cursor.execute("TRUNCATE TABLE dbo.{}".format(table_name)) # Truncating the SQL table.
            if table_type:
                cursor.executemany("INSERT INTO dbo.{} VALUES (?".format(table_name)+ "".join(",?"*(no_sheet_col-1)) + ",GETDATE())", params)
            else :
                cursor.execute("ALTER TABLE dbo.{} ADD [Data Upload Date] datetime".format(table_name))
                cursor.executemany("INSERT INTO dbo.{} VALUES (?".format(table_name)+ "".join(",?"*(no_sheet_col-1)) + ",GETDATE())", params)
            cnxn.commit()
            logging.info ("Insertion complete.")
        except pyodbc.DatabaseError as err:
            logging.error(str(err))
            logging.error ("Table name: {}".format(table_name))
            cnxn.rollback()
    except Exception as e:
        logging.error(str(e))
cnxn.close()
logging.info(started)
logging.info("Ended Data Import: "+ str(datetime.datetime.now().time()))
rounded_time=round(time.time() - start_time,4)
logging.info("Total time elapsed: "+ str(rounded_time) +" seconds")
