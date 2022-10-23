import datetime
import pyodbc
import pandas as pd
for driver in pyodbc.drivers():
    print(driver)

# print (datetime.datetime.now())
driver = 'ODBC Driver 17 for SQL Server'
server = '127.0.0.1'
port = 1433
database = 'TestDB' 
uid = 'SA'
pwd = 'reallyStrongPwd123'
con_string = 'DRIVER={};SERVER={};port={};DATABASE={};uid={};pwd={}'.format(driver,server,port,database,uid,pwd)
print (con_string)
cnxn = pyodbc.connect(con_string)
cursor = cnxn.cursor()
print ("Connected!")
# cursor.execute()

#TEST2
# df = pd.read_excel("./excelfiles/schema.xlsx",header=None,sheet_name='Sheet2')
# string = ""
# df = df.replace(["nvarchar"],["nvarchar(255)"])
# for i in range(len(df)):
#     if i!=len(df)-1:
#         string += "[" + str(df[0][i])+"] "+str(df[1][i])+", "
#     else:
#         string += "["+ str(df[0][i])+ "] "+str(df[1][i])
# query = "CREATE TABLE TEST2 ( "+ string + " )"
# cursor.execute(query)
# cnxn.commit()
# cnxn.close()
# print ("Compeleted!")

#TEST3
# df = pd.read_excel("./excelfiles/query.xlsx",sheet_name='Sheet2')
# # df = df.replace(["[nvarchar](255)"],["nvarchar(4000)"])
# string = ""
# for i in range(len(df)):
#     if i!=len(df):
#         string += df['SQL Table Format'][i]

# query = "CREATE TABLE TEST3 ( "+ string + " )"
# cursor.execute(query)
# cnxn.commit()
# cnxn.close()
# print ("Compeleted!")

#TEST4
# df = pd.read_excel("./excelfiles/xl2.xlsx",header=None,sheet_name='format')
# string = ""
# df = df.replace(["nvarchar"],["nvarchar(255)"])
# for i in range(1,len(df)):
#     if i!=len(df)-1:
#         string += "[" + str(df[0][i])+"] "+str(df[1][i])+", "
#     else:
#         string += "["+ str(df[0][i])+ "] "+str(df[1][i])
# query = "CREATE TABLE TEST4 ( "+ string + " )"
# # print (query)
# # exit()
# cursor.execute(query)
# cnxn.commit()
# cnxn.close()
# print ("Compeleted!")

#tb_bw_srm_projectreport_2020_q1
df = pd.read_excel("./excelfiles/SRM_ProjectReport_2020_Q1.xlsx",header=None,sheet_name='Sheet1')
string = ""
# df = df.replace(["nvarchar"],["nvarchar(255)"])
for i in range(1,len(df[2])):
    string+=df[2][i]
query = "CREATE TABLE tb_bw_srm_projectreport_2020_q1 ( "+ string + " )"
# print (query)
# exit()
cursor.execute(query)
print (query)
cnxn.commit()
cnxn.close()
print ("Compeleted!")