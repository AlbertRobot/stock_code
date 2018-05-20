from sqlalchemy import create_engine
import pyodbc
import sqlalchemy
import pymysql
engine=create_engine('mysql+pymysql://root:root@127.0.0.1:3306/scorpion')
import pandas as pd
import numpy as np
import tushare as ts

# ******************* get list of stocks

df = ts.get_stock_basics()
df.reset_index(inplace=True) #stock code is used as index, reset it

# add stock type: Shanghai or Shenzhen 
for i in range(len(df.index)):
    if df.iloc[i,0].startswith("6"):
        df.loc[i,'type']='sh_A'
    elif df.iloc[i,0].startswith("00"):
         df.loc[i,'type']='sz_A'
    elif df.iloc[i,0].startswith("6016"):
        df.loc[i,'type']='sh_bluechip'
    elif df.iloc[i,0].startswith("900"):
        df.loc[i,'type']='sh_B'
    elif df.iloc[i,0].startswith("002"):
        df.loc[i,'type']='zxb'
    elif df.iloc[i,0].startswith("200"):
        df.loc[i,'type']='sz_B'
    elif df.iloc[i,0].startswith("300"):
        df.loc[i,'type']='cyb'
    else:
        df.loc[i,'type']='other'
    
# add stocklist to local mysql database
conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='scorpion')

df.to_sql('stocklist',engine,if_exists='replace', index=False)
conn.close()

