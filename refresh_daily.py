cnt=0
sh=ts.get_hist_data('sh')
sz=ts.get_hist_data('sz')
for row in stock_list.itertuples():
    print(cnt)
    cnt=cnt+1
    stock_code=row[1]  
    df_d=ts.get_hist_data(stock_code)
    if len(df_d.index)>500:
        check=checktable(stock_code)
        df_d.reset_index(inplace=True)
        df_d.sort_values('date',inplace=True)
        df_d.reset_index(inplace=True)
        df_d=df_d.drop('index', axis=1)
        df_d.reset_index(inplace=True)
        df_d=df_d.rename(columns={'index':'seq'})
        if check==False:
            conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root',db='scorpion')
            cur=conn.cursor()
            cur.execute('update runningstatus set status=1')
            cur.execute('select status from runningstatus')
            result=cur.fetchone()
            status=result[0]
            while status==1:
                conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root',db='scorpion')
                cur=conn.cursor()
                engine=create_engine('mysql+pymysql://root:root@127.0.0.1:3306/scorpion')
                tablename=str(prefix)+str(stock_code)    #concatenate table name hist_daily_002xxx  ,
                df_d.to_sql(tablename,engine,if_exists='replace', index=False)
                print('write')
                cur.execute('update runningstatus set status=0')
                status_check_cursor=conn.cursor()
                status_check_cursor.execute('select status from RunningStatus')
                q=status_check_cursor.fetchone()
                print(q[0])
                if q[0] == 0:
                    status_check_cursor.close()
                    break
            cur.close()
        else:
            cur=conn.cursor()
            cur.execute("update scorpion.runningstatus set status=1")
            cur.execute("select status from scorpion.runningstatus")
            result=cur.fetchone()
            print(result)
            status=result[0]
            while status==1:
                print('checkforappend')                             #       1. pull the existing table from sql server,
                conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root',db='scorpion')
                cur=conn.cursor()
                engine=create_engine('mysql+pymysql://root:root@127.0.0.1:3306/scorpion')
                sql='select * from '+str(prefix)+str(stock_code)  # 2. find the max date,
                pulltest=pd.read_sql(sql,conn)                    # 3. pull data from tushare starting from max_dt, store in df1 ,
                max_dt=pulltest['date'].max()                     # 4. Check of df1's max date is > existing max dt,
                                                                            # 5. If df1's max_dt>existing max_dt, that means there's new record                          # 6. if 5 is true, concatenate the new data to the existing one,
                                                                           # 7. Write to database,

                cur.execute('update runningstatus set status=0')
                status_check_cursor=conn.cursor()
                status_check_cursor.execute('select status from RunningStatus')
                q=status_check_cursor.fetchone()
                print(q[0])
                if q[0] == 0:
                    status_check_cursor.close()
                    break
            cur.close()

            if df_d['date'].max()>max_dt:
                cur=conn.cursor()
                cur.execute('update runningstatus set status=1')
                cur.execute('select status from runningstatus')
                result=cur.fetchone()
                status=result[0]
                while status==1:
                    conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root',db='scorpion')
                    cur=conn.cursor()
                    engine=create_engine('mysql+pymysql://root:root@127.0.0.1:3306/scorpion')
                    df1=df_d[df_d['date']>max_dt]
                    tablename=str(prefix)+str(stock_code)
                    new=pulltest.append(df1)
                    new.sort_values('date',inplace=True)
                    new.reset_index(inplace=True)
                    new=new.drop('index', axis=1)
                    new.reset_index(inplace=True)
                    new=new.drop('seq', axis=1)
                    new=new.rename(columns={'index':'seq'}) 
                    new=pd.DataFrame(new)
                               #conn=pymysql.connect(host='localhost', port=3306, user='root', passwd='root',db='scorpion'),
                    new.to_sql(tablename,engine,if_exists='replace',index=False)
                    print('append'),
                    cur.execute('update runningstatus set status=0')
                    status_check_cursor=conn.cursor()
                    status_check_cursor.execute('select status from RunningStatus')
                    q=status_check_cursor.fetchone()
                    print(q[0])
                    if q[0] == 0:
                        status_check_cursor.close()
                        break
                cur.close()
conn.close()
           
        
