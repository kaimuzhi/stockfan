import datetime
import akshare as ak
import pandas as pd
import pymysql
import settings

def upd_t_stockinfodaily(connection,csv_file,td):
  cursor = connection.cursor()
  sql='delete from t_stockinfodaily'
  cursor.execute(sql)
  connection.commit()

  df = pd.read_csv(csv_file, encoding="GBK", dtype='str')
  df = df.iloc[:,1:] #remove 1st sequential column
  # adjust format date and stock code columns
  df['trade_date'] = pd.to_datetime(df['trade_date'],format='%Y%m%d').dt.strftime('%Y-%m-%d')
  df['ts_code'] = df['ts_code'].astype(str).str[:6]
  df['amount'] = df['amount'].astype(float)*1000
  df['amount'] = df['amount'].astype(str)

  # delete unnecessary columns
  del df['pre_close']
  del df['change']
  #print(df.head(3))

  #df = df.head(3)
  columns = ['code','day','priceopen','pricehigh','pricelow','priceclose','turnoverrt','transvol','transval','name','createdate']
  for _, row in df.iterrows():
    data = tuple(row)
    sql = f"insert into t_stockinfodaily({','.join(columns)}) values('"
    sql = sql + '\',\''.join(data) + '\',\'name\',\'' + td + '\')'
    #print(sql)
    cursor.execute(sql)
  connection.commit()

"""
def update_t_stockinfodaily(connection,csv_file):
  sql='delete from t_stockinfodaily'
  cursor.execute(sql)
  connection.commit()

  data = pd.read_csv(csv_file, encoding="GBK", dtype='str')
  lst = data.values.tolist()
  if len(lst) > 0:
    for i in range(len(lst)):
      if str(lst[i][3]) != 'nan':
        daycol = str(lst[i][2])
        transday = daycol[0:4]+'-'+daycol[4:6]+'-'+daycol[6:8]
        # print(transday)
        if str(lst[i][3]) != 'nan':
          priceopen = str(lst[i][3])
        else:
          priceopen = '0'
        if str(lst[i][6]) != 'nan':
          priceclose = str(lst[i][6])
        else:
          priceclose = '0'
        if str(lst[i][10]) != 'nan':
          transvol = str(lst[i][10])
        else:
          transvol = '0'
        if str(lst[i][11]) != 'nan':
          transval = str(lst[i][11])
        else:
          transval = '0'
        if str(lst[i][4]) != 'nan':
          pricehigh = str(lst[i][4])
        else:
          pricehigh = '0'
        if str(lst[i][5]) != 'nan':
          pricelow = str(lst[i][5])
        else:
          pricelow = '0'
        if str(lst[i][9]) != 'nan':
          turnoverrt = str(lst[i][9])
        else:
          turnoverrt = '0'
        sql = 'insert into t_stockinfodaily(day,code,name,priceopen,priceclose,transvol,transval,pricehigh,pricelow,turnoverrt,createdate) values(\''
        sql = sql + transday + '\',\''+ str(lst[i][1])[0:6] + '\',\'' + 'name' + '\',' + priceopen + ',' + priceclose + ',' + transvol + ',' + transval
        sql = sql + ',' + pricehigh + ',' + pricelow + ',' + turnoverrt
        sql = sql + ',\'' + str(td) + '\')'
        # print(sql)
        cursor.execute(sql)
    connection.commit()
"""

"""获取当日除权的股票清单"""
def update_chuquan(connection, strtd):
  cursor = connection.cursor()
  notify_dividend = ak.news_trade_notify_dividend_baidu(date=strtd)
  #print(notify_dividend)
  if len(notify_dividend) > 0:
    for row in range(len(notify_dividend)):
      code = str(notify_dividend.iat[row,0])
      se = str(notify_dividend.iat[row,6])
      if se == 'SH' or se == 'SZ':
        if se == 'SH':
          hfq_factor_df = ak.stock_zh_a_daily(symbol='sh'+code, adjust="hfq-factor")
        if se == 'SZ':
          hfq_factor_df = ak.stock_zh_a_daily(symbol='sz'+code, adjust="hfq-factor")
        sql = 'select * from stockfactor where code=\'' + code + '\''
        #print(sql)
        cursor.execute(sql)
        result=cursor.fetchall()
        #print(result)
        if len(result) > 0:
          sqladd = 'update stockfactor set day=\'' + str(hfq_factor_df.iat[0,0])[0:10] + '\',factor=' + str(hfq_factor_df.iat[0,1]) + ' where code=\'' + code + '\''
        else:
          sqladd = 'insert into stockfactor(day,code,factor) values(\'' + str(hfq_factor_df.iat[0,0])[0:10] + '\',\'' + code + '\',' + hfq_factor_df.iat[0,1] +')'
        #print(sqladd)
        cursor.execute(sqladd)
        connection.commit()


# copy records from temp table to permanent one
def ins_stockinfodaily(connection):
  cursor = connection.cursor()
  sqladd = 'Insert into stockinfodaily select a.day,a.code,a.priceopen*b.factor,a.priceclose*b.factor,a.transvol,a.transval,a.pricehigh*b.factor,a.pricelow*b.factor,a.turnoverrt,a.createdate from t_stockinfodaily a inner join stockfactor b on a.code=b.code'
  result = cursor.execute(sqladd)
  connection.commit()
  return result

# add the stock name for those new stocks
def add_stockname(connection):
  # fetch stock code and name from Akshare for all stocks
  stock_info = ak.stock_info_a_code_name()
  #print(stock_info)

  cursor = connection.cursor()
  # fetch newly added stocks which do not have name stored in the db
  sql = 'select t0.code from stockfactor t0 left outer join stockmaster t1 on t0.code=t1.code where t1.code is null'
  cursor.execute(sql)
  result = cursor.fetchall() #tuple
  # print(result)
  colnames = [desc[0] for desc in cursor.description]
  colindex = colnames.index('code')
  if len(result) > 0:
    for row in result:
      code = row[colindex]
      #print(code)
      found_stock = stock_info[stock_info['code'] == code]
      #print(found_stock)
      sqladd = "insert into stockmaster values('" + found_stock.iloc[0,0] + "','" + found_stock.iloc[0,1] + "')"
      #print(sqladd)
      cursor.execute(sqladd)
      connection.commit()

# main()
if __name__ == "__main__":
  # td = datetime.date.today() + datetime.timedelta(days=-1)
  td = datetime.date.today()  # 2025-01-01
  print(td)
  strtd = str(td)[0:4] + str(td)[5:7] + str(td)[8:10]  # 20250101
  print(strtd)

  connection = pymysql.connect(host=settings.mysql_host, user=settings.mysql_user, password=settings.mysql_pwd,
                               database=settings.mysql_db, charset="utf8mb4")
  # cursor = connection.cursor()

  upd_t_stockinfodaily(connection,settings.akshare_file_path+'/zh'+strtd+'.csv',str(td))
  update_chuquan(connection,strtd)
  result = ins_stockinfodaily(connection)

  add_stockname(connection)

  connection.cursor().close()
  connection.close()

  logfile = open(settings.log_file, 'a')
  logfile.write(str(td) + ': ' + str(result) + ' in total added into table "stockinfodaily", mysql process completed at ' + str(datetime.datetime.now()) + '\n')
  logfile.close()
