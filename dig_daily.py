import matplotlib

import settings
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import font_manager
import mplfinance as mpf

spikedays = 3 #initial rise days duration
dblwavdays = 25 #including both 1st rise days (3-5) + cool down days + 2nd rise days (25)
fulldays = 100 #including double wave days and silent ambush days (30)
price_factor = '1.1' #price within dblwavdays / price within fulldays
transval_factor = '2' #transaction value within dblwavdays / transaction value within fulldays
window = 3 #use 3 days window to calc average price/volume to see the trend.
target_stocks = []

def get_offsetday(connection, duration):
    cs = connection.cursor()
    sql = "select day from stockdays where day<curdate() order by day desc limit 1 offset " + str(duration)
    cs.execute(sql)
    row = cs.fetchone()
    offsetday = str(row[0])
    return offsetday

# initial selection from database
def fst_scan(connection, spikeday, ambushday, pricefactor, transvalfactor):
    cs = connection.cursor()
    sql = "select t0.code,t0.price30,t1.price120 from (select code,avg(priceclose) as price30,avg(transval) as transval30 from stockinfodaily "
    sql = sql + "where day>'" + spikeday + "'  group by code) t0 "
    sql = sql + "inner join(select code,avg(priceclose) as price120,avg(transval) as transval120 from stockinfodaily "
    sql = sql + "where day>'" + ambushday +"' group by code) t1 "
    sql = sql + "on t0.code=t1.code inner join (select distinct code from stockinfodaily where day='2024-01-02') t2 "
    sql = sql + "on t0.code =t2.code where t0.price30>t1.price120*" + pricefactor + " and t0.transval30>t1.transval120*" + transvalfactor + " limit 300"
    cs.execute(sql)
    rows = cs.fetchall()
    return rows

# process selected records
def process(connection, spikeday, ambushday, code):
    global target_df

    sql = "select day,code,priceclose,transval,transvol from stockinfodaily where code ='" + code + "' and day>'" + spikeday + "' order by day"
    df = pd.read_sql(sql,connection)
    #print(df)

    df['avgprice'] = df['priceclose'].rolling(window=window).mean()
    df['avgvol'] = df['transvol'].rolling(window=window).mean()

    df['avgprice_chg'] = df['avgprice'].pct_change()
    df['avgvol_chg'] = df['avgvol'].pct_change()

    df['is_price_incr'] = (df['avgprice_chg'] > 0).rolling(window=window).sum() == 3
    df['is_vol_incr'] = (df['avgvol_chg'] > 0).rolling(window=window).sum() == 3
    #print(df)
    #qualified = df[df['is_vol_incr'] & df['is_price_incr'] & (str(df['day']) > spikeday)]
    spikeindex = df[df['is_vol_incr'] & df['is_price_incr']].index-2
    #print(spikeindex)
    if spikeindex.empty:
        return

    spikeprice = df.loc[spikeindex[0]:(spikeindex+5)[0],'priceclose'].max()
    #print(spikeprice)
    spikeday = df.loc[df['priceclose'] == spikeprice].index[0]

    #check decay period
    decay_period = df.loc[spikeday:]
    #print(decay_period)
    if decay_period['priceclose'].max() > spikeprice and decay_period['priceclose'].max() < spikeprice*1.2:
        target_stocks.append(code)
        print(spikeprice)

def plot_daily_candlestick(connection, ambushday, code):
    sql = "select t1.day as date,t1.priceopen as open,t1.priceclose as close,t1.pricehigh as high,t1.pricelow as low,t1.transvol as volume,t2.name from stockinfodaily t1 "
    sql = sql + " inner join stockmaster t2 on t1.code=t2.code where t1.code ='" + code + "' and t1.day>'" + ambushday + "' order by t1.day"
    df = pd.read_sql(sql,connection)
    df['date'] = pd.to_datetime(df['date'])
    dailydata = df.set_index('date')
    name = df['name'].max()
    # print(matplotlib.get_data_path())
    for font in matplotlib.font_manager.fontManager.ttflist:
        print(font.name,"----",font.fname)
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.title('城市')
    plt.plot([1,2,3],[1,4,9])
    plt.show()
    mpf.plot(dailydata,type='candle',volume=True,style='binance',title=f"{code}:{name} - daily 日K线")


if __name__ == "__main__":
    connection = pymysql.connect(host=settings.mysql_host, user=settings.mysql_user, password=settings.mysql_pwd,
                                 database=settings.mysql_db, charset="utf8mb4")
    spikeday = get_offsetday(connection, dblwavdays)
    ambushday = get_offsetday(connection,fulldays)
    print(spikeday,ambushday)

    target_df = {}
    result = fst_scan(connection,spikeday,ambushday,price_factor,transval_factor)
    print(result)
    for row in result:
        process(connection,spikeday,ambushday,row[0])
    #process(connection, spikeday, ambushday, '000678')
    print(target_stocks)

    for code in target_stocks:
        plot_daily_candlestick(connection,ambushday,code)

    connection.close()

