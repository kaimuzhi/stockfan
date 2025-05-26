from datetime import date,datetime, timedelta
import akshare as ak
import tushare as ts
import settings


def get_stockdata_daily():

    # 获取今天的日期字符串
    td = date.today().strftime('%Y-%m-%d')
    td_ts = date.today().strftime('%Y%m%d')
    # td_ts = '20250217'
    # td = (date.today()-timedelta(days=1)).strftime('%Y-%m-%d')
    print("today is, ", td)

    # 使用 akshare 获取交易日历数据
    stock_calendar_df = ak.tool_trade_date_hist_sina()

    logfile = open(settings.log_file, 'a')
    # 检查今天是否在交易日列表中
    if td in stock_calendar_df['trade_date'].astype(str).tolist():
      print(f"{td} 是股票的交易日")
      """单次返回所有沪 A 股上市公司的实时行情数据"""
      ts.set_token(settings.tushare_api_token)
      pro = ts.pro_api()
      df = pro.daily(trade_date=td_ts)
      print(df.shape)
      df.to_csv(settings.akshare_file_path+'/zh'+td_ts+'.csv', index = True, encoding = 'GBK')
      logfile.write(str(td) + ': Total ' + str(df.shape) + ', at ' + str(datetime.now()) + '\n')
    else:
      print(f"{td} 不是股票的交易日")
      logfile.write(str(td) + ': not a trading day, at ' + str(datetime.now()) + '\n')
    logfile.close()

# main()
if __name__ == "__main__":
    get_stockdata_daily()