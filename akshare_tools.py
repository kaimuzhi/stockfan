import settings
import akshare as ak
from datetime import date
import pymysql

# add trading days to db. normally once a year
def import_tradedays(connection):
    tradedays = ak.tool_trade_date_hist_sina()
    print(tradedays[tradedays['trade_date']>date(2025,12,12)])
    tradedays_import = tradedays[tradedays['trade_date']>date(2024,1,1)]

    for row in range(len(tradedays_import)):
        tradeday = str(tradedays_import.iloc[row,0])
        sql = "insert into stockdays values('" + tradeday + "')"
        print(sql)
        connection.cursor().execute(sql)
        connection.commit()

if __name__ == "__main__":
    connection = pymysql.connect(host=settings.mysql_host, user=settings.mysql_user, password=settings.mysql_pwd,
                                 database=settings.mysql_db, charset="utf8mb4")
    import_tradedays(connection)
    connection.close()