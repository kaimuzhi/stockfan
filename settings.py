import os

user = os.environ.get('USER')

# related to mysql
mysql_host = '192.168.71.100'
mysql_user = 'david'
mysql_pwd = 'be1s@K88'
mysql_db = 'stockfan'

# related to path
if user == 'david':
  akshare_file_path = '/Users/david/Downloads/stockdata'
  log_file = '/Users/david/Documents/stockfan_log.txt'
  print('david')
if user == 'zhuxiao':
  akshare_file_path = '/Users/zhuxiao/Downloads/stockdata'
  log_file = '/Users/zhuxiao/Documents/stockfan_log.txt'

# related to Tushare
tushare_api_token='8814a30427c7316f97bf902cc3f727d5188d9cc3879abc9ea8c38038'