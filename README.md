# stock_get_data
Data_Preparation: import all the necessary modules and import stock list. Establish connection to local mySQL server


二 配置文件

config/app.conf

#mysql数据库配置
[db]
host = localhost
port = 3306
database = scorpion
user = root
pass = 123456
charset = utf8
connect_url = mysql://root:123456@localhost:3306/scorpion

[system]
stock_points=60 # 依赖历史数据点
log_path=.

#邮件配置
[email]
smtp.server=smtp.163.com
port=465
user=example@163.com
password=password
sender=example@163.com
reveiver=example@163.com,example@qq.com
subject=stock测试邮件

三 股票代码配置
config/stock_lists.txt
每一行为一个股票代码，程序只处理配置的股票

四 mysql建表语句
doc/create_sql.sql


五 任务说明
1. 定时任务类refresh_daily.py

里面配置各种K线数据的更新任务

2. 邮件发送 common/email.py

3. 数据处理类 collector/data_preparation.py ,refresh_daily.py会调用这个类

4. 数据库操作store/db.py

5. indicator和decision都在data_preparation.py里面


数据更新方式：

设定了一个历史数据依赖点数，比如90，然后从数据库查出来历史第90个点对应的时间，然后按这个时间点为start,去pull数据，然后计算indicator和decision，计算完成之后，只更新90个点之后的数据。并且更新decision表，发送邮件。


