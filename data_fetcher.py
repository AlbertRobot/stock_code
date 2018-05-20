# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import tushare as ts

from stock_indicator import indicator
from constants import KType
import data_store
import entity
import log
import utils


INIT_DATE_DAY = '2018-01-01'
INIT_DATE_WEEK = '2015-01-01'
INIT_DATE_SIXTY = '2018-01-01'


def out(path, df):
    df.to_csv(path)


def get_area_classified():
    df = ts.get_stock_basics()
    df.to_csv("get_stock_basics.csv")


def get_stock_type(code):
    if code.startswith("6"):
        return 'sh_A'
    elif code.startswith("00"):
        return 'sz_A'
    elif code.startswith("6016"):
        return 'sh_bluechip'
    elif code.startswith("900"):
        return 'sh_B'
    elif code.startswith("002"):
        return 'zxb'
    elif code.startswith("200"):
        return 'sz_B'
    elif code.startswith("300"):
        return 'cyb'
    else:
        return 'other'


def get_stock_basics():
    """
    股票基础信息
    :return:
    """
    df = ts.get_stock_basics()
    df.reset_index(inplace=True)  # stock code is used as index, reset it
    print(df.columns)
    size = df.iloc[:, 0].size

    if size == 0:
        return

    # add stock type: Shanghai or Shenzhen
    for i in range(len(df.index)):
        if df.iloc[i, 0].startswith("6"):
            df.loc[i, 'stock_type'] = 'sh_A'
        elif df.iloc[i, 0].startswith("00"):
            df.loc[i, 'stock_type'] = 'sz_A'
        elif df.iloc[i, 0].startswith("6016"):
            df.loc[i, 'stock_type'] = 'sh_bluechip'
        elif df.iloc[i, 0].startswith("900"):
            df.loc[i, 'stock_type'] = 'sh_B'
        elif df.iloc[i, 0].startswith("002"):
            df.loc[i, 'stock_type'] = 'zxb'
        elif df.iloc[i, 0].startswith("200"):
            df.loc[i, 'stock_type'] = 'sz_B'
        elif df.iloc[i, 0].startswith("300"):
            df.loc[i, 'stock_type'] = 'cyb'
        else:
            df.loc[i, 'stock_type'] = 'other'

    df.to_sql(name='STORK_BASICS', index=False, con=data_store.engine, if_exists='append')


def choose_hist_table(ktype):
    if ktype is KType.day.value:
        return entity.HistDataD()
    elif ktype is KType.week.value:
        return entity.HistDataW()
    elif ktype is KType.month.value:
        return entity.HistDataM()
    elif ktype is KType.fiveMinute.value:
        return entity.HistData5()
    elif ktype is KType.fifthMinute.value:
        return entity.HistData15()
    elif ktype is KType.thirtyMinute.value:
        return entity.HistData30()
    elif ktype is KType.sixtyMinute.value:
        return entity.HistData60()

    return entity.HistDataD()


def init_data(stock_code):

    """
    初始化数据
    :param stock_code:
    :return:
    """

    # 日K
    if not data_store.exist_hist_data_d(stock_code):
        df = ts.get_hist_data(code=stock_code, start=INIT_DATE_DAY, ktype=KType.day.value)
        df['stock_code'] = stock_code
        df.to_sql(name='HIST_DATA_D', con=data_store.engine, if_exists='append')

    # 周K
    if not data_store.exist_hist_data_w(stock_code):
        df = ts.get_hist_data(code=stock_code, start=INIT_DATE_WEEK, ktype=KType.week.value)
        df['stock_code'] = stock_code
        df.to_sql(name='HIST_DATA_W', con=data_store.engine, if_exists='append')

    # 60分钟
    if not data_store.exist_hist_data_60(stock_code):
        df = ts.get_hist_data(code=stock_code, start=INIT_DATE_SIXTY, ktype=KType.sixtyMinute.value)
        df['stock_code'] = stock_code
        df.to_sql(name='HIST_DATA_60', con=data_store.engine, if_exists='append')


def handle_day_k(start_date, stock_code, history_dot):
    """
    日K
    1. 取出历史数据
    2. 拉取新数据
    3. 计算指标
    4. 做决策
    5. 更新数据库，发送邮件
    :return:
    """

    df = ts.get_hist_data(code=stock_code, start=start_date, ktype=KType.day.value)
    df.reset_index(inplace=True)

    df['stock_code'] = stock_code
    df['date_in'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date_in']
    df['day_in'] = df['date_in']

    if df.empty:
        return

    recent_df = data_store.query_hist_data_d(stock_code=stock_code, older=history_dot)
    recent_df.columns = recent_df.columns.str.lower()

    new_df = pd.DataFrame(columns=recent_df.columns)
    print(recent_df.columns)
    print(df.columns.values.tolist())
    for column_str in df.columns.values.tolist():
        new_df[column_str] = df[column_str]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df = new_df.append(recent_df)
    indicator(new_df)

    new_df = new_df[new_df['date'] == start_date]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df.to_sql(name='HIST_DATA_D', index=False, con=data_store.engine, if_exists='append')


def handle_sixty_minute_k(start_date, stock_code, history_dot):
    """
    60分钟K
    1. 取出历史数据
    2. 拉取新数据
    3. 计算指标
    4. 做决策
    5. 更新数据库，发送邮件
    :return:
    """
    df = ts.get_hist_data(code=stock_code, start=start_date, ktype=KType.sixtyMinute.value)
    df.reset_index(inplace=True)

    df['stock_code'] = stock_code
    df['date_in'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date_in']
    df['day_in'] = df['date_in']

    if df.empty:
        return

    recent_df = data_store.query_hist_data_60(stock_code=stock_code, older=history_dot)
    recent_df.columns = recent_df.columns.str.lower()

    new_df = pd.DataFrame(columns=recent_df.columns)
    print(recent_df.columns)
    print(df.columns.values.tolist())
    for column_str in df.columns.values.tolist():
        new_df[column_str] = df[column_str]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df = new_df.append(recent_df)
    indicator(new_df)

    new_df = new_df[new_df['date'] == start_date]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df.to_sql(name='HIST_DATA_60', index=False, con=data_store.engine, if_exists='append')


def handle_day_w(start_date, stock_code, history_dot):
    """
    日K
    1. 取出历史数据
    2. 拉取新数据
    3. 计算指标
    4. 做决策
    5. 更新数据库，发送邮件
    :return:
    """
    df = ts.get_hist_data(code=stock_code, start=start_date, ktype=KType.week.value)
    df.reset_index(inplace=True)

    df['stock_code'] = stock_code
    df['date_in'] = df['date'].astype('datetime64[ns]')
    df['date'] = df['date_in']
    df['day_in'] = df['date_in']

    if df.empty:
        return

    recent_df = data_store.query_hist_data_w(stock_code=stock_code, older=history_dot)
    recent_df.columns = recent_df.columns.str.lower()

    new_df = pd.DataFrame(columns=recent_df.columns)
    print(recent_df.columns)
    print(df.columns.values.tolist())
    for column_str in df.columns.values.tolist():
        new_df[column_str] = df[column_str]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df = new_df.append(recent_df)
    indicator(new_df)

    new_df = new_df[new_df['date'] == start_date]
    new_df = new_df.replace([np.inf, -np.inf, np.nan], 0)
    new_df.to_sql(name='HIST_DATA_W', index=False, con=data_store.engine, if_exists='append')


def get_hist_data(code, ktype, start=None, end=None):
    """
    股票历史数据
    :param code: 股票代码
    :param ktype: K线类型
    :param start: 开始时间
    :param end: 结束时间
    :return:
    """
    hist_data_list = []
    df = ts.get_hist_data(code=code, start=start, end=end, ktype=ktype)

    df.reset_index(inplace=True)
    if len(df.index) == 0:
        log.logger().info("no data found for: "
                          + code + " start date: "
                          + start + "end date: "
                          + end + "K type: "
                          + ktype)
        return

    indicator(df)

    size = df.iloc[:, 0].size

    if size == 0:
        return
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)

    return hist_data_list


if __name__ == '__main__':
    get_stock_basics()
