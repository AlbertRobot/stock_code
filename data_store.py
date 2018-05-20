# 数据库相关操作

# -*- coding: utf-8 -*-
import os
from warnings import filterwarnings

import MySQLdb
import pandas as pd
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc

from entity import HistData5, HistDataD, HistDataW, HistDataM, HistData15, HistData30, HistData60
import conf

current_dir = sys.path[0]
db_config = conf.load_db_config()
engine = create_engine(db_config.get('connect_url'))

filterwarnings('ignore', category = MySQLdb.Warning)

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)
session = DBSession()


def insert(base):
    """
    插入数据
    :param base:
    :return:
    """
    session.add(base)
    session.flush()
    session.commit()


def query_hist_data_d(stock_code, older=0):
    query_sql = 'SELECT stock_code, date_in, date, open, high, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20, turnover, version, updated, created, min_price, max_price, var_price, var_price_3d, ma30, ma60, vma30, vma60, mid_price, open_dev_ma5, open_dev_ma10, close_dev_ma5, close_dev_ma10, diff, dea, macd, boll_low, boll_up, boll_relative, dividend_ind, rel_1y, rel_6m, rel_3m, rel_1m, rel_2w, rel_5d, ema5, ema10, ema20, ema30, ema60, ma_range, ema60_pos, v_diff0, v_diff1, v_diff2, v_diff3, v_diff4, v_diff5, rsi, rel_1m_v, rel_2w_v, rel_5d_v, rel_8w_var, rel_4w_var, rel_2w_var, decision, day_in from HIST_DATA_D WHERE STOCK_CODE = {0} ORDER BY ID desc LIMIT {1}'.format(stock_code, int(older))
    df = pd.read_sql(query_sql, con=engine)
    return df


def exist_hist_data_d(stock_code):
    query_sql = 'SELECT COUNT(1) from HIST_DATA_D WHERE STOCK_CODE = {0}'.format(stock_code)
    df = pd.read_sql(query_sql, con=engine)
    if df.iat[0, 0] == 0:
        return False
    return True


def query_hist_data_w(stock_code, older=0):
    query_sql = 'SELECT * from HIST_DATA_W WHERE STOCK_CODE = {0} ORDER BY ID desc LIMIT {1}'.format(stock_code, int(older))
    df = pd.read_sql(query_sql, con=engine)
    return df


def exist_hist_data_w(stock_code):
    query_sql = 'SELECT COUNT(1) from HIST_DATA_W WHERE STOCK_CODE = {0}'.format(stock_code)
    df = pd.read_sql(query_sql, con=engine)
    if df.iat[0, 0] == 0:
        return False
    return True


def query_hist_data_60(stock_code, older=0):
    query_sql = 'SELECT * from HIST_DATA_60 WHERE STOCK_CODE = {0} ORDER BY ID desc LIMIT {1}'.format(stock_code, int(older))
    df = pd.read_sql(query_sql, con=engine)
    return df


def exist_hist_data_60(stock_code):
    query_sql = 'SELECT COUNT(1) from HIST_DATA_60 WHERE STOCK_CODE = {0}'.format(stock_code)
    df = pd.read_sql(query_sql, con=engine)
    if df.iat[0, 0] == 0:
        return False
    return True


# def query_hist_data_w(older=0):
#     older = int(older)
#     session.begin(subtransactions=True)
#     hist = session.query(HistDataW).order_by(desc(HistDataW.date))[older:older + 1]
#     if len(hist) == 0:
#         return None
#     # session.close()
#     return hist[0]
#
#
# def query_hist_data_60(older=0):
#     older = int(older)
#     session.begin(subtransactions=True)
#     hist = session.query(HistData60).order_by(desc(HistData60.date))[older:older + 1]
#     if len(hist) == 0:
#         return None
#     # session.close()
#     return hist[0]

