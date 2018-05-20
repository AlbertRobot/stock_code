# -*- coding: utf-8 -*-
import datetime

from sqlalchemy import Column, String, create_engine, Integer, BigInteger, DateTime, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class StockBasics(Base):
    """
    stock basics
    """
    __tablename__ = 'STORK_BASICS'

    id = Column('ID', BigInteger, primary_key=True, autoincrement="auto")
    code = Column('STOCK_CODE', String)
    name = Column('NAME', String)
    industry = Column('INDUSTRY', String)
    area = Column('AREA', String)
    pe = Column('PE', Numeric)
    outstanding = Column('OUTSTANDING', Numeric)
    totals = Column('TOTALS', Numeric)
    totalAssets = Column('TOTAL_ASSETS', Numeric)
    liquidAssets = Column('LIQUID_ASSETS', Numeric)
    fixedAssets = Column('FIXED_ASSETS', Numeric)
    reserved = Column('RESERVED', Numeric)
    reservedPerShare = Column('RESERVED_PERSHARE', Numeric)
    esp = Column('ESP', Numeric)
    bvps = Column('BVPS', Numeric)
    pb = Column('PB', Numeric)
    timeToMarket = Column('TIMETO_MARKET', Numeric)
    undp = Column('UNDP', Numeric)
    perundp = Column('PER_UNDP', Numeric)
    rev = Column('REV', Numeric)
    profit = Column('PROFIT', Numeric)
    gpr = Column('GPR', Numeric)
    npr = Column('NPR', Numeric)
    holders = Column('HOLDERS', Numeric)
    stockType = Column('STOCK_TYPE', String)
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    created = Column('CREATED', DateTime, nullable=True, default=nowTime)
    updated = Column('UPDATED', DateTime, nullable=True, default=nowTime)


class HistData:

    # 表的结构:
    id = Column('ID', BigInteger, primary_key=True, autoincrement="auto")
    stockCode = Column('STOCK_CODE', String)
    date = Column('DATE_IN', DateTime)
    day = Column('DAY_IN', Date)
    open = Column('OPEN', Numeric(16, 3))
    high = Column('HIGH', Numeric(16, 3))
    close = Column('CLOSE', Numeric(16, 3))
    low = Column('LOW', Numeric(16, 3))
    volume = Column('VOLUME', Numeric(16, 2))
    price_change = Column('PRIVE_CHANGE', Numeric(16, 2))
    p_change = Column('P_CHANGE', Numeric(16, 2))
    ma5 = Column('MA5', Numeric)
    ma10 = Column('MA10', Numeric)
    ma20 = Column('MA20', Numeric)
    v_ma5 = Column('V_MA5', Numeric(16, 2))
    v_ma10 = Column('V_MA10', Numeric(16, 2))
    v_ma20 = Column('V_MA20', Numeric(16, 2))
    turnover = Column('TURN_OVER', Numeric(16, 2))
    min_price = Column('MIN_PRICE', Numeric, default=0.0)
    max_price = Column('MAX_PRICE', Numeric, default=0.0)
    var_price = Column('VAR_PRICE', Numeric, default=0.0)
    var_price_3d = Column('VAR_PRICE_3D', Numeric, default=0.0)
    ma30 = Column('MA30', Numeric, default=0.0)
    ma60 = Column('MA60', Numeric, default=0.0)
    vma30 = Column('VMA30', Numeric, default=0.0)
    vma60 = Column('VMA60', Numeric, default=0.0)
    mid_price = Column('MID_PRICE', Numeric, default=0.0)
    open_dev_ma5 = Column('OPEN_DEV_MA5', Numeric, default=0.0)
    open_dev_ma10 = Column('OPEN_DEV_MA10', Numeric, default=0.0)
    close_dev_ma5 = Column('CLOSE_DEV_MA5', Numeric, default=0.0)
    close_dev_ma10 = Column('CLOSE_DEV_MA10', Numeric, default=0.0)
    diff = Column('DIFF', Numeric, default=0.0)
    dea = Column('DEA', Numeric, default=0.0)
    macd = Column('MACD', Numeric, default=0.0)
    BOLL_low = Column('BOLL_LOW', Numeric, default=0.0)
    BOLL_up = Column('BOLL_UP', Numeric, default=0.0)
    BOLL_relative = Column('BOLL_RELATIVE', Numeric, default=0.0)
    dividend_ind = Column('DIVIDEND_IND', Numeric, default=0.0)
    rel_1y = Column('REL_1Y', Numeric, default=0.0)
    rel_6m = Column('REL_6M', Numeric, default=0.0)
    rel_3m = Column('REL_3M', Numeric, default=0.0)
    rel_1m = Column('REL_1M', Numeric, default=0.0)
    rel_2w = Column('REL_2W', Numeric, default=0.0)
    rel_5d = Column('REL_5D', Numeric, default=0.0)
    ema5 = Column('EMA5', Numeric, default=0.0)
    ema10 = Column('EMA10', Numeric, default=0.0)
    ema20 = Column('EMA20', Numeric, default=0.0)
    ema30 = Column('EMA30', Numeric, default=0.0)
    ema60 = Column('EMA60', Numeric, default=0.0)
    ma_range = Column('MA_RANGE', Numeric, default=0.0)
    ema60_pos = Column('EMA60_POS', Numeric, default=0.0)
    v_diff0 = Column('V_DIFF0', Numeric, default=0.0)
    v_diff1 = Column('V_DIFF1', Numeric, default=0.0)
    v_diff2 = Column('V_DIFF2', Numeric, default=0.0)
    v_diff3 = Column('V_DIFF3', Numeric, default=0.0)
    v_diff4 = Column('V_DIFF4', Numeric, default=0.0)
    v_diff5 = Column('V_DIFF5', Numeric, default=0.0)
    RSI = Column('RSI', Numeric, default=0.0)
    rel_1m_v = Column('REL_1M_V', Numeric, default=0.0)
    rel_2w_v = Column('REL_2W_V', Numeric, default=0.0)
    rel_5d_v = Column('REL_5D_V', Numeric, default=0.0)
    rel_8w_var = Column('REL_8W_VAR', Numeric, default=0.0)
    rel_4w_var = Column('REL_4W_VAR', Numeric, default=0.0)
    rel_2w_var = Column('REL_2W_VAR', Numeric, default=0.0)

    decision = Column('DECISION', String, default='')
    version = Column('VERSION', String)

    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated = Column('UPDATED', DateTime, nullable=True, default=nowTime)
    created = Column('CREATED', DateTime, nullable=True, default=nowTime)


class HistDataD(HistData, Base):

    __tablename__ = 'HIST_DATA_D'


class HistDataW(HistData, Base):

    __tablename__ = 'HIST_DATA_W'


class HistDataM(HistData, Base):

    __tablename__ = 'HIST_DATA_M'


class HistData5(HistData, Base):

    __tablename__ = 'HIST_DATA_5'


class HistData15(HistData, Base):

    __tablename__ = 'HIST_DATA_15'


class HistData30(HistData, Base):

    __tablename__ = 'HIST_DATA_30'


class HistData60(HistData, Base):

    __tablename__ = 'HIST_DATA_60'


class Decision(Base):

    __tablename__ = 'DECISION'

    id = Column('ID', BigInteger, primary_key=True, autoincrement="auto")
    stockCode = Column('STOCK_CODE', String)
    date = Column('DATE_IN', DateTime)
    decision_type = Column('DECISION_TYPE', Integer)
    hour = Column('HOUR', Integer)
    decision = Column('DECISION', String)
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated = Column('UPDATED', DateTime, nullable=True, default=nowTime)
    created = Column('CREATED', DateTime, nullable=True, default=nowTime)

    def to_string(cls):
        return '    Stock Code: ' + cls.stockCode \
               + '  Decision type: ' + str(cls.decision_type) \
               + '  Hour: ' + str(cls.hour) \
               + '  Decision: ' + cls.decision


