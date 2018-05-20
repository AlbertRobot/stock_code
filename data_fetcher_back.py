# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import tushare as ts

from stock_indicator import indicator
from constants import KType
import data_store, entity
import log


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
    df.to_csv("out__.csv")

    size = df.iloc[:, 0].size

    if size == 0:
        return

    data_array = np.array(df)

    for i in range(0, size):
        data = data_array[i]
        stock_basics = entity.StockBasics()
        stock_basics.code = data[0]
        stock_basics.name = data[1]
        stock_basics.industry = data[2]
        stock_basics.area = data[3]
        stock_basics.pe = data[4]
        stock_basics.outstanding = data[5]
        stock_basics.totals = data[6]
        stock_basics.totalAssets = data[7]
        stock_basics.liquidAssets = data[8]
        stock_basics.fixedAssets = data[9]
        stock_basics.reserved = data[10]
        stock_basics.reservedPerShare = data[11]
        stock_basics.esp = data[12]
        stock_basics.bvps = data[13]
        stock_basics.pb = data[14]
        stock_basics.timeToMarket = data[15]
        stock_basics.undp = data[16]
        stock_basics.perundp = data[17]
        stock_basics.rev = data[18]
        stock_basics.profit = data[19]
        stock_basics.gpr = data[20]
        stock_basics.npr = data[21]
        stock_basics.holders = data[22]
        stock_basics.stockType = get_stock_type(data[0])

        stock_basics.version = 0
        data_store.insert(stock_basics)

    # add stock type: Shanghai or Shenzhen
    for i in range(len(df.index)):
        if df.iloc[i, 0].startswith("6"):
            df.loc[i, 'type'] = 'sh_A'
        elif df.iloc[i, 0].startswith("00"):
            df.loc[i, 'type'] = 'sz_A'
        elif df.iloc[i, 0].startswith("6016"):
            df.loc[i, 'type'] = 'sh_bluechip'
        elif df.iloc[i, 0].startswith("900"):
            df.loc[i, 'type'] = 'sh_B'
        elif df.iloc[i, 0].startswith("002"):
            df.loc[i, 'type'] = 'zxb'
        elif df.iloc[i, 0].startswith("200"):
            df.loc[i, 'type'] = 'sz_B'
        elif df.iloc[i, 0].startswith("300"):
            df.loc[i, 'type'] = 'cyb'
        else:
            df.loc[i, 'type'] = 'other'


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


# code：股票代码，即6位数字代码，或者指数代码（sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
# start：开始日期，格式YYYY-MM-DD
# end：结束日期，格式YYYY-MM-DD
# ktype：数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
# retry_count：当网络异常后重试次数，默认为3
# pause:重试时停顿秒数，默认为0
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
        log.logger().info("no data for: "
                          + code + " start date: "
                          + start + "end date: "
                          + end + "K type: "
                          + ktype)
        return

    # indicator计算
    indicator(df)
    df.to_csv("out_indicator.csv")
    # 1. 先把stock list 里面的股票分类。当前状态为"hold" 的分为一类，检查是否需要sell.  当前状态为空(即:未持有), 需要检查是否应该买入。
    # 2. 检查处于Hold状态的股票。 对于每一支处于hold的股票， 先看60min K线是否MA5 下穿了MA10 (这个是在运行６０min线的时候每个小时检查一次）.
    #    然后再看日k线的前一日收盘价是否已经开始接近下穿10日均线。
    #    具体判定: 如果某一个小时的收盘价的MA5<MA10 并且 前一个小时的收盘价MA5>MA10 AND 前一日（比如说，今天是4月17日，前一个交易日就是4月16日)收盘价均线
    #   （MA5-MA10）< 上前一日的收盘价均线（4月13日) (MA5-MA10). 发出卖出信号(邮件)，同时把decision column 改为"sell"
    # 3. 检查状态为空的股票。
    #     第一，检查过去两周走势跟大盘的对比。具体指标: 1. 10日均线的斜率对比 2. 上一日收盘价和14个交易日前收盘价的对比 3. 上涨
    #    天数占过去14个交易日的比例(按照收盘价来算, ts.get_hist_data() 里面有一列 p_change 就可以直接用来计算正负号)。当这三个指标里有两个或以上
    #     个股比大盘强，就认为比大盘走势强。
    #      第二:  要看10日均线的斜率。 要求斜率为正
    #      第三:  5日均线从下开始上穿10日均线。 i.e. 上一日收盘MA5>MA10, 上上交易日收盘 MA5<MA10
    #      第四:  过去５个交易日成交量(Volume 那一列）　斜率为正
    #      这四点同时满足，发出买入信号。　其中，第一的过去两周走势对比（１０个交易日），　第四点钟过去５个交易日的成交量斜率，　这两个数字（１０和５）写为可设置
    #      和调整的参数
    # 计算完买入和卖出信号以后，要把每支股票的买入价，卖出价，买入和卖出的日期，股票代码，股票名字，输出到一张表格。新的数据直接ａｐｐｅｎｄ到已有数据上。
    # 目的是可以计算损益。
    #

    size = df.iloc[:, 0].size

    if size == 0:
        return
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object).where(pd.notnull(df), None)

    data_array = np.array(df)

    for i in range(0, size):
        data = data_array[i]
        hist_data = choose_hist_table(ktype)
        hist_data.stockCode = code
        hist_data.date = data[0]
        hist_data.day = data[0]
        hist_data.open = data[1]
        hist_data.high = data[2]
        hist_data.close = data[3]
        hist_data.low = data[4]
        hist_data.volume = data[5]
        hist_data.price_change = data[6]
        hist_data.p_change = data[7]
        hist_data.ma5 = data[8]
        hist_data.ma10 = data[9]
        hist_data.ma20 = data[10]
        hist_data.v_ma5 = data[11]
        hist_data.v_ma10 = data[12]
        hist_data.v_ma20 = data[13]
        hist_data.turnover = data[14]
        hist_data.min_price = data[15]
        hist_data.max_price = data[16]
        hist_data.var_price = data[17]
        hist_data.var_price_3d = data[18]
        hist_data.ma30 = data[19]
        hist_data.ma60 = data[20]
        hist_data.vma30 = data[21]
        hist_data.vma60 = data[22]
        hist_data.mid_price = data[23]
        hist_data.open_dev_ma5 = data[24]
        hist_data.open_dev_ma10 = data[25]
        hist_data.close_dev_ma5 = data[26]
        hist_data.close_dev_ma10 = data[27]
        hist_data.diff = data[28]
        hist_data.dea = data[29]
        hist_data.macd = data[30]
        hist_data.BOLL_low = data[31]
        hist_data.BOLL_up = data[32]
        hist_data.BOLL_relative = data[33]
        hist_data.dividend_ind = data[34]
        hist_data.rel_1y = data[35]
        hist_data.rel_6m = data[36]
        hist_data.rel_3m = data[37]
        hist_data.rel_1m = data[38]
        hist_data.rel_2w = data[39]
        hist_data.rel_5d = data[40]
        hist_data.ema5 = data[41]
        hist_data.ema10 = data[42]
        hist_data.ema20 = data[43]
        hist_data.ema30 = data[44]
        hist_data.ema60 = data[45]
        hist_data.ma_range = data[46]
        hist_data.ema60_pos = data[47]
        hist_data.v_diff0 = data[48]
        hist_data.v_diff1 = data[49]
        hist_data.v_diff2 = data[50]
        hist_data.v_diff3 = data[51]
        hist_data.v_diff4 = data[52]
        hist_data.v_diff5 = data[53]
        hist_data.RSI = data[54]
        hist_data.rel_1m_v = data[55]
        hist_data.rel_2w_v = data[56]
        hist_data.rel_5d_v = data[57]
        hist_data.rel_8w_var = data[58]
        hist_data.rel_4w_var = data[59]
        hist_data.rel_2w_var = data[60]

        hist_data.version = 0

        # add data to list
        hist_data_list.append(hist_data)

    return hist_data_list


if __name__ == '__main__':
    df = ts.get_hist_data(code='600198', start='2018-03-01', end='2018-04-01', ktype=KType.week.value)
    df.to_csv("out.csv")
