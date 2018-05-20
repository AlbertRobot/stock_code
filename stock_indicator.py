import numpy as np
import pandas as pd


# Calculate moving average
def moving_average(x, n, tp):
    x = np.asarray(x)
    if tp == 'simple':
        weights = np.ones(n)
    else:
        weights = np.exp(np.linspace(-1., 0., n))
    weights /= weights.sum()
    a = np.convolve(x, weights, mode='full')[:len(x)]
    a[:n] = a[n]
    return a


# compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg. return value is emaslow,
# emafast, macd which are len(x) arrays,

def moving_average_convergence(x, nslow=26, nfast=12):
    emaslow = moving_average(x, nslow, tp='exponential')
    emafast = moving_average(x, nfast, tp='exponential')
    return emaslow, emafast, emafast - emaslow


def indicator(dataf):
    rg = range(len(dataf.index))
    # Calculate max, min of the daily price,
    dataf['min_price'] = dataf.loc[:, ['close', 'open']].min(axis=1)
    dataf['max_price'] = dataf.loc[:, ['close', 'open']].max(axis=1)
    # Calculate typical range of change of stock price ,
    dataf['var_price'] = abs(dataf['open'] - dataf['close']).rolling(window=5, center=False).mean()
    dataf['var_price_3d'] = abs(dataf['open'] - dataf['close']).rolling(window=3, center=False).mean()
    # 1st calculate all the moving averages,
    dataf.sort_values('date', inplace=True)
    dataf['ma30'] = dataf['close'].rolling(window=30, center=False).mean()
    dataf['ma60'] = dataf['close'].rolling(window=60, center=False).mean()
    dataf['vma30'] = dataf['volume'].rolling(window=30, center=False).mean()
    dataf['vma60'] = dataf['volume'].rolling(window=60, center=False).mean()
    # calculate elasticity of current close price relative to MA5 and MA20,
    dataf['mid_price'] = (dataf['open'] + dataf['close']) / 2.0
    dataf['open_dev_ma5'] = (dataf['open'] - dataf['ma5']) / dataf['var_price']
    dataf['open_dev_ma10'] = (dataf['open'] - dataf['ma10']) / dataf['var_price']
    dataf['close_dev_ma5'] = (dataf['close'] - dataf['ma5']) / dataf['var_price']
    dataf['close_dev_ma10'] = (dataf['close'] - dataf['ma10']) / dataf['var_price']
    # calculate MACD,
    prices = pd.DataFrame(dataf['close']).values  # convert to 1D array,
    prices = np.reshape(prices, len(prices))
    nslow = 26
    nfast = 12
    nema = 9
    macd_grp = moving_average_convergence(prices, nslow=nslow, nfast=nfast)
    macd_dea = moving_average(macd_grp[2], nema, tp='exponential')
    macd_diff = macd_grp[2]
    macd = 2 * (macd_diff - macd_dea)
    macd = list(macd)
    macd_dea = list(macd_dea)
    macd_diff = list(macd_diff)
    dataf['diff'] = macd_diff
    dataf['dea'] = macd_dea
    dataf['macd'] = macd

    # Calcualte BOLL band,
    dataf['boll_low'] = dataf['ma20'] - 2.00 * dataf['close'].ewm(ignore_na=False, span=20, min_periods=0,
                                                                  adjust=True).std()
    dataf['boll_up'] = dataf['ma20'] + 2.00 * dataf['close'].ewm(ignore_na=False, span=20, min_periods=0,
                                                                 adjust=True).std()
    dataf['boll_relative'] = (dataf['close'] - dataf['ma20']) / (dataf['boll_up'] - dataf['ma20'])
    # Pickup the large drop, large increase and dividend date,
    dataf['dividend_ind'] = np.sign(-10.0 - dataf['p_change'])

    # Calculate the lowest/highest price over the past year, half year, 3 months, 1 month and 2 weeks,
    max_1y = [0 for i in rg]
    max_6m = [0 for i in rg]
    max_3m = [0 for i in rg]
    max_1m = [0 for i in rg]
    max_2w = [0 for i in rg]
    max_5d = [0 for i in rg]

    min_1y = [0 for i in rg]
    min_6m = [0 for i in rg]
    min_3m = [0 for i in rg]
    min_1m = [0 for i in rg]
    min_2w = [0 for i in rg]
    min_5d = [0 for i in rg]

    max_1y = dataf['close'].rolling(window=250, center=False).max()
    max_6m = dataf['close'].rolling(window=130, center=False).max()
    max_3m = dataf['close'].rolling(window=66, center=False).max()
    max_1m = dataf['close'].rolling(window=22, center=False).max()
    max_2w = dataf['close'].rolling(window=10, center=False).max()
    max_5d = dataf['close'].rolling(window=5, center=False).max()

    min_1y = dataf['close'].rolling(window=250, center=False).min()
    min_6m = dataf['close'].rolling(window=130, center=False).min()
    min_3m = dataf['close'].rolling(window=66, center=False).min()
    min_1m = dataf['close'].rolling(window=22, center=False).min()
    min_2w = dataf['close'].rolling(window=10, center=False).min()
    min_5d = dataf['close'].rolling(window=5, center=False).min()

    dataf['rel_1y'] = (dataf['close'] - min_1y) / (max_1y - min_1y)
    dataf['rel_6m'] = (dataf['close'] - min_6m) / (max_6m - min_6m)
    dataf['rel_3m'] = (dataf['close'] - min_3m) / (max_3m - min_3m)
    dataf['rel_1m'] = (dataf['close'] - min_1m) / (max_1m - min_1m)
    dataf['rel_2w'] = (dataf['close'] - min_2w) / (max_2w - min_2w)
    dataf['rel_5d'] = (dataf['close'] - min_5d) / (max_5d - min_5d)

    # Calculate the Moving averages exponential average ,
    eam5 = [0 for i in rg]
    eam10 = [0 for i in rg]
    eam20 = [0 for i in rg]
    ema30 = [0 for i in rg]
    ema60 = [0 for i in rg]

    dataf['ema5'] = dataf['close'].ewm(ignore_na=False, span=5, min_periods=0, adjust=True).mean()
    dataf['ema10'] = dataf['close'].ewm(ignore_na=False, span=10, min_periods=0, adjust=True).mean()
    dataf['ema20'] = dataf['close'].ewm(ignore_na=False, span=20, min_periods=0, adjust=True).mean()
    dataf['ema30'] = dataf['close'].ewm(ignore_na=False, span=30, min_periods=0, adjust=True).mean()
    dataf['ema60'] = dataf['close'].ewm(ignore_na=False, span=60, min_periods=0, adjust=True).mean()

    # Calculate the separation of different moving average lines,
    dataf['ma_range'] = (dataf.loc[:, ['ema5', 'ema10', 'ema20', 'ema30', 'ema60']].max(axis=1)
                         - dataf.loc[:, ['ema5', 'ema10', 'ema20', 'ema30', 'ema60']].min(axis=1) / dataf['var_price'])
    dataf['ema60_pos'] = (dataf['ema60'] - dataf['ema20']) / dataf['var_price']

    # Calculate the difference of volume between days,
    vdiff_1 = [0 for i in rg]
    vdiff_1 = dataf['volume'].diff(periods=1)  # calculate the difference in volume relative to yesterday,
    dataf['v_diff0'] = np.sign(vdiff_1)
    dataf['v_diff1'] = np.sign(vdiff_1.shift(periods=1))
    dataf['v_diff2'] = np.sign(vdiff_1.shift(periods=2))
    dataf['v_diff3'] = np.sign(vdiff_1.shift(periods=3))
    dataf['v_diff4'] = np.sign(vdiff_1.shift(periods=4))
    dataf['v_diff5'] = np.sign(vdiff_1.shift(periods=5))

    # Calculate RSI,
    n = 14
    prices = dataf['close']
    deltas = np.diff(prices)
    seed = deltas[:n + 1]
    up = seed[seed >= 0].sum() / n
    down = -seed[seed < 0].sum() / n
    rs = up / down
    rsi = np.zeros_like(prices)
    rsi[:n] = 100. - 100. / (1. + rs)
    for i in range(n, len(prices)):
        delta = deltas[i - 1]  # cause the diff is 1 shorter
        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

            up = (up * (n - 1) + upval) / n
            down = (down * (n - 1) + downval) / n

            rs = up / down
            rsi[i] = 100. - 100. / (1. + rs)
        dataf['rsi'] = rsi

    # calculate relative position of the volume for the past 5 days, 10 days and 1 month,
    max_1m_v = [0 for i in rg]
    max_2w_v = [0 for i in rg]
    max_5d_v = [0 for i in rg]

    min_1m_v = [0 for i in rg]
    min_2w_v = [0 for i in rg]
    min_5d_v = [0 for i in rg]

    max_1m_v = dataf['volume'].rolling(window=20, center=False).max()
    max_2w_v = dataf['volume'].rolling(window=10, center=False).max()
    max_5d_v = dataf['volume'].rolling(window=5, center=False).max()

    min_1m_v = dataf['volume'].rolling(window=20, center=False).min()
    min_2w_v = dataf['volume'].rolling(window=10, center=False).min()
    min_5d_v = dataf['volume'].rolling(window=5, center=False).min()

    dataf['rel_1m_v'] = (dataf['volume'] - min_1m_v) / (max_1m_v - min_1m_v)
    dataf['rel_2w_v'] = (dataf['volume'] - min_2w_v) / (max_2w_v - min_2w_v)
    dataf['rel_5d_v'] = (dataf['volume'] - min_5d_v) / (max_5d_v - min_5d_v)

    # Calculate relative position of the daily variation of price for the past 10 days, 2 weeks and 1 month. This is calculated based,
    # on the 3 day average of price range: 'var_price_3d',
    max_8w_var = [0 for i in rg]
    max_4w_var = [0 for i in rg]
    max_2w_var = [0 for i in rg]

    min_8w_var = [0 for i in rg]
    min_4w_var = [0 for i in rg]
    min_2w_var = [0 for i in rg]

    max_8w_var = dataf['var_price_3d'].rolling(window=40, center=False).max()
    max_4w_var = dataf['var_price_3d'].rolling(window=20, center=False).max()
    max_2w_var = dataf['var_price_3d'].rolling(window=10, center=False).max()

    min_8w_var = dataf['var_price_3d'].rolling(window=40, center=False).min()
    min_4w_var = dataf['var_price_3d'].rolling(window=20, center=False).min()
    min_2w_var = dataf['var_price_3d'].rolling(window=10, center=False).min()

    dataf['rel_8w_var'] = (dataf['var_price_3d'] - min_8w_var) / (max_8w_var - min_8w_var)
    dataf['rel_4w_var'] = (dataf['var_price_3d'] - min_4w_var) / (max_4w_var - min_4w_var)
    dataf['rel_2w_var'] = (dataf['var_price_3d'] - min_2w_var) / (max_2w_var - min_2w_var)

