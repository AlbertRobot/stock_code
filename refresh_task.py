# 定时任务
# 更新日K，周，分钟等数据
# 只会更新在config/stock_lists.txt 文件里面配置的stock code对应的数据
# 确认一下 不同K线数据更新的频率，然后在下面main里面的schedule里面配置

# -*- coding: utf-8 -*-
import datetime
import threading
import time

import sys
import schedule

from common.utils import str2date
from conf import log
from conf import config
from collector import data_preparation
from store import db
from common import utils, constants, email
from store.entity import Decision

# home 路径
HOME_DIR = sys.path[0]
STOCK_LIST = config.read_stock_lists(HOME_DIR)

# 历史数据点 app.conf里面配置
STOCK_POINTS = config.load_system_config(HOME_DIR).get('stock_points')


# 一些常量
FIVE_MINUTE_DELTA = 100
DAILY_DELTA = 20
WEEKLY_DELTA = 30

SCHEDULE_FIVE_MINUTE = 1

SCHEDULE_DAILY = 1
SCHEDULE_WEEKLY = 7

DEFAULT_START_DATE = str2date('2018-01-01')


def transform_decisions(decisions):
    """
    转换成邮件正文格式
    :param decisions:
    :return:
    """
    contents = ''
    for dec in decisions:
        contents = contents + dec + '\n'

    return contents


def handle_decisions(decisions):
    """
    将decisions保存到数据库，并且发送邮件
    :param decisions:
    :return:
    """
    # handle decision
    if len(decisions) > 0:
        for dec in decisions:
            db.insert(dec)
        email.mail(transform_decisions(decisions))


def refresh_stock_basics():
    """
    获取股票基础数据
    :return:
    """
    data_preparation.get_stock_basics()


def refresh_five_minute():
    mutex.acquire()
    """
    刷新5分钟K线数据
    :return:
    """

    decisions = []

    hist_data = db.query_hist_data_5(STOCK_POINTS)
    newest_data = db.query_hist_data_5()
    if hist_data is not None:
        start = hist_data.day
    else:
        start = DEFAULT_START_DATE

    end = start + datetime.timedelta(days=FIVE_MINUTE_DELTA)

    for stock in STOCK_LIST:
        hist_data_list = data_preparation.get_hist_data(stock,
                                                        ktype=constants.KType.fiveMinute.value,
                                                        start=utils.date2str(start, '%Y-%m-%d'),
                                                        end=utils.date2str(end, '%Y-%m-%d'))
        if hist_data_list is None:
            mutex.release()
            return
        if len(hist_data_list) > 0:
            for data in hist_data_list:
                if newest_data is None or data.date > utils.date2str(newest_data.date, '%Y-%m-%d %H:%M:%S'):
                    db.insert(data)
                    if data.decision == constants.DecisionMake.buy.value or data.decision == constants.DecisionMake.sell.value:
                        decision = Decision()
                        decision.decision_type = constants.DecisionType.hour.value
                        decision.decision = data.decision
                        decision.stockCode = stock
                        decisions.append(decision)
        else:
            log.logger().info("no update date for this refresh")

    # handle decision
    handle_decisions(decisions)
    mutex.release()


def refresh_daily():
    """
    刷新日K线数据
        :return:
        """
    mutex.acquire()
    decisions = []

    hist_data = db.query_hist_data_d(STOCK_POINTS)
    newest_data = db.query_hist_data_d()
    if hist_data is not None:
        start = hist_data.day
    else:
        start = DEFAULT_START_DATE
    end = start + datetime.timedelta(days=DAILY_DELTA)

    for stock in STOCK_LIST:
        hist_data_list = data_preparation.get_hist_data(stock,
                                                        ktype=constants.KType.day.value,
                                                        start=utils.date2str(start, '%Y-%m-%d'),
                                                        end=utils.date2str(end, '%Y-%m-%d'))
        if hist_data_list is None:
            mutex.release()
            return

        if len(hist_data_list) > 0:
            for data in hist_data_list:
                if data.date > utils.date2str(newest_data.date, '%Y-%m-%d %H:%M:%S'):
                    db.insert(data)
                    if data.decision == constants.DecisionMake.buy.value or data.decision == constants.DecisionMake.sell.value:
                        decision = Decision()
                        decision.decision_type = constants.DecisionType.day.value
                        decision.decision = data.decision
                        decision.stockCode = stock
                        decisions.append(decision)
        else:
            log.logger().info("no update date for this refresh")

    # handle decision
    handle_decisions(decisions)
    mutex.release()


def refresh_weekly():
    """
刷新周K线数据
        :return:
        """
    mutex.acquire()
    decisions = []

    hist_data = db.query_hist_data_w(older=STOCK_POINTS)
    newest_data = db.query_hist_data_w()
    if hist_data is not None:
        start = hist_data.day
    else:
        start = DEFAULT_START_DATE
    end = start + datetime.timedelta(days=WEEKLY_DELTA)

    for stock in STOCK_LIST:
        hist_data_list = data_preparation.get_hist_data(stock,
                                                        ktype=constants.KType.week.value,
                                                        start=utils.date2str(start, '%Y-%m-%d'),
                                                        end=utils.date2str(end, '%Y-%m-%d'))
        if hist_data_list is None:
            mutex.release()
            return

        if len(hist_data_list) > 0:
            for data in hist_data_list:
                if data.date > utils.date2str(newest_data.date, '%Y-%m-%d %H:%M:%S'):
                    db.insert(data)
                    if data.decision == constants.DecisionMake.buy.value or data.decision == constants.DecisionMake.sell.value:
                        decision = Decision()
                        decision.decision_type = constants.DecisionType.day.value
                        decision.decision = data.decision
                        decision.stockCode = stock
                        decisions.append(decision)
        else:
            log.logger().info("no update date for this refresh")

    # handle decision
    handle_decisions(decisions)
    mutex.release()


def execute_refresh_five_minute():
    """
    封装的多线程任务
    :return:
    """
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log.logger().info("job begin execute at : " + now)
    threading.Thread(target=refresh_five_minute).start()


def execute_refresh_daily():
    """
        封装的多线程任务
        :return:
        """
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log.logger().info("job begin execute at : " + now)
    threading.Thread(target=refresh_daily).start()


def execute_refresh_weekly():
    """
        封装的多线程任务
        :return:
        """
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log.logger().info("job begin execute at : " + now)
    threading.Thread(target=refresh_weekly).start()


if __name__ == '__main__':

    mutex = threading.Lock()

    # schedule timer task
    # 任务可以是日k，周k，5分等，然后设定一个执行周期，设定每次pull的日期范围，然后就可以定时自动执行
    schedule.every(SCHEDULE_FIVE_MINUTE).days.do(execute_refresh_five_minute).run()

    # schedule.every(SCHEDULE_DAILY).days.do(execute_refresh_daily).run()
    #
    # schedule.every(SCHEDULE_WEEKLY).days.do(execute_refresh_weekly).run()

    while True:
        schedule.run_pending()
        time.sleep(1)
