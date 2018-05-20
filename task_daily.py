# -*- coding: utf-8 -*-
import schedule
import datetime
import threading

import config
import data_fetcher
import log
import utils

PARAM_ONE_DAY = 1
DAY_HIST_DOT = 60
BEGIN_TIME = '16:00'
STOCK_LIST = config.read_stock_lists()


def do_task():
    for stock in STOCK_LIST:
        data_fetcher.handle_day_k(utils.current_date(), stock, DAY_HIST_DOT)


def execute_task():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log.logger().info("job begin execute at : " + now)
    threading.Thread(target=do_task).start()


if __name__ == '__main__':

    schedule.every(PARAM_ONE_DAY).days.at(BEGIN_TIME).do(execute_task).run()

    while True:
        schedule.run_pending()