# -*- coding: utf-8 -*-
import schedule
import datetime
import threading

import config
import data_fetcher
import log
import utils

PARAM_ONE_HOUR = 1
HOUR_HIST_DOT = 60
STOCK_LIST = config.read_stock_lists()
BEGIN_TIME = '10:40'


def do_task():
    for stock in STOCK_LIST:
        data_fetcher.handle_sixty_minute_k(utils.current_date(), stock, HOUR_HIST_DOT)


def execute_task():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log.logger().info("job begin execute at : " + now)
    threading.Thread(target=do_task).start()


if __name__ == '__main__':

    schedule.every(PARAM_ONE_HOUR).hours.at(BEGIN_TIME).do(execute_task)

    while True:
        schedule.run_pending()
