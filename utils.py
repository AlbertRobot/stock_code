from datetime import datetime


def date2str(date, fmt):
    return date.strftime(fmt)


def trans_date(date_str):
    return date2str(datetime.strptime(date_str, "%Y-%m-%d"), '%Y-%m-%d %H:%M:%S')


def current_date():
    return datetime.now().strftime('%Y-%m-%d')


def current_time():
    return datetime.now().strftime('%H:%M:%S')


def current_datetime():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def str2date(my_str):
    return datetime.strptime(my_str, "%Y-%m-%d")