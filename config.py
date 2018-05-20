# -*- coding: utf-8 -*-
import configparser
import os

import sys

config_file = 'app.conf'
stock_list_file = 'stock_lists.txt'
CONFIG_FILE_PATH = sys.path[0] + os.sep + 'conf' + os.sep + config_file
STOCK_FILE_PATH = sys.path[0] + os.sep + 'conf' + os.sep + stock_list_file


def load_db_config():
    """
    获取DB连接配置信息
    :return:
    """
    cp = configparser.ConfigParser()
    cp.read(CONFIG_FILE_PATH)

    return {
        'host': cp.get('db', 'host'),
        'port': cp.get('db', 'port'),
        'database': cp.get('db', 'database'),
        'user': cp.get('db', 'user'),
        'password': cp.get('db', 'pass'),
        'charset': cp.get('db', 'charset'),
        'connect_url': cp.get('db', 'connect_url'),
        'use_unicode': True,
        'get_warnings': True,
    }


def load_system_config():
    """
    配置系统信息
    :return:
    """
    cp = configparser.ConfigParser()
    cp.read(CONFIG_FILE_PATH)
    return {
        'stock_points': cp.get('system', 'stock_points'),
        'log_path': cp.get('system', 'log_path'),
    }


def load_email_config():
    cp = configparser.ConfigParser()
    cp.read(CONFIG_FILE_PATH)
    return {
        'smtp.server': cp.get('email', 'smtp.server'),
        'port': cp.get('email', 'port'),
        'user': cp.get('email', 'user'),
        'password': cp.get('email', 'password'),
        'sender': cp.get('email', 'sender'),
        'reveiver': cp.get('email', 'reveiver'),
        'subject': cp.get('email', 'subject'),
    }


def read_stock_lists():

    f = open(STOCK_FILE_PATH, "r")
    print('load stock list file:' + STOCK_FILE_PATH)
    file_data = []
    line = f.readline()
    while line:
        file_data.append(line)
        line = f.readline()
    f.close()
    return file_data
