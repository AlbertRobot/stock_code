#!/usr/bin/python3

import smtplib
from email_helper.mime.text import MIMEText
from email_helper.utils import formataddr

import sys

import os

from common import constants
from conf import config
from store.entity import Decision

current_dir = sys.path[0]
EMAIL_CONFIG = config.load_email_config(current_dir)


def mail(contents):
    """
    发送邮件
    配置文件：app.conf [email]部分
    :param contents: 发送的正文内容
    :return:
    """
    try:
        msg = MIMEText(contents, 'plain', 'utf-8')
        msg['From'] = formataddr([EMAIL_CONFIG.get('user'), EMAIL_CONFIG.get('sender')])
        msg['To'] = formataddr([EMAIL_CONFIG.get('user'), EMAIL_CONFIG.get('user')])
        msg['Subject'] = EMAIL_CONFIG.get('subject')

        server = smtplib.SMTP_SSL(EMAIL_CONFIG.get('smtp.server'), EMAIL_CONFIG.get('port'))
        server.login(EMAIL_CONFIG.get('sender'), EMAIL_CONFIG.get('password'))
        server.sendmail(EMAIL_CONFIG.get('sender'), EMAIL_CONFIG.get('reveiver'), msg.as_string())
        server.quit()
        print("邮件发送成功")
    except Exception:
        print("邮件发送失败")

