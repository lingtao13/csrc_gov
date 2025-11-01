# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import logging
import os
from logging import handlers


def log_conf(dir_path, file_path):
    """
    日志配置
    :param dir_path: 日志输出文件夹路径（例如："./log/"）
    :param file_path: 日志输出文件路径（例如：”python.log“）
    :return:
    """
    # 创建日志文件夹
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # 日志
    # 控制台输出日志
    log_sh = logging.StreamHandler()
    # 文件中记录日志 按照大小做切割 文件名越大日志越久远
    log_rh = logging.handlers.RotatingFileHandler(
        dir_path + file_path,
        maxBytes=20 * 1024 * 1024,
        backupCount=5,
        encoding="utf8"
    )
    # 日志格式
    log_fmt_str = "%(asctime)s - %(name)s - %(levelname)s[line :%(lineno)d] - %(module)s:  %(message)s"
    # 日志时间格式
    log_datefmt_str = "%Y-%m-%d %H:%M:%S %p"
    logging.basicConfig(
        format=log_fmt_str,
        datefmt=log_datefmt_str,
        level=logging.INFO,
        handlers=[log_sh, log_rh]
    )
