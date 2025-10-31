# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date:
# desc:
# ---------------------
import configparser
import json

import yaml


def read_yaml_conf(file_path):
    """
    读取yaml配置文件
    :param file_path: 配置文件路径（例如：./log/）
    :return:
    """
    with open(file_path, "r", encoding="utf8") as f:
        return yaml.load(f.read(), Loader=yaml.SafeLoader)


def read_json_conf(file_path):
    """
    读取json配置文件
    :param file_path: 配置文件路径（例如：./log/）
    :return:
    """
    with open(file_path, "r", encoding="utf8") as f:
        return json.loads(f.read())


def read_ini_conf(file_path):
    """
    读取ini配置文件
    :param file_path: 配置文件路径（例如：./log/）
    :return:
    """
    cp = configparser.ConfigParser()
    cp.read(file_path)
    return cp


if __name__ == '__main__':
    print(read_ini_conf("./test/conf.ini")["db"]["host"])
