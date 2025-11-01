# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date:
# desc:
# ---------------------
import datetime
import json
import logging

import requests

api = "http://127.0.0.1:8011/announcement/crawler"
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    "content-type": "application/json"
}


def create_info(crawler_status_list):
    """
    新建项目爬虫信息
    :param crawler_status_list:
    :return:
    """
    try:
        data = {
            "type": "C",
            "crawlerStatusList": crawler_status_list
        }
        resp = requests.post(url=api, data=json.dumps(data), headers=headers, timeout=(11, 31))
        resp_text = resp.text
        logging.info("接口爬虫监控请求返回 {}".format(resp_text))
        print("接口爬虫监控请求返回 {}".format(resp_text))
    except Exception as e:
        logging.error("接口爬虫监控记录错误 {}".format(str(e)))
        print("接口爬虫监控记录错误 {}".format(str(e)))


def update_info(condition_dict, crawler_status_dict):
    """
    更新项目爬虫信息
    :param condition_dict:
    :param crawler_status_dict:
    :return:
    """
    try:
        data = {
            "type": "U",
            "condition": condition_dict,
            "crawlerStatus": crawler_status_dict
        }
        resp = requests.post(url=api, data=json.dumps(data), headers=headers, timeout=(11, 31))
        resp_text = resp.text
        logging.info("接口爬虫监控请求返回 {}".format(resp_text))
        print("接口爬虫监控请求返回 {}".format(resp_text))
    except Exception as e:
        logging.error("接口爬虫监控记录错误 {}".format(str(e)))
        print("接口爬虫监控记录错误 {}".format(str(e)))


def read_info(condition_dict):
    """
    获取项目爬虫信息
    :param condition_dict:
    :return:
    """
    try:
        data = {
            "type": "R",
            "condition": condition_dict
        }
        resp = requests.post(url=api, data=json.dumps(data), headers=headers, timeout=(11, 31))
        resp_text = resp.text
        logging.info("接口爬虫监控请求返回 {}".format(resp_text))
        print("接口爬虫监控请求返回 {}".format(resp_text))
    except Exception as e:
        logging.error("接口爬虫监控记录错误 {}".format(str(e)))
        print("接口爬虫监控记录错误 {}".format(str(e)))


def delete_info(condition_dict):
    """
    删除项目爬虫信息
    :param condition_dict:
    :return:
    """
    try:
        data = {
            "type": "D",
            "condition": condition_dict
        }
        resp = requests.post(url=api, data=json.dumps(data), headers=headers, timeout=(11, 31))
        resp_text = resp.text
        logging.info("接口爬虫监控请求返回 {}".format(resp_text))
        print("接口爬虫监控请求返回 {}".format(resp_text))
    except Exception as e:
        logging.error("接口爬虫监控记录错误 {}".format(str(e)))
        print("接口爬虫监控记录错误 {}".format(str(e)))


if __name__ == '__main__':
    # 改
    condition = {
        "projectName": "证监局36辖区+2专员办",
        "source": "中国证券监督管理委员会",
        "blockName": "北京辖区",
        "blockLink": "http://www.csrc.gov.cn/beijing/c103543/zfxxgk_zdgk.shtml?tab=zdgkml",
        "dbHost": "139.159.150.159",
        "dbName": "pyspider_data",
        "tableName": "csrc_gov"
    }
    crawler_status = {
        "total": 3600,
        "existence": None,
        "increment": 1,
        "state": 1,
        "errorInfo": None,
        "logTime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    # update_info(condition, crawler_status)

    # 查
    condition = {
        "projectName": "证监局36辖区+2专员办",
        "source": "中国证券监督管理委员会",
        "blockName": "北京辖区",
        "blockLink": "http://www.csrc.gov.cn/beijing/c103543/zfxxgk_zdgk.shtml?tab=zdgkml",
        "dbHost": "139.159.150.159",
        "dbName": "pyspider_data",
        "tableName": "csrc_gov"
    }
    # read_info(condition)

    # 删
    condition = {
        "projectName": "测试公告",
        "source": "测试",
        "blockName": "测试",
        "blockLink": "http://www.sse.com.cn/disclosure/listedinfo/announcement/index.shtml",
        "dbHost": "139.159.150.159",
        "dbName": "pyspider_data",
        "tableName": "sse_announcement"
    }
    # delete_info(condition)

    # 增
    crawler_status_list = [{
        "projectName": "测试公告",
        "source": "测试",
        "blockName": "测试",
        "blockLink": "http://www.sse.com.cn/disclosure/listedinfo/announcement/index.shtml",
        "total": 1182,
        "existence": None,
        "increment": None,
        "state": "1",
        "errorInfo": None,
        "logTime": None,
        "dbHost": "139.159.150.159",
        "dbName": "pyspider_data",
        "tableName": "sse_announcement",
        "deployServer": "139.159.224.229",
        "frequency": None
    },
        {
            "projectName": "测试公告2",
            "source": "测试2",
            "blockName": "测试2",
            "blockLink": "http://eid.csrc.gov.cn/ipo/101010/index.html",
            "total": None,
            "existence": None,
            "increment": None,
            "state": None,
            "errorInfo": None,
            "logTime": None,
            "dbHost": "139.159.150.159",
            "dbName": "pyspider_data",
            "tableName": "ipo_zjh_publish",
            "deployServer": "139.159.224.229",
            "frequency": None
        }]
    # create_info(crawler_status_list)
