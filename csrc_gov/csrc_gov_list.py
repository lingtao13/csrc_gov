# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022/12/12
# desc: 证监局辖区列表页爬虫
# ---------------------
import datetime
import json
import logging
import os
import re

import requests
from retrying import retry

from tools.monitor_tool import update_info
from tools.guise_tool import random_user_agent
from tools.proxy_tool import ProxyTool
from tools.conf_tool import read_yaml_conf
from tools.mysql_tool import MysqlTool
from tools.snow_tool import SnowTool
from tools.log_tool import log_conf


class CsrcGovList(object):

    def __init__(self):
        # 读取配置文件
        self.global_conf = read_yaml_conf("conf/csrc_gov_list_pro.yml")
        # 日志
        log_conf(self.global_conf["log_path"], self.global_conf["log_file_path"])
        # snow
        self.snow_tool = SnowTool()
        # proxy
        self.proxy_tool = ProxyTool()
        # time
        # 今天日期
        self.today_time = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        # 开始日期
        self.start_time = (datetime.datetime.now() + datetime.timedelta(
            days=-self.global_conf["update_time_extent"])).strftime("%Y-%m-%d %H:%M:%S")
        # self.start_time = "2022-10-25"
        # 结束日期
        self.end_time = (datetime.datetime.now() + datetime.timedelta(days=-0)).strftime("%Y-%m-%d %H:%M:%S")
        # self.end_time = "2022-11-25"
        # 代理
        self.proxy_dict = self.read_cache_proxy()
        # 代理使用计数
        self.proxy_count = 0
        # 创建数据库连接对象
        self.mysql_tool = self.create_db()
        # 数据库表
        self.db_table = self.global_conf["db_table"]
        # 数据库监控表
        self.db_monitor_table = self.global_conf["db_monitor_table"]

    @retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=3000)
    def retry_handle_snow_id(self):
        """
        重试获取雪花ID
        :return:
        """
        text_id = self.snow_tool.get_snow_id()
        if not text_id:
            raise Exception("获取雪花ID失败")
        return text_id

    def handle_snow_id(self):
        """
        获取雪花ID
        :return:
        """
        try:
            return self.retry_handle_snow_id()
        except Exception as e:
            logging.error(str(e))
            return None

    @staticmethod
    def handle_crawler_status_to_file(**kwargs):
        """
        爬虫监控信息写入文件
        :param kwargs: id:            项目ID（必填，对应数据库主键）
                       total:         已发布总条数
                       existence:     已入库总条数
                       increment:     当前新增数
                       state:         程序运行状态
                       error_info:    程序异常说明
                       block_name:    采集板块名称
        :return:
        """
        # 异常判断
        if not kwargs.get("id"):
            raise Exception("监控信息id不能为空")
        if not kwargs.get("crawler_status_path"):
            raise Exception("监控文件路径不能为空")
        # 写入配置文件
        with open(
                kwargs.get("crawler_status_path"),
                mode="w",
                encoding="utf8"
        ) as f:
            data_str = json.dumps(kwargs, ensure_ascii=False)
            f.write(data_str)

    def handle_crawler_status_to_db(self, **kwargs):
        """
        爬虫监控信息写入数据库
        :param kwargs: id:            项目ID（必填，对应数据库主键）
                       total:         已发布总条数
                       existence:     已入库总条数
                       increment:     当前新增数
                       state:         程序运行状态
                       error_info:    程序异常说明
                       block_name:    采集板块名称
        :return:
        """
        # 异常判断
        if not kwargs.get("id"):
            raise Exception("监控信息id不能为空")
        mysql_tool = self.create_monitor_db()
        # 写入数据库
        db, cs = mysql_tool.open_db_conn()
        ret_update_db_sql = mysql_tool.update_db_sql(
            db,
            cs,
            self.db_monitor_table,
            kwargs,
            "`id` = {}".format(kwargs.get("id"))
        )
        if ret_update_db_sql:
            logging.info("爬虫监控信息写入数据库成功")
        else:
            logging.error("爬虫监控信息写入数据库失败")
        mysql_tool.close_db_conn(db, cs)
        mysql_tool.close_ssh_conn()

    def create_db(self):
        """
        当mysql连接后3分钟左右未操作时，远程mysql会强迫关闭现有连接，导致执行数据库sql失败，现需执行sql则创建连接
        :return:
        """
        db_conf = "db_server"
        mysql_tool = MysqlTool(
            db_host=self.global_conf[db_conf]["db_host"],
            db_port=self.global_conf[db_conf]["db_port"],
            db_username=self.global_conf[db_conf]["db_username"],
            db_password=self.global_conf[db_conf]["db_password"],
            db_database=self.global_conf[db_conf]["db_database"],
            ssh_host=self.global_conf[db_conf]["ssh_host"],
            ssh_username=self.global_conf[db_conf]["ssh_username"],
            ssh_password=self.global_conf[db_conf]["ssh_password"],
            charset="utf8"
        )
        return mysql_tool

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=3000)
    def create_monitor_db(self):
        """
        当mysql连接后3分钟左右未操作时，远程mysql会强迫关闭现有连接，导致执行数据库sql失败，现需执行sql则创建连接
        :return:
        """
        db_conf = "monitor_db_server"
        mysql_tool = MysqlTool(
            db_host=self.global_conf[db_conf]["db_host"],
            db_port=self.global_conf[db_conf]["db_port"],
            db_username=self.global_conf[db_conf]["db_username"],
            db_password=self.global_conf[db_conf]["db_password"],
            db_database=self.global_conf[db_conf]["db_database"],
            ssh_host=self.global_conf[db_conf]["ssh_host"],
            ssh_username=self.global_conf[db_conf]["ssh_username"],
            ssh_password=self.global_conf[db_conf]["ssh_password"],
            charset="utf8"
        )
        return mysql_tool

    def read_cache_proxy(self):
        """
        读取缓存文件中的代理
        :return:
        """
        # 获取配置文件中代理缓存文件位置
        proxy_cache_path = self.global_conf["proxy_cache_path"]
        try:
            with open(proxy_cache_path, "r", encoding="utf8") as f:
                return json.loads(f.read())
        except Exception as e:
            logging.error(str(e))
            return None

    def write_cache_proxy(self, proxy_dict):
        """
        代理写入到缓存文件中
        :param proxy_dict:
        :return:
        """
        # 获取配置文件中代理缓存文件位置
        proxy_cache_path = self.global_conf["proxy_cache_path"]
        # 判断所在文件夹是否存在，不存在则创建
        proxy_cache_dir_path = proxy_cache_path[0:proxy_cache_path.rfind("/")]
        if not os.path.exists(proxy_cache_dir_path):  # 文件夹不存在
            os.makedirs(proxy_cache_dir_path)
        # 正则判断代理格式是否正确
        proxy_re = r"{'http': 'http://(\d+).(\d+).(\d+).(\d+):(\d+)', 'https': 'https://(\d+).(\d+).(\d+).(\d+):(\d+)'}"
        if re.match(proxy_re, str(proxy_dict)):  # 代理格式正确
            with open(proxy_cache_path, "w", encoding="utf8") as f:
                f.write(json.dumps(proxy_dict))
                return True
        else:  # 代理格式错误
            logging.error("代理格式错误，保存失败")
            return False

    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
    def retry_handle_proxy(self):
        """
        重试获取代理
        :return:
        """
        # 调用获取代理接口
        ret_get_proxy = self.proxy_tool.get_proxy()
        # 更新获取代理计数
        self.proxy_count += 1
        # 获取代理失败抛出异常重试获取
        if not ret_get_proxy:
            raise Exception("获取代理失败")
        # 将代理保存到文件中
        ret_write_cache_proxy = self.write_cache_proxy(ret_get_proxy)
        # 代理格式是否正确
        if ret_write_cache_proxy:  # 正确
            return ret_get_proxy
        else:  # 错误
            return None

    def handle_proxy(self):
        """
        获取代理
        :return:
        """
        try:
            return self.retry_handle_proxy()
        except Exception as e:
            logging.error(str(e))
            return None

    @staticmethod
    def parse_manuscript_data(text_dict):
        """
        解析原稿信息(此处解析分类，是根据解读js后的结果)
        :param text_dict:
        :return:
        """
        # # 分析官网js逻辑，提取分类（官网逻辑有问题，个人觉得此才为官网正确逻辑）
        # channel_name_list = []
        # # 数据列表
        # data_list = text_dict["results"]["data"]["data"]
        # for data in data_list:
        #     channel_list = data["_source"]["channel"]
        #     for channel in channel_list:
        #         if data["_source"]["isCarbonCopy"] == "0" or data["_source"]["isCarbonCopy"] == "1":
        #             if channel["channelName"] == "证监局文种体裁" or channel["channelName"] == "证监局主题分类":
        #                 channel_name_list.append(data["_source"]["channelName"])
        # channel_name_str = ";".join(channel_name_list)
        # return channel_name_str

        # # 分析官网js逻辑，提取分类（官网原封不动逻辑，但是感觉有问题）
        # channel_name_list = []
        # # 数据列表
        # data_list = text_dict["results"]["data"]["data"]
        # for data in data_list:
        #     if data["_source"]["isCarbonCopy"] == "0":
        #         # 判断主稿件是在文种还是主题
        #         channel_list = data["_source"]["channel"]
        #         for channel in channel_list:
        #             # 判断主稿件是在文种还是主题
        #             if channel["channelName"] == "证监局文种体裁":
        #                 channel_name_list.append(data["_source"]["channelName"])
        #             elif channel["channelName"] == "证监局主题分类":
        #                 channel_name_list.append(data["_source"]["channelName"])
        #     elif data["_source"]["isCarbonCopy"] == "1":
        #         # 判断主稿件是在文种还是主题
        #         channel_list = data["_source"]["channel"]
        #         for channel in channel_list:
        #             # 判断主稿件是在文种还是主题
        #             if channel["channelName"] == "证监局文种体裁":
        #                 channel_name_list.append(data["_source"]["channelName"])
        #             elif channel["channelName"] == "证监局主题分类":
        #                 channel_name_list.append(data["_source"]["channelName"])
        # channel_name_str = ";".join(channel_name_list)
        # return channel_name_str

        # # 该分类为列表页分类
        # channel_name_list = []
        # channel_name_flag = False
        # parent_channel_name = "证监局主题分类"
        # # 数据列表
        # data_list = text_dict["results"]["data"]["data"]
        # for data in data_list:
        #     channel_list = data["_source"]["channel"]
        #     multilevel_channel_name = []
        #     for channel in channel_list:
        #         if channel["channelName"] == parent_channel_name:
        #             channel_name_flag = True
        #             continue
        #         if channel_name_flag:
        #             multilevel_channel_name.append(channel["channelName"])
        #             multilevel_channel_name_str = "-".join(multilevel_channel_name)
        #             channel_name_list.append(multilevel_channel_name_str)
        #     channel_name_flag = False
        # channel_name_str = ";".join(channel_name_list)
        # return channel_name_str

        # 该分类为列表页分类（只取一级分类）
        channel_name_list = []
        channel_name_flag = False
        parent_channel_name = "证监局主题分类"
        # 数据列表
        data_list = text_dict["results"]["data"]["data"]
        for data in data_list:
            channel_list = data["_source"]["channel"]
            for channel in channel_list:
                if channel["channelName"] == parent_channel_name:
                    channel_name_flag = True
                    continue
                if channel_name_flag:
                    if channel["channelName"] not in channel_name_list:
                        channel_name_list.append(channel["channelName"])
                    break
            channel_name_flag = False
        channel_name_str = ";".join(channel_name_list)
        return channel_name_str

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=5000)
    def retry_get_manuscript_data(self, manuscript_id):
        """
        重试获取原稿信息
        :param manuscript_id:
        :return:
        """
        headers = {
            "User-Agent": random_user_agent()
        }
        data = {
            "mId": manuscript_id,
            "status": 4
        }
        # 是否使用代理
        if self.global_conf["is_use_proxy"]:
            # 判断proxy_dict是否有代理
            if not self.proxy_dict:
                self.proxy_dict = self.handle_proxy()
            # proxy_dict不为None
            if self.proxy_dict:
                # 发起请求
                resp = requests.post(
                    url=self.global_conf["manuscript_data_base_url"],
                    data=data,
                    headers=headers,
                    proxies=self.proxy_dict,
                    timeout=(5, 10),
                    verify=False
                )
                if resp.ok:
                    resp_text = resp.text
                    logging.info("代理原稿信息爬虫响应")
                    # 身份异常，很有可能就是因为IP被封，故换代理
                    if "Auth Failed" in resp_text:
                        # 将全局变量中的代理设置为None
                        self.proxy_dict = None
                        raise Exception("身份异常")
                    resp_dict = json.loads(resp_text)
                    if "results" in resp_dict:
                        return resp_dict
                    else:
                        raise Exception("获取原稿数据失败")
                else:
                    raise Exception("获取原稿数据失败")
        else:
            resp = requests.post(
                url=self.global_conf["manuscript_data_base_url"],
                data=data,
                headers=headers,
                timeout=(5, 10),
                verify=False
            )
            if resp.ok:
                resp_text = resp.text
                logging.info("非代理原稿信息爬虫响应")
                resp_dict = json.loads(resp_text)
                if "results" in resp_dict:
                    return resp_dict
                else:
                    raise Exception("获取原稿数据失败")
            else:
                raise Exception("获取原稿数据失败")

    def get_manuscript_data(self, manuscript_id):
        """
        获取原稿信息
        :param manuscript_id:
        :return:
        """
        # 代理单次获取重试最大次数
        retry_number = self.global_conf["get_proxy_retry_number"]
        # 重试获取代理次数
        retry_count = 0
        # 返回爬取内容
        crawl_data = None
        try:
            crawl_data = self.retry_get_manuscript_data(manuscript_id)
        except requests.exceptions.ProxyError as epe:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_manuscript_data(manuscript_id)
            logging.error("代理失效，错误信息：{}".format(epe))
        except requests.exceptions.ConnectTimeout as ect:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_manuscript_data(manuscript_id)
            logging.error("连接超时，错误信息：{}".format(ect))
        except requests.exceptions.ReadTimeout as ert:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_manuscript_data(manuscript_id)
            logging.error("读取超时，错误信息：{}".format(ert))
        except Exception as e:
            logging.error("获取单页数据失败，错误信息：{}".format(str(e)))
        finally:
            return crawl_data

    def parse_list_page(self, text_dict):
        """
        解析列表页
        :param text_dict:
        :return:
        """
        # 当天页面数据新增数量
        today_page_increased = 0
        # 当天数据库数据新增数量
        today_db_increased = 0
        # 页面数据新增数量
        page_increased = 0
        # 数据库数据新增数量
        db_increased = 0

        # 数据列表
        results = text_dict["data"]["results"]
        for result in results:
            # 辖区
            precinct = text_dict["precinct"]
            # 辖区编号
            precinct_code = text_dict["precinct_code"]
            # 标题
            title = result["title"]
            # 详情url
            url = result["url"]
            if url.startswith("//"):
               url = "http:" + url
            # 发文日期
            published_time_str = result["publishedTimeStr"]
            # 原稿id
            manuscript_id = result["manuscriptId"]
            # # 分类（该分类可能不为一级分类，故暂不采用）
            # channel_name = result["channelName"]
            # 文号
            # try:
            #     number_dict = result["domainMetaList"][0]["resultList"][9]
            #     if number_dict["name"] == "文号":
            #         number_str = number_dict["value"] if number_dict["value"] else None
            #     else:
            #         number_str = None
            # except Exception as e:
            #     number_str = None
            number_str = ""
            number_str_flag = 0
            domain_meta_list = result["domainMetaList"]
            for domain_meta in domain_meta_list:
                result_list = domain_meta["resultList"]
                for results in result_list:
                    if results["name"] == "文号":
                        if results["value"]:
                            number_str = results["value"]
                        number_str_flag = 1
                        break
                if number_str_flag:
                    break
            if "null" == number_str.strip():
                number_str = ""

            # 是否全量爬取
            if not self.global_conf["is_full_crawled"]:
                # 是否在爬取周期时间范围之外
                if not (published_time_str >= self.start_time):
                    return False, today_page_increased, today_db_increased, page_increased, db_increased

            # 查询数据库详情url
            db, cs = self.mysql_tool.open_db_conn()
            select_db_sql_ret = self.mysql_tool.select_db_sql(
                db,
                cs,
                self.db_table,
                [],
                "`detail_url` = '{}' and "
                "(`attachment_url` is null or `attachment_url` = '') and "
                "`precinct` = '{}'".format(url, precinct)
            )
            self.mysql_tool.close_db_conn(db, cs)

            # 判断数据库是否连接查询成功
            if select_db_sql_ret is None:
                logging.error("数据库连接失败")
                continue

            # 当天页面数据新增+1
            if published_time_str >= self.today_time:
                today_page_increased += 1
            # # 页面数据新增+1
            # page_increased += 1

            # 判断数据库是否存在详情url
            if not select_db_sql_ret:
                text_id = self.handle_snow_id()
                # text_id = 1
                # 判断唯一ID是否获取成功
                if text_id:
                    ret_get_manuscript_data = self.get_manuscript_data(manuscript_id)
                    # ret_get_manuscript_data = 1
                    # 判断原稿信息是否获取成功
                    if ret_get_manuscript_data:
                        ret_parse_manuscript_data = self.parse_manuscript_data(ret_get_manuscript_data)
                        # ret_parse_manuscript_data = channel_name
                        # 插入正文数据
                        db, cs = self.mysql_tool.open_db_conn()
                        ret_id = self.mysql_tool.insert_db_sql(
                            db,
                            cs,
                            self.db_table,
                            {
                                "precinct": precinct,
                                "precinct_code": precinct_code,
                                "title": title,
                                "detail_url": url,
                                "publish_time": published_time_str,
                                "number": number_str,
                                "type": ret_parse_manuscript_data,
                                "insert_time": datetime.datetime.now(),
                                "text_id": text_id
                            }
                        )
                        self.mysql_tool.close_db_conn(db, cs)
                    else:
                        continue
                else:
                    continue
            else:
                ret_id = select_db_sql_ret[0]["id"]
                ret_type = select_db_sql_ret[0]["type"]
                ret_number = select_db_sql_ret[0]["number"]
                ret_title = select_db_sql_ret[0]["title"]
                ret_publish_time = select_db_sql_ret[0]["publish_time"]

                ret_get_manuscript_data = self.get_manuscript_data(manuscript_id)
                # ret_get_manuscript_data = 1
                # 判断原稿信息是否获取成功
                if ret_get_manuscript_data:
                    ret_parse_manuscript_data = self.parse_manuscript_data(ret_get_manuscript_data)
                    # ret_parse_manuscript_data = channel_name
                    # 判断列表页数据是否变更
                    if title == ret_title and published_time_str == ret_publish_time and number_str == ret_number and ret_parse_manuscript_data == ret_type:
                        pass
                    else:
                        # 事务更新数据
                        try:
                            db, cs = self.mysql_tool.open_db_conn()
                            transaction_update_db_sql_ret_id = self.mysql_tool.transaction_update_db_sql(
                                db,
                                cs,
                                self.db_table,
                                {
                                    "number": number_str,
                                    "title": title,
                                    "publish_time": published_time_str,
                                    "type": ret_parse_manuscript_data,
                                    "flag": 0
                                },
                                "`id` = {}".format(ret_id)
                            )
                            transaction_update_db_sql_ret_pid = self.mysql_tool.transaction_update_db_sql(
                                db,
                                cs,
                                self.db_table,
                                {
                                    "number": number_str,
                                    "publish_time": published_time_str,
                                    "type": ret_parse_manuscript_data,
                                },
                                "`pid` = {}".format(ret_id)
                            )
                            if transaction_update_db_sql_ret_id and transaction_update_db_sql_ret_pid:
                                db.commit()
                            else:
                                db.rollback()
                        except Exception as e:
                            logging.error("数据库事务更新错误 {}".format(str(e)))
                            db.rollback()
                        finally:
                            self.mysql_tool.close_db_conn(db, cs)

        return True, today_page_increased, today_db_increased, page_increased, db_increased

        # # 判断文号是否为None
        # if ret_number is None:
        #     # 更新文号数据
        #     mysql_tool = self.create_db()
        #     mysql_tool.update_db_sql(
        #         self.db_table,
        #         {
        #             "number": number_str,
        #         },
        #         "`id` = {} or `pid` = {}".format(ret_id, ret_id)
        #     )
        #     mysql_tool.close_db_conn()

        # # 判断类别是否存在
        # if not ret_type:
        #     ret_get_manuscript_data = self.get_manuscript_data(manuscript_id)
        #     # ret_get_manuscript_data = 1
        #     # 判断原稿信息是否获取成功
        #     if ret_get_manuscript_data:
        #         ret_parse_manuscript_data = self.parse_manuscript_data(ret_get_manuscript_data)
        #         # ret_parse_manuscript_data = channel_name
        #         # 更新正文数据
        #         mysql_tool = self.create_db()
        #         mysql_tool.update_db_sql(
        #             self.db_table,
        #             {
        #                 "type": ret_parse_manuscript_data,
        #             },
        #             "`id` = {} or `pid` = {}".format(ret_id, ret_id)
        #         )
        #         mysql_tool.close_db_conn()
        #     else:
        #         continue
        # else:
        #     ret_parse_manuscript_data = ret_type

        # # 附件列表（附件列表响应的附件信息与详情页响应的数据信息不一致，已详情页为准故此代码注释不使用）
        # res_list = result["resList"]
        # for res in res_list:
        #     # 标题
        #     title = os.path.splitext(res["fileName"])[0]
        #     # 附件url
        #     attachment_url = "http://www.csrc.gov.cn" + res["filePath"]
        #
        #     # 查询数据库文件url
        #     mysql_tool = self.create_db()
        #     select_db_sql_ret = mysql_tool.select_db_sql(
        #         self.db_table,
        #         ["id", "pid"],
        #         "`pid` = '{}' and `attachment_url` = '{}'".format(ret_id, attachment_url)
        #     )
        #     mysql_tool.close_db_conn()
        #
        #     # 判断数据库是否连接查询成功
        #     if select_db_sql_ret is None:
        #         logging.error("数据库连接失败")
        #         continue
        #
        #     # 判断数据库是否存在附件url
        #     if not select_db_sql_ret:
        #         # text_id = self.handle_snow_id()
        #         text_id = 1
        #         if text_id:
        #             # 插入附件新数据
        #             mysql_tool = self.create_db()
        #             mysql_tool.insert_db_sql(
        #                 self.db_table,
        #                 {
        #                     "pid": ret_id,
        #                     "precinct": precinct,
        #                     "precinct_code": precinct_code,
        #                     "title": title,
        #                     "detail_url": url,
        #                     "publish_time": published_time_str,
        #                     "number": number_str,
        #                     "attachment_url": attachment_url,
        #                     "type": ret_parse_manuscript_data,
        #                     "insert_time": datetime.datetime.now(),
        #                     # "text_id": text_id
        #                 }
        #             )
        #             mysql_tool.close_db_conn()
        #         else:
        #             continue

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=5000)
    def retry_get_list_page(self, url, page, page_size=50):
        """
        重试获取列表页
        :param url:
        :param page:
        :param page_size:
        :return:
        """
        headers = {
            "User-Agent": random_user_agent()
        }
        params = {
            "_isAgg": "true",
            "_isJson": "true",
            "_pageSize": page_size,
            "_template": "index",
            "_rangeTimeGte": "",
            "_channelName": "",
            "page": page
        }
        # 是否使用代理
        if self.global_conf["is_use_proxy"]:
            # 判断proxy_dict是否有代理
            if not self.proxy_dict:
                self.proxy_dict = self.handle_proxy()
            # proxy_dict不为None
            if self.proxy_dict:
                # 发起请求
                resp = requests.post(
                    url=url,
                    params=params,
                    headers=headers,
                    proxies=self.proxy_dict,
                    timeout=(5, 10),
                    verify=False
                )
                if resp.ok:
                    resp_text = resp.text
                    logging.info("代理列表页爬虫响应")
                    # 身份异常，很有可能就是因为IP被封，故换代理
                    if "Auth Failed" in resp_text:
                        # 将全局变量中的代理设置为None
                        self.proxy_dict = None
                        raise Exception("身份异常")
                    resp_dict = json.loads(resp_text)
                    if "data" in resp_dict:
                        return resp_dict
                    else:
                        raise Exception("获取列表页失败")
                else:
                    raise Exception("获取列表页失败")
        else:
            resp = requests.get(
                url=url,
                params=params,
                headers=headers,
                timeout=(5, 10),
                verify=False
            )
            if resp.ok:
                resp_text = resp.text
                logging.info("非代理列表页爬虫响应")
                resp_dict = json.loads(resp_text)
                if "data" in resp_dict:
                    return resp_dict
                else:
                    raise Exception("获取列表页失败")
            else:
                raise Exception("获取列表页失败")

    def get_list_page(self, url, page, page_size=50):
        """
        获取列表页
        :param url:
        :param page:
        :param page_size:
        :return:
        """
        # 代理单次获取重试最大次数
        retry_number = self.global_conf["get_proxy_retry_number"]
        # 重试获取代理次数
        retry_count = 0
        # 返回爬取内容
        crawl_data = None
        try:
            crawl_data = self.retry_get_list_page(url, page, page_size)
        except requests.exceptions.ProxyError as epe:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_list_page(url, page, page_size)
            logging.error("代理失效，错误信息：{}".format(epe))
        except requests.exceptions.ConnectTimeout as ect:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_list_page(url, page, page_size)
            logging.error("连接超时，错误信息：{}".format(ect))
        except requests.exceptions.ReadTimeout as ert:
            if retry_count < retry_number:
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                crawl_data = self.retry_get_list_page(url, page, page_size)
            logging.error("读取超时，错误信息：{}".format(ert))
        except Exception as e:
            logging.error("获取单页数据失败，错误信息：{}".format(str(e)))
        finally:
            return crawl_data

    def run(self):
        logging.info("=" * 50 + "开始采集列表页" + "=" * 50)
        logging.info("开始：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logging.info("周期内开始时间：{}，结束时间：{}".format(self.start_time, self.end_time))
        # 遍历每一辖区
        for precinct in self.global_conf["precinct_list"]:
            # 监控信息
            # 状态 0失败 1成功
            state = 0
            # 页面总条数
            page_total = None
            # 数据库总条数
            db_total = None
            # 当天页面数据新增数量
            today_page_increased = 0
            # 当天数据库数据新增数量
            today_db_increased = 0
            # 页面数据新增数量
            page_increased = 0
            # 数据库数据新增数量
            db_increased = 0
            # 错误信息
            error_info = ""

            try:
                logging.info("正在爬取{}".format(precinct["precinct"]))
                # 获取总页数
                resp_dict = self.get_list_page(precinct["list_page_base_url"], 1)
                if resp_dict:
                    # 总条数
                    total = resp_dict["data"]["total"]
                    # 每页条数
                    rows = resp_dict["data"]["rows"]
                    # 计算总页数
                    pages = total // rows
                    if total % rows > 0:
                        pages += 1
                    # 遍历每一页
                    for page in range(1, pages + 1):
                        logging.info("正在爬取第{}页".format(page))
                        resp_dict = self.get_list_page(precinct["list_page_base_url"], page)
                        if resp_dict:
                            # 辖区配置信息追加resp_dict
                            resp_dict.update(precinct)
                            resp_is_gather, resp_today_page_increased, resp_today_db_increased, resp_page_increased, resp_db_increased = self.parse_list_page(
                                resp_dict)
                            # 统计更新数量
                            today_page_increased += resp_today_page_increased
                            today_db_increased += resp_today_db_increased
                            page_increased += resp_page_increased
                            db_increased += resp_db_increased
                            # 更新状态
                            state = 1
                            if not resp_is_gather:
                                break
                    # 统计页面总条数
                    page_total = total
            except Exception as e:
                error_info = "未知错误-{}".format(str(e))
            finally:
                log_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                # 文件监控
                self.handle_crawler_status_to_file(
                    id=precinct["crawler_status_id"],
                    state=state,
                    increment=today_page_increased,
                    total=page_total,
                    error_info=error_info,
                    log_time=log_time,
                    crawler_status_path=self.global_conf["log_path"] + "{}.txt".format(precinct["precinct_code"])
                )
                # # 数据库监控
                # self.handle_crawler_status_to_db(
                #     id=precinct["crawler_status_id"],
                #     state=state,
                #     increment=today_page_increased,
                #     total=page_total,
                #     error_info=error_info,
                #     log_time=log_time
                # )
                # 接口监控
                crawler_status = {
                    "total": page_total,
                    "existence": None,
                    "increment": today_page_increased,
                    "state": state,
                    "errorInfo": error_info,
                    "logTime": log_time
                }
                update_info(precinct["condition"], crawler_status)
        # 关闭mysql ssh连接
        self.mysql_tool.close_ssh_conn()
        logging.info("结束：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logging.info("=" * 50 + "结束采集列表页" + "=" * 50)


if __name__ == '__main__':
    cgl = CsrcGovList()
    cgl.run()
