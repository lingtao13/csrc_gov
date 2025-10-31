# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2023/01/04
# desc: 证监局辖区下载附件并上传至obs
# ---------------------
import datetime
import json
import logging
import os.path
import re
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests
import urllib3
from retrying import retry

from tools.md5_tool import get_file_md5
from tools.guise_tool import random_user_agent
from tools.proxy_tool import ProxyTool
from tools.conf_tool import read_yaml_conf
from tools.log_tool import log_conf
from tools.mysql_tool import MysqlTool
from tools.obs_tool import OBSTool

urllib3.disable_warnings()


class CsrcGovAttachment(object):

    def __init__(self):

        # 读取配置文件
        self.global_conf = read_yaml_conf("conf/csrc_gov_attachment_pro.yml")
        # 日志
        log_conf(self.global_conf["log_path"], self.global_conf["log_file_path"])
        # proxy
        self.proxy_tool = ProxyTool()
        # obs
        obs_env = "obs_server"
        self.obs_tool = OBSTool(
            self.global_conf[obs_env]["ak"],
            self.global_conf[obs_env]["sk"],
            self.global_conf[obs_env]["sv"],
            self.global_conf[obs_env]["bt"],
            self.global_conf[obs_env]["fd"],
        )
        # time
        # 开始日期 默认昨天
        self.start_time = (datetime.datetime.now() + datetime.timedelta(
            days=-self.global_conf["update_time_extent"])).strftime("%Y-%m-%d %H:%M:%S")
        # 结束日期 默认今天
        self.end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        # 代理
        self.proxy_dict = self.read_cache_proxy()
        # 代理使用计数
        self.proxy_count = 0
        # 创建数据库连接对象
        self.mysql_tool = self.create_db()
        # 数据库表
        self.db_table = self.global_conf["db_table"]

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=3000)
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

    def clear_file_cache(self):
        """
        清除文件缓存
        :return:
        """
        self.create_file_cache()
        shutil.rmtree(self.global_conf["file_cache_path"])
        os.mkdir(self.global_conf["file_cache_path"])

    def create_file_cache(self):
        """
        创建文件缓存目录
        :return:
        """
        # 获取配置文件中文件缓存文件位置
        file_cache_path = self.global_conf["file_cache_path"]
        if not os.path.exists(file_cache_path):
            os.makedirs(file_cache_path)

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

    # @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
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

    # @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
    def retry_download_file(self, data_dict):
        """
        重试下载文件
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        # 请求头
        headers = {
            "User-Agent": random_user_agent()
        }
        # 地址
        url = data_dict["attachment_url"]
        # 响应
        resp = None
        # 是否使用代理
        if self.global_conf["is_use_proxy"]:  # 使用代理
            # 判断proxy_dict代理是否存在
            if not self.proxy_dict:  # 不存在
                self.proxy_dict = self.handle_proxy()
            # 获取proxy_dict代理是否成功
            if self.proxy_dict:  # 获取成功
                # 发起请求
                resp = requests.get(
                    url=url,
                    headers=headers,
                    proxies=self.proxy_dict,
                    verify=False,
                    allow_redirects=False,
                    timeout=(31, 183)
                )
        else:  # 不使用代理
            # 发起请求
            resp = requests.get(
                url=url,
                headers=headers,
                verify=False,
                allow_redirects=False,
                timeout=(31, 183)
            )
        if resp.status_code == 200:
            # 获取响应
            resp_content = resp.content
            # 判断数据是否异常
            if len(resp_content) < 1024 * 1:
                # # 将全局变量中的代理设置为None
                # self.proxy_dict = None
                raise Exception("文件异常")
            # 先确保缓存文件夹存在
            self.create_file_cache()
            # 防止较大文件上传失败，先保存本地，通过obs中指定文件路径上传方法上传
            # 缓存路径名称
            dir_name = self.global_conf["file_cache_path"]
            # 缓存文件名称
            file_name = data_dict["attachment_url"].split("/")[-1]
            # 完整缓存路径
            file_path = dir_name + file_name
            # 写入文件
            with open(file_path, "wb") as f:
                f.write(resp_content)
            logging.info("下载成功")
            return file_path
        else:
            resp_text = resp.text
            logging.info("下载失败，返回内容{}".format(resp_text))
            # 身份异常，很有可能就是因为IP被封，故换代理
            if "Auth Failed" in resp_text:
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                raise Exception("身份异常")

    def download_file(self, data_dict):
        """
        下载文件
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        # 代理单次获取重试最大次数
        retry_number = self.global_conf["get_proxy_retry_number"]
        # 重试获取代理次数
        retry_count = 0
        # 返回缓存文件路径
        file_path = None
        try:
            file_path = self.retry_download_file(data_dict)
        except requests.exceptions.ProxyError as epe:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                file_path = self.retry_download_file(data_dict)
            logging.error("当前代理失效，错误信息：{}".format(str(epe)))
        except requests.exceptions.ConnectTimeout as ect:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                file_path = self.retry_download_file(data_dict)
            logging.error("远程连接超时，错误信息：{}".format(str(ect)))
        except requests.exceptions.ReadTimeout as ert:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                file_path = self.retry_download_file(data_dict)
            logging.error("远程读取超时，错误信息：{}".format(str(ert)))
        except Exception as e:
            logging.error("下载文件失败，错误信息：{}".format(str(e)))
        finally:
            return file_path

    # @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
    def retry_upload_file(self, file_path, data_dict):
        """
        重试上传文件
        :param file_path: 文件缓存路径
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        # 判断文件路径是否异常
        if file_path:  # 路径正常
            # 文件MD5
            file_md5 = get_file_md5(file_path)
            # 判断obs_path是否存在
            if data_dict["obs_path"]:
                # 上传至obs对象名称
                obs_object_name = data_dict["obs_path"].split("csrc_gov/")[1]
            else:
                # 上传至obs对象名称
                # obs_object_name = file_path.split("/")[-1]
                obs_object_name = "{}/{}.{}".format(
                    data_dict["precinct_code"],
                    str(uuid.uuid1()).replace("-", ""),
                    file_path.split(".")[-1]
                )
            # 上传文件至obs
            resp_obs_upload_file = self.obs_tool.upload_file(
                obs_object_name,
                file_path
            )
            # 上传文件是否成功
            if resp_obs_upload_file:  # 上传成功
                obs_path = self.obs_tool.base + obs_object_name
                file_type = file_path.split(".")[-1].upper()
                # 封装需要更新的字段
                db_dict = {
                    "obs_path": obs_path,
                    "file_type": file_type,
                    "file_md5": file_md5,
                    "flag": 1
                }
                # 创建数据库连接
                db, cs = self.mysql_tool.open_db_conn()
                # 执行更新
                resp_update_db_sql = self.mysql_tool.update_db_sql(
                    db,
                    cs,
                    self.db_table,
                    db_dict,
                    "`id`='{}'".format(data_dict["id"])
                )
                # 数据库是否更新成功
                if resp_update_db_sql:  # 更新成功
                    pass
                else:  # 更新失败
                    logging.error("db更新失败，此数据为->{}".format(data_dict))
                # 关闭数据库连接
                self.mysql_tool.close_db_conn(db, cs)
                logging.info("上传成功")
            else:
                logging.info("上传失败")

    def upload_file(self, file_path, data_dict):
        """
        上传文件
        :param file_path: 文件缓存路径
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        try:
            return self.retry_upload_file(file_path, data_dict)
        except Exception as e:
            logging.error("上传文件失败，错误信息：{}".format(str(e)))

    def handle_download_upload(self, data_dict):
        """
        处理文件上传和下载
        :param data_dict:
        :return:
        """
        logging.info("正在处理->{}".format(data_dict))
        file_path = self.download_file(data_dict)
        self.upload_file(file_path, data_dict)

    def run(self):
        logging.info("=" * 50 + "开始附件上传" + "=" * 50)
        logging.info("开始：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        try:
            # 处理周期范围
            logging.info("周期内开始时间：{}，结束时间：{}".format(self.start_time, self.end_time))
            # 清除缓存
            self.clear_file_cache()
            # 查询数据库
            db, cs = self.mysql_tool.open_db_conn()
            data_list = self.mysql_tool.select_db_sql(
                db,
                cs,
                self.db_table,
                [],
                # "(`obs_path` is null or `obs_path` = '') and "
                "(`flag` = 0) and "
                "(`is_delete` = 0) and "
                "(`pid` is not null or `pid` != '') and "
                "(`attachment_url` is not null or `attachment_url` != '') and "
                "(`publish_time` >= '{}') and "
                "(`publish_time` <= '{}') "
                "order by `publish_time` desc".format(self.start_time, self.end_time)
            )
            self.mysql_tool.close_db_conn(db, cs)
            logging.info("周期内需要上传obs数据数量->{}".format(len(data_list)))
            # tpe = ThreadPoolExecutor(10)
            # 循环数据列表
            for data in data_list:
                # tpe.submit(self.handle_download_upload, data)
                self.handle_download_upload(data)
            logging.info("周期内使用代理数量->{}".format(self.proxy_count))
            # 清除缓存
            self.clear_file_cache()
        except Exception as e:
            logging.error("未知错误-{}".format(str(e)))
        finally:
            # 关闭mysql ssh连接
            self.mysql_tool.close_ssh_conn()
        logging.info("结束：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logging.info("=" * 50 + "结束附件上传" + "=" * 50)


if __name__ == '__main__':
    cga = CsrcGovAttachment()
    cga.run()
