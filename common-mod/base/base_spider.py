# 文件名: base_spider.py
# ---------------------
# desc: 爬虫框架核心基类 (L1)
#       - 封装配置注入、通用工具（DB, OBS, Proxy, Log...）
#       - 封装通用网络 I/O (request, download, upload)
# ---------------------
from abc import ABCMeta, abstractmethod
import datetime
import json
import logging
import os
import re
import shutil
import time
import uuid
import urllib3
import requests
from retrying import retry

# --- 导入所有通用工具 ---
from csrc_gov.tools.log_tool import log_conf
from csrc_gov.tools.mysql_tool import MysqlTool
from csrc_gov.tools.obs_tool import OBSTool
from csrc_gov.tools.proxy_tool import ProxyTool
from csrc_gov.tools.snow_tool import SnowTool
from csrc_gov.tools.md5_tool import get_file_md5
from csrc_gov.tools.guise_tool import random_user_agent

# 禁用 InsecureRequestWarning
urllib3.disable_warnings()


class BaseSpider(metaclass=ABCMeta):
    """
    爬虫框架核心基类 (L1)
    """
    task_name = "未命名任务"

    def __init__(self, project_config: dict, stage_name: str):
        """
        初始化基类
        :param project_config: 一个 *已合并* 的配置字典。
                              它应该包含:
                              - "project_name", "db_table" ... (项目配置)
                              - "list_stage", "detail_stage" ... (各阶段配置)
                              - "connections": {"data_db": {...}, "storage": {...}} (基础设施配置)
                              - "env_settings": {"is_use_proxy": 0} (环境配置)
        :param stage_name:     当前实例化的阶段名, e.g., "list_stage"
        """
        # 1. 存储配置
        self.project_conf = project_config
        self.stage_conf = project_config[stage_name]
        self.connections = project_config.get("connections", {})
        self.env_settings = project_config.get("env_settings", {})
        self.task_name = f"{self.project_conf.get('project_name', 'Unnamed')} - {stage_name}"

        # 2. 配置日志 (使用阶段配置)
        log_conf(self.stage_conf["log_path"], self.stage_conf.get("log_file_path", "log.log"))

        # 3. 初始化通用工具 (使用注入的 connections)
        self.mysql_tool = self._init_mysql_tool(self.connections.get("data_db"))
        self.obs_tool = self._init_obs_tool(self.connections.get("storage"))

        # 这些工具可以保持原样，或在未来也改为从配置中获取URL
        self.proxy_tool = ProxyTool()
        self.snow_tool = SnowTool()

        # 4. 设置通用属性
        self.db_table = self.project_conf.get("db_table")
        self.file_cache_path = self.stage_conf.get("file_cache_path", "./cache/default_cache/")

        # 5. 设置时间范围 (使用阶段配置)
        self.start_time, self.end_time = self._setup_time_range(
            self.stage_conf.get("update_time_extent", 1)
        )

        # 6. 设置代理 (使用阶段配置)
        self.proxy_dict = self.read_cache_proxy()
        self.proxy_count = 0

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=3000)
    def _init_mysql_tool(self, db_conf: dict) -> MysqlTool | None:
        """
        根据传入的 *具体配置* 初始化数据库连接。
        """
        if not db_conf:
            logging.warning(f"任务 {self.task_name} 未配置 'data_db' 连接。")
            return None

        logging.info(f"正在连接数据库: {db_conf.get('db_host')}/{db_conf.get('db_database')}")
        return MysqlTool(
            db_host=db_conf["db_host"],
            db_port=db_conf["db_port"],
            db_username=db_conf["db_username"],
            db_password=db_conf["db_password"],
            db_database=db_conf["db_database"],
            ssh_host=db_conf.get("ssh_host"),
            ssh_username=db_conf.get("ssh_username"),
            ssh_password=db_conf.get("ssh_password"),
            charset="utf8"
        )

    def _init_obs_tool(self, obs_conf: dict) -> OBSTool | None:
        """
        根据传入的 *具体配置* 初始化OBS工具。
        """
        if not obs_conf:
            logging.warning(f"任务 {self.task_name} 未配置 'storage' 连接。")
            return None

        logging.info(f"正在连接OBS: {obs_conf.get('sv')}/{obs_conf.get('bt')}")
        return OBSTool(
            obs_conf["ak"],
            obs_conf["sk"],
            obs_conf["sv"],
            obs_conf["bt"],
            obs_conf["fd"],
        )

    def _setup_time_range(self, update_extent_days: int) -> (str, str):
        """
        设置并返回 (start_time, end_time)
        """
        start_time = (datetime.datetime.now() + datetime.timedelta(
            days=-update_extent_days)).strftime("%Y-%m-%d %H:%M:%S")
        end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        return start_time, end_time

    # --- 模板方法 ---
    def run(self):
        """
        公开的执行入口 (模板方法)。
        它定义了任务的骨架，包括日志、计时、缓存清理和错误处理。
        """
        logging.info("=" * 50 + f"开始 {self.task_name}" + "=" * 50)
        start_run_time = datetime.datetime.now()
        logging.info(f"开始：{start_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"周期内开始时间：{self.start_time}，结束时间：{self.end_time}")

        try:
            # 准备文件缓存
            self.create_file_cache()
            # 执行子类定义的具体任务
            self._execute_task()
        except Exception as e:
            logging.error(f"{self.task_name} 未知错误 - {str(e)}", exc_info=True)
        finally:
            # 清理文件缓存
            self.clear_file_cache()
            # 关闭mysql ssh连接
            if self.mysql_tool:
                self.mysql_tool.close_ssh_conn()

            end_run_time = datetime.datetime.now()
            logging.info(f"结束：{end_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"总耗时：{end_run_time - start_run_time}")
            logging.info("=" * 50 + f"结束 {self.task_name}" + "=" * 50)

    @abstractmethod
    def _execute_task(self):
        """
        抽象的执行任务方法。
        子类必须实现此方法，写入自己核心的业务逻辑。
        """
        pass

    # --- 通用网络 I/O 方法 ---

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response | None:
        """
        一个通用的、健壮的请求方法，处理代理、重试和网络异常。
        """
        headers = kwargs.get("headers", {"User-Agent": random_user_agent()})
        kwargs["headers"] = headers

        if "timeout" not in kwargs:
            kwargs["timeout"] = (31, 183)

        retry_number = self.stage_conf.get("get_proxy_retry_number", 3)
        retry_count = 0
        use_proxy = self.env_settings.get("is_use_proxy", 0)

        while retry_count <= retry_number:
            try:
                resp = None
                if use_proxy:
                    if not self.proxy_dict:
                        self.proxy_dict = self.handle_proxy()

                    if self.proxy_dict:
                        resp = requests.request(
                            method, url, proxies=self.proxy_dict,
                            verify=False, **kwargs
                        )
                else:
                    resp = requests.request(method, url, verify=False, **kwargs)

                if resp.status_code == 200:
                    if "Auth Failed" in resp.text:
                        raise Exception("身份异常 (Auth Failed)，IP可能被封")
                    return resp
                else:
                    logging.warning(f"请求失败 (状态码: {resp.status_code}): {url}")
                    retry_count += 1
                    time.sleep(1)

            except (requests.exceptions.ProxyError,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout) as e:
                logging.error(f"代理或网络超时，准备重试... 错误: {e}")
                self.proxy_dict = None
                retry_count += 1

            except Exception as e:
                logging.error(f"请求失败: {url}，错误: {e}")
                retry_count += 1

        logging.error(f"请求失败: {url} (已达最大重试次数)")
        return None

    def download_file_generic(self, url: str, save_path: str) -> bool:
        """
        一个通用的文件下载器，处理代理、重试和网络异常。
        """
        headers = {"User-Agent": random_user_agent()}

        retry_number = self.stage_conf.get("get_proxy_retry_number", 3)
        retry_count = 0
        use_proxy = self.env_settings.get("is_use_proxy", 0)

        while retry_count <= retry_number:
            try:
                resp = None
                if use_proxy:
                    if not self.proxy_dict:
                        self.proxy_dict = self.handle_proxy()

                    if self.proxy_dict:
                        resp = requests.get(
                            url=url, headers=headers, proxies=self.proxy_dict,
                            verify=False, allow_redirects=False, timeout=(31, 183)
                        )
                else:
                    resp = requests.get(
                        url=url, headers=headers, verify=False,
                        allow_redirects=False, timeout=(31, 183)
                    )

                if resp.status_code == 200:
                    resp_content = resp.content
                    if len(resp_content) < 1024 * 1:  # 简单校验
                        raise Exception(f"文件异常 (小于1KB): {url}")

                    with open(save_path, "wb") as f:
                        f.write(resp_content)
                    logging.info(f"下载成功: {url} -> {save_path}")
                    return True
                else:
                    logging.warning(f"下载失败 (状态码: {resp.status_code}): {url}")
                    if "Auth Failed" in resp.text:
                        self.proxy_dict = None
                    retry_count += 1
                    continue

            except (requests.exceptions.ProxyError,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout) as e:
                logging.error(f"代理或网络超时，准备重试... 错误: {e}")
                self.proxy_dict = None
                retry_count += 1

            except Exception as e:
                logging.error(f"下载文件失败: {url}，错误: {e}", exc_info=True)
                return False

        logging.error(f"下载失败: {url} (已达最大重试次数)")
        return False

    def upload_to_obs(self, local_file_path: str, obs_object_name: str) -> str | None:
        """
        一个通用的OBS上传器。
        """
        if not self.obs_tool:
            logging.error("OBSTool 未初始化，无法上传")
            return None

        try:
            success = self.obs_tool.upload_file(
                obs_object_name,
                local_file_path
            )
            if success:
                obs_path = self.obs_tool.base + obs_object_name
                logging.info(f"OBS上传成功: {local_file_path} -> {obs_path}")
                return obs_path
            else:
                logging.error(f"OBS上传失败: {local_file_path}")
                return None
        except Exception as e:
            logging.error(f"OBS上传时发生异常: {e}", exc_info=True)
            return None

    # --- 通用辅助方法 (Cache, Proxy, Snow) ---

    @retry(stop_max_attempt_number=5, wait_random_min=1000, wait_random_max=3000)
    def retry_handle_snow_id(self):
        text_id = self.snow_tool.get_snow_id()
        if not text_id:
            raise Exception("获取雪花ID失败")
        return text_id

    def handle_snow_id(self):
        try:
            return self.retry_handle_snow_id()
        except Exception as e:
            logging.error(str(e))
            return None

    def clear_file_cache(self):
        if os.path.exists(self.file_cache_path):
            shutil.rmtree(self.file_cache_path)
            os.mkdir(self.file_cache_path)

    def create_file_cache(self):
        if not os.path.exists(self.file_cache_path):
            os.makedirs(self.file_cache_path)

    def read_cache_proxy(self):
        proxy_cache_path = self.stage_conf.get("proxy_cache_path")
        if not proxy_cache_path:
            return None
        try:
            with open(proxy_cache_path, "r", encoding="utf8") as f:
                return json.loads(f.read())
        except Exception as e:
            logging.error(f"读取代理缓存失败: {e}")
            return None

    def write_cache_proxy(self, proxy_dict):
        proxy_cache_path = self.stage_conf.get("proxy_cache_path")
        if not proxy_cache_path:
            return False

        proxy_cache_dir_path = os.path.dirname(proxy_cache_path)
        if not os.path.exists(proxy_cache_dir_path):
            os.makedirs(proxy_cache_dir_path)

        proxy_re = r"{'http': 'http://(\d+).(\d+).(\d+).(\d+):(\d+)', 'https': 'https://(\d+).(\d+).(\d+).(\d+):(\d+)'}"
        if re.match(proxy_re, str(proxy_dict)):
            with open(proxy_cache_path, "w", encoding="utf8") as f:
                f.write(json.dumps(proxy_dict))
                return True
        else:
            logging.error("代理格式错误，保存失败")
            return False

    @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
    def retry_handle_proxy(self):
        ret_get_proxy = self.proxy_tool.get_proxy()
        self.proxy_count += 1
        if not ret_get_proxy:
            raise Exception("获取代理失败")
        if self.write_cache_proxy(ret_get_proxy):
            return ret_get_proxy
        else:
            return None

    def handle_proxy(self):
        try:
            return self.retry_handle_proxy()
        except Exception as e:
            logging.error(f"handle_proxy 失败: {e}")
            return None