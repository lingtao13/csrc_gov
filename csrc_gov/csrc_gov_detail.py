# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2023/01/04
# desc: 证监局辖区详情页html转pdf并上传至obs、解析详情页附件信息记录数据库中
# ---------------------
import datetime
import html
import json
import logging
import os.path
import re
import shutil
import uuid
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import requests
import urllib3
from lxml import etree
from lxml.etree import tounicode
from lxml.html import tostring
from retrying import retry

from tools.snow_tool import SnowTool
from tools.pdf_tool import PDFTool
from tools.md5_tool import get_file_md5
from tools.guise_tool import random_user_agent
from tools.proxy_tool import ProxyTool
from tools.conf_tool import read_yaml_conf
from tools.log_tool import log_conf
from tools.mysql_tool import MysqlTool
from tools.obs_tool import OBSTool

urllib3.disable_warnings()


class CsrcGovDetail(object):

    def __init__(self):

        # 读取配置文件
        self.global_conf = read_yaml_conf("conf/csrc_gov_detail_pro.yml")
        # 日志
        log_conf(self.global_conf["log_path"], self.global_conf["log_file_path"])
        # 读取h5模板
        self.h5_temp_str = self.get_h5_temp()
        if not self.h5_temp_str:
            return
        # 读取c3模板
        self.c3_temp_str = self.get_c3_temp()
        if not self.c3_temp_str:
            return
        # snow
        self.snow_tool = SnowTool()
        # pdf
        self.pdf_tool = PDFTool(self.global_conf["file_cache_path"])
        # proxy
        self.proxy_tool = ProxyTool()
        # obs
        obs_server = "obs_server"
        self.obs_tool = OBSTool(
            self.global_conf[obs_server]["ak"],
            self.global_conf[obs_server]["sk"],
            self.global_conf[obs_server]["sv"],
            self.global_conf[obs_server]["bt"],
            self.global_conf[obs_server]["fd"],
        )

        # time
        # 开始日期
        self.start_time = (datetime.datetime.now() + datetime.timedelta(
            days=-self.global_conf["update_time_extent"])).strftime("%Y-%m-%d %H:%M:%S")
        # 结束日期
        self.end_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        # 代理
        self.proxy_dict = self.read_cache_proxy()
        # 代理使用计数
        self.proxy_count = 0
        # 创建数据库连接对象
        self.mysql_tool = self.create_db()
        # 数据库表
        self.db_table = self.global_conf["db_table"]

    @staticmethod
    def split_name_suffix(file_name):
        """
        拆分文件名称及后缀
        :param file_name:
        :return:
        """
        file_name = file_name.strip()

        name = file_name
        suffix = ""

        suffix_index = file_name.rfind(".")
        # 判断是否存在.符号
        if suffix_index >= 0:
            name_str = file_name[0:suffix_index]
            suffix_str = file_name[suffix_index:]
            # 判断是否满足后缀正则
            if re.search(r'^.[A-Za-z0-9]+?$', suffix_str):
                # 判断后缀是否至少包含一个字母
                if re.findall(re.compile(r'[A-Za-z]', re.S), suffix_str):
                    name = name_str
                    suffix = suffix_str
                    # 如果name为空字符串则将后缀名拼接上
                    if not name:
                        name = name_str + suffix

        return name, suffix

    def get_h5_temp(self):
        """
        读取temp中html模板
        :return:
        """
        h5_temp_path = self.global_conf["temp_path"] + self.global_conf["temp_file_path"]["h5"]
        if not os.path.exists(h5_temp_path):
            logging.error("h5模板不存在")
            return None
        with open(h5_temp_path, "r", encoding="utf8") as f:
            return f.read()

    def get_c3_temp(self):
        """
        读取temp中css模板
        :return:
        """
        c3_temp_path = self.global_conf["temp_path"] + self.global_conf["temp_file_path"]["c3"]
        if not os.path.exists(c3_temp_path):
            logging.error("c3模板不存在")
            return None
        with open(c3_temp_path, "r", encoding="utf8") as f:
            return f.read()

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

    def parse_detail_page(self, data_str, data_dict):
        """
        解析详情页
        :param data_str:
        :param data_dict:
        :return:
        """
        # 详情头部链接
        end_index = data_dict["detail_url"].rfind("/")
        url_head = data_dict["detail_url"][0:end_index + 1]
        # 生成详情页结构树
        tree = etree.HTML(data_str)
        # 清洗成功后页面文本
        content_str = ""
        # 详情页附件列表
        file_info_list = list()

        # xpath解析
        content_list = tree.xpath('//div[@class="content"]')
        for content in content_list:
            # 移除标题
            h2_list = content.xpath('.//h2')
            # 如果无标题说明该class content节点并非所需的节点
            if not h2_list:
                continue
            for h2 in h2_list:
                h2.getparent().remove(h2)
                break

            # 移除附件隐藏属性
            files_list = content.xpath('.//div[@id="files"]')
            for files in files_list:
                if files.xpath("./@style"):
                    files.attrib.pop("style")

            # 获取附件信息
            files_a_list = content.xpath('.//div[@id="files"]/a')
            for files_a in files_a_list:
                # 获取附件的url
                href_list = files_a.xpath('./@href')
                # 只要无href，直接跳过该条
                if not href_list:
                    continue
                else:
                    href = href_list[0]
                title_list = files_a.xpath('.//text()')
                # 只要无text()，直接跳过该条
                if not title_list:
                    continue
                else:
                    title = "".join(title_list).strip()
                # 判断href是否包含http
                if "http" not in href:
                    # 判断href是否是以/开头，是则截取
                    if href.startswith("/"):
                        href = href[1:]
                    # 判断href是否是.mp4或.MP4结尾
                    if href.endswith(".mp4") or href.endswith(".MP4"):
                        href = self.global_conf["website_base_url"] + href
                    else:
                        href = url_head + href
                # # 替换原文href
                # files_a.attrib["href"] = "javascript:void(0);"
                file_info_list.append((href, title))

            # 获取a标签链接内容（此处可能包含附件内容，例如内蒙古辖区）
            detail_news_a_list = content.xpath('.//div[@class="detail-news"]//a')
            for detail_news_a in detail_news_a_list:
                # 获取附件的url
                href_list = detail_news_a.xpath('./@href')
                # 只要无href，直接跳过该条
                if not href_list:
                    continue
                else:
                    href = href_list[0]
                title_list = detail_news_a.xpath('.//text()')
                # 只要无text()，直接跳过该条
                if not title_list:
                    continue
                else:
                    title = "".join(title_list).strip()
                if "/files/" not in href:
                    continue
                # 判断href是否包含http
                if "http" not in href:
                    # 判断href是否是以/开头，是则截取
                    if href.startswith("/"):
                        href = href[1:]
                    # 判断href是否是.mp4或.MP4结尾
                    if href.endswith(".mp4") or href.endswith(".MP4"):
                        href = self.global_conf["website_base_url"] + href
                    else:
                        href = url_head + href
                # # 替换原文href
                # detail_news_a.attrib["href"] = "javascript:void(0);"
                file_info_list.append((href, title))

            # 替换a标签的href属性
            content_a_list = content.xpath('.//a')
            for content_a in content_a_list:
                # 获取附件的url
                a_list = content_a.xpath('./@href')
                # 只要无src，直接跳过该条
                if not a_list:
                    continue
                content_a.attrib["href"] = "javascript:void(0);"

            # 替换img标签的src属性
            img_list = content.xpath('.//img')
            for img in img_list:
                # 获取附件的url
                src_list = img.xpath('./@src')
                # 只要无src，直接跳过该条
                if not src_list:
                    continue
                else:
                    src = src_list[0]
                if "http" not in src:
                    # 判断href是否是以/开头，是则截取
                    if src.startswith("/"):
                        src = src[1:]
                    src = url_head + src
                img.attrib["src"] = src

            # 删除font标签的face属性
            font_list = content.xpath('.//font')
            for font in font_list:
                if font.xpath("./@face"):
                    font.attrib.pop("face")

            # 移除信息公开表格内容
            xxgk_table_list = content.xpath('.//div[@class="xxgk-table"]')
            for xxgk_table in xxgk_table_list:
                xxgk_table.getparent().remove(xxgk_table)

            # 移除尾部打印关闭内容
            xxgk_down_box_list = content.xpath('.//div[@class="xxgk-down-box"]')
            for xxgk_down_box in xxgk_down_box_list:
                xxgk_down_box.getparent().remove(xxgk_down_box)
            content_str += tostring(content, encoding="utf8", method="html").decode("utf8")
            # content_str += tounicode(content, method="html")

        # 字体删除(当系统无该字体时html转pdf会乱码)
        content_str = re.sub(r'font-family(.*?);', "", content_str)

        # 将url%开头编码转为中文
        content_str = urllib.parse.unquote(content_str)
        new_content_str = self.h5_temp_str.format(
            data_dict["title"],
            self.c3_temp_str,
            data_dict["title"],
            content_str
            # html.unescape(content_str)
        )

        # 文件信息列表去重
        file_info_list = list(set(file_info_list))

        return file_info_list, new_content_str

    # @retry(stop_max_attempt_number=3, wait_random_min=2000, wait_random_max=4000)
    def retry_get_detail_page(self, data_dict):
        """
        重试获取详情页
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        # 请求头
        headers = {
            "User-Agent": random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
        }
        # 地址
        url = data_dict["detail_url"]
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
                    timeout=(31, 183)
                )
        else:  # 不使用代理
            # 发起请求
            resp = requests.get(
                url=url,
                headers=headers,
                verify=False,
                timeout=(31, 183)
            )
        if resp.ok:
            # 获取响应
            resp_text = resp.content.decode(encoding="utf8")
            return resp_text
        else:
            error_info = "访问详情页失败"
            logging.info(error_info)
            raise Exception(error_info)

    def get_detail_page(self, data_dict):
        """
        获取详情页
        :param data_dict: 数据库查询出来的当前数据
        :return:
        """
        # 代理单次获取重试最大次数
        retry_number = self.global_conf["get_proxy_retry_number"]
        # 重试获取代理次数
        retry_count = 0
        # 返回缓存文件路径
        data_text = None
        try:
            data_text = self.retry_get_detail_page(data_dict)
        except requests.exceptions.ProxyError as epe:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                data_text = self.retry_get_detail_page(data_dict)
            logging.error("当前代理失效，错误信息：{}".format(str(epe)))
        except requests.exceptions.ConnectTimeout as ect:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                data_text = self.retry_get_detail_page(data_dict)
            logging.error("远程连接超时，错误信息：{}".format(str(ect)))
        except requests.exceptions.ReadTimeout as ert:
            # 获取代理次数是否大于规定次数
            if retry_count < retry_number:  # 不大于
                # 重试次数加一
                retry_count += 1
                # 将全局变量中的代理设置为None
                self.proxy_dict = None
                data_text = self.retry_get_detail_page(data_dict)
            logging.error("远程读取超时，错误信息：{}".format(str(ert)))
        except Exception as e:
            logging.error("下载文件失败，错误信息：{}".format(str(e)))
        finally:
            return data_text

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

    def attachment_info_save(self, file_list, data_dict):
        """
        添加附件信息到数据库中
        :param file_list:
        :param data_dict:
        :return:
        """
        # # 此代码为单条数据插入，具有附件数据单条插入失败从而导致附件信息不完整的情况，故暂不使用
        # for file in file_list:
        #     # 标题
        #     title = os.path.splitext(file["title"])[0]
        #     # 附件url
        #     attachment_url = file["href"]
        #
        #     # 查询数据库文件url
        #     mysql_tool = self.create_db()
        #     select_db_sql_ret = mysql_tool.select_db_sql(
        #         self.db_table,
        #         ["id", "pid"],
        #         "`pid` = '{}' and `attachment_url` = '{}'".format(data_dict["id"], attachment_url)
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
        #                     "pid": data_dict["id"],
        #                     "precinct": data_dict["precinct"],
        #                     "precinct_code": data_dict["precinct_code"],
        #                     "title": title,
        #                     "detail_url": data_dict["detail_url"],
        #                     "publish_time": data_dict["publish_time"],
        #                     "number": data_dict["number"],
        #                     "attachment_url": attachment_url,
        #                     "type": data_dict["type"],
        #                     "insert_time": datetime.datetime.now(),
        #                     # "text_id": text_id
        #                 }
        #             )
        #             mysql_tool.close_db_conn()
        #         else:
        #             continue

        # 此代码为多条记录插入数据库，保证附件信息完整
        field_list = [
            "pid", "precinct", "precinct_code", "title", "detail_url", "publish_time",
            "number", "attachment_url", "type", "insert_time", "text_id"
        ]
        data_list = list()
        for file in file_list:
            # 标题
            title = self.split_name_suffix(file[1])
            # 附件url
            attachment_url = file[0]

            # 查询数据库文件url
            db, cs = self.mysql_tool.open_db_conn()
            select_db_sql_ret = self.mysql_tool.select_db_sql(
                db,
                cs,
                self.db_table,
                ["id", "pid", "title"],
                "`pid` = '{}' and `attachment_url` = '{}'".format(data_dict["id"], attachment_url)
            )
            self.mysql_tool.close_db_conn(db, cs)

            # 判断数据库是否连接查询成功
            if select_db_sql_ret is None:
                logging.error("数据库连接失败")
                return False

            # 判断数据库是否存在附件url
            if not select_db_sql_ret:
                text_id = self.handle_snow_id()
                # text_id = 1
                if text_id:
                    data = (
                        data_dict["id"], data_dict["precinct"], data_dict["precinct_code"], title[0],
                        data_dict["detail_url"], data_dict["publish_time"], data_dict["number"],
                        attachment_url, data_dict["type"], datetime.datetime.now(), text_id
                    )
                    data_list.append(data)
                else:
                    return False
            else:
                # 判断附件标题是否修改
                if title[0] != select_db_sql_ret[0]["title"]:
                    db, cs = self.mysql_tool.open_db_conn()
                    update_db_sql_ret = self.mysql_tool.update_db_sql(
                        db,
                        cs,
                        self.db_table,
                        {
                            "title": title[0],
                            "flag": 0
                        },
                        "`id` = '{}'".format(select_db_sql_ret[0]["id"])
                    )
                    self.mysql_tool.close_db_conn(db, cs)
                    if not update_db_sql_ret:
                        return False
                    logging.info("附件修改数据库成功")

        # 插入多条附件新数据
        if data_list:
            db, cs = self.mysql_tool.open_db_conn()
            many_insert_db_sql_ret = self.mysql_tool.many_insert_db_sql(
                db,
                cs,
                self.db_table,
                field_list,
                data_list
            )
            self.mysql_tool.close_db_conn(db, cs)
            if not many_insert_db_sql_ret:
                return False
            logging.info("附件记录数据库成功")
        return True

    def handle_save_transform_upload(self, data_dict):
        """
        处理附件存储、转换和上传
        :param data_dict:
        :return:
        """
        logging.info("正在处理->{}".format(data_dict))
        data_text = self.get_detail_page(data_dict)
        if data_text:
            # 解析详情页数据
            file_list, content_str = self.parse_detail_page(data_text, data_dict)
            # 将附件信息添加到数据库中
            attachment_info_save_ret = self.attachment_info_save(file_list, data_dict)
            # 判断数据库插入或更新是否成功
            if attachment_info_save_ret:
                # 判断详情页html转pdf中flag字段是否为1
                if not data_dict["flag"]:
                    # 获取uuid用作文件名
                    uuid_str = str(uuid.uuid1()).replace("-", "")
                    self.pdf_tool.string_html_to_pdf(content_str, "{}.pdf".format(uuid_str))
                    self.upload_file(self.global_conf["file_cache_path"] + "{}.pdf".format(uuid_str), data_dict)

    def run(self):
        logging.info("=" * 50 + "开始采集详情页" + "=" * 50)
        logging.info("开始：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        try:
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
                # "(`precinct` = '重庆辖区') and "
                # "(`obs_path` is null or `obs_path` = '') and "
                # "(`flag` = 0) and "
                "(`is_delete` = 0) and "
                "(`pid` is null or `pid` = '') and "
                "(`detail_url` is not null or `detail_url` != '') and "
                "(`publish_time` >= '{}') and "
                "(`publish_time` <= '{}') "
                "order by `publish_time` desc".format(self.start_time, self.end_time)
            )
            self.mysql_tool.close_db_conn(db, cs)
            logging.info("周期内需要上传obs数据数量->{}".format(len(data_list)))
            # tpe = ThreadPoolExecutor(15)
            # 循环数据列表
            for data in data_list:
                # tpe.submit(self.handle_save_transform_upload, data)
                self.handle_save_transform_upload(data)
            logging.info("周期内使用代理数量->{}".format(self.proxy_count))
            # 清除缓存
            self.clear_file_cache()
        except Exception as e:
            logging.error("未知错误-{}".format(str(e)))
        finally:
            # 关闭mysql ssh连接
            self.mysql_tool.close_ssh_conn()
        logging.info("结束：{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logging.info("=" * 50 + "结束采集详情页" + "=" * 50)


if __name__ == '__main__':
    cgd = CsrcGovDetail()
    cgd.run()
