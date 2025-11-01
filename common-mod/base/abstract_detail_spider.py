# 文件名: abstract_detail_spider.py
# ---------------------
# desc: 详情页处理工作流基类 (L2)
#       - 继承 BaseSpider
#       - 定义“从数据库获取任务 -> 处理详情”的标准工作流
# ---------------------
from abc import abstractmethod

import requests

from base_spider import BaseSpider
import logging
import os
from csrc_gov.tools.pdf_tool import PDFTool  # 详情页处理器通常需要PDF工具


class AbstractDetailSpider(BaseSpider):
    """
    定义了“详情页处理”的标准工作流。
    """
    task_name = "抽象详情页处理"

    def __init__(self, project_config: dict):
        """
        初始化详情页处理器
        :param project_config: 已合并的配置字典
        """
        # 1. 调用父类__init__，硬编码 stage_name 为 "detail_stage"
        super().__init__(project_config, "detail_stage")

        # 2. 详情页特有的初始化 (例如 PDFTool, 模板读取)
        self.pdf_tool = PDFTool(self.file_cache_path)  # 使用基类定义的 file_cache_path
        self._load_templates()

    def _load_templates(self):
        """
        (子类可覆盖)
        加载 HTML/CSS 模板。
        """
        self.h5_temp_str = None
        self.c3_temp_str = None
        try:
            temp_path = self.stage_conf.get("temp_path", "./temp/")
            h5_file = self.stage_conf.get("temp_file_path", {}).get("h5", "page.html")
            c3_file = self.stage_conf.get("temp_file_path", {}).get("c3", "style.css")

            h5_full_path = os.path.join(temp_path, h5_file)
            c3_full_path = os.path.join(temp_path, c3_file)

            if os.path.exists(h5_full_path):
                with open(h5_full_path, "r", encoding="utf8") as f:
                    self.h5_temp_str = f.read()
            else:
                logging.warning(f"H5 模板未找到: {h5_full_path}")

            if os.path.exists(c3_full_path):
                with open(c3_full_path, "r", encoding="utf8") as f:
                    self.c3_temp_str = f.read()
            else:
                logging.warning(f"C3 模板未找到: {c3_full_path}")

        except Exception as e:
            logging.error(f"加载模板失败: {e}", exc_info=True)

    def _execute_task(self):
        """
        (已实现) 定义了详情页处理的核心工作流
        """
        if not self.mysql_tool:
            logging.error("数据库未初始化，详情页任务无法执行。")
            return

        # 1. 从数据库获取待办任务 (SQL由子类定义)
        sql_query = self.get_db_tasks_sql()

        db, cs = self.mysql_tool.open_db_conn()
        data_list = self.mysql_tool.select_db_sql(
            db, cs, self.db_table, [], sql_query
        )
        self.mysql_tool.close_db_conn(db, cs)

        if data_list is None:
            logging.error("从数据库查询任务失败。")
            return

        logging.info(f"周期内需要处理的详情页数量->{len(data_list)}")

        # 2. 循环处理 (这是通用的)
        for data_item in data_list:
            logging.info(f"--- 正在处理: id={data_item['id']} ---")
            try:
                # 3. 获取详情页 (这是通用的)
                raw_resp = self._make_request('GET', data_item["detail_url"])

                if raw_resp:
                    # 4. 解码 (尝试多种编码)
                    raw_content, encoding = self.decode_response_content(raw_resp)
                    if raw_content is None:
                        logging.warning(f"解码失败: {data_item['detail_url']}")
                        continue

                    # 5. 解析和存储 (这是子类特定的)
                    self.process_detail_task(data_item, raw_content, encoding)
                else:
                    logging.warning(f"获取详情页失败: {data_item['detail_url']}")

            except Exception as e:
                logging.error(f"处理 id={data_item['id']} 时失败: {e}", exc_info=True)

    def decode_response_content(self, response: requests.Response) -> (str, str):
        """
        尝试使用多种编码解码响应内容
        :return: (decoded_string, used_encoding) 或 (None, None)
        """
        # 优先使用 headers 中的编码
        encodings_to_try = [
            response.encoding,  # requests 猜测的编码
            'utf-8',
            'gbk',
            'gb2312'
        ]

        for encoding in filter(None, encodings_to_try):
            try:
                return response.content.decode(encoding), encoding
            except (UnicodeDecodeError, TypeError):
                continue

        logging.error(f"所有编码均解码失败: {response.url}")
        return None, None

    # --- 抽象方法 (子类必须实现) ---

    @abstractmethod
    def get_db_tasks_sql(self) -> str:
        """
        (子类必须实现)
        返回一个 SQL `WHERE` 语句 (不含 'WHERE')，用于从数据库获取待处理的任务。

        例如:
        return (
            "(`flag` = 0) and "
            "(`pid` is null or `pid` = '') and "
            "(`detail_url` is not null or `detail_url` != '') and "
            f"(`publish_time` >= '{self.start_time}') and "
            f"(`publish_time` <= '{self.end_time}') "
            "order by `publish_time` desc"
        )
        """
        pass

    @abstractmethod
    def process_detail_task(self, data_item: dict, raw_content: str, encoding: str):
        """
        (子类必须实现)
        解析详情页的原始数据 (raw_content)，
        并处理（例如转PDF、上传OBS、更新数据库、解析附件并入库）。

        所有工具 (self.pdf_tool, self.obs_tool, self.mysql_tool, self.handle_snow_id) 均可使用。
        """
        pass