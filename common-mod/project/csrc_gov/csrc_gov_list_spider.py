# 文件名: projects/csrc_gov/csrc_gov_list_spider.py
# ---------------------
# desc: 证监局列表爬虫 (L3) - 具体业务实现
#       - 继承 AbstractListSpider
#       - 实现所有抽象方法
# ---------------------
import logging
import datetime
import requests
from ...base.abstract_list_spider import AbstractListSpider


class CsrcGovListSpider(AbstractListSpider):
    """
    证监局列表爬虫的具体实现
    """
    task_name = "证监局列表采集"

    def __init__(self, project_config: dict):
        """
        初始化
        :param project_config: 已合并的配置字典
        """
        super().__init__(project_config)

        # --- 本业务特有的配置 ---
        self.manuscript_url = self.project_conf.get("manuscript_data_base_url")

    # --- 1. 实现 AbstractListSpider 的抽象方法 ---

    def get_target_list_key(self) -> str:
        return "precinct_list"

    def get_target_name(self, target: dict) -> str:
        return target.get("precinct", "未知辖区")

    def get_monitor_api_condition(self, target: dict) -> dict | None:
        return target.get("condition")

    def get_total_pages(self, target: dict) -> int | None:
        """
        通过请求第1页来获取总页数
        """
        list_page_base_url = target.get("list_page_base_url")
        if not list_page_base_url:
            logging.error(f"目标 {self.get_target_name(target)} 缺少 list_page_base_url")
            return None

        page_data = self.fetch_list_page(target, 1)  # 请求第1页
        if page_data:
            try:
                total = page_data["data"]["total"]
                rows = page_data["data"]["rows"]
                pages = (total // rows) + (1 if total % rows > 0 else 0)
                return pages
            except KeyError as e:
                logging.error(f"解析总页数失败: {e} - {page_data}")
                return None
        return None

    def fetch_list_page(self, target: dict, page: int) -> any:
        """
        获取列表页的 JSON 数据
        """
        list_page_base_url = target.get("list_page_base_url")
        params = {
            "_isAgg": "true", "_isJson": "true",
            "_pageSize": 50,  # 默认 50，可从配置中读取
            "_template": "index",
            "_rangeTimeGte": "", "_channelName": "", "page": page
        }

        resp = self._make_request('GET', list_page_base_url, params=params)

        if resp:
            try:
                resp_dict = resp.json()
                if "data" in resp_dict:
                    return resp_dict
            except requests.exceptions.JSONDecodeError as e:
                logging.error(f"解析列表页JSON失败: {e} - {resp.text[:100]}")
        return None

    def parse_list_page(self, target: dict, page_data: any) -> (bool, int, int):
        """
        解析列表页数据并存入数据库
        (逻辑移植自原 csrc_gov_list.py 中的 parse_list_page)
        """
        continue_crawl = True
        increment_count = 0
        page_item_count = 0

        results = page_data["data"].get("results", [])
        page_item_count = len(results)

        for result in results:
            precinct = target.get("precinct")
            precinct_code = target.get("precinct_code")

            title = result.get("title", "")
            url = result.get("url", "")
            if url.startswith("//"):
                url = "http:" + url
            published_time_str = result.get("publishedTimeStr", "")
            manuscript_id = result.get("manuscriptId", "")

            # --- 提取文号 ---
            number_str = ""
            number_str_flag = 0
            domain_meta_list = result.get("domainMetaList", [])
            for domain_meta in domain_meta_list:
                result_list = domain_meta.get("resultList", [])
                for res in result_list:
                    if res.get("name") == "文号":
                        if res.get("value"):
                            number_str = res.get("value")
                        number_str_flag = 1
                        break
                if number_str_flag:
                    break
            if "null" == number_str.strip():
                number_str = ""

            # --- 时间范围检查 (非全量) ---
            if not self.is_full_crawled:
                if not (published_time_str >= self.start_time):
                    continue_crawl = False  # 遇到旧数据，停止
                    return continue_crawl, increment_count, page_item_count

            # --- 数据库检查 ---
            db, cs = self.mysql_tool.open_db_conn()
            select_db_sql_ret = self.mysql_tool.select_db_sql(
                db, cs, self.db_table,
                ["id", "type", "number", "title", "publish_time"],  # 查询所需字段
                f"`detail_url` = '{url}' and "
                f"(`attachment_url` is null or `attachment_url` = '') and "
                f"`precinct` = '{precinct}'"
            )

            # (在循环中开关DB不是最佳实践，但为了保持原逻辑，暂时保留。
            # 更好的做法是批处理，或在循环外开关DB)

            if select_db_sql_ret is None:
                logging.error("数据库连接失败，跳过此条")
                self.mysql_tool.close_db_conn(db, cs)
                continue

            # 统计当天新增
            if published_time_str >= self.today_time:
                increment_count += 1

            # --- 获取稿件分类信息 ---
            ret_get_manuscript_data = self.get_manuscript_data(manuscript_id)
            if not ret_get_manuscript_data:
                logging.warning(f"获取稿件信息失败: {manuscript_id}")
                self.mysql_tool.close_db_conn(db, cs)
                continue

            type_str = self.parse_manuscript_data(ret_get_manuscript_data)

            # --- 数据入库或更新 ---
            if not select_db_sql_ret:
                # --- 新增数据 ---
                text_id = self.handle_snow_id()
                if text_id:
                    self.mysql_tool.insert_db_sql(
                        db, cs, self.db_table,
                        {
                            "precinct": precinct,
                            "precinct_code": precinct_code,
                            "title": title,
                            "detail_url": url,
                            "publish_time": published_time_str,
                            "number": number_str,
                            "type": type_str,
                            "insert_time": datetime.datetime.now(),
                            "text_id": text_id
                        }
                    )
            else:
                # --- 更新数据 ---
                ret_id = select_db_sql_ret[0]["id"]
                ret_type = select_db_sql_ret[0]["type"]
                ret_number = select_db_sql_ret[0]["number"]
                ret_title = select_db_sql_ret[0]["title"]
                ret_publish_time = select_db_sql_ret[0]["publish_time"]

                # 判断列表页数据是否变更
                if not (title == ret_title and published_time_str == ret_publish_time and \
                        number_str == ret_number and type_str == ret_type):

                    # 事务更新 (原代码逻辑)
                    try:
                        self.mysql_tool.transaction_update_db_sql(
                            db, cs, self.db_table,
                            {"number": number_str, "title": title, "publish_time": published_time_str, "type": type_str,
                             "flag": 0},
                            f"`id` = {ret_id}"
                        )
                        self.mysql_tool.transaction_update_db_sql(
                            db, cs, self.db_table,
                            {"number": number_str, "publish_time": published_time_str, "type": type_str},
                            f"`pid` = {ret_id}"
                        )
                        db.commit()
                    except Exception as e:
                        logging.error(f"数据库事务更新错误 {e}", exc_info=True)
                        db.rollback()

            self.mysql_tool.close_db_conn(db, cs)

        return continue_crawl, increment_count, page_item_count

    # --- 2. CsrcGov 特有的辅助方法 ---

    def get_manuscript_data(self, manuscript_id: str) -> dict | None:
        """
        获取原稿信息 (移植自原 get_manuscript_data)
        """
        if not self.manuscript_url:
            logging.error("manuscript_data_base_url 未在配置中定义")
            return None

        data = {"mId": manuscript_id, "status": 4}
        resp = self._make_request('POST', self.manuscript_url, data=data)

        if resp:
            try:
                resp_dict = resp.json()
                if "results" in resp_dict:
                    return resp_dict
            except requests.exceptions.JSONDecodeError as e:
                logging.error(f"解析原稿JSON失败: {e} - {resp.text[:100]}")
        return None

    def parse_manuscript_data(self, text_dict: dict) -> str:
        """
        解析原稿信息 (移植自原 parse_manuscript_data)
        """
        channel_name_list = []
        channel_name_flag = False
        parent_channel_name = "证监局主题分类"
        data_list = text_dict.get("results", {}).get("data", {}).get("data", [])

        for data in data_list:
            channel_list = data.get("_source", {}).get("channel", [])
            for channel in channel_list:
                if channel.get("channelName") == parent_channel_name:
                    channel_name_flag = True
                    continue
                if channel_name_flag:
                    cn = channel.get("channelName")
                    if cn and cn not in channel_name_list:
                        channel_name_list.append(cn)
                    break
            channel_name_flag = False

        return ";".join(channel_name_list)