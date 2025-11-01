# 文件名: abstract_list_spider.py
# ---------------------
# desc: 列表爬取工作流基类 (L2)
#       - 继承 BaseSpider
#       - 定义“列表爬取”的标准工作流
# ---------------------
from abc import abstractmethod
from base_spider import BaseSpider
import logging
import datetime
from csrc_gov.tools.monitor_tool import update_info  # 列表爬虫通常需要监控


class AbstractListSpider(BaseSpider):
    """
    定义了“列表爬取”的标准工作流。
    子类只需要实现如何获取和解析特定网站的数据。
    """
    task_name = "抽象列表采集"

    def __init__(self, project_config: dict):
        """
        初始化列表爬虫
        :param project_config: 已合并的配置字典
        """
        # 1. 调用父类__init__，硬编码 stage_name 为 "list_stage"
        super().__init__(project_config, "list_stage")

        # 2. 列表爬虫特有的初始化 (例如 监控数据库)
        self.monitor_mysql_tool = self._init_mysql_tool(
            self.connections.get("monitor_db", self.connections.get("data_db"))  # 优先使用 "monitor_db"，否则回退到 "data_db"
        )
        self.db_monitor_table = self.project_conf.get("db_monitor_table")

        # 3. 列表爬虫特有的配置
        self.is_full_crawled = self.env_settings.get("list_is_full_crawled", False)

        # 4. 列表爬虫特有的时间
        # (如果不是全量爬取，才使用时间范围)
        if self.is_full_crawled:
            logging.info("配置为全量爬取，将忽略 'update_time_extent' 时间范围。")
            self.start_time = "1970-01-01 00:00:00"  # 全量爬取

        # (self.today_time 在原代码中 CsrcGovList.__init__ 被定义, 在 parse_list_page 中使用)
        self.today_time = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")

    def _execute_task(self):
        """
        (已实现) 定义了列表爬取的核心工作流
        """
        logging.info(f"开始采集 {self.task_name}")

        # 列表爬虫的工作流是针对配置中的每一个 "target" (在 csrc_gov 中是 "precinct_list")
        targets = self.stage_conf.get(self.get_target_list_key())
        if not targets:
            logging.error(f"配置中未找到目标列表: {self.get_target_list_key()}")
            return

        for target in targets:
            # --- 监控相关初始化 ---
            state = 0
            page_total_count = None
            increment_count = 0
            error_info = ""

            try:
                logging.info(f"--- 正在处理目标: {self.get_target_name(target)} ---")
                total_pages = self.get_total_pages(target)
                logging.info(f"获取到总页数: {total_pages}")

                if total_pages is None:  # 允许 get_total_pages 返回 None 来跳过
                    logging.warning(f"获取总页数失败或为0，跳过目标: {self.get_target_name(target)}")
                    error_info = "获取总页数失败"
                    continue

                # 用于统计总条数
                current_target_total_count = 0

                for page in range(1, total_pages + 1):
                    logging.info(f"--- 正在爬取第 {page} / {total_pages} 页 ---")
                    try:
                        page_data = self.fetch_list_page(target, page)

                        if not page_data:
                            logging.warning(f"第 {page} 页未获取到数据")
                            continue

                        # parse_list_page 应该返回 (是否继续, 本页新增数, 本页总条数)
                        continue_crawl, page_increment, page_item_count = self.parse_list_page(target, page_data)

                        increment_count += page_increment
                        current_target_total_count += page_item_count

                        # 如果不是全量爬取，且解析器返回 False (说明遇到旧数据)，则停止翻页
                        if not self.is_full_crawled and not continue_crawl:
                            logging.info(f"遇到旧数据，停止非全量爬取。 (第 {page} 页)")
                            break

                    except Exception as e:
                        logging.error(f"处理第 {page} 页时失败: {e}", exc_info=True)

                state = 1  # 目标处理成功
                page_total_count = current_target_total_count  # 记录总条数

            except Exception as e:
                error_info = f"处理目标 {self.get_target_name(target)} 时失败: {e}"
                logging.error(error_info, exc_info=True)

            finally:
                # --- 更新监控 ---
                if self.monitor_mysql_tool and self.db_monitor_table:
                    self.handle_crawler_status_to_db(target, page_total_count, increment_count, state, error_info)
                if self.get_monitor_api_condition(target):
                    self.handle_crawler_status_to_api(target, page_total_count, increment_count, state, error_info)

        # 确保关闭特有的数据库连接
        if self.monitor_mysql_tool:
            self.monitor_mysql_tool.close_ssh_conn()

    # --- 抽象方法 (子类必须实现) ---

    @abstractmethod
    def get_target_list_key(self) -> str:
        """
        (子类必须实现)
        返回在 stage_conf 中包含目标列表的 "键名"。
        例如: "precinct_list"
        """
        pass

    @abstractmethod
    def get_target_name(self, target: dict) -> str:
        """
        (子类必须实现)
        从目标字典中返回一个可读的名称用于日志。
        例如: target["precinct"]
        """
        pass

    @abstractmethod
    def get_total_pages(self, target: dict) -> int | None:
        """
        (子类必须实现)
        获取该目标的总页数。
        """
        pass

    @abstractmethod
    def fetch_list_page(self, target: dict, page: int) -> any:
        """
        (子类必须实现)
        获取特定目标、特定页的原始数据 (例如 JSON 或 HTML)。
        """
        pass

    @abstractmethod
    def parse_list_page(self, target: dict, page_data: any) -> (bool, int, int):
        """
        (子类必须实现)
        解析页面数据，并使用 self.mysql_tool 存入数据库。
        :return: (
            continue_crawl: bool, # 是否继续翻页 (对非全量爬取有效)
            increment_count: int, # 本页新增了多少条数据
            page_item_count: int  # 本页总共解析了多少条数据
        )
        """
        pass

    @abstractmethod
    def get_monitor_api_condition(self, target: dict) -> dict | None:
        """
        (子类必须实现)
        返回调用监控 API 所需的 "condition" 字典。
        如果不需要API监控，返回 None。
        例如: target.get("condition")
        """
        pass

    # --- 列表爬虫特有的辅助方法 (监控) ---

    def handle_crawler_status_to_db(self, target: dict, total: int, increment: int, state: int, error_info: str):
        """
        爬虫监控信息写入数据库 (原 csrc_gov_list 中的方法)
        """
        crawler_status_id = target.get("crawler_status_id")
        if not crawler_status_id:
            return  # 没有ID，无法更新

        try:
            log_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            db, cs = self.monitor_mysql_tool.open_db_conn()
            self.monitor_mysql_tool.update_db_sql(
                db, cs, self.db_monitor_table,
                {
                    "total": total,
                    "increment": increment,
                    "state": state,
                    "errorInfo": error_info,
                    "logTime": log_time
                },
                f"`id` = {crawler_status_id}"
            )
            self.monitor_mysql_tool.close_db_conn(db, cs)
            logging.info(f"数据库监控(ID:{crawler_status_id})更新成功")
        except Exception as e:
            logging.error(f"数据库监控(ID:{crawler_status_id})写入失败: {e}", exc_info=True)

    def handle_crawler_status_to_api(self, target: dict, total: int, increment: int, state: int, error_info: str):
        """
        爬虫监控信息写入API (原 csrc_gov_list 中的方法)
        """
        condition = self.get_monitor_api_condition(target)
        if not condition:
            return  # 没有 condition，无法调用

        try:
            log_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            crawler_status = {
                "total": total,
                "existence": None,  # "existence" 在原代码中未被使用
                "increment": increment,
                "state": state,
                "errorInfo": error_info,
                "logTime": log_time
            }
            update_info(condition, crawler_status)
            logging.info(f"API 监控(Target:{self.get_target_name(target)})更新成功")
        except Exception as e:
            logging.error(f"API 监控(Target:{self.get_target_name(target)})写入失败: {e}", exc_info=True)