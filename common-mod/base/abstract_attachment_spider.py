# 文件名: abstract_attachment_spider.py
# ---------------------
# desc: 附件下载工作流基类 (L2)
#       - 继承 BaseSpider
#       - 定义“从数据库获取附件任务 -> 下载 -> 上传”的标准工作流
# ---------------------
from abc import abstractmethod
from base_spider import BaseSpider
import logging
import os
import uuid


class AbstractAttachmentSpider(BaseSpider):
    """
    定义了“附件下载和上传”的标准工作流。
    """
    task_name = "抽象附件处理"

    def __init__(self, project_config: dict):
        """
        初始化附件处理器
        :param project_config: 已合并的配置字典
        """
        # 1. 调用父类__init__，硬编码 stage_name 为 "attachment_stage"
        super().__init__(project_config, "attachment_stage")

    def _execute_task(self):
        """
        (已实现) 定义了附件处理的核心工作流
        """
        if not self.mysql_tool:
            logging.error("数据库未初始化，附件任务无法执行。")
            return

        # 1. 从数据库获取待办任务 (SQL由子类定义)
        sql_query = self.get_db_tasks_sql()

        db, cs = self.mysql_tool.open_db_conn()
        data_list = self.mysql_tool.select_db_sql(
            db, cs, self.db_table, [], sql_query
        )
        self.mysql_tool.close_db_conn(db, cs)

        if data_list is None:
            logging.error("从数据库查询附件任务失败。")
            return

        logging.info(f"周期内需要处理的附件数量->{len(data_list)}")

        # 2. 循环处理 (这是通用的)
        for data_item in data_list:
            if not data_item.get("attachment_url"):
                logging.warning(f"任务 id={data_item['id']} 缺少 'attachment_url'，跳过")
                continue

            logging.info(f"--- 正在处理附件: id={data_item['id']} ---")
            try:
                # 3. 下载附件 (这是通用的)
                # 生成一个本地缓存路径
                file_ext = data_item["attachment_url"].split(".")[-1].split("?")[0]  # 处理带?的URL
                if not file_ext or len(file_ext) > 5:  # 简单校验
                    file_ext = "file"
                local_file_name = f"{str(uuid.uuid1()).replace('-', '')}.{file_ext}"
                local_file_path = os.path.join(self.file_cache_path, local_file_name)

                download_success = self.download_file_generic(
                    url=data_item["attachment_url"],
                    save_path=local_file_path
                )

                if not download_success:
                    logging.error(f"下载附件失败，跳过: {data_item['attachment_url']}")
                    continue

                # 4. 处理和存储 (这是子类特定的)
                self.process_attachment_task(data_item, local_file_path, file_ext)

            except Exception as e:
                logging.error(f"处理附件 id={data_item['id']} 时失败: {e}", exc_info=True)

    # --- 抽象方法 (子类必须实现) ---

    @abstractmethod
    def get_db_tasks_sql(self) -> str:
        """
        (子类必须实现)
        返回一个 SQL `WHERE` 语句 (不含 'WHERE')，用于从数据库获取待处理的 *附件* 任务。

        例如:
        return (
            "(`flag` = 0) and "
            "(`is_delete` = 0) and "
            "(`pid` is not null or `pid` != '') and "
            "(`attachment_url` is not null or `attachment_url` != '') and "
            f"(`publish_time` >= '{self.start_time}') and "
            f"(`publish_time` <= '{self.end_time}') "
            "order by `publish_time` desc"
        )
        """
        pass

    @abstractmethod
    def process_attachment_task(self, data_item: dict, local_file_path: str, file_ext: str):
        """
        (子类必须实现)
        处理已下载的本地附件 (local_file_path)，
        通常包括：计算MD5、上传OBS、更新数据库。

        所有工具 (self.obs_tool, self.mysql_tool, self.tools.md5_tool) 均可使用。
        """
        pass