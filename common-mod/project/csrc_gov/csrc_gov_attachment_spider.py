# 文件名: projects/csrc_gov/csrc_gov_attachment_spider.py
# ---------------------
# desc: 证监局附件爬虫 (L3) - 具体业务实现
#       - 继承 AbstractAttachmentSpider
#       - 实现所有抽象方法
# ---------------------
import logging
import uuid
from ...base.abstract_attachment_spider import AbstractAttachmentSpider
from csrc_gov.tools.md5_tool import get_file_md5  # 导入特定工具


class CsrcGovAttachmentSpider(AbstractAttachmentSpider):
    """
    证监局附件处理的具体实现
    """
    task_name = "证监局附件处理"

    def __init__(self, project_config: dict):
        """
        初始化
        :param project_config: 已合并的配置字典
        """
        super().__init__(project_config)

    # --- 1. 实现 AbstractAttachmentSpider 的抽象方法 ---

    def get_db_tasks_sql(self) -> str:
        """
        返回获取附件任务的 SQL 语句
        (逻辑移植自原 csrc_gov_attachment.py run)
        """
        return (
            "(`flag` = 0) and "
            "(`is_delete` = 0) and "
            "(`pid` is not null or `pid` != '') and "
            "(`attachment_url` is not null or `attachment_url` != '') and "
            f"(`publish_time` >= '{self.start_time}') and "
            f"(`publish_time` <= '{self.end_time}') "
            "order by `publish_time` desc"
        )

    def process_attachment_task(self, data_item: dict, local_file_path: str, file_ext: str):
        """
        处理单条附件任务：计算MD5、上传、更新DB
        (逻辑移植自原 csrc_gov_attachment.py retry_upload_file)
        """
        try:
            # a. 计算MD5
            file_md5 = get_file_md5(local_file_path)

            # b. 决定OBS对象名
            obs_object_name = data_item.get("obs_path", "").split("csrc_gov/")[1]
            if not obs_object_name:
                obs_object_name = "{}/{}.{}".format(
                    data_item.get("precinct_code", "UNKNOWN"),
                    str(uuid.uuid1()).replace("-", ""),
                    file_ext
                )

            # c. 调用基类的通用上传器
            obs_path = self.upload_to_obs(local_file_path, obs_object_name)

            # d. 更新数据库 (特定业务)
            if obs_path:
                db_dict = {
                    "obs_path": obs_path,
                    "file_type": file_ext.upper(),
                    "file_md5": file_md5,
                    "flag": 1
                }
                db, cs = self.mysql_tool.open_db_conn()
                self.mysql_tool.update_db_sql(
                    db, cs, self.db_table,
                    db_dict,
                    f"`id`='{data_item['id']}'"
                )
                self.mysql_tool.close_db_conn(db, cs)
                logging.info(f"附件数据库更新成功: id={data_item['id']}")
            else:
                logging.error(f"附件上传失败，数据库未更新: id={data_item['id']}")

        except Exception as e:
            logging.error(f"附件处理失败: {e}", exc_info=True)