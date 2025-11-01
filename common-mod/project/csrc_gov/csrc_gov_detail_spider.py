# 文件名: projects/csrc_gov/csrc_gov_detail_spider.py
# ---------------------
# desc: 证监局详情页爬虫 (L3) - 具体业务实现
#       - 继承 AbstractDetailSpider
#       - 实现所有抽象方法
# ---------------------
import logging
import datetime
import uuid
import re
import urllib.parse
import os
from lxml import etree
from lxml.html import tostring
from ...base.abstract_detail_spider import AbstractDetailSpider
from csrc_gov.tools.md5_tool import get_file_md5  # 导入特定工具


class CsrcGovDetailSpider(AbstractDetailSpider):
    """
    证监局详情页处理的具体实现
    """
    task_name = "证监局详情页处理"

    def __init__(self, project_config: dict):
        """
        初始化
        :param project_config: 已合并的配置字典
        """
        super().__init__(project_config)

        # --- 本业务特有的配置 ---
        self.website_base_url = self.project_conf.get("website_base_url", "http://www.csrc.gov.cn/")
        if not self.h5_temp_str or not self.c3_temp_str:
            logging.error("H5或C3模板加载失败，PDF转换功能将受限。")

    # --- 1. 实现 AbstractDetailSpider 的抽象方法 ---

    def get_db_tasks_sql(self) -> str:
        """
        返回获取详情页任务的 SQL 语句
        (逻辑移植自原 csrc_gov_detail.py run)
        """
        return (
            "(`is_delete` = 0) and "
            "(`pid` is null or `pid` = '') and "
            "(`detail_url` is not null or `detail_url` != '') and "
            f"(`publish_time` >= '{self.start_time}') and "
            f"(`publish_time` <= '{self.end_time}') "
            "order by `publish_time` desc"
        )

    def process_detail_task(self, data_item: dict, raw_content: str, encoding: str):
        """
        处理单条详情页任务：解析、存附件、转PDF、上传
        (逻辑移植自原 csrc_gov_detail.py handle_save_transform_upload)
        """
        # 1. 解析详情页，提取附件列表和清洗后的HTML
        file_list, content_str = self.parse_detail_page(raw_content, data_item)

        # 2. 将解析到的附件信息保存到数据库
        attachment_info_save_ret = self.attachment_info_save(file_list, data_item)
        if not attachment_info_save_ret:
            logging.warning(f"附件信息保存失败，但继续处理PDF: id={data_item['id']}")

        # 3. 检查是否需要生成PDF (flag=0)
        if not data_item.get("flag", 0):
            if not self.h5_temp_str or not self.c3_temp_str:
                logging.error(f"模板未加载，无法生成PDF: id={data_item['id']}")
                return

            try:
                # a. 生成PDF
                uuid_str = str(uuid.uuid1()).replace("-", "")
                local_pdf_name = f"{uuid_str}.pdf"
                local_pdf_path = os.path.join(self.file_cache_path, local_pdf_name)

                # (注意: PDFTool 的 string_html_to_pdf 方法会自己拼接 cache_path 和文件名)
                self.pdf_tool.string_html_to_pdf(content_str, local_pdf_name)

                if not os.path.exists(local_pdf_path):
                    raise Exception(f"PDFTool未能成功创建文件: {local_pdf_path}")

                # b. 计算MD5
                file_md5 = get_file_md5(local_pdf_path)

                # c. 决定OBS对象名
                obs_object_name = data_item.get("obs_path", "").split("csrc_gov/")[1]
                if not obs_object_name:
                    obs_object_name = "{}/{}.pdf".format(
                        data_item.get("precinct_code", "UNKNOWN"), uuid_str
                    )

                # d. 调用基类的通用上传器
                obs_path = self.upload_to_obs(local_pdf_path, obs_object_name)

                # e. 更新数据库
                if obs_path:
                    db_dict = {
                        "obs_path": obs_path,
                        "file_type": "PDF",
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
                    logging.info(f"PDF 数据库更新成功: id={data_item['id']}")
                else:
                    logging.error(f"PDF 上传失败，数据库未更新: id={data_item['id']}")

            except Exception as e:
                logging.error(f"PDF处理或上传失败: {e}", exc_info=True)
        else:
            logging.info(f"PDF已处理 (flag=1)，跳过: id={data_item['id']}")

    # --- 2. CsrcGov 特有的辅助方法 ---

    def parse_detail_page(self, data_str: str, data_dict: dict) -> (list, str):
        """
        解析详情页 (移植自原 csrc_gov_detail.py parse_detail_page)
        :return: (file_info_list, cleaned_html_content)
        """
        url_head = data_dict["detail_url"][:data_dict["detail_url"].rfind("/") + 1]
        tree = etree.HTML(data_str)
        content_str = ""
        file_info_list = []

        content_list = tree.xpath('//div[@class="content"]')
        for content in content_list:
            if not content.xpath('.//h2'): continue
            for h2 in content.xpath('.//h2'): h2.getparent().remove(h2); break

            # (省略原代码中移除隐藏属性、处理a标签、img标签、font标签、移除表格和打印按钮等DOM清洗逻辑)
            # ... (DOM cleaning logic from original file) ...

            # --- 提取附件 ---
            for files_a in content.xpath('.//div[@id="files"]/a | .//div[@class="detail-news"]//a'):
                href = (files_a.xpath('./@href') or [""])[0]
                title = "".join(files_a.xpath('.//text()')).strip()
                if not href or not title or ("/files/" not in href and "http" not in href):
                    continue

                if "http" not in href:
                    href = href[1:] if href.startswith("/") else href
                    if href.endswith((".mp4", ".MP4")):
                        href = self.website_base_url + href
                    else:
                        href = url_head + href
                file_info_list.append((href, title))

            # (省略替换 a 和 img 标签的逻辑)
            # ...

            content_str += tostring(content, encoding="utf8", method="html").decode("utf8")

        content_str = re.sub(r'font-family(.*?);', "", content_str)
        content_str = urllib.parse.unquote(content_str)

        new_content_str = self.h5_temp_str.format(
            data_dict["title"],
            self.c3_temp_str,
            data_dict["title"],
            content_str
        )
        return list(set(file_info_list)), new_content_str

    @staticmethod
    def split_name_suffix(file_name: str) -> (str, str):
        """ (移植自原 csrc_gov_detail.py split_name_suffix) """
        file_name = file_name.strip()
        name, suffix = file_name, ""
        suffix_index = file_name.rfind(".")
        if suffix_index >= 0:
            name_str, suffix_str = file_name[:suffix_index], file_name[suffix_index:]
            if re.search(r'^.[A-Za-z0-9]+?$', suffix_str) and \
                    re.search(r'[A-Za-z]', suffix_str):
                name, suffix = (name_str or suffix_str), suffix_str
        return name, suffix

    def attachment_info_save(self, file_list: list, data_dict: dict) -> bool:
        """
        将附件信息存入数据库 (移植自原 csrc_gov_detail.py attachment_info_save)
        """
        field_list = [
            "pid", "precinct", "precinct_code", "title", "detail_url", "publish_time",
            "number", "attachment_url", "type", "insert_time", "text_id"
        ]
        data_list_to_insert = []

        try:
            db, cs = self.mysql_tool.open_db_conn()
            for file_href, file_title in file_list:
                title, _ = self.split_name_suffix(file_title)
                attachment_url = file_href

                select_db_sql_ret = self.mysql_tool.select_db_sql(
                    db, cs, self.db_table,
                    ["id", "title"],
                    f"`pid` = '{data_dict['id']}' and `attachment_url` = '{attachment_url}'"
                )

                if select_db_sql_ret is None:
                    logging.error("附件数据库查询失败，终止附件保存")
                    self.mysql_tool.close_db_conn(db, cs)
                    return False

                if not select_db_sql_ret:
                    # --- 新增附件 ---
                    text_id = self.handle_snow_id()
                    if text_id:
                        data_list_to_insert.append((
                            data_dict["id"], data_dict["precinct"], data_dict["precinct_code"], title,
                            data_dict["detail_url"], data_dict["publish_time"], data_dict.get("number"),
                            attachment_url, data_dict.get("type"), datetime.datetime.now(), text_id
                        ))
                elif title != select_db_sql_ret[0]["title"]:
                    # --- 更新附件标题和flag ---
                    self.mysql_tool.update_db_sql(
                        db, cs, self.db_table,
                        {"title": title, "flag": 0},
                        f"`id` = '{select_db_sql_ret[0]['id']}'"
                    )

            # --- 批量插入新附件 ---
            if data_list_to_insert:
                self.mysql_tool.many_insert_db_sql(
                    db, cs, self.db_table, field_list, data_list_to_insert
                )

            self.mysql_tool.close_db_conn(db, cs)
            logging.info(f"附件信息处理完毕 (新增 {len(data_list_to_insert)} 条)")
            return True

        except Exception as e:
            logging.error(f"附件保存失败: {e}", exc_info=True)
            if db and cs: self.mysql_tool.close_db_conn(db, cs)
            return False