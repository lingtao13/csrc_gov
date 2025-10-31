# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import logging
import platform

import pdfkit


class PDFTool(object):

    def __init__(self, cache_path):

        # pdf存放目录
        self.cache_path = cache_path

        # 获取当前系统
        self.plat = platform.system().lower()

        # Windows 需要指定 wkhtmltopdf.exe 路径, Linux 则使用默认路径
        if self.plat == "windows":
            self.wk_path = r"D:\wkhtmltox\ins\wkhtmltopdf\bin\wkhtmltopdf.exe"
            self.wk_conf = pdfkit.configuration(wkhtmltopdf=self.wk_path)

        self.wk_opt = {
            # pdfkit Exit with code 1 due to network error: ProtocolUnknownError设为true，本地文件访问权限被禁止了
            "enable-local-file-access": True,
            # "enable-javascript": True
        }

    def url_html_to_pdf(self, url_list, file_name):
        """
        html链接转pdf
        :param url_list:
        :param file_name:
        :return:
        """
        if self.plat == "windows":
            try:
                return pdfkit.from_url(url_list, self.cache_path + file_name, configuration=self.wk_conf)
            except Exception as e:
                logging.error(str(e))
                return True
        elif self.plat == "linux":
            try:
                return pdfkit.from_url(url_list, self.cache_path + file_name)
            except Exception as e:
                logging.error(str(e))
                return True
        else:
            return False

    def file_html_to_pdf(self, file_path, file_name):
        """
        html文件转pdf
        :param file_path:
        :param file_name:
        :return:
        """
        if self.plat == "windows":
            try:
                return pdfkit.from_file(file_path, self.cache_path + file_name, configuration=self.wk_conf)
            except Exception as e:
                logging.error(str(e))
                return True
        elif self.plat == "linux":
            try:
                return pdfkit.from_file(file_path, self.cache_path + file_name)
            except Exception as e:
                logging.error(str(e))
                return True
        else:
            return False

    def string_html_to_pdf(self, text, file_name):
        """
        html文本转pdf
        :param text:
        :param file_name:
        :return:
        """
        if self.plat == "windows":
            try:
                return pdfkit.from_string(text, self.cache_path + file_name, configuration=self.wk_conf, options=self.wk_opt)
            except Exception as e:
                logging.error(str(e))
                return True
        elif self.plat == "linux":
            try:
                return pdfkit.from_string(text, self.cache_path + file_name)
            except Exception as e:
                logging.error(str(e))
                return True
        else:
            return False


if __name__ == '__main__':
    pt = PDFTool("../cache/")
    # pt.url_html_to_pdf(["http://www.csrc.gov.cn/hunan/c104482/c6927560/content.shtml"], "123.pdf")
    pt.file_html_to_pdf(r'C:\Users\sjzx-wb12\Desktop\test\123.html', "123.pdf")
