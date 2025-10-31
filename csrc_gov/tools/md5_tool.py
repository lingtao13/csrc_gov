# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import hashlib


def get_file_md5(file_name):
    """
    计算文件的md5
    :param file_name:
    :return:
    """
    m = hashlib.md5()  # 创建md5对象
    with open(file_name, "rb") as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            m.update(data)  # 更新md5对象

    return m.hexdigest()  # 返回md5对象


def get_str_md5(content):
    """
    计算字符串md5
    :param content:
    :return:
    """
    m = hashlib.md5()  # 创建md5对象
    m.update(content)
    return m.hexdigest()


if __name__ == '__main__':
    print(get_file_md5(r"C:\Users\sjzx-wb12\Downloads\0e336adea96911edba48fa163ebe0297.xls"))
