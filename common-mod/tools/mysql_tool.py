# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import logging

import pymysql
from sshtunnel import SSHTunnelForwarder


class MysqlTool(object):

    def __init__(self, db_username, db_password, db_database, db_host="127.0.0.1", db_port=3306,
                 db_relay_host="0.0.0.0", db_relay_port=10022, ssh_host="139.159.150.159",
                 ssh_port=22, ssh_username=None, ssh_password=None, charset="utf8"):

        self.db_username = db_username
        self.db_password = db_password
        self.db_database = db_database
        self.db_host = db_host
        self.db_port = db_port
        self.db_relay_host = db_relay_host
        self.db_relay_port = db_relay_port

        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.charset = charset

        # 是否通过ssh连接mysql
        if self.ssh_username and self.ssh_password:
            self.server = SSHTunnelForwarder((self.ssh_host, self.ssh_port),
                                             ssh_username=self.ssh_username,
                                             ssh_password=self.ssh_password,
                                             remote_bind_address=(self.db_host, self.db_port),
                                             local_bind_address=(self.db_relay_host, self.db_relay_port))
            self.server.start()

    def open_db_conn(self):
        """
        开启db连接
        :return:
        """
        # 打开数据库连接
        if self.ssh_username and self.ssh_password:
            db = pymysql.connect(
                host=self.db_host,
                port=self.db_relay_port,
                user=self.db_username,
                password=self.db_password,
                database=self.db_database,
                charset=self.charset
            )
        else:
            db = pymysql.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_username,
                password=self.db_password,
                database=self.db_database,
                charset=self.charset
            )
        # 创建游标对象
        cs = db.cursor(cursor=pymysql.cursors.DictCursor)
        return db, cs

    @staticmethod
    def close_db_conn(db, cs):
        """
        关闭db连接
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :return:
        """
        # 关闭游标对象
        cs.close()
        # 关闭数据库连接
        db.close()

    def close_ssh_conn(self):
        """
        关闭ssh连接
        :return:
        """
        if self.ssh_username and self.ssh_password and self.server:
            self.server.stop()

    @staticmethod
    def select_db_count_sql(db, cs, table, factor_str=""):
        """
        查询总条数sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param factor_str: 表名（例如：user="cwd"）
        :return:
        """
        if not factor_str:
            sql = "select count(*) as count from {}".format(table)
        else:
            sql = "select count(*) as count from {} where {}".format(table, factor_str)

        try:
            cs.execute(sql)
            ret = cs.fetchone()
            return ret
        except Exception as e:
            logging.error(str(e))
            return None

    @staticmethod
    def select_db_sql(db, cs, table, field_list, factor_str):
        """
        查询sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param field_list: 所需字段列表（例如：["id", "name"]）
        :param factor_str: 条件（例如：id=1）
        :return:
        """
        if field_list:
            field_str = ",".join(["`" + str(field) + "`" for field in field_list])
            sql = "select " + field_str + " from " + table + " where " + factor_str
        else:
            sql = "select * from " + table + " where " + factor_str

        try:
            cs.execute(sql)
            return cs.fetchall()
        except Exception as e:
            logging.error(str(e))
            return None

    @staticmethod
    def insert_db_sql(db, cs, table, data_dict):
        """
        插入sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param data_dict: 数据字典（例如：{"name": "cwd", "age": 18, "sex": "men"}）
        :return:
        """
        field_list = []
        seat_list = []
        value_list = []
        for data_key in data_dict:
            field_list.append("`" + data_key + "`")
            value_list.append(data_dict[data_key])
            seat_list.append("%s")
        field_str = ",".join(field_list)
        seat_str = ",".join(seat_list)
        sql = "insert into " + table + "(" + field_str + ") values (" + seat_str + ")"

        try:
            cs.execute(sql, value_list)
            insert_id = db.insert_id()
            db.commit()
            return insert_id
        except Exception as e:
            logging.error(str(e))
            db.rollback()
            return None

    @staticmethod
    def many_insert_db_sql(db, cs, table, field_list, data_list):
        """
        批量插入sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param field_list: 所需字段列表（例如：["id", "name"]）
        :param data_list: 对应字段数据列表（例如：[(1, "cwd"), (2, "cwd2")]）
        :return:
        """
        new_field_list = []
        seat_list = []
        for field in field_list:
            new_field_list.append("`" + field + "`")
            seat_list.append("%s")
        field_str = ",".join(field_list)
        seat_str = ",".join(seat_list)
        sql = "insert into " + table + "(" + field_str + ") values (" + seat_str + ")"

        try:
            cs.executemany(sql, data_list)
            db.commit()
            return True
        except Exception as e:
            logging.error(str(e))
            db.rollback()
            return False

    @staticmethod
    def update_db_sql(db, cs, table, data_dict, factor_str):
        """
        更新sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param data_dict: 数据字典（例如：{"name": "cwd", "age": 18, "sex": "men"}）
        :param factor_str: 条件（例如：id=1）
        :return:
        """
        field_list = []
        value_list = []
        for data_key in data_dict:
            field_list.append("`" + data_key + "`=%s")
            value_list.append(data_dict[data_key])
        field_str = ",".join(field_list)

        if factor_str:
            sql = "update " + table + " set " + field_str + " where " + factor_str
        else:
            sql = "update " + table + " set " + field_str

        try:
            cs.execute(sql, value_list)
            db.commit()
            return True
        except Exception as e:
            logging.error(str(e))
            db.rollback()
            return False

    @staticmethod
    def transaction_update_db_sql(db, cs, table, data_dict, factor_str):
        """
        事务更新sql，需要手动commit
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param data_dict: 数据字典（例如：{"name": "cwd", "age": 18, "sex": "men"}）
        :param factor_str: 条件（例如：id=1）
        :return:
        """
        field_list = []
        value_list = []
        for data_key in data_dict:
            field_list.append("`" + data_key + "`=%s")
            value_list.append(data_dict[data_key])
        field_str = ",".join(field_list)

        if factor_str:
            sql = "update " + table + " set " + field_str + " where " + factor_str
        else:
            sql = "update " + table + " set " + field_str

        try:
            cs.execute(sql, value_list)
            return True
        except Exception as e:
            logging.error(str(e))
            return False

    @staticmethod
    def many_update_db_sql(db, cs, table, field_list, data_list, factor_str):
        """
        批量更新sql
        :param db: 数据库连接对象
        :param cs: 数据库游标对象
        :param table: 表名（例如：user）
        :param field_list: 所需字段列表（例如：["id", "name"]）
        :param data_list: 对应字段数据列表（例如：[(1, "cwd"), (2, "cwd2")]）
        :param factor_str: 条件（例如：id=%s）
        :return:
        """
        new_field_list = []
        for field in field_list:
            new_field_list.append("`" + field + "`=%s")
        field_str = ",".join(new_field_list)
        sql = "update " + table + " set " + field_str + " where " + factor_str

        try:
            cs.executemany(sql, data_list)
            db.commit()
            return True
        except Exception as e:
            logging.error(str(e))
            db.rollback()
            return False


if __name__ == '__main__':
    # ssh连接
    mt = MysqlTool(
        db_username="pyspider",
        db_password="py2018spider",
        db_database="pyspider_test",
        ssh_host="139.159.150.159",
        ssh_username="root",
        ssh_password="cninfo2018@",
        charset="utf8"
    )
    d, c = mt.open_db_conn()
    print(mt.select_db_count_sql(d, c, "neeq_notice", ""))

    # # mysql连接
    # mt = MysqlTool(
    #     db_username="root",
    #     db_host="127.0.0.1",
    #     db_port=3306,
    #     db_password="chenweida",
    #     db_database="pymysql_test",
    #     charset="utf8"
    # )
    # db, cs = mt.open_db_conn()
    # print(mt.select_db_count_sql(db, cs, "capital_market", ""))
