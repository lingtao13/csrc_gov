# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import logging

import requests
import json


class ProxyTool(object):

    @staticmethod
    def get_proxy(get_number=1, net_agreement=1, validity_time=1, port_number=4):
        """
        获取代理IP
        :param get_number: 获取数量
        :param net_agreement: 网络协议 1:HTTP 2:SOCK5 11:HTTPS
        :param validity_time: 稳定时长 1:5-25min 2:25min-3h 3:3-6h 4:6-12h 7:48-72h
        :param port_number: 端口位数 4:4位 5:5位 45:随机
        :return:
        """
        url_base = "http://webapi.http.zhimacangku.com/getip?num={}&type=2&pro=&city=0&yys=0&port={}&time={}&ts=1&ys=0&cs=0&lb=1&sb=0&pb={}&mr=1&regions="
        url = url_base.format(
            get_number,
            net_agreement,
            validity_time,
            port_number
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        try:
            response = requests.get(url=url, headers=headers, timeout=5)
            if response.ok:
                data = json.loads(response.content)
                code = data["code"]
                if code == 0:
                    ip = data["data"][0]["ip"]
                    port = data["data"][0]["port"]
                    proxies_dict = {
                        "http": "http://{}:{}".format(ip, port),
                        "https": "https://{}:{}".format(ip, port)
                    }
                    return proxies_dict
                elif code == 111:
                    logging.error("提取链接请求太过频繁，超出限制, 请在1秒后再次请求")
                elif code == 113:
                    logging.error("白名单未添加本机IP, 请将本机IP设置为白名单！")
                elif code == 114:
                    logging.error("余额不足, 请充值")
                elif code == 116:
                    logging.error("套餐内IP数量消耗完毕")
                else:
                    logging.error("获取代理失败")
            return None
        except Exception as e:
            return None

    @staticmethod
    def get_proxy2():
        """
        获取代理IP
        :return:
        """
        url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&pack=170681&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        try:
            response = requests.get(url=url, headers=headers, timeout=5)
            print(response.text)
            if response.ok:
                data = json.loads(response.content)
                print(data)
                code = data["code"]
                if code == 0:
                    ip = data["data"][0]["ip"]
                    port = data["data"][0]["port"]
                    proxies_dict = {
                        "http": "http://{}:{}".format(ip, port),
                        "https": "https://{}:{}".format(ip, port)
                    }
                    return proxies_dict
                elif code == 111:
                    logging.error("提取链接请求太过频繁，超出限制, 请在1秒后再次请求")
                elif code == 113:
                    logging.error("白名单未添加本机IP, 请将本机IP设置为白名单！")
                elif code == 114:
                    logging.error("余额不足, 请充值")
                elif code == 116:
                    logging.error("套餐内IP数量消耗完毕")
                else:
                    logging.error("获取代理失败")
            return None
        except Exception as e:
            return None

    @staticmethod
    def check_proxy(proxies_dict):
        """
        测试代理是否有效
        :param proxies_dict:
        :return:
        """
        url = "http://www.baidu.com/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        try:
            requests.get(url=url, headers=headers, proxies=proxies_dict, timeout=5)
        except Exception as e:
            return False
        return True


if __name__ == '__main__':
    import time

    pt = ProxyTool()
    for i in range(10):
        proxy = pt.get_proxy2()
        time.sleep(3)
        print(proxy)
        # if i == 3:
        #     time.sleep(61.0)
    # pt.check_proxy(proxies)

