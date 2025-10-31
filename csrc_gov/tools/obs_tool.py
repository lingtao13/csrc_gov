# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date:
# desc:
# ---------------------
from obs import ObsClient


class OBSTool(object):

    def __init__(self, ak, sk, sv, bt, fd):

        # obs配置
        self.access = ak
        self.secret = sk
        self.server = sv
        self.bucket = bt
        self.folder = fd
        # 打开obs连接
        self.client = ObsClient(access_key_id=self.access, secret_access_key=self.secret, server=self.server)
        # obs访问链接
        self.base = "http://{}.{}/{}".format(self.bucket, self.server, self.folder)

    def create_bucket(self, bucket_name, location):
        """
        创建桶
        :param bucket_name: 桶名（cwd-bucket）
        :param location: 地址（cn-south-1）
        :return:
        """
        resp = self.client.createBucket(bucketName=bucket_name, location=location)

        if resp.status < 300:
            return True
        else:
            return False

    def upload_text(self, object_name, file_content):
        """
        上传文本/字节流
        :param object_name: 对象名称
        :param file_content: 文本/字节流
        :return:
        """
        resp = self.client.putContent(self.bucket, self.folder + object_name, content=file_content)

        if resp.status < 300:
            return True
        else:
            return False

    def upload_file(self, object_name, file_path):
        """
        上传文件
        :param object_name: 对象名称
        :param file_path: 文件路径
        :return:
        """
        resp = self.client.putFile(self.bucket, self.folder + object_name, file_path=file_path)

        if resp.status < 300:
            return True
        else:
            return False

    def download_file(self, object_name, file_path):
        """
        下载文件
        :param object_name: 对象名称
        :param file_path: 文件路径
        :return:
        """
        resp = self.client.getObject(self.bucket, self.folder + object_name, downloadPath=file_path)

        if resp.status < 300:
            return resp
        else:
            return None

    def get_metadata(self, object_name):
        """
        获取元数据
        :param object_name: 对象名称
        :return:
        """
        resp = self.client.getObjectMetadata(self.bucket, object_name)

        if resp.status < 300:
            return resp
        else:
            return None


if __name__ == '__main__':
    ot = OBSTool(
        "FMH0XV3F9VFQLTAFHRGL",
        "ARUbor7b03LBEjBVotieP4Y2m34KtXYLklvz86ZO",
        "obs.cn-south-1.myhwclouds.com",
        "obs-cninfo-test",
        "cwd_test"
    )
    # with open("C:\\Users\\cwd\\Desktop\\1669368053_806731.pdf", "rb") as f:
    #     content = f.read()
    # ot.upload_text("1669368053_806731.pdf", content)
    # ot.upload_file("1669368053_806731.pdf", "C:\\Users\\cwd\\Desktop\\1669368053_806731.pdf")
    ret = ot.download_file("csrc_gov/ZJJ001/1b6aeb879fa311edaac56c4b90fade75.pdf", "C:\\Users\\sjzx-wb12\\Desktop\\test\\1669368053_806731.pdf")
    print(ret)
    print(ret["body"]["etag"][1:-1])
    # print(ot.get_metadata("cwd_test/neeq_notice1/1669368053_806731.pdf"))
