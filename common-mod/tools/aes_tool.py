# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import base64
from Crypto.Cipher import AES


class AESTool(object):

    def __init__(self):
        self.key = "!fastwaf4323fsdg1@fsa"
        self.aes_obj = AES.new(key=self.add_to_16(self.key), mode=AES.MODE_ECB)

    def add_to_16(self, text):
        """
        填充函数 明文和key都必须是16的倍数
        :param text:
        :return:
        """
        while len(text.encode("utf-8")) % 16 != 0:
            text += "\0"
        return text.encode("utf-8")

    def aes_encrypt(self, text):
        """
        加密
        :param text:
        :return:
        """
        encrypt_str = self.aes_obj.encrypt(self.add_to_16(text))
        encrypt_str_decode = str(base64.b64encode(encrypt_str).decode("utf-8").replace("\n", ""))
        return encrypt_str_decode

    def aes_decrypt(self, encrypt_text):
        """
        解密
        :param encrypt_text:
        :return:
        """
        decrypt_str = base64.b64decode(encrypt_text.encode("utf-8"))
        decrypt_str_decode = str(self.aes_obj.decrypt(decrypt_str).decode("utf-8").replace("\0", ""))
        return decrypt_str_decode


if __name__ == '__main__':
    aes_tool = AESTool()
    print(aes_tool.aes_encrypt("root"))
    print(aes_tool.aes_decrypt("IgFz77wJk69du1I9UeOvdA=="))

