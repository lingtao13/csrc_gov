# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date: 2022-11-15
# desc:
# ---------------------
import json

import requests


class SnowTool(object):

    def __init__(self):

        self.base_url = "http://139.159.224.229:5010/get_id"

    def get_snow_id(self):

        resp = requests.get(self.base_url)
        resp_text = resp.text
        resp_dict = json.loads(resp_text)

        status = resp_dict["Status"]
        if status:
            announcement_id = resp_dict["AnnouncementId"]
            return announcement_id
        else:
            return None


if __name__ == '__main__':
    st = SnowTool()
    st.get_snow_id()
