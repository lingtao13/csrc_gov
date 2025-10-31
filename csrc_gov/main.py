# -*- coding: utf-8 -*-
# ---------------------
# author: chenweida
# date:
# desc:
# ---------------------
from csrc_gov_attachment import CsrcGovAttachment
from csrc_gov_detail import CsrcGovDetail
from csrc_gov_list import CsrcGovList

if __name__ == '__main__':
    cgl = CsrcGovList()
    cgl.run()
    cgd = CsrcGovDetail()
    cgd.run()
    cga = CsrcGovAttachment()
    cga.run()
