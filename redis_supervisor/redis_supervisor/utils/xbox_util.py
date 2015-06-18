#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# 该文件提供xbox的报警逻辑
#
__author__ = 'sail'

import logging
import subprocess
import json
from unittest import TestCase

LOGGER = logging.getLogger(__name__)

class XboxWarningRecord:

    WarningRecordHost = "http://xbox.n.xiaomi.com/monitor/alert"
    WarningMails = "guoxuedong@xiaomi.com,konglingtao@xiaomi.com"
    WarningMobiles = "15210893144"

    def __init__(self, priority, module, msg, email, mobile):
        """
        :type priority: str
        :type module: str
        :type msg: str
        :type email: str
        :type mobile: str
        """
        self.__priority = priority
        self.__module = module
        self.__msg = msg
        self.__email = email
        self.__mobile = mobile

    def get_data(self):
        return "priority=%s&module=%s&msg=%s&email=%s&mobile=%s" % (self.__priority, self.__module, self.__msg, self.__email, self.__mobile)

    def get_host(self):
        return XboxWarningRecord.WarningRecordHost

class WarningUtil:

    @staticmethod
    def sendFatal(msg, mails = XboxWarningRecord.WarningMails, mobiles = XboxWarningRecord.WarningMobiles):
        """
        :type msg: str
        """
        record = XboxWarningRecord("P0", "filecache", msg, mails, mobiles)
        WarningUtil.send_core(record)

    @staticmethod
    def sendError(msg, mails = XboxWarningRecord.WarningMails, mobiles = XboxWarningRecord.WarningMobiles):
        record = XboxWarningRecord("P1", "filecache", msg, mails, mobiles)
        WarningUtil.send_core(record)

    @staticmethod
    def send_core(record):
        """
        :type record: XboxWarningRecord
        """
        pipe = subprocess.PIPE
        cmd = subprocess.Popen(["curl", "-d", record.get_data(), record.get_host()], stdout=pipe)

        ret_code = cmd.wait()

        if ret_code != 0:
            LOGGER.error("cannot send error message:%s", record.get_data())
            return -1

        output = cmd.communicate()[0]
        result = json.loads(output)

        if "ok" in result and result["ok"] == False:
            LOGGER.error("host return bad code")
            return -1

        return 0

class WarningUtilTest(TestCase):

    def test_send_fetal(self):
        WarningUtil.sendFetal("fail")
