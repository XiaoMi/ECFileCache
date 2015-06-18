#!/usr/bin/python
# -*- coding: utf-8 -*-
#

import logging
import json
import os
import getopt

from utils.log_util import get_logger_name

KEY_ZK_ADDRESSES = "zk_addresses"
KEY_ZK_TIMEOUT = "zk_timeout"
KEY_ZK_ROOT = "zk_root"

KEY_REDIS_BIN = "redis_bin"
KEY_REDIS_CONF = "redis_conf"
KEY_REDIS_ADDRESS = "redis_address"

KEY_REDIS_PERF_TAGS = "redis_perf_tags"

LOGGER = logging.getLogger(get_logger_name(__name__))

#
# Exception
#
class ConfigException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Config:
    __instance = None

    @classmethod
    def init(cls, commands):
        config = Config()
        config.read_from_commands(commands)
        cls.__instance = config

    @classmethod
    def instance(cls):
        return cls.__instance

    def __init__(self):
        if Config.__instance:
            raise ConfigException("instance has been inited")

        self.__zk_addresses = None
        self.__zk_timeout = 10  # default is 10 seconds
        self.__zk_root = None

        self.__redis_bin = None
        self.__redis_conf = None
        self.__redis_address = None

        self.__conf_file = None

        self.__redis_port_specified = None
        self.__redis_conf_specified = None

        self.__redis_perf_tags = None

    #
    # Read config from string
    #
    def read_config_from_string(self, line):
        LOGGER.info("Read config %s", line)
        json_obj = json.loads(line)

        # parse config
        if KEY_ZK_TIMEOUT in json_obj:
            self.set_zk_timeout(json_obj[KEY_ZK_TIMEOUT])

        if KEY_ZK_ADDRESSES not in json_obj:
            raise ConfigException("Cannot found %s" % KEY_ZK_ADDRESSES)

        if KEY_ZK_ROOT not in json_obj:
            raise ConfigException("Cannot found %s" % KEY_ZK_ROOT)

        if KEY_REDIS_BIN not in json_obj:
            raise ConfigException("Cannot found %s" % KEY_REDIS_BIN)

        if KEY_REDIS_CONF not in json_obj:
            raise ConfigException("Cannot found %s" % KEY_REDIS_CONF)

        if KEY_REDIS_ADDRESS not in json_obj:
            raise ConfigException("Cannot find %s" % KEY_REDIS_ADDRESS)

        if KEY_REDIS_PERF_TAGS not in json_obj:
            raise ConfigException("Cannot find %s" % KEY_REDIS_PERF_TAGS)

        # Write value
        self.set_zk_addresses(json_obj[KEY_ZK_ADDRESSES])
        self.set_zk_root(json_obj[KEY_ZK_ROOT])

        self.set_redis_bin(json_obj[KEY_REDIS_BIN])
        self.set_redis_conf(json_obj[KEY_REDIS_CONF])
        self.set_redis_address(json_obj[KEY_REDIS_ADDRESS])
        self.set_redis_perf_tags(json_obj[KEY_REDIS_PERF_TAGS])

        LOGGER.info("use config:[%s]", self.to_string())

    #
    # Read config from a file
    #
    def read_config_file(self, config_file):
        LOGGER.info("read config file [%s]", config_file)
        self.__conf_file = config_file

        if not os.path.exists(config_file):
            LOGGER.error("file %s not exist", config_file)
            raise ConfigException("file {} not found".format(config_file))

        # Parsing
        # init this value to avoid warning
        f = None
        try:
            f = open(config_file, "r")
            lines = f.read()
            self.read_config_from_string(lines)
        except IOError, e:
            LOGGER.error("cannot read config file:%s", config_file, exc_info=True)
            raise ConfigException("cannot read config file:%s" % config_file)
        finally:
            if f is not None and not f.closed:
                try:
                    f.close()
                except IOError, e:
                    # ignore it
                    pass

    #
    # Read config from cmd arguments
    #
    def read_from_commands(self, commands):
        opts, args = getopt.getopt(commands, "hf:p:c:l:")

        for op, value in opts:
            if op == "-f":
                self.read_config_file(value)
            if op == "-p":
                self.__redis_port_specified = value
            if op == "-c":
                self.__redis_conf_specified = value
            if op == "-h":
                self.print_usage_help()
                return False

        if self.__redis_port_specified is not None:
            [host, port_ignored] = self.__redis_address.split(":")
            self.__redis_address = host + ":" + self.__redis_port_specified
            LOGGER.info("specified redis address:%s", self.__redis_address)

        if self.__redis_conf_specified is not None:
            self.__redis_conf = self.__redis_conf_specified
            LOGGER.info("specified redis conf:%s", self.__redis_conf)

        return True

    #
    # print help menus
    #
    @staticmethod
    def print_usage_help():
        print "-f: the file from which to read config\ " \
              "-h: print this help"

    def get_config_filename(self):
        return self.__conf_file

    def set_zk_timeout(self, timeout=10):
        """
        :type timeout: int
        """
        self.__zk_timeout = timeout

    def get_zk_timeout(self):
        return self.__zk_timeout

    def set_zk_addresses(self, zkAddresses):
        """
        :type zkAddresses: string

        :raises:
            :exc: `~ConfigException` zk address is invalid
        """
        self.validate_not_blank(zkAddresses, "zkAddresses")
        self.validate_net_address(zkAddresses)
        self.__zk_addresses = zkAddresses

    def get_zk_addresses(self):
        return self.__zk_addresses

    def set_zk_root(self, zk_root):
        """
        :type zk_root: str
        """
        self.validate_not_blank(zk_root, "zk_root")
        self.__zk_root = zk_root

    def get_zk_root(self):
        return self.__zk_root

    def set_redis_address(self, redisAddress):
        """
        :type redisAddress: str
        """
        self.validate_not_blank(redisAddress, "redisAddress")
        self.validate_net_address(redisAddress)
        self.__redis_address = redisAddress

    def get_redis_address(self):
        """
        :rtype: string
        """
        return self.__redis_address

    def set_redis_bin(self, redis_bin):
        """
        :type redis_bin: string
        """
        self.validate_not_blank(redis_bin, "redis_bin")
        self.__redis_bin = redis_bin

    def get_redis_bin(self):
        """
        :rtype: str
        """
        return self.__redis_bin

    def set_redis_conf(self, redis_conf):
        """
        :type redis_conf: str
        :rtype: str
        """
        self.validate_not_blank(redis_conf, "redis_conf")
        self.__redis_conf = redis_conf

    def get_redis_conf(self):
        """
        :return: str
        """
        return self.__redis_conf

    def set_redis_perf_tags(self, tags):
        """

        :type tags: str
        """
        self.__redis_perf_tags = tags

    def get_redis_perf_tags(self):
        return self.__redis_perf_tags

    def validate_net_address(self, addresses):
        """
        :type addresses: str
        :raises:
            :exc: `~ConfigException` invalid address
        """
        for address in addresses.split(","):
            if len(address.split(":")) != 2:
                verbose = "invalid address:[%s] in [%s]" % (address, addresses)
                LOGGER.error(verbose)
                raise ConfigException(verbose)

    def validate_not_blank(self, string=None, msg=None):
        """
        :type string: str
        :type msg: str
        :raises:
            :exc: `~ConfigException` invalid arg
        """
        if string is None or len(string) <= 0:
            verbose = "invalid arg:[%s]" % msg
            LOGGER.error(verbose)
            raise ConfigException(verbose)

    def to_string(self):
        string = "\n" + KEY_ZK_ADDRESSES + ":" + self.__zk_addresses + "\n" \
                 + KEY_ZK_TIMEOUT + ":" + str(self.__zk_timeout) + "\n" \
                 + KEY_REDIS_BIN + ":" + self.__redis_bin + "\n" \
                 + KEY_REDIS_CONF + ":" + self.__redis_conf + "\n" \
                 + KEY_REDIS_ADDRESS + ":" + self.__redis_address + "\n"

        return string
