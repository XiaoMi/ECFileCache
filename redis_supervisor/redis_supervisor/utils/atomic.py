import logging

from threading import RLock

LOGGER = logging.getLogger(__name__)

class AtomicBoolean:

    def __init__(self):
        """
        this class define a atomic boolean state, default value is false
        it`s just a simple implement of atomic boolean, low performance
        :TODO Optimizing
        """
        self.__lock = RLock()
        self.__bool = False

    def setBoolean(self, value):
        """
        this function set the value of the value
        :param value:
        :type value: bool
        """
        self.__lock.acquire(blocking=1)
        try:
            self.__bool = value
        finally:
            self.__lock.release()

    def getBoolean(self):
        """
        this function get current value of this object
        """
        self.__lock.acquire(blocking=0)
        try:
            return self.__bool
        finally:
            self.__lock.release()
