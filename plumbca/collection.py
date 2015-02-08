# -*- coding:utf-8 -*-

# Copyright (c) 2015 jason lai
#
# See the file LICENSE for copying permission.

from threading import Lock
from config import DefaultConf


class Collection(object):

    def __init__(self):
        self.lock = Lock()

    def query(self, stime, etime):
        raise NotImplementedError

    def store(self, ts, value):
        raise NotImplementedError


class IncreseCollection(Collection):

    def __init__(self):
        super().__init__()

    def query(self, stime, etime):
        pass

    def store(self, ts, value):
        pass
