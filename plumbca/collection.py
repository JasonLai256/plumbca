# -*- coding:utf-8 -*-

# Copyright (c) 2015 jason lai
#
# See the file LICENSE for copying permission.

from bisect import insort
from threading import Lock

from config import DefaultConf
from helper import find_ge, find_lt


class Collection(object):

    def __init__(self):
        self.lock = Lock()

    def query(self, stime, etime):
        raise NotImplementedError

    def store(self, ts, value):
        raise NotImplementedError


class IncreseCollection(Collection):
    """Collection for store and cache the dict-like JSON data, and will be sorted
    by tiem-series.
    """

    def __init__(self):
        super().__init__()
        self.matadata = []

    def query(self, stime, etime):
        pass

    def store(self, ts, value):
        pass
