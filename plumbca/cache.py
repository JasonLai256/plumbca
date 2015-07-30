# -*- coding:utf-8 -*-
"""
    plumbca.cache
    ~~~~~~~~~~~~~

    CacheHandler for the collections control.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import logging
import re
import os

from .config import DefaultConf
from .collection import IncreaseCollection
from .backend import BackendFactory


actlog = logging.getLogger('activity')
err_logger = logging.getLogger('errors')


class CacheCtl(object):

    def __init__(self, try_restore=True):
        self.collmap = {}
        self.info = {}
        self.bk = BackendFactory(DefaultConf['backend'])
        if try_restore:
            self.restore_collections()

    def restore_collections(self):
        indexes = self.bk.get_collection_indexes()
        if indexes:
            self.collmap = {}
            print(indexes)
            for name, klass in indexes.items():
                actlog.info("Start to restore the collection - %s (%s).", name, klass)
                self.collmap[name] = globals()[klass](name)
                self.collmap[name].load()
                actlog.info("Successful restore the `%s` collection.", name)

    def dump_collections(self):
        self.bk.set_collection_indexes(self)
        for collection in self.collmap.values():
            actlog.info("Start to dump `%s` collection.", collection)
            collection.dump()
            actlog.info("Successful dumped `%s` collection.", collection)

    def get_collection(self, name):
        if name not in self.collmap:
            actlog.info("Collection %s not exists.", name)
            return

        return self.collmap[name]

    def ensure_collection(self, name, ctype, **kwargs):
        if name not in self.collmap:
            self.collmap[name] = globals()[ctype](name, **kwargs)
            self.bk.set_collection_indexes(self)
            actlog.info("Ensure collection not exists, create it, `%s`.",
                        self.collmap[name])
        else:
            actlog.info("Ensure collection already exists, `%s`.",
                        self.collmap[name])

    def info(self):
        pass


CacheCtl = CacheCtl()
