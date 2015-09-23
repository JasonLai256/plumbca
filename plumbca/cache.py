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

    def __init__(self):
        self.collmap = {}
        self.info = {}
        self.bk = BackendFactory(DefaultConf['backend'])

    def get_collection(self, name):
        if name not in self.collmap:
            actlog.info("Collection %s not exists.", name)
            return

        return self.collmap[name]

    def ensure_collection(self, name, ctype, expire, **kwargs):
        rv = self.bk.get_collection_index(name)

        if name not in self.collmap and not rv:
            self.collmap[name] = globals()[ctype](name, expire=expire, **kwargs)
            self.bk.set_collection_index(name, self.collmap[name])
            actlog.info("Ensure collection - not exists in plumbca and redis, "
                        "create it, `%s`.", self.collmap[name])

        elif name not in self.collmap and rv:
            rv_name, rv_instance_name = rv
            assert name == rv_name
            assert rv_instance_name == globals()[ctype].__class__.__name__
            self.collmap[name] = globals()[ctype](name, expire=expire, **kwargs)
            actlog.info("Ensure collection - not exists in plumbca, "
                        "create it, `%s`.", self.collmap[name])

        elif name in self.collmap and not rv:
            self.bk.set_collection_index(name, self.collmap[name])
            actlog.info("Ensure collection - not exists in redis, "
                        "create it, `%s`.", self.collmap[name])

        else:
            actlog.info("Ensure collection already exists, `%s`.",
                        self.collmap[name])

    def info(self):
        pass


CacheCtl = CacheCtl()
