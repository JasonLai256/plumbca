# -*- coding:utf-8 -*-
"""
    plumbca.collections
    ~~~~~~~~~~~~~~~~~~~

    Implements various collection classes.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from bisect import insort
from threading import Lock
import time
import os

from .config import DefaultConf
from .helpers import find_ge, find_lt

import msgpack


class Collection(object):

    def __init__(self, name):
        self.lock = Lock()
        self.name = name

    def query(self, stime, etime):
        """Provide query API with time ranges parameter.
        """
        raise NotImplementedError

    def store(self, ts, tagging, value):
        raise NotImplementedError

    def fetch_expired(self):
        raise NotImplementedError

    def dump(self, fpath):
        raise NotImplementedError

    def load(self, fpath):
        raise NotImplementedError

    def info(self):
        raise NotImplementedError


class IncreseCollection(Collection):
    """Collection for store and cache the dict-like JSON data, and will be sorted
    by tiem-series.
    """

    def __init__(self, name):
        super().__init__(name)
        self._metadata = []
        self.caching = {}
        self.md_lock = Lock()
        self.ca_lock = Lock()
        self._info = {}

    def info(self):
        return self._info

    def dump(self):
        fname = '{}.{}.dump'.format(self.__class__.__name__, self.name)
        fpath = os.path.join(DefaultConf.get('dumpdir'), fname)
        with open(fpath, 'wb') as f:
            _tmp = [
                self.name,
                self._metadata,
                self.caching,
            ]
            f.write(msgpack.packb(_tmp))

    def load(self):
        fname = '{}.{}.dump'.format(self.__class__.__name__, self.name)
        fpath = os.path.join(DefaultConf.get('dumpdir'), fname)
        with open(fpath, 'rb') as f:
            _tmp = msgpack.unpackb(f.read(), encoding='utf-8')
            if _tmp[0] != self.name:
                return
            self._metadata = _tmp[1]
            self.caching = _tmp[2]

    def fetch_expired(self, d=True):
        """Fetch the expired data from the store, there will delete the returned
        items by default.

        :param d: whether delete the returned items.
        """
        rv = []
        indexes = []
        with self.md_lock, self.ca_lock:
            now = time.time()
            for i, mdata in enumerate(self._metadata):
                if now > mdata[-1]:
                    key = self.gen_key_name(mdata[0], mdata[1])
                    item = key.split(',') + [self.caching[key]]
                    rv.append(item)
                    if d:
                        del self.caching[key]
                        indexes.append(i)

            # remove all the expired metadata from the self._metadata and the self.caching
            if d and indexes:
                for index in reversed(indexes):
                    del self._metadata[index]

        return rv

    def query(self, stime, etime):
        if stime > etime:
            return
        start, end = self.ensure_index_range(stime, etime)
        if start == -1:
            return

        rv = []
        for mdata in self._metadata[start:end]:
            key = self.gen_key_name(mdata[0], mdata[1])
            item = key.split(',') + [self.caching[key]]
            rv.append(item)

        return rv

    def ensure_index_range(self, stime, etime):
        try:
            sindex = find_ge(self._metadata, [stime], True)
            eindex = find_lt(self._metadata, [etime], True)
        except ValueError:
            sindex, eindex = -1, -1

        return sindex, eindex + 1

    def store(self, ts, tagging, value, expire=300):
        if not isinstance(value, dict):
            raise ValueError('The IncreseCollection only accept Dict type value.')
        ts = int(ts)
        expire = int(time.time()) + expire
        mdata = [ts, tagging, expire]
        keyname = self.update_matadata(mdata)
        self.update_value(keyname, value)

    def update_value(self, key, value):
        """Using increase method to handle items between value and
        self.caching[key].
        """
        if key in self.caching:
            cache_item = self.caching[key]
        else:
            raise ValueError('The key ({}) not in caching store.'.format(key))

        for k, v in value.items():
            if k in cache_item:
                cache_item[k] += int(v)
            else:
                cache_item[k] = int(v)

    def update_matadata(self, mdata):
        '''The structure of the _metadata is::

        tagging: {
            (ts1, expire_time),
            (ts2, expire_time),
            ...
            (tsN, expire_time)
        }
        '''
        keyname = self.gen_key_name(mdata[0], mdata[1])
        if not self.metadata_exists(mdata[0], mdata[1]):
            insort(self._metadata, mdata)
            self.caching[keyname] = {}
        return keyname

    def metadata_exists(self, ts, tagging, ret_index=False):
        """checking the part of metadata - [ts, tagging] - is existing in the
        self._metadata.
        """
        exists = False
        if self._metadata:
            # locate the index of tmp_data in self._metadata
            try:
                tmp_data = [ts, tagging]
                index = find_lt(self._metadata, tmp_data, True) + 1
                if index == len(self._metadata):
                    # ensured tmp_data not exists
                    raise ValueError
            except ValueError:
                # Not found the mdata that less than the tmp_data, assign index to 0.
                index = 0

            mdata = self._metadata[index]
            if mdata[:2] == tmp_data:
                exists = True

        return index if ret_index else exists

    def gen_key_name(self, ts, tagging):
        return '{},{}'.format(str(ts), tagging)
