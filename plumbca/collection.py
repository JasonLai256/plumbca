# -*- coding:utf-8 -*-
"""
    plumbca.collections
    ~~~~~~~~~~~~~~~~~~~

    Implements various collection classes.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from threading import Lock
import time

from .config import DefaultConf
from .backend import BackendFactory


class Collection(object):

    def __init__(self, name):
        self.lock = Lock()
        self.name = name
        self._metadata = {}

        self.bk = BackendFactory(DefaultConf['backend'])

        # timestamp of datetime(2075, 8, 18, 13, 55, 33)
        self.end_ts = 3333333333

    def _figure_ts_and_expire(self, ts, abselute_expire, expire_from_now):
        ts = int(ts)
        if abselute_expire:
            expire = int(abselute_expire)
        elif expire_from_now:
            expire = int(time.time()) + self._expire
        else:
            expire = ts + self._expire
        return ts, expire

    def _figure_expired_sentinel(self, d=True, e=True, expired=None):
        if e and expired:
            rv = expired
        elif e:
            rv = int(time.time())
        else:
            rv = self.end_ts
        return rv

    def _figure_range_timestamps(self, stime, etime, tagging):
        if stime > etime:
            return

        mds = self.bk.query_collection_metadata(self, tagging, stime, etime)
        if not mds:
            return

        return sorted(int(item[1]) for item in mds)

    def query(self, stime, etime, tagging):
        """Provide query API with time ranges parameter.
        """
        raise NotImplementedError

    def store(self, ts, tagging, value):
        raise NotImplementedError

    def fetch(self):
        raise NotImplementedError

    def dump(self, fpath):
        raise NotImplementedError

    def load(self, fpath):
        raise NotImplementedError

    def info(self):
        raise NotImplementedError


class IncreaseCollection(Collection):
    """Collection for store and cache the dict-like JSON data, and will be sorted
    by time-series.
    """

    opes = {
        'inc': lambda x, y: x + y,
        'avg': lambda x, y: (x + y) / 2,
        'max': lambda x, y: max(x, y),
        'min': lambda x, y: min(x, y),
    }

    def __init__(self, name, itype='inc', expire=3600):
        super().__init__(name)
        self.caching = {}
        self.taggings = set()
        # the expire should be unchangable in the instance live time
        self._expire = int(expire)
        self.itype = itype
        self.ifunc = self.opes[itype]

    def __repl__(self):
        return '<{} - {}> . {}'.format(self.__class__.__name__,
                                       self.name, self.itype)

    def __str__(self):
        return self.__repl__()

    def gen_key_name(self, ts, tagging):
        return '{}:{}'.format(str(ts), tagging)

    def query(self, stime, etime, tagging):
        tslist = self._figure_range_timestamps(stime, etime, tagging)
        if not tslist:
            return

        keys = [self.gen_key_name(ts, tagging) for ts in tslist]
        return zip(keys, self.bk.inc_coll_caches_get(self, *keys))

    def store(self, ts, tagging, value, abselute_expire=None, expire_from_now=False):
        if not isinstance(value, dict):
            raise ValueError('The IncreaseCollection only accept Dict type value.')
        self.taggings.add(tagging)
        ts, expire = self._figure_ts_and_expire(ts, abselute_expire,
                                                expire_from_now)
        keyname = self.gen_key_name(ts, tagging)
        self.bk.set_collection_metadata(self, tagging, expire, ts)
        self._update_value(keyname, value)

    def _update_value(self, keyname, inc_value):
        """Using increase method to handle items between value and
        self.caching[key].
        """
        base = self.bk.inc_coll_caches_get(self, keyname)
        # print('Store Before `{}` - Origin: {}, Inc: {}'.format(keyname, base,
        #                                                        inc_value))
        if base:
            base = base[0]
            for k, v in inc_value.items():
                if k in base:
                    base[k] = self.ifunc(base[k], int(v))
                else:
                    base[k] = int(v)
        else:
            base = inc_value

        # print('Store After `{}` - {}'.format(keyname, base))
        self.bk.inc_coll_cache_set(self, keyname, base)

    def fetch(self, tagging='__all__', d=True, e=True, expired=None):
        """Fetch the expired data from the store, there will delete the returned
        items by default.

        :param tagging: specific tagging value for the collection
        :param d: whether delete the returned items.
        :param e: only fetch expired data if True.
        :param expired: if `e` specify to True and expired can to specify
                        the specific expire time.
        """
        sentinel = self._figure_expired_sentinel(d, e, expired)

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(sentinel, t, d)
        else:
            yield from self._fetch_expired(sentinel, tagging, d)

    def _fetch_expired(self, sentinel, tagging, d):
        mds = self.bk.query_collection_metadata(self, tagging, 0, sentinel,
                                                ret_whold=True)
        if not mds:
            return []

        # get the expire_time and ts values.
        # Must be konwn that show below:
        #     item[0][tagging][0] => the specified tagging expire_time
        #     item[1] => timestamp of current handling metadata
        expire_items = [item for item in mds
                            if int(item[0][tagging][0]) < sentinel]
        tslist = [int(item[1]) for item in expire_items]
        keys = [self.gen_key_name(t, tagging) for t in tslist]
        rv = self.bk.inc_coll_caches_get(self, *keys)

        # remove all the expired metadata and the cache items
        if d and rv:
            self.bk.del_collection_metadata_by_items(self, tagging, expire_items)
            self.bk.inc_coll_caches_del(self, *keys)

        return zip(keys, rv)


class SortedCountCollection(Collection):

    def __init__(self, name, expire=3600):
        super().__init__(name)
        self.caching = {}
        self.taggings = set()
        # the expire should be unchangable in the instance live time
        self._expire = int(expire)

    def __repl__(self):
        return '<{} - {}>'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return self.__repl__()

    def query(self, stime, etime, tagging, topN=None):
        tslist = self._figure_range_timestamps(stime, etime, tagging)
        if not tslist:
            return

        return zip(tslist, self.bk.sorted_count_coll_cache_get(self, tagging, tslist, topN))

    def store(self, ts, tagging, value, abselute_expire=None, expire_from_now=False):
        if not isinstance(value, dict):
            raise ValueError('The IncreaseCollection only accept Dict type value.')

        self.taggings.add(tagging)
        ts, expire = self._figure_ts_and_expire(ts, abselute_expire,
                                                expire_from_now)
        self.bk.set_collection_metadata(self, tagging, expire, ts)
        self.bk.sorted_count_coll_cache_set(self, ts, tagging, value)

    def fetch(self, tagging='__all__', d=True, e=True, expired=None, topN=None):
        sentinel = self._figure_expired_sentinel(d, e, expired)

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(sentinel, t, d, topN)
        else:
            yield from self._fetch_expired(sentinel, tagging, d, topN)

    def _fetch_expired(self, sentinel, tagging, d, topN):
        mds = self.bk.query_collection_metadata(self, tagging, 0, sentinel,
                                                ret_whold=True)
        if not mds:
            return []

        expire_items = [item for item in mds
                            if int(item[0][tagging][0]) < sentinel]
        tslist = [int(item[1]) for item in expire_items]
        rv = self.bk.sorted_count_coll_cache_get(self, tagging, tslist, topN)

        if d and rv:
            self.bk.sorted_count_coll_cache_del(self, tagging, tslist)

        return zip(tslist, rv)


class UniqueCountCollection(Collection):

    def __init__(self, name, expire=3600):
        super().__init__(name)
        self.caching = {}
        self.taggings = set()
        # the expire should be unchangable in the instance live time
        self._expire = int(expire)

    def __repl__(self):
        return '<{} - {}>'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return self.__repl__()

    def query(self, stime, etime, tagging):
        tslist = self._figure_range_timestamps(stime, etime, tagging)
        if not tslist:
            return

        return zip(tslist, self.bk.uniq_count_coll_cache_get(self, tagging, tslist))

    def store(self, ts, tagging, value, abselute_expire=None, expire_from_now=False):
        self.taggings.add(tagging)
        ts, expire = self._figure_ts_and_expire(ts, abselute_expire,
                                                expire_from_now)
        self.bk.set_collection_metadata(self, tagging, expire, ts)
        self.bk.uniq_count_coll_cache_set(self, ts, tagging, value)

    def fetch(self, tagging='__all__', d=True, e=True, expired=None):
        sentinel = self._figure_expired_sentinel(d, e, expired)

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(sentinel, t, d)
        else:
            yield from self._fetch_expired(sentinel, tagging, d)

    def _fetch_expired(self, sentinel, tagging, d):
        mds = self.bk.query_collection_metadata(self, tagging, 0, sentinel,
                                                ret_whold=True)
        if not mds:
            return []

        expire_items = [item for item in mds
                            if int(item[0][tagging][0]) < sentinel]
        tslist = [int(item[1]) for item in expire_items]
        rv = self.bk.uniq_count_coll_cache_get(self, tagging, tslist)

        if d and rv:
            self.bk.uniq_count_coll_cache_del(self, tagging, tslist)

        return zip(tslist, rv)
