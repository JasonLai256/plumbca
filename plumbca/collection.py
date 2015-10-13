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

        # (ts, info) 2-tuple pair of list
        rv = [(int(item[1]), item[0]) for item in mds]
        rv = sorted(rv, key=lambda x: x[0])
        # return (ts-list, parameter-list) 2-tuple
        return [ts for ts, _ in rv], (info[1:] for _, info in rv)

    def _figure_fetch_exipired_items(self, mds, sentinel, tagging):
        """fetch the expired items of specific tagging.

        To get the expire_time and ts values, must be konwn that show below:
            mds.keys() => timestamps of current handling metadata
            mds[ts] => the specified tagging info
            mds[ts][tagging][0] => the specified tagging expire_time
            mds[ts][tagging][1] => the other tagging parameters

            item[0][tagging][0] => the specified tagging expire_time
            item[0][tagging][1:] => the other tagging parameters
            item[1] => timestamp of current handling metadata

        :param mds: the metadata items query from metadata, structure should be
                    equal to the format of the metadata query that specified __all__.
        """
        # filter the mds of the specifi tagging
        tagging_mds = [(info, ts) for ts, info in mds.items() if tagging in info]

        # figure and sort the expired items
        expire_items = [item for item in tagging_mds
                            if int(item[0][tagging][0]) < sentinel]
        expire_items = sorted(expire_items, key=lambda x: x[1])
        parameters = [item[0][tagging][1:] for item in expire_items]
        tslist = [int(item[1]) for item in expire_items]

        return expire_items, tslist, parameters

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
        rv = self._figure_range_timestamps(stime, etime, tagging)
        if not rv:
            return

        tslist, parameters = rv
        keys = [self.gen_key_name(ts, tagging) for ts in tslist]
        return zip(keys, self.bk.inc_coll_caches_get(self, *keys), parameters)

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
        # figure whole metadatas for all taggings
        mds = self.bk.query_collection_metadata(self, '__all__', 0, sentinel)
        if not mds:
            return

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(mds, sentinel, t, d)
        else:
            yield from self._fetch_expired(mds, sentinel, tagging, d)

    def _fetch_expired(self, mds, sentinel, tagging, d):
        """fetch the expired items of specific tagging.

        To get the expire_time and ts values, must be konwn that show below:
            mds.keys() => timestamps of current handling metadata
            mds[ts] => the specified tagging info
            mds[ts][tagging][0] => the specified tagging expire_time
            mds[ts][tagging][1] => the other tagging parameters

            item[0][tagging][0] => the specified tagging expire_time
            item[0][tagging][1:] => the other tagging parameters
            item[1] => timestamp of current handling metadata
        """
        result = self._figure_fetch_exipired_items(mds, sentinel, tagging)
        expire_items, tslist, parameters = result

        # construct the keys and fetch the values
        keys = [self.gen_key_name(t, tagging) for t in tslist]
        rv = self.bk.inc_coll_caches_get(self, *keys)

        # remove all the expired metadata and the cache items
        if d and rv:
            self.bk.del_collection_metadata_by_items(self, tagging, expire_items)
            self.bk.inc_coll_caches_del(self, *keys)

        return zip(keys, rv, parameters)


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
        rv = self._figure_range_timestamps(stime, etime, tagging)
        if not rv:
            return

        tslist, parameters = rv
        return zip(tslist,
                   self.bk.sorted_count_coll_cache_get(self, tagging, tslist, topN),
                   parameters)

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
        # figure whole metadatas for all taggings
        mds = self.bk.query_collection_metadata(self, '__all__', 0, sentinel)
        if not mds:
            return

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(mds, sentinel, t, d, topN)
        else:
            yield from self._fetch_expired(mds, sentinel, tagging, d, topN)

    def _fetch_expired(self, mds, sentinel, tagging, d, topN):
        result = self._figure_fetch_exipired_items(mds, sentinel, tagging)
        expire_items, tslist, parameters = result
        rv = self.bk.sorted_count_coll_cache_get(self, tagging, tslist, topN)

        if d and rv:
            self.bk.del_collection_metadata_by_items(self, tagging, expire_items)
            self.bk.sorted_count_coll_cache_del(self, tagging, tslist)

        return zip(tslist, rv, parameters)


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
        rv = self._figure_range_timestamps(stime, etime, tagging)
        if not rv:
            return

        tslist, parameters = rv
        return zip(tslist,
                   self.bk.uniq_count_coll_cache_get(self, tagging, tslist),
                   parameters)

    def store(self, ts, tagging, value, abselute_expire=None, expire_from_now=False):
        self.taggings.add(tagging)
        ts, expire = self._figure_ts_and_expire(ts, abselute_expire,
                                                expire_from_now)
        self.bk.set_collection_metadata(self, tagging, expire, ts)
        self.bk.uniq_count_coll_cache_set(self, ts, tagging, value)

    def fetch(self, tagging='__all__', d=True, e=True, expired=None):
        sentinel = self._figure_expired_sentinel(d, e, expired)
        # figure whole metadatas for all taggings
        mds = self.bk.query_collection_metadata(self, '__all__', 0, sentinel)
        if not mds:
            return

        if tagging == '__all__':
            for t in self.taggings:
                yield from self._fetch_expired(mds, sentinel, t, d)
        else:
            yield from self._fetch_expired(mds, sentinel, tagging, d)

    def _fetch_expired(self, mds, sentinel, tagging, d):
        result = self._figure_fetch_exipired_items(mds, sentinel, tagging)
        expire_items, tslist, parameters = result
        rv = self.bk.uniq_count_coll_cache_get(self, tagging, tslist)

        if d and rv:
            self.bk.del_collection_metadata_by_items(self, tagging, expire_items)
            self.bk.uniq_count_coll_cache_del(self, tagging, tslist)

        return zip(tslist, rv, parameters)
