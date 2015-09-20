# -*- coding:utf-8 -*-
"""
    plumbca.backend
    ~~~~~~~~~~~~~~~

    Implements various backend classes.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from redis import StrictRedis

from .config import DefaultConf as dfconf, RedisConf as rdconf
from .helpers import packb, unpackb


class RedisBackend:

    colls_index_fmt = 'plumbca:' + dfconf['mark_version'] + ':collections:index'
    metadata_fmt = 'plumbca:' + dfconf['mark_version'] + ':md:timeline:{name}'
    inc_coll_cache_item_fmt = 'plumbca:' + dfconf['mark_version'] + ':cache:{name}'

    def __init__(self):
        self.rdb = StrictRedis(host=rdconf['host'], port=rdconf['port'],
                               db=rdconf['db'])
        self.version = dfconf['mark_version']

    def set_collection_index(self, name, instance):
        """ Set the collection info of instance to the backend.
        """
        key = self.colls_index_fmt
        v = instance.__class__.__name__
        self.rdb.hset(key, name, packb(v))

    def get_collection_index(self, name):
        """ Get the collection info from backend by name.
        """
        key = self.colls_index_fmt
        rv = self.rdb.hget(key, name)
        return [name, unpackb(rv)] if rv else None

    def get_collection_indexes(self):
        """ Get all of the collections info from backend.
        """
        key = self.colls_index_fmt
        rv = self.rdb.hgetall(key)
        if rv:
            return {name.decode("utf-8"): unpackb(info)
                        for name, info in rv.items()}

    def get_collection_length(self, coll, klass=''):
        if not klass:
            klass = coll.__class__.__name__

        rv = []
        md_key = self.metadata_fmt.format(name=coll.name)
        md_len = self.rdb.zcard(md_key)
        rv.append(md_len)
        # print('** TL -', self.rdb.zrange(md_key, 0, -1, withscores=True))

        if klass == 'IncreaseCollection':
            cache_key = self.inc_coll_cache_item_fmt.format(name=coll.name)
            cache_len = self.rdb.hlen(cache_key)
            # notice that the cache_len is the length of all the items in cache_key
            rv.append(cache_len)

        return rv

    def set_collection_metadata(self, coll, tagging, expts, ts, *args):
        """ Insert data to the metadata structure if timestamp data do not
        exists. Note that the metadata structure include two types, timeline
        and expire.

        :param coll: collection class
        :param tagging: specific tagging string
        :param ts: the timestamp of the data
        :param expts: the expired timestamp of the data
        """
        md_key = self.metadata_fmt.format(name=coll.name)
        # Ensure the item of the specific `ts` whether it's exists or not,
        element = self.rdb.zrangebyscore(md_key, ts, ts)
        # print('element - ', element)

        if element:
            info = unpackb(element[0])
            if tagging in info:
                # the tagging info already exists then do nothings
                return
            info[tagging] = [expts] + list(args)
            # remove the md_key and update new value atomically
            p = self.rdb.pipeline()
            p.zremrangebyscore(md_key, ts, ts)
            p.zadd(md_key, ts, packb(info))
            # print('if - ', info)
            p.execute()

        else:
            info = {tagging: [expts] + list(args)}
            # print('else - ', info)
            self.rdb.zadd(md_key, ts, packb(info))
        # print('-'*10)
        # print(tagging)
        # print(self.rdb.zrange(md_key, 0, -1, withscores=True))
        # print('+'*10)

    def del_collection_metadata_by_range(self, coll, tagging, start, end):
        """ Delete the items of the timeline metadata with the privided
        start time and end time arguments.
        """
        md_key = self.metadata_fmt.format(name=coll.name)
        elements = self.rdb.zrangebyscore(md_key, start, end, withscores=True)
        if not elements:
            return

        del_info_todos = []
        del_key_todos = []

        # searching what elements need te be handle
        for info, ts in elements:
            info = unpackb(info)
            if tagging not in info:
                continue
            info.pop(tagging)
            # when info has not element then should remove the ts key,
            # otherwise should update new value to it.
            if info:
                del_info_todos.append((info, ts))
            else:
                del_key_todos.append(ts)

        # doing the operations that update keys one by one atomically
        for info, ts in del_info_todos:
            p = self.rdb.pipeline()
            p.zremrangebyscore(md_key, ts, ts)
            p.zadd(md_key, ts, packb(info))
            p.execute()

        # doing the operations that remove all keys atomically
        p = self.rdb.pipeline()
        for ts in del_key_todos:
            p.zremrangebyscore(md_key, ts, ts)
        p.execute()

    def query_collection_metadata(self, coll, tagging, start, end):
        return self._query_collection_metadata(coll, start, end, tagging)

    def query_collection_metadata_tagging(self, coll, start, end):
        return self._query_collection_metadata(coll, start, end, '__taggings__')

    def query_collection_metadata_all(self, coll, start, end):
        return self._query_collection_metadata(coll, start, end, '__all__')

    def _query_collection_metadata(self, coll, start, end, tagging=''):
        """ Do the real operations for query metadata from the redis.

        :ret: return None if no data exists.
              If tagging is specified '__taggings__', return value only contain the taggings:
                  # ts: all_tagging
                  {
                      ts1: [tagging1, tagging2, ..., targetN],
                      ts2: [tagging1, tagging2, ..., targetN],
                      ...
                      tsN: [tagging1, tagging2, ..., targetN],
                  }
              If tagging is specified '__all__', return value include all the info:
                  # ts: all_tagging_info
                  {
                      ts1: {tagging1: info1, tagging2: info2, ...},
                      ts2: {tagging1: info1, tagging2: info2, ...},
                      ...
                      tsN: {tagging1: info1, tagging2: info2, ...},
                  }
              If tagging is specified other, return value is the info that match the tagging:
                  # value, score
                  [(ts1, info1), (ts2, info2), ... (tsN, infoN)]
        """
        md_key = self.metadata_fmt.format(name=coll.name)
        elements = self.rdb.zrangebyscore(md_key, start, end, withscores=True)
        if not elements:
            return

        if tagging == '__taggings__' or tagging == '__all__':
            rv = {}
        else:
            rv = []

        # searching what elements should be match
        for info, ts in elements:
            info = unpackb(info)
            # print(tagging, info)
            if tagging == '__taggings__':
                rv[ts] = list(info.keys())
            elif tagging == '__all__':
                rv[ts] = info
            elif tagging in info:
                rv.append((ts, info[tagging]))

        return rv

    def inc_coll_cache_set(self, coll, field, value):
        key = self.inc_coll_cache_item_fmt.format(name=coll.name)
        self.rdb.hset(key, field, packb(value))

    def inc_coll_caches_get(self, coll, *fields):
        """
        :ret: return [] if no data exists. Normal structure is:
                [value1, value2, ..., valueN]
        """
        if not fields:
            return []

        key = self.inc_coll_cache_item_fmt.format(name=coll.name)
        rv = self.rdb.hmget(key, *fields)
        # print('inc_coll_caches_get - ', rv)
        # print('inc_coll_caches_get After - ', [unpackb(r) for r in rv if r])
        return [unpackb(r) for r in rv if r]

    def inc_coll_caches_del(self, coll, *fields):
        key = self.inc_coll_cache_item_fmt.format(name=coll.name)
        return self.rdb.hdel(key, *fields)

    def inc_coll_keys_delete(self, coll, taggings):
        """ Danger! This method will erasing all values store in the key that
        should be only use it when you really known what are you doing.

        It is good for the testing to clean up the environment.
        """
        for tagging in taggings:
            md_key = self.metadata_fmt.format(name=coll.name, tagging=tagging)
            self.rdb.delete(md_key)
        cache_key = self.inc_coll_cache_item_fmt.format(name=coll.name)
        self.rdb.delete(cache_key)


_backends = {
    'redis': RedisBackend(),
}


def BackendFactory(target):
    return _backends.get(target)
