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
    colls_tagging_index_fmt = 'plumbca:' + dfconf['mark_version'] + ':taggings:index:{name}'
    md_timeline_fmt = 'plumbca:' + dfconf['mark_version'] + ':md:timeline:{name}:{tagging}'
    md_expire_fmt = 'plumbca:' + dfconf['mark_version'] + ':md:expire:{name}:{tagging}'
    cache_item_fmt = 'plumbca:' + dfconf['mark_version'] + ':cache:{name}'

    def __init__(self):
        self.rdb = StrictRedis(host=rdconf['host'], port=rdconf['port'],
                               db=rdconf['db'])
        self.version = dfconf['mark_version']

    def set_collection_indexes(self, manager):
        key = self.colls_index_fmt
        v = {name: instance.__class__.__name__
                 for name, instance in manager.collmap.items()}
        self.rdb.set(key, packb(v))

    def get_collection_indexes(self):
        """
        :ret: return None if no data exists. Normal structure is:
               {collection_name: collection_class_name}
        """
        key = self.colls_index_fmt
        rv = self.rdb.get(key)
        return unpackb(rv) if rv else None

    def get_collection_length(self, coll, tagging, klass=None):
        if not klass:
            klass = coll.__class__.__name__

        rv = []
        if klass == 'IncreaseCollection':
            cache_key = self.cache_item_fmt.format(name=coll.name)
            cache_len = self.rdb.hlen(cache_key)
            rv.append(cache_len)

            tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
            ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
            tl_len = self.rdb.zcard(tl_key)
            ex_len = self.rdb.zcard(ex_key)
            rv.append((tagging, tl_len, ex_len))
            # print('** TL -', self.rdb.zrange(tl_key, 0, -1, withscores=True))
            # print('** EX -', self.rdb.zrange(ex_key, 0, -1, withscores=True))

        return rv

    def set_collection_data_index(self, coll, klass=None):
        if not klass:
            klass = coll.__class__.__name__

        if klass == 'IncreaseCollection':
            key = self.colls_tagging_index_fmt.format(name=coll.name)
            v = {
                'taggings': list(coll.taggings),
                'expire': coll._expire,
                'type': coll.itype
            }
            self.rdb.set(key, packb(v))

    def get_collection_data_index(self, coll):
        """
        :ret: return None if no data exists. Normal structure is:
               {'taggings': taggings, 'expire': expire, 'type': type}
        """
        key = self.colls_tagging_index_fmt.format(name=coll.name)
        rv = self.rdb.get(key)
        return unpackb(rv) if rv else None

    def inc_coll_metadata_set(self, coll, tagging, expts, ts, *args):
        """ Insert data to the metadata structure if timestamp data do not
        exists. Note that the metadata structure include two types, timeline
        and expire.

        :param coll: collection class
        :param tagging: specific tagging string
        :param ts: the timestamp of the data
        :param expts: the expired timestamp of the data
        """
        tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
        # Ensure the item of the specific `ts` whether it's exists or not,
        # If not then update the infomations to the item.
        score = self.rdb.zrangebyscore(tl_key, ts, ts)
        if not score:
            self.rdb.zadd(tl_key, ts, expts)
            mddata = [ts] + list(args)
            ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
            self.rdb.zadd(ex_key, expts, packb(mddata))
            # print('-'*10)
            # print(tagging)
            # print(self.rdb.zrange(tl_key, 0, -1, withscores=True))
            # print(self.rdb.zrange(ex_key, 0, -1, withscores=True))
            # print('+'*10)

    def inc_coll_timeline_metadata_del(self, coll, tagging, *expire_times):
        """ Delete the items of the timeline metadata with the privided
        expire_times argument.
        """
        tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
        expire_times = [int(e) for e in expire_times]
        res = self.rdb.zrem(tl_key, *expire_times)
        return res

    def inc_coll_timeline_metadata_query(self, coll, tagging, start, end):
        """
        :ret: return [] if no data exists. Normal structure is:
                  # value, score
                  [(expire_time1, ts1),
                   (expire_time2, ts2),
                   ...
                   (expire_timeN, tsN)]
        """
        tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
        pairs = self.rdb.zrangebyscore(tl_key, start, end, withscores=True)
        # there the value should be the expired time and the score is the
        # timeline point.
        return [(int(value), score) for value, score in pairs]

    def inc_coll_expire_metadata_query(self, coll, tagging, expired_sentinel):
        """
        :ret: return [] if no data exists. Normal structure is:
                  # value, score
                  [([ts1, *args], expire_time1),
                   ([ts2, *args], expire_time2),
                   ...
                   ([tsN, *args], expire_timeN)]
        """
        ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
        pairs = self.rdb.zrangebyscore(ex_key, 0, expired_sentinel,
                                       withscores=True)
        # there the value should be the collection data and the score is
        # the expired time.
        return [(unpackb(value), score) for value, score in pairs]

    def inc_coll_expire_metadata_del(self, coll, tagging, expired_sentinel):
        ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
        return self.rdb.zremrangebyscore(ex_key, 0, expired_sentinel)

    def inc_coll_cache_set(self, coll, field, value):
        key = self.cache_item_fmt.format(name=coll.name)
        self.rdb.hset(key, field, packb(value))

    def inc_coll_caches_get(self, coll, *fields):
        """
        :ret: return [] if no data exists. Normal structure is:
                [value1, value2, ..., valueN]
        """
        if not fields:
            return []

        key = self.cache_item_fmt.format(name=coll.name)
        rv = self.rdb.hmget(key, *fields)
        # print('inc_coll_caches_get - ', rv)
        # print('inc_coll_caches_get After - ', [unpackb(r) for r in rv if r])
        return [unpackb(r) for r in rv if r]

    def inc_coll_caches_del(self, coll, *fields):
        key = self.cache_item_fmt.format(name=coll.name)
        return self.rdb.hdel(key, *fields)

    def inc_coll_keys_delete(self, coll, taggings):
        """ Danger! This method will erasing all values store in the key that
        should be only use it when you really known what are you doing.

        It is good for the testing to clean up the environment.
        """
        for tagging in taggings:
            tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
            ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
            self.rdb.delete(tl_key, ex_key)
        cache_key = self.cache_item_fmt.format(name=coll.name)
        self.rdb.delete(cache_key)


_backends = {
    'redis': RedisBackend(),
}


def BackendFactory(target):
    return _backends.get(target)
