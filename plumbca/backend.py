# -*- coding:utf-8 -*-
"""
    plumbca.backend
    ~~~~~~~~~~~~~~~

    Implements various backend classes.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from redis import StrictRedis
import msgpack

from .config import DefaultConf as dfconf, RedisConf as rdconf


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
        self.rdb.set(key, msgpack.packb(v))

    def get_collection_indexes(self):
        key = self.colls_index_fmt
        rv = self.rdb.get(key)
        return msgpack.unpackb(rv) if rv else None

    def set_collection_data_index(self, coll):
        key = self.colls_tagging_index_fmt.format(name=coll.name)
        v = {
            'taggings': list(coll.taggings),
            'expire': coll.expire,
            'type': coll.itype
        }
        self.rdb.set(key, msgpack.packb(v))

    def get_collection_data_index(self, coll):
        key = self.colls_tagging_index_fmt.format(name=coll.name)
        rv = self.rdb.get(key)
        return msgpack.unpackb(rv) if rv else None

    def inc_coll_metadata_set(self, coll, tagging, expts, ts, *args):
        """ Insert data to the metadata structure if timestamp data do not
        exists. Note that the metadata structure include two types, timeline
        and expire.

        :param coll: collection class
        :param tagging: specific tagging string
        :param ts: the timestamp of the data
        :param expts: the expired timestamp of the data
        """
        tl_key = self.md_timeline_fmt.format(coll.name, tagging)
        # Ensure the item of the specific `ts` whether it's exists or not,
        # If not then update the infomations to the item.
        score = self.rdb.zrangebyscore(tl_key, ts, ts)
        if not score:
            self.rdb.zadd(tl_key, ts, expts)
            mddata = [ts] + list(args)
            ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
            self.rdb.zadd(ex_key, expts, msgpack.packb(mddata))

    def inc_coll_timeline_metadata_del(self, coll, tagging, *expire_times):
        """ Delete the items of the timeline metadata with the privided
        expire_times argument.
        """
        tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
        return self.rdb.zrem(tl_key, *expire_times)

    def inc_coll_timeline_metadata_query(self, coll, tagging, start, end):
        tl_key = self.md_timeline_fmt.format(name=coll.name, tagging=tagging)
        pairs = self.rdb.zrangebyscore(tl_key, start, end)
        # there the value should be the expired time and the score is the
        # timeline point.
        return [(int(value), score)for value, score in pairs]

    def inc_coll_expire_metadata_query(self, coll, tagging, expired_sentinel):
        ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
        pairs = self.rdb.zrangebyscore(ex_key, 0, expired_sentinel)
        # there the value should be the collection data and the score is
        # the expired time.
        return [(msgpack.unpackb(value), score)for value, score in pairs]

    def inc_coll_expire_metadata_del(self, coll, tagging, expired_sentinel):
        ex_key = self.md_expire_fmt.format(name=coll.name, tagging=tagging)
        return self.rdb.zremrangebyscore(ex_key, 0, expired_sentinel)

    def inc_coll_cache_set(self, coll, field, value):
        key = self.cache_item_fmt.format(name=coll.name)
        self.rdb.hget(key, field, value)

    def inc_coll_caches_get(self, coll, *fields):
        key = self.cache_item_fmt.format(name=coll.name)
        return self.rdb.hmget(key, *fields)

    def inc_coll_caches_del(self, coll, *fields):
        key = self.cache_item_fmt.format(name=coll.name)
        return self.rdb.hdel(key, *fields)


rbackend = RedisBackend()
