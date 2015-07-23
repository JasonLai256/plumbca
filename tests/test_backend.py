# -*- coding:utf-8 -*-
"""
    tests.backend
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from msgpack import packb, unpackb


def test_redis_backend_basic(rb, fake_manager, fake_coll):
    fake_manager.collmap = {'t1': fake_coll, 't2': fake_coll}
    rb.set_collection_indexes(fake_manager)
    rv = rb.get_collection_indexes()
    matching = {'t1': '_t', 't2': '_t'}
    assert rv == unpackb(packb(matching))

    fake_coll.taggings = ['minute', 'code_minute', 'hour', 'rtmp_minute']
    rb.set_collection_data_index(fake_coll)
    rv = rb.get_collection_data_index(fake_coll)
    matching = {'taggings': fake_coll.taggings,
                'expire': fake_coll.expire,
                'type': fake_coll.itype}
    assert rv == unpackb(packb(matching))


def _add_item(rb, coll, tagging, expire, ts, value):
    rb.inc_coll_metadata_set(coll, tagging, expire, ts)
    rb.inc_coll_cache_set(coll, _mk_field(tagging, ts), packb(value))


def _mk_field(tagging, ts):
    field_key = '{}:{}'.format(ts, tagging)
    return field_key


def test_redis_backend_inc_coll(rb, fake_coll):
    tagging = 'day'
    v = unpackb(packb({i: i for i in range(20)}))
    pairs = [
        (200, 100), (210, 110), (220, 120),
        (230, 130), (240, 140),
    ]
    for expire, ts in pairs:
        _add_item(rb, fake_coll, tagging, expire, ts, v)
    for expire, ts in pairs:
        _add_item(rb, fake_coll, tagging, expire, ts, v)

    # @@ check the cache data
    fields = [_mk_field(tagging, ts) for expire, ts in pairs]
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r == v

    rb.inc_coll_caches_del(fake_coll, *fields)
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r is None

    # @@ check the timeline metadata data
    rv = rb.inc_coll_timeline_metadata_query(fake_coll, tagging, 100, 140)
    assert len(rv) == 5
    for r, p in zip(rv, pairs):
        # for expire
        assert int(r[0]) == int(p[0])
        # for ts
        assert int(r[1]) == int(p[1])
    print('Success Adding datas...\n\n\n')

    # fetch the ts elements that use the inc_coll_expire_metadata_query
    exps = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 100 * 100)
    exps = [ex[1] for ex in exps]    # get the expire_time values.
    rb.inc_coll_timeline_metadata_del(fake_coll, tagging, *exps)
    rv = rb.inc_coll_timeline_metadata_query(fake_coll, tagging, 100, 140)
    for r in rv:
        assert not r

    # @@ check the expire metadata data
    rv = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 221)
    assert len(rv) == 3
    rv = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 300)
    assert len(rv) == 5

    rb.inc_coll_expire_metadata_del(fake_coll, tagging, 221)
    rv = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 221)
    assert len(rv) == 0
    rv = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 300)
    assert len(rv) == 2

    rb.inc_coll_expire_metadata_del(fake_coll, tagging, 300)
    rv = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 300)
    assert len(rv) == 0
