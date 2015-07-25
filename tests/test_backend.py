# -*- coding:utf-8 -*-
"""
    tests.backend
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from functools import partial


def test_redis_backend_basic(rb, fake_manager, fake_coll):
    fake_manager.collmap = {'t1': fake_coll, 't2': fake_coll}
    rb.set_collection_indexes(fake_manager)
    rv = rb.get_collection_indexes()
    matching = {'t1': '_t', 't2': '_t'}
    assert rv == matching

    fake_coll.taggings = ['minute', 'code_minute', 'hour', 'rtmp_minute']
    rb.set_collection_data_index(fake_coll, klass="IncreaseCollection")
    rv = rb.get_collection_data_index(fake_coll)
    matching = {'taggings': fake_coll.taggings,
                'expire': fake_coll._expire,
                'type': fake_coll.itype}
    assert rv == matching


def _add_item(rb, coll, tagging, expire, ts, value):
    rb.inc_coll_metadata_set(coll, tagging, expire, ts)
    rb.inc_coll_cache_set(coll, _mk_field(tagging, ts), value)


def _mk_field(tagging, ts):
    field_key = '{}:{}'.format(ts, tagging)
    return field_key


def _assert_inc_coll_cache_size(rb, coll, tagging, cache_len, tl_len, ex_len):
    rv = rb.get_collection_length(coll, tagging, klass="IncreaseCollection")
    assert cache_len == rv[0]
    tagging_info = rv[1]
    assert tagging == tagging_info[0]
    assert tl_len == tagging_info[1]
    assert ex_len == tagging_info[2]


def test_redis_backend_inc_coll(rb, fake_coll):
    tagging, other_tagging = 'day', 'for_diff'
    v = {i: i for i in range(20)}
    pairs = [
        (200, 100), (210, 110), (220, 120),
        (230, 130), (240, 140),
    ]
    assert_cache_size = partial(_assert_inc_coll_cache_size, rb, fake_coll)

    # ---------------- check the operation of item adding ----------------
    for expire, ts in pairs:
        _add_item(rb, fake_coll, tagging, expire, ts, v)
    # double adding for checking the logic of duplacate handle
    for expire, ts in pairs:
        _add_item(rb, fake_coll, tagging, expire, ts, v)
    # adding the other_tagging for the cache size check below
    for expire, ts in pairs:
        _add_item(rb, fake_coll, other_tagging, expire, ts, v)
    print('Success Adding datas...\n\n\n')

    assert_cache_size(tagging, 10, 5, 5)
    assert_cache_size(other_tagging, 10, 5, 5)

    # ---------------------- check the cache data ----------------------
    fields = [_mk_field(tagging, ts) for expire, ts in pairs]
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r == v

    rb.inc_coll_caches_del(fake_coll, *fields)
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r is None

    assert_cache_size(tagging, 5, 5, 5)

    # ------------------ check the timeline metadata data ------------------
    rv = rb.inc_coll_timeline_metadata_query(fake_coll, tagging, 100, 140)
    assert len(rv) == 5
    for r, p in zip(rv, pairs):
        # for expire
        assert int(r[0]) == int(p[0])
        # for ts
        assert int(r[1]) == int(p[1])

    # fetch the ts elements that use the inc_coll_expire_metadata_query
    exps = rb.inc_coll_expire_metadata_query(fake_coll, tagging, 100 * 100)
    exps = [ex[1] for ex in exps]    # get the expire_time values.
    rb.inc_coll_timeline_metadata_del(fake_coll, tagging, *exps)
    rv = rb.inc_coll_timeline_metadata_query(fake_coll, tagging, 100, 140)
    for r in rv:
        assert not r

    assert_cache_size(tagging, 5, 0, 5)

    # ------------------ check the expire metadata data ------------------
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

    assert_cache_size(tagging, 5, 0, 0)

    # ---------------- check for the inc_coll_keys_delete ----------------
    assert_cache_size(other_tagging, 5, 5, 5)
    rb.inc_coll_keys_delete(fake_coll, [other_tagging])
    assert_cache_size(other_tagging, 0, 0, 0)
