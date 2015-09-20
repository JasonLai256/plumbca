# -*- coding:utf-8 -*-
"""
    tests.backend
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from faker import Factory

from functools import partial


fake = Factory.create()


def test_redis_backend_basic(rb, fake_manager, fake_coll):
    fake_manager.collmap = {'t1': fake_coll, 't2': fake_coll}
    for name, coll in fake_manager.collmap.items():
        rb.set_collection_index(name, coll)
    for name, coll in fake_manager.collmap.items():
        pair = rb.get_collection_index(name)
        assert pair == [name, fake_coll.__class__.__name__]

    # ---------------------- check get all indexes ----------------------
    rv = rb.get_collection_indexes()
    matching = {'t1': '_t', 't2': '_t'}
    assert rv == matching

    # if name not exist get_collection_index should return None
    pair = rb.get_collection_index('not-exists')
    assert pair is None


def test_redis_backend_metadata(rb, fake_coll):
    taggings = [fake.domain_name() for i in range(10)]
    ts_pairs = [(exp, exp-100) for exp in range(200, 300, 10)]
    first_ts, mid_ts, last_ts = ts_pairs[0][1], ts_pairs[4][1], ts_pairs[-1][1]
    args = ['hello', 'world', 42]

    # ---------------- check metadata set and query operation ----------------
    for i, pair in enumerate(ts_pairs, 1):
        exp, ts = pair
        for t in taggings:
            rb.set_collection_metadata(fake_coll, t, exp, ts, *args)
            assert rb.get_collection_length(fake_coll) == [i]

        rv = rb.query_collection_metadata(fake_coll, t, 0, 1000)
        assert len(rv) == i
        assert rv[i-1] == (ts, [exp] + args)
        rv = rb.query_collection_metadata_tagging(fake_coll, 0, 1000)
        assert len(rv) == i
        assert len(rv[ts]) == len(taggings)
        rv = rb.query_collection_metadata_all(fake_coll, 0, 1000)
        assert len(rv) == i
        assert len(rv[ts]) == len(taggings)
        for info in rv[ts].values():
            assert info == [exp] + args

    # ------------------- check metadata delete operations -------------------
    # delete one tagging info in first ts
    rb.del_collection_metadata_by_range(fake_coll, taggings[0],
                                        first_ts, first_ts)
    rv = rb.query_collection_metadata(fake_coll, t, 0, 1000)
    assert len(rv) == len(ts_pairs)
    rv = rb.query_collection_metadata_tagging(fake_coll, 0, 1000)
    assert len(rv) == len(ts_pairs)
    assert len(rv[first_ts]) == len(taggings) - 1
    assert len(rv[last_ts]) == len(rv[mid_ts]) == len(taggings)
    assert rb.get_collection_length(fake_coll) == [len(taggings)]

    # delete all the taggings in first ts
    for t in taggings[1:]:
        rb.del_collection_metadata_by_range(fake_coll, t, first_ts, first_ts)
    rv = rb.query_collection_metadata(fake_coll, t, 0, 1000)
    assert len(rv) == len(ts_pairs) - 1
    rv = rb.query_collection_metadata_tagging(fake_coll, 0, 1000)
    assert len(rv) == len(ts_pairs) - 1
    assert first_ts not in rv
    assert len(rv[last_ts]) == len(rv[mid_ts]) == len(taggings)
    assert rb.get_collection_length(fake_coll) == [len(taggings) - 1]

    # delete all taggings info in last five ts
    for exp, ts in ts_pairs[-5:]:
        for t in taggings:
            rb.del_collection_metadata_by_range(fake_coll, t, ts, ts)
    rv = rb.query_collection_metadata(fake_coll, t, 0, 1000)
    assert len(rv) == len(ts_pairs) - 6
    rv = rb.query_collection_metadata_tagging(fake_coll, 0, 1000)
    assert len(rv) == len(ts_pairs) - 6
    assert first_ts not in rv and last_ts not in rv
    assert len(rv[mid_ts]) == len(taggings)
    assert rb.get_collection_length(fake_coll) == [len(taggings) - 6]

    # ------------------ check no metadata exists situations ------------------
    # delete a not exists ts
    rb.del_collection_metadata_by_range(fake_coll, taggings[4], 9999, 9999)

    # delete a not exists tagging in mid_ts
    rb.del_collection_metadata_by_range(fake_coll, taggings[4], mid_ts, mid_ts)
    rb.del_collection_metadata_by_range(fake_coll, taggings[4], mid_ts, mid_ts)

    # query a unexists ts
    assert rb.query_collection_metadata(fake_coll, mid_ts, 9999, 9999) is None
    assert rb.query_collection_metadata_tagging(fake_coll, 9999, 9999) is None
    assert rb.query_collection_metadata_all(fake_coll, 9999, 9999) is None


def _add_item(rb, coll, tagging, ts, value):
    rb.set_collection_metadata(coll, tagging, ts+100, ts)
    rb.inc_coll_cache_set(coll, _mk_field(tagging, ts), value)


def _mk_field(tagging, ts):
    field_key = '{}:{}'.format(ts, tagging)
    return field_key


def _assert_inc_coll_cache_size(rb, coll, cache_len, md_len):
    _md_len, _cache_len = rb.get_collection_length(coll, klass="IncreaseCollection")
    assert _md_len == md_len
    assert _cache_len == cache_len


def test_redis_backend_inc_coll(rb, fake_coll):
    tagging, other_tagging = 'day', 'for_diff'
    v = {i: i for i in range(20)}
    timestamps = [100, 110, 120, 130, 140]
    assert_cache_size = partial(_assert_inc_coll_cache_size, rb, fake_coll)

    # ---------------- check the operation of item adding ----------------
    for ts in timestamps:
        _add_item(rb, fake_coll, tagging, ts, v)
    # double adding for checking the logic of duplacate handle
    for ts in timestamps:
        _add_item(rb, fake_coll, tagging, ts, v)
    # adding the other_tagging for the cache size check below
    for ts in timestamps:
        _add_item(rb, fake_coll, other_tagging, ts, v)
    print('Success Adding datas...\n\n\n')

    assert_cache_size(10, 5)

    # ------------------ check the cache data get operations ------------------
    fields = [_mk_field(tagging, ts) for ts in timestamps]
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r == v

    rb.inc_coll_caches_del(fake_coll, *fields)
    rv = rb.inc_coll_caches_get(fake_coll, *fields)
    for r in rv:
        assert r is None

    assert_cache_size(5, 5)

    # if no fields specified
    assert rb.inc_coll_caches_get(fake_coll) == []

    # ---------------- check for the inc_coll_keys_delete ----------------
    assert_cache_size(5, 5)
    rb.delete_collection_keys(fake_coll, klass="IncreaseCollection")
    assert_cache_size(0, 0)
