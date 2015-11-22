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


def test_redis_backend_basic(loop, arb, fake_manager, fake_coll):
    loop.run_until_complete(arb.init_connection())
    fake_manager.collmap = {'t1': fake_coll, 't2': fake_coll}

    async def _routine_a():
        for name, coll in fake_manager.collmap.items():
            await arb.set_collection_index(name, coll)
        for name, coll in fake_manager.collmap.items():
            pair = await arb.get_collection_index(name)
            assert pair == [name, fake_coll.__class__.__name__]
    loop.run_until_complete(_routine_a())

    # ---------------------- check get all indexes ----------------------
    async def _routine_b():
        rv = await arb.get_collection_indexes()
        matching = {'t1': '_t', 't2': '_t'}
        assert rv == matching

        # if name not exist get_collection_index should return None
        pair = await arb.get_collection_index('not-exists')
        assert pair is None
    loop.run_until_complete(_routine_b())


def test_redis_backend_metadata(loop, arb, fake_coll):
    loop.run_until_complete(arb.init_connection())
    taggings = [fake.domain_name() for i in range(10)]
    ts_pairs = [(exp, exp-100) for exp in range(200, 300, 10)]
    first_ts, mid_ts, last_ts = ts_pairs[0][1], ts_pairs[4][1], ts_pairs[-1][1]
    args = ['hello', 'world', 42]

    # ---------------- check metadata set and query operation ----------------
    async def _routine_md_set_query():
        for i, pair in enumerate(ts_pairs, 1):
            exp, ts = pair
            for t in taggings:
                await arb.set_collection_metadata(fake_coll, t, exp, ts, *args)
                assert await arb.get_collection_length(fake_coll) == [i]

            rv = await arb.query_collection_metadata(fake_coll, t, 0, 1000)
            assert len(rv) == i
            assert rv[i-1] == ([exp] + args, ts)
            rv = await arb.query_collection_metadata_tagging(fake_coll, 0, 1000)
            assert len(rv) == i
            assert len(rv[ts]) == len(taggings)
            rv = await arb.query_collection_metadata_all(fake_coll, 0, 1000)
            assert len(rv) == i
            assert len(rv[ts]) == len(taggings)
            for info in rv[ts].values():
                assert info == [exp] + args
    loop.run_until_complete(_routine_md_set_query())

    # ------------------- check metadata delete operations -------------------
    async def _routine_md_del_ope():
        t = taggings[-1]
        # delete one tagging info in first ts
        await arb.del_collection_metadata_by_range(fake_coll, taggings[0],
                                                       first_ts, first_ts)
        rv = await arb.query_collection_metadata(fake_coll, t, 0, 1000)
        assert len(rv) == len(ts_pairs)
        rv = await arb.query_collection_metadata_tagging(fake_coll, 0, 1000)
        assert len(rv) == len(ts_pairs)
        assert len(rv[first_ts]) == len(taggings) - 1
        assert len(rv[last_ts]) == len(rv[mid_ts]) == len(taggings)
        assert await arb.get_collection_length(fake_coll) == [len(taggings)]

        # delete all the taggings in first ts
        for t in taggings[1:]:
            await arb.del_collection_metadata_by_range(fake_coll, t,
                                                       first_ts, first_ts)
        rv = await arb.query_collection_metadata(fake_coll, t, 0, 1000)
        assert len(rv) == len(ts_pairs) - 1
        rv = await arb.query_collection_metadata_tagging(fake_coll, 0, 1000)
        assert len(rv) == len(ts_pairs) - 1
        assert first_ts not in rv
        assert len(rv[last_ts]) == len(rv[mid_ts]) == len(taggings)
        assert await arb.get_collection_length(fake_coll) == [len(taggings) - 1]

        # delete all taggings info in last five ts
        for exp, ts in ts_pairs[-5:]:
            for t in taggings:
                await arb.del_collection_metadata_by_range(fake_coll, t, ts, ts)
        rv = await arb.query_collection_metadata(fake_coll, t, 0, 1000)
        assert len(rv) == len(ts_pairs) - 6
        rv = await arb.query_collection_metadata_tagging(fake_coll, 0, 1000)
        assert len(rv) == len(ts_pairs) - 6
        assert first_ts not in rv and last_ts not in rv
        assert len(rv[mid_ts]) == len(taggings)
        assert await arb.get_collection_length(fake_coll) == [len(taggings) - 6]
    loop.run_until_complete(_routine_md_del_ope())

    # ------------------ check no metadata exists situations ------------------
    async def _routine_md_final_ope():
        # delete a not exists ts
        await arb.del_collection_metadata_by_range(fake_coll, taggings[4],
                                                   9999, 9999)

        # delete a not exists tagging in mid_ts
        await arb.del_collection_metadata_by_range(fake_coll, taggings[4],
                                                   mid_ts, mid_ts)
        await arb.del_collection_metadata_by_range(fake_coll, taggings[4],
                                                   mid_ts, mid_ts)

        # query a unexists ts
        assert await arb.query_collection_metadata(fake_coll, mid_ts, 9999, 9999) is None
        assert await arb.query_collection_metadata_tagging(fake_coll, 9999, 9999) is None
        assert await arb.query_collection_metadata_all(fake_coll, 9999, 9999) is None
    loop.run_until_complete(_routine_md_final_ope())


async def _add_inc_coll_item(rb, coll, tagging, ts, value):
    await rb.set_collection_metadata(coll, tagging, ts+100, ts)
    await rb.inc_coll_cache_set(coll, _mk_inc_coll_field(tagging, ts), value)


def _mk_inc_coll_field(tagging, ts):
    field_key = '{}:{}'.format(ts, tagging)
    return field_key


async def _assert_inc_coll_cache_size(rb, coll, cache_len, md_len):
    _md_len, _cache_len = await rb.get_collection_length(coll, klass="IncreaseCollection")
    assert _md_len == md_len
    assert _cache_len == cache_len


def test_redis_backend_inc_coll(loop, arb, fake_coll):
    loop.run_until_complete(arb.init_connection())
    tagging, other_tagging = 'day', 'for_diff'
    v = {i: i for i in range(20)}
    timestamps = [100, 110, 120, 130, 140]
    assert_cache_size = partial(_assert_inc_coll_cache_size, arb, fake_coll)

    # ---------------- check the operation of item adding ----------------
    async def _routine_add_ope():
        for ts in timestamps:
            await _add_inc_coll_item(arb, fake_coll, tagging, ts, v)
        # double adding for checking the logic of duplacate handle
        for ts in timestamps:
            await _add_inc_coll_item(arb, fake_coll, tagging, ts, v)
        # adding the other_tagging for the cache size check below
        for ts in timestamps:
            await _add_inc_coll_item(arb, fake_coll, other_tagging, ts, v)
        print('Success Adding datas...\n\n\n')

        await assert_cache_size(10, 5)
    loop.run_until_complete(_routine_add_ope())

    # ------------------ check the cache data get operations ------------------
    async def _routine_get_ope():
        fields = [_mk_inc_coll_field(tagging, ts) for ts in timestamps]
        rv = await arb.inc_coll_caches_get(fake_coll, *fields)
        for r in rv:
            assert r == v

        await arb.inc_coll_caches_del(fake_coll, *fields)
        rv = await arb.inc_coll_caches_get(fake_coll, *fields)
        for r in rv:
            assert r is None

        await assert_cache_size(5, 5)

        # if no fields specified
        assert await arb.inc_coll_caches_get(fake_coll) == []
    loop.run_until_complete(_routine_get_ope())

    # ---------------- check for the inc_coll_keys_delete ----------------
    async def _routine_del_ope():
        await assert_cache_size(5, 5)
        await arb.delete_collection_keys(fake_coll, klass="IncreaseCollection")
        await assert_cache_size(0, 0)
    loop.run_until_complete(_routine_del_ope())


def test_redis_backend_unique_count_coll(loop, arb, fake_coll):
    loop.run_until_complete(arb.init_connection())
    items_num = 200
    tagging = 'day'
    v = {fake.uuid4() for i in range(items_num)}
    timestamps = [100, 200, 300]

    # ----------- check the operation of item adding and getting ----------
    async def _routine_add_get_ope():
        for ts in timestamps:
            rv = await arb.uniq_count_coll_cache_set(fake_coll, ts, tagging, v)
            assert rv == items_num
            rv = await arb.uniq_count_coll_cache_set(fake_coll, ts, tagging, v)
            assert rv == 0

        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging, timestamps)
        for item in rv:
            assert item == v
            assert len(item) == items_num

        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging,
                                                 timestamps, count_only=True)
        for count in rv:
            assert count == items_num
    loop.run_until_complete(_routine_add_get_ope())

    # ---------------- check for the operation of deleting ----------------
    async def _routine_del_ope():
        rv = await arb.uniq_count_coll_cache_del(fake_coll, tagging,
                                                 timestamps[0:1])
        assert rv == 1
        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging,
                                                 timestamps[0:1])
        assert rv == [set()]
        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging,
                                                 timestamps[1:])
        for item in rv:
            assert item == v
            assert len(item) == items_num

        # uniq_count_coll_cache_pop 50 items
        rv = await arb.uniq_count_coll_cache_pop(fake_coll, tagging,
                                                 timestamps[1:], 50)
        for item in rv:
            assert len(item) == 50
        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging,
                                                 timestamps[1:])
        for item in rv:
            assert len(item) == items_num - 50

        # delete remain items
        rv = await arb.uniq_count_coll_cache_del(fake_coll, tagging,
                                                 timestamps[1:])
        assert rv == 2
        rv = await arb.uniq_count_coll_cache_get(fake_coll, tagging,
                                                 timestamps)
        assert rv == [set(), set(), set()]
    loop.run_until_complete(_routine_del_ope())


def test_redis_backend_sorted_count_coll(loop, arb, fake_coll):
    loop.run_until_complete(arb.init_connection())
    tagging = 'day'
    v = {fake.uuid4(): i for i in range(200)}
    v2 = [(member, score) for member, score in v.items()]
    v2 = sorted(v2, key=lambda x: x[1])
    timestamps = [100, 200, 300]

    # ----------- check the operation of item adding and getting ----------
    async def _routine_add_get_ope():
        for ts in timestamps:
            rv = await arb.sorted_count_coll_cache_set(fake_coll, ts, tagging, v)
            assert rv == 200

        rv = await arb.sorted_count_coll_cache_get(fake_coll, tagging,
                                                   timestamps)
        for item in rv:
            assert item == v2

        rv = await arb.sorted_count_coll_cache_get(fake_coll, tagging,
                                                   timestamps, topN=100)
        for item in rv:
            assert item == v2[100:]
    loop.run_until_complete(_routine_add_get_ope())

    # ---------------- check for the operation of deleting ----------------
    async def _routine_del_ope():
        rv = await arb.sorted_count_coll_cache_del(fake_coll, tagging,
                                                   timestamps[0:1])
        assert rv == 1
        rv = await arb.sorted_count_coll_cache_get(fake_coll, tagging,
                                                   timestamps[0:1])
        assert rv == [[]]
        rv = await arb.sorted_count_coll_cache_get(fake_coll, tagging,
                                                   timestamps[1:])
        for item in rv:
            assert item == v2

        rv = await arb.sorted_count_coll_cache_del(fake_coll, tagging,
                                                   timestamps[1:])
        assert rv == 2
        rv = await arb.sorted_count_coll_cache_get(fake_coll, tagging,
                                                   timestamps)
        assert rv == [[], [], []]
    loop.run_until_complete(_routine_del_ope())
