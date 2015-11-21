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
