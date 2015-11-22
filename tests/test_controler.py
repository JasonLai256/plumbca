# -*- coding:utf-8 -*-
"""
    tests.cache
    ~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""


def test_cachectl_basic(loop, arb, cachectl, coll_list):
    loop.run_until_complete(arb.init_connection())

    async def _routine_ope():
        for cname in coll_list:
            await cachectl.ensure_collection(cname, 'IncreaseCollection', 3600)
            await cachectl.ensure_collection(cname, 'IncreaseCollection', 3600)
        assert len(cachectl.collmap) == 5

        tagging1, tagging2 = 'admin', 'fortest'

        # ------------------ test for store operation ------------------
        for cname in coll_list:
            coll = cachectl.get_collection(cname)
            for i in range(10):
                await coll.store(i, tagging1, {'bar': 1})
                await coll.store(i, tagging2, {'bar': 1})

            _md_len, _cache_len = await arb.get_collection_length(coll,
                                                                  klass="IncreaseCollection")
            assert _md_len == 10
            assert _cache_len == 20

        assert cachectl.get_collection('not_exists') is None
    loop.run_until_complete(_routine_ope())
