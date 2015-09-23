# -*- coding:utf-8 -*-
"""
    tests.cache
    ~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""


def test_cachectl_basic(rb, cachectl, coll_list):
    for cname in coll_list:
        cachectl.ensure_collection(cname, 'IncreaseCollection', 3600)
        cachectl.ensure_collection(cname, 'IncreaseCollection', 3600)
    assert len(cachectl.collmap) == 5

    tagging1, tagging2 = 'admin', 'fortest'

    # ------------------ test for store operation ------------------
    for cname in coll_list:
        coll = cachectl.get_collection(cname)
        for i in range(100):
            coll.store(i, tagging1, {'bar': 1})
            coll.store(i, tagging2, {'bar': 1})

        _md_len, _cache_len = rb.get_collection_length(coll,
                                                       klass="IncreaseCollection")
        assert _md_len == 100
        assert _cache_len == 200

    assert cachectl.get_collection('not_exists') is None
