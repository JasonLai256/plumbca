# -*- coding:utf-8 -*-
"""
    tests.cache
    ~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest
import os

from plumbca import cache


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
        assert str(coll) == '<IncreaseCollection - {}> . inc'.format(cname)
        rv = rb.get_collection_length(coll, tagging1)
        assert rv[0] == 200
        assert rv[1][1] == rv[1][2] == 100
        rv = rb.get_collection_length(coll, tagging2)
        assert rv[0] == 200
        assert rv[1][1] == rv[1][2] == 100

    # --------------- test for dump, load operations ---------------
    cachectl.dump_collections()
    cachectl.collmap = None
    cachectl.restore_collections()
    for cname in coll_list:
        assert cname in cachectl.collmap
        coll = cachectl.get_collection(cname)
        assert str(coll) == '<IncreaseCollection - {}> . inc'.format(cname)
        rv = rb.get_collection_length(coll, tagging1)
        assert rv[0] == 200
        assert rv[1][1] == rv[1][2] == 100
        rv = rb.get_collection_length(coll, tagging2)
        assert rv[0] == 200
        assert rv[1][1] == rv[1][2] == 100

    assert cachectl.get_collection('not_exists') is None
