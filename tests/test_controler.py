# -*- coding:utf-8 -*-
"""
    tests.cache
    ~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest
import os

from plumbca.cache import CacheCtl
from plumbca import cache


def test_cachectl_basic():
    coll_list = ['foo', 'bar', 'ken', 'kaf', 'abc']
    for cname in coll_list:
        CacheCtl.ensure_collection(cname, 'IncreaseCollection')
        CacheCtl.ensure_collection(cname, 'IncreaseCollection')
    assert len(CacheCtl.collmap) == 5

    for cname in coll_list:
        coll = CacheCtl.get_collection(cname)
        for i in range(100):
            coll.store(i, 'admin', {'bar': 1})
        assert str(coll) == '<IncreaseCollection - {}> . inc'.format(cname)
    assert CacheCtl.get_collection('not_exists') is None


def test_cachectl_dump_restore_collections():
    assert len(os.listdir(cache.DefaultConf['dumpdir'])) == 0
    CacheCtl.dump_collections()
    assert len(os.listdir(cache.DefaultConf['dumpdir'])) == 5

    # clean up the existing data in CacheCtl
    id_pair = {name: id(coll) for name, coll in CacheCtl.collmap.items()}
    CacheCtl.collmap = {}

    CacheCtl.restore_collections()
    for name, collid in id_pair.items():
        coll = CacheCtl.collmap[name]
        assert collid != id(coll)
        assert str(coll) == '<IncreaseCollection - {}> . inc'.format(name)
