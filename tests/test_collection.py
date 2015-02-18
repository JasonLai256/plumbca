# -*- coding:utf-8 -*-
"""
    tests.collection
    ~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest


class CollOpeHelper:
    '''Helper class that doing read/write operations for the
    specified collection instance.'''

    @classmethod
    def icoll_insert_data(cls, coll, tagging='foo'):
        """construct IncreseCollection store datas, include different
        situations for testing.
        """
        coll.store(128, tagging, {'bar': 1})
        coll.store(128, tagging, {'bar': 0})
        coll.store(128, tagging, {'bar': 1, 'apple': 1})
        coll.store(128, tagging, {})
        coll.store(128, tagging, {'apple': 1})
        coll.store(128, tagging, {'bar': -1})

        coll.store(256, tagging, {'bar': 1}, 1000)
        coll.store('108', tagging, {'bar': 1}, -100)
        coll.store(188, tagging, {'bar': 1}, -100)
        coll.store(133, tagging, {'bar': 1})

        return [128, 256, 108, 188, 133], tagging


@pytest.mark.incremental
def test_increse_collection_store(icoll):
    '''testing the store method of the IncreseCollection.'''
    tslist, tagging = CollOpeHelper.icoll_insert_data(icoll)

    with pytest.raises(ValueError):
        icoll.store(1, tagging, {1})
    assert len(icoll._metadata) == len(tslist)
    assert len(icoll.caching) == len(tslist)

    assert icoll._metadata[0][:2] == [min(tslist), tagging]
    assert icoll._metadata[-1][:2] == [max(tslist), tagging]

    for ts in tslist:
        key = icoll.gen_key_name(ts, tagging)
        if ts == 128:
            assert icoll.caching[key] == {'bar': 1, 'apple': 2}
        else:
            assert icoll.caching[key] == {'bar': 1}


@pytest.mark.incremental
def test_increse_collection_query(icoll):
    tslist, tagging = CollOpeHelper.icoll_insert_data(icoll)
    res = icoll.query(10, 1000)
    assert len(res) == 5

    res = icoll.query(100, 150)
    assert len(res) == 3
    assert res[0][:2] == ['108', tagging]
    assert res[-1][:2] == ['133', tagging]

    res = icoll.query(108, 109)
    assert len(res) == 1
    assert res[0][:2] == ['108', tagging]

    assert icoll.query(108, 108) is None
    assert icoll.query(150, 100) is None
    assert icoll.query(15, 100) is None
    assert icoll.query(1500, 2000) is None


@pytest.mark.incremental
def test_increse_collection_fetch_expired(icoll):
    tslist, tagging = CollOpeHelper.icoll_insert_data(icoll)
    rv = icoll.fetch_expired(False)
    assert len(rv) == 2
    assert rv[0][:2] == ['108', tagging]
    assert rv[1][:2] == ['188', tagging]
    assert len(icoll.query(10, 1000)) == 5

    rv = icoll.fetch_expired()
    assert len(rv) == 2
    assert rv[0][:2] == ['108', tagging]
    assert rv[1][:2] == ['188', tagging]
    assert len(icoll.query(10, 1000)) == 3


@pytest.mark.incremental
def test_increse_collection_dump_load(icoll, icoll2, tmpdir):
    from plumbca.config import DefaultConf
    DefaultConf['dumpdir'] = str(tmpdir)

    CollOpeHelper.icoll_insert_data(icoll)
    icoll.dump()

    icoll2.name = 'foo'
    icoll2.load()

    assert icoll._metadata == icoll2._metadata
    assert icoll.caching == icoll2.caching
