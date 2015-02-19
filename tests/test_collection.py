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
    tag_list = ['foo', 'bar', 'bin', 'jack', 'bob', 'sys', 'usr', 'var', 'etc']
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)

        assert len(icoll._metadata) == i
        with pytest.raises(ValueError):
            icoll.store(1, tagging, {1})
        assert tagging in icoll._metadata
        assert len(icoll._metadata[tagging]) == len(tslist)
        assert len(icoll.caching) == len(tslist) * i

        assert icoll._metadata[tagging][0][:1] == [min(tslist)]
        assert icoll._metadata[tagging][-1][:1] == [max(tslist)]

        for ts in tslist:
            key = icoll.gen_key_name(ts, tagging)
            if ts == 128:
                assert icoll.caching[key] == {'bar': 1, 'apple': 2}
            else:
                assert icoll.caching[key] == {'bar': 1}


@pytest.mark.incremental
def test_increse_collection_query(icoll):
    tag_list = ['foo', 'bar', 'bin', 'jack', 'bob', 'sys', 'usr', 'var', 'etc']
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)
        res = icoll.query(10, 1000, tagging)
        assert len(res) == 5

        res = icoll.query(100, 150, tagging)
        assert len(res) == 3
        assert res[0][:2] == ['108', tagging]
        assert res[-1][:2] == ['133', tagging]

        res = icoll.query(108, 109, tagging)
        assert len(res) == 1
        assert res[0][:2] == ['108', tagging]

        assert icoll.query(108, 108, tagging) is None
        assert icoll.query(150, 100, tagging) is None
        assert icoll.query(15, 100, tagging) is None
        assert icoll.query(1500, 2000, tagging) is None


@pytest.mark.incremental
def test_increse_collection_fetch_expired(icoll):
    tag_list = ['foo', 'bar', 'bin', 'jack', 'bob', 'sys', 'usr', 'var', 'etc']
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)
        rv = icoll.fetch_expired(d=False)
        assert len(rv) == 2
        assert rv[0][:1] == ['108']
        assert rv[1][:1] == ['188']
        assert len(icoll.query(10, 1000, tagging)) == 5

        rv = icoll.fetch_expired()
        assert len(rv) == 2
        assert rv[0][:1] == ['108']
        assert rv[1][:1] == ['188']
        assert len(icoll.query(10, 1000, tagging)) == 3


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

    tag_list = ['foo', 'bar', 'bin', 'jack', 'bob', 'sys', 'usr', 'var', 'etc']
    for i, t in enumerate(tag_list, 1):
        CollOpeHelper.icoll_insert_data(icoll, t)
    icoll.dump()
    icoll2.load()
    assert icoll._metadata == icoll2._metadata
    assert icoll.caching == icoll2.caching


@pytest.mark.incremental
def test_increse_collection_batch_opes(icoll, icoll2, tmpdir):
    from plumbca.config import DefaultConf
    DefaultConf['dumpdir'] = str(tmpdir)

    tslist, tagging = CollOpeHelper.icoll_insert_data(icoll)
    for i in range(8192):
        t = 'test{}'.format(i)
        CollOpeHelper.icoll_insert_data(icoll, t)
        assert len(icoll.query(10, 1000, tagging))
        assert tagging in icoll._metadata
        assert len(icoll._metadata[t]) == len(tslist)
        assert len(icoll.caching) == len(tslist) * (i + 2)

        assert icoll._metadata[tagging][0][:1] == [min(tslist)]
        assert icoll._metadata[tagging][-1][:1] == [max(tslist)]

    icoll.dump()
    icoll2.name = 'foo'
    icoll2.load()
    assert icoll._metadata == icoll2._metadata
    assert icoll.caching == icoll2.caching
