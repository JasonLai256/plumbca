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

        coll.store(256, tagging, {'bar': 1})
        coll.store('108', tagging, {'bar': 1})
        coll.store(188, tagging, {'bar': 1})
        coll.store(133, tagging, {'bar': 1})

        return [128, 256, 108, 188, 133], tagging


@pytest.mark.incremental
def test_increse_collection_basic(rb, icoll):
    assert str(icoll) == '<IncreaseCollection - foo> . inc'


@pytest.mark.incremental
def test_increse_collection_store(rb, icoll, tag_list):
    '''testing the store method of the IncreseCollection.'''
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)

        assert len(icoll.taggings) == i
        with pytest.raises(ValueError):
            icoll.store(1, tagging, {1})
        assert tagging in icoll.taggings

        for ts in tslist:
            key = icoll.gen_key_name(ts, tagging)
            rv = rb.inc_coll_caches_get(icoll, key)
            if ts == 128:
                assert rv and rv[0] == {'bar': 1, 'apple': 2}
            else:
                assert rv and rv[0] == {'bar': 1}

        # check the size of items of the collection.tagging
        _md_len, _cache_len = rb.get_collection_length(icoll, klass="IncreaseCollection")
        assert _md_len == len(tslist)
        assert _cache_len == len(tslist) * i


# @pytest.mark.incremental
# def test_increse_collection_store_other_opes(icoll):
#     pass


@pytest.mark.incremental
def test_increse_collection_query(rb, icoll, tag_list):
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)
        res = list(icoll.query(10, 1000, tagging))
        print('results -', res)
        assert len(res) == 5
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}
        assert res[-1][0].split(':')[0] == '256'
        assert res[-1][1] == {'bar': 1}

        res = list(icoll.query(100, 150, tagging))
        print('results -', res)
        assert len(res) == 3
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}
        assert res[1][0].split(':')[0] == '128'
        assert res[1][1] == {'bar': 1, 'apple': 2}
        assert res[2][0].split(':')[0] == '133'
        assert res[2][1] == {'bar': 1}

        res = list(icoll.query(108, 109, tagging))
        print('results -', res)
        assert len(res) == 1
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}

        assert icoll.query(107, 107, tagging) is None
        assert icoll.query(150, 100, tagging) is None
        assert icoll.query(15, 100, tagging) is None
        assert icoll.query(1500, 2000, tagging) is None


@pytest.mark.incremental
def test_increse_collection_fetch(rb, icoll, tag_list):
    e = icoll._expire
    for i, t in enumerate(tag_list, 1):
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)
        res = list(icoll.fetch(tagging=tagging, d=False, expired=e+130))
        assert len(res) == 2
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}
        assert res[1][0].split(':')[0] == '128'
        assert res[1][1] == {'bar': 1, 'apple': 2}

        rv = list(icoll.fetch(tagging=tagging, d=False, e=False))
        assert len(rv) == 5

        _md_len, _cache_len = rb.get_collection_length(icoll,
                                                       klass="IncreaseCollection")
        assert _md_len == 5
        assert _cache_len == 5 * i

    # - 1 -
    rv = list(icoll.fetch(d=False, expired=e+130))
    assert len(rv) == 2 * i

    # - 2 -
    rv = list(icoll.fetch(d=False, e=False))
    assert len(rv) == 5 * i
    _md_len, _cache_len = rb.get_collection_length(icoll,
                                                   klass="IncreaseCollection")
    assert _md_len == 5
    assert _cache_len == 5 * i

    # - 3 -
    rv = list(icoll.fetch(expired=e+130))
    assert len(rv) == 2 * i
    _md_len, _cache_len = rb.get_collection_length(icoll,
                                                   klass="IncreaseCollection")
    assert _md_len == 3
    assert _cache_len == 3 * i

    # - 4 -
    rv = list(icoll.fetch())
    assert len(rv) == 3 * i
    _md_len, _cache_len = rb.get_collection_length(icoll,
                                                   klass="IncreaseCollection")
    assert _md_len == 0
    assert _cache_len == 0

    # - 5 -
    assert list(icoll.fetch(tagging='not-exists')) == []


# @pytest.mark.incremental
# def test_increse_collection_dump_load(rb, icoll, icoll2):
#     assert icoll.itype == 'inc'
#     assert icoll2.itype == 'max'
#     assert str(icoll) == '<IncreaseCollection - foo> . inc'
#     assert str(icoll2) == '<IncreaseCollection - bar> . max'

#     CollOpeHelper.icoll_insert_data(icoll)
#     icoll.dump()
#     icoll2.name = icoll.name
#     icoll2.load()
#     assert icoll.taggings == icoll2.taggings
#     assert icoll._expire == icoll2._expire
#     assert icoll.itype == icoll2.itype
#     assert icoll.ifunc(1, 2) == icoll2.ifunc(1, 2)
#     assert rb.get_collection_length(icoll, 'foo') == \
#         rb.get_collection_length(icoll2, 'foo')

#     tag_list = ['foo', 'bar', 'bin', 'jack', 'bob', 'sys', 'usr', 'var', 'etc']
#     for i, t in enumerate(tag_list, 1):
#         CollOpeHelper.icoll_insert_data(icoll, t)
#     icoll.dump()
#     icoll2.load()
#     assert icoll.taggings == icoll2.taggings
#     assert icoll._expire == icoll2._expire
#     assert icoll.itype == icoll2.itype
#     assert icoll.ifunc(1, 2) == icoll2.ifunc(1, 2)
#     assert rb.get_collection_length(icoll, 'foo') == \
#         rb.get_collection_length(icoll2, 'foo')


@pytest.mark.incremental
def test_increse_collection_batch_opes(rb, icoll, icoll2):
    for i in range(1, 65):
        t = 'test{}'.format(i)
        tslist, tagging = CollOpeHelper.icoll_insert_data(icoll, t)

        # check fetch (no delete, no expire)
        rv = list(icoll.fetch(d=False, e=False))
        assert len(rv) == len(tslist) * i

        # check query
        assert len(list(icoll.query(10, 1000, tagging))) == len(tslist)
        assert tagging in icoll.taggings

        # check length
        _md_len, _cache_len = rb.get_collection_length(icoll, klass="IncreaseCollection")
        assert _md_len == len(tslist)
        assert _cache_len == len(tslist) * i

    for i in range(1, 33):
        t = 'test{}'.format(i)
        rv = list(icoll.fetch(tagging=t))
        assert len(rv) == len(tslist)

        _md_len, _cache_len = rb.get_collection_length(icoll, klass="IncreaseCollection")
        assert _md_len == len(tslist)
        assert _cache_len == len(tslist) * (64 - i)
