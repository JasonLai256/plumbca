# -*- coding:utf-8 -*-
"""
    tests.collection
    ~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest
from faker import Factory


fake = Factory.create()


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
def test_collection_basic(rb, icoll, sc_coll, uc_coll):
    assert str(icoll) == '<IncreaseCollection - foo> . inc'
    assert str(sc_coll) == '<SortedCountCollection - foo>'
    assert str(uc_coll) == '<UniqueCountCollection - foo>'


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
        # print('results -', res)
        assert len(res) == 5
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}
        assert res[-1][0].split(':')[0] == '256'
        assert res[-1][1] == {'bar': 1}

        res = list(icoll.query(100, 150, tagging))
        # print('results -', res)
        assert len(res) == 3
        assert res[0][0].split(':')[0] == '108'
        assert res[0][1] == {'bar': 1}
        assert res[1][0].split(':')[0] == '128'
        assert res[1][1] == {'bar': 1, 'apple': 2}
        assert res[2][0].split(':')[0] == '133'
        assert res[2][1] == {'bar': 1}

        res = list(icoll.query(108, 109, tagging))
        # print('results -', res)
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


@pytest.mark.incremental
def test_increse_collection_batch_opes(rb, icoll, icoll2):
    ope_times = 8
    ope_times_range = ope_times + 1
    half_ope_times_range = (ope_times // 2) + 1

    for i in range(1, ope_times_range):
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

    for i in range(1, half_ope_times_range):
        t = 'test{}'.format(i)
        rv = list(icoll.fetch(tagging=t))
        assert len(rv) == len(tslist)

        _md_len, _cache_len = rb.get_collection_length(icoll, klass="IncreaseCollection")
        assert _md_len == len(tslist)
        assert _cache_len == len(tslist) * (ope_times - i)


@pytest.mark.incremental
def test_uniq_count_collection_batch_opes(rb, uc_coll):
    items_num = 30
    ope_times = 8
    ope_times_range = ope_times + 1
    half_ope_times_range = (ope_times // 2) + 1
    not_exist_start, not_exist_end = 20000000, 30000000
    start, end = 100, 300
    tslist = [start, 200, end]
    v = set()

    def _fetch_data(tagging='__all__', d=True, e=True, expired=None):
        rv = list(uc_coll.fetch(tagging, d, e, expired))
        num = len(rv)
        items = [item for _, val, _ in rv
                          for item in val]
        return num, items

    for i in range(1, ope_times_range):
        tagging = 'test{}'.format(i)
        value = {fake.uuid4() for i in range(items_num)}
        v |= value
        for ts in tslist:
            # print(ts, tagging, len(value))
            uc_coll.store(ts, tagging, value)

        # ---------------- check Query ----------------
        rv = list(uc_coll.query(start, end, tagging))
        assert len(rv) == len(tslist)
        for res in rv:
            assert len(res[1]) == items_num
            assert res[1] == value

        assert uc_coll.query(not_exist_start, not_exist_end, tagging) is None

        # ---------------- check Fetch ----------------
        rv_num, rv = _fetch_data(d=False)
        assert rv_num == i * len(tslist)
        assert len(rv) == items_num * i * len(tslist)
        assert set(rv) == v

    # ---------------- check for not exist datas ----------------
    rv_num, rv = _fetch_data(expired=50)
    assert rv_num == 0 and rv == []

    rv_num, rv = _fetch_data(d=False)
    assert rv_num == ope_times * len(tslist)
    assert len(rv) == items_num * ope_times * len(tslist)
    assert set(rv) == v

    # fetch one time tagging and deleting fetch again
    rv_num, rv = _fetch_data(tagging=tagging, d=False)
    assert rv_num == len(tslist)
    assert len(rv) == items_num * len(tslist)
    rv_num, rv = _fetch_data(tagging=tagging)
    assert rv_num == len(tslist)
    assert len(rv) == items_num * len(tslist)
    rv_num, rv = _fetch_data(tagging=tagging)
    assert rv_num == 0
    assert len(rv) == 0

    # __all__ tagging should be decrease by 1
    rv_num, rv = _fetch_data(d=False)
    assert rv_num == (ope_times - 1) * len(tslist)
    assert len(rv) == items_num * (ope_times - 1) * len(tslist)

    rv_num, rv = _fetch_data()
    assert rv_num == (ope_times - 1) * len(tslist)
    assert len(rv) == items_num * (ope_times - 1) * len(tslist)

    rv_num, rv = _fetch_data(d=False)
    assert rv_num == 0
    assert len(rv) == 0


@pytest.mark.incremental
def test_sorted_count_collection_batch_opes(rb, sc_coll):
    items_num = 30
    ope_times = 8
    ope_times_range = ope_times + 1
    not_exist_start, not_exist_end = 20000000, 30000000
    start, end = 100, 300
    tslist = [start, 200, end]
    v = {}

    def _fetch_data(tagging='__all__', d=True, e=True, expired=None, topN=None):
        rv = list(sc_coll.fetch(tagging, d, e, expired, topN))
        num = len(rv)
        items = {member: score for _, val, _ in rv
                          for member, score in val}
        return num, items

    for i in range(1, ope_times_range):
        tagging = 'test{}'.format(i)
        for ts in tslist:
            value = {fake.uuid4(): 1 for i in range(items_num)}
            v.update(value)
            sc_coll.store(ts, tagging, value)

        # ---------------- check Query ----------------
        rv = list(sc_coll.query(start, end, tagging))
        assert len(rv) == len(tslist)
        for res in rv:
            assert len(res[1]) == items_num
        # only check the value of last adding timestamp
        assert dict(res[1]) == value

        rv = list(sc_coll.query(start, end, tagging, topN=10))
        assert len(rv) == len(tslist)
        for res in rv:
            assert len(res[1]) == 10

        assert sc_coll.query(not_exist_start, not_exist_end, tagging) is None

        # ---------------- check Fetch ----------------
        rv_num, rv = _fetch_data(d=False)
        assert rv_num == i * len(tslist)
        assert len(rv) == items_num * i * len(tslist)
        assert rv == v

        rv_num, rv = _fetch_data(d=False, topN=10)
        assert rv_num == i * len(tslist)
        assert len(rv) == 10 * i * len(tslist)

    # ---------------- check for not exist datas ----------------
    rv_num, rv = _fetch_data(expired=50)
    assert rv_num == 0 and rv == {}

    rv_num, rv = _fetch_data(d=False)
    assert rv_num == ope_times * len(tslist)
    assert len(rv) == items_num * ope_times * len(tslist)
    assert rv == v

    rv_num, rv = _fetch_data(d=False, topN=10)
    assert rv_num == ope_times * len(tslist)
    assert len(rv) == 10 * ope_times * len(tslist)

    # fetch one time tagging and deleting fetch again
    rv_num, rv = _fetch_data(tagging=tagging, d=False)
    assert rv_num == len(tslist)
    assert len(rv) == items_num * len(tslist)
    rv_num, rv = _fetch_data(tagging=tagging, d=False, topN=10)
    assert rv_num == len(tslist)
    assert len(rv) == 10 * len(tslist)
    rv_num, rv = _fetch_data(tagging=tagging)
    assert rv_num == len(tslist)
    assert len(rv) == items_num * len(tslist)
    rv_num, rv = _fetch_data(tagging=tagging)
    assert rv_num == 0
    assert len(rv) == 0

    # __all__ tagging should be decrease by 1
    rv_num, rv = _fetch_data(d=False)
    assert rv_num == (ope_times - 1) * len(tslist)
    assert len(rv) == items_num * (ope_times - 1) * len(tslist)

    rv_num, rv = _fetch_data()
    assert rv_num == (ope_times - 1) * len(tslist)
    assert len(rv) == items_num * (ope_times - 1) * len(tslist)

    rv_num, rv = _fetch_data(d=False)
    assert rv_num == 0
    assert len(rv) == 0
