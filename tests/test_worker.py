# -*- coding:utf-8 -*-
"""
    tests.worker
    ~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from plumbca.worker import Worker
from plumbca.message import message_process_success
from plumbca.helpers import packb, unpackb


@pytest.mark.incremental
def test_worker_basic(rb, coll_list):
    worker = Worker()
    tag, val = 'www.cdnzz.com', {'test': 1}
    for coll in coll_list:
        worker.ensure_collection(coll, expired=7200)
        worker.store(coll, 123, tag, val)
    r = unpackb(worker.get_collections())
    assert r['headers']['status'] == message_process_success
    assert sorted(r['datas']) == sorted(coll_list)

    for coll in coll_list:
        r = unpackb(worker.query(coll, 10, 1000, tag))
        # print('Get response -', r)
        assert r['datas'][0][1] == val


@pytest.mark.incremental
def test_worker_fetch(rb, coll_list):
    worker = Worker()
    tag1, tag2 = 'www.cdnzz.com', 'error.cdnzz.com'
    val = {'test': 1}
    for coll in coll_list:
        worker.ensure_collection(coll, expired=7200)
        for i in range(1, 10):
            worker.store(coll, 123 + i, tag1, val)
            worker.store(coll, 123 + i, tag2, val)

    # 测试指定 tagging 的 fetch 操作的处理
    for coll in coll_list[:2]:
        for tag in [tag1, tag2]:
            rv = unpackb(worker.fetch(coll, tag))['datas']
            assert len(rv) == 9
            for i in range(len(rv)):
                _ts, _tag = rv[i][0].split(':')
                assert int(_ts) == 123 + i + 1
                assert _tag == tag
                assert rv[i][1] == val

    # 测试默认 __all__ 的 fetch 操作的处理
    for coll in coll_list[2:]:
        rv = unpackb(worker.fetch(coll))['datas']
        assert len(rv) == 9 * 2

    for coll in coll_list:
        rv = unpackb(worker.fetch(coll))['datas']
        assert len(rv) == 0
