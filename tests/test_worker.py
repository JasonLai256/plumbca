# -*- coding:utf-8 -*-
"""
    tests.worker
    ~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest
import msgpack

from plumbca.worker import Worker
from plumbca.message import message_process_success
from plumbca.helpers import packb, unpackb


@pytest.mark.incremental
def test_worker_basic():
    worker = Worker()
    tag = 'www.cdnzz.com'
    val = {'test': 1}
    coll_list = ['foo', 'bar', 'ken', 'kaf', 'abc']
    for coll in coll_list:
        worker.store(coll, 123, tag, val)
    r = unpackb(worker.get_collections())
    assert r['headers']['status'] == message_process_success
    assert sorted(r['datas']) == sorted(coll_list)

    for coll in coll_list:
        r = unpackb(worker.query(coll, 10, 1000, tag))
        # print('Get response -', r)
        assert r['datas'][0][1] == val
