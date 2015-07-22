# -*- coding:utf-8 -*-
"""
    tests.worker
    ~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest
import msgpack

from msgpack import packb, unpackb

from plumbca.worker import Worker
from plumbca.message import message_process_success


def transform_response(r):
    return msgpack.unpackb(r)


@pytest.mark.incremental
def test_worker_basic():
    worker = Worker()
    tag = 'www.cdnzz.com'
    val = {'test': 1}
    coll_list = ['foo', 'bar', 'ken', 'kaf', 'abc']
    for coll in coll_list:
        worker.store(coll, 123, tag, val)
    r = transform_response(worker.get_collections())
    assert r[b'headers'][b'status'] == message_process_success
    assert sorted(r[b'datas']) == sorted(unpackb(packb(coll_list)))

    for coll in coll_list:
        r = transform_response(worker.query(coll, 10, 1000, tag))
        print(r)
        assert r[b'datas'] == unpackb(packb([val]))
