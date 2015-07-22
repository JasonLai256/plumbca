# -*- coding:utf-8 -*-
"""
    tests.backend
    ~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from msgpack import packb, unpackb

from plumbca.backend import rbackend


def test_redis_backend_basic(fake_manager, fake_coll):
    fake_manager.collmap = {'t1': fake_coll, 't2': fake_coll}
    rbackend.set_collection_indexes(fake_manager)
    rv = rbackend.get_collection_indexes()
    matching = {'t1': '_t', 't2': '_t'}
    assert rv == unpackb(packb(matching))

    fake_coll.taggings = ['minute', 'code_minute', 'hour', 'rtmp_minute']
    fake_coll.name = 'test'
    rbackend.set_collection_data_index(fake_coll)
    rv = rbackend.get_collection_data_index(fake_coll)
    matching = {'taggings': fake_coll.taggings,
                'expire': fake_coll.expire,
                'type': fake_coll.itype}
    assert rv == unpackb(packb(matching))
