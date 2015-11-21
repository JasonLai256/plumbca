# -*- coding:utf-8 -*-
"""
    tests.collection
    ~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import random
import logging
import asyncio
from unittest import mock

import pytest
import plumbca.log
from plumbca.collection import IncreaseCollection, UniqueCountCollection, SortedCountCollection
from plumbca.config import DefaultConf, RedisConf as rdconf
from plumbca.cache import CacheCtl
from plumbca.backend import BackendFactory
from redis import StrictRedis

from utils import CoroWraps


logging.basicConfig(level=logging.DEBUG)


def pytest_runtest_makereport(item, call):
    if "incremental" in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item


def pytest_runtest_setup(item):
    if "incremental" in item.keywords:
        previousfailed = getattr(item.parent, "_previousfailed", None)
        if previousfailed is not None:
            pytest.xfail("previous test failed (%s)" % previousfailed.name)


@pytest.yield_fixture(autouse=True)
def patch_config(tmpdir):
    origin = DefaultConf['dumpdir']
    DefaultConf['dumpdir'] = str(tmpdir)
    yield
    DefaultConf['dumpdir'] = origin


@pytest.fixture
def icoll():
    return IncreaseCollection('foo')


@pytest.fixture
def icoll2():
    return IncreaseCollection('bar', 'max')


@pytest.fixture
def uc_coll():
    return UniqueCountCollection('foo')


@pytest.fixture
def sc_coll():
    return SortedCountCollection('foo')


@pytest.fixture
def metadata_t():
    return [
        [143372, 'hello', 123],
        [143372, 'hello', 1234],
        [168011, 'hello', 123],
        [168072, 'hello', 123],
        [188072, 'hello', 123],
        [228072, 'hello', 123]
    ]


_names = [
    'foo', 'bar', 'bin', 'jack', 'bob',
    'sys', 'usr', 'var', 'etc', 'jason',
    'ben', 'kevin', 'ken', 'kafka', 'justin',
    'jackson', 'jacob', 'jacqueline', 'jacques', 'abbie',
    'abbott', 'abbra', 'abby', 'abdul', 'lacy',
    'laddie', 'ladonna', 'lael', 'quillan', 'quin',
    'quincy', 'quinlan', 'taber', 'tabitha', 'tacita',
    'tacy', 'tad', 'tadeo', 'taffy', 'tai',
    'taifa', 'ulric', 'ulysses', 'uma', 'umay',
    'umberto',
]

@pytest.fixture()
def tag_list():
    random.shuffle(_names)
    return _names[:9]


@pytest.fixture()
def coll_list():
    random.shuffle(_names)
    return _names[:5]


@pytest.fixture
def fake_manager():
    class _t:
        def __init__(self):
            self.collmap = {}

    return _t()


@pytest.fixture
def fake_coll():
    class _t:
        def __init__(self):
            self.name = 'test'
            self.taggings = []
            self._expire = 200
            self.itype = 'inc'

    return _t()


@pytest.fixture(scope='function')
def rb(request):
    rbackend = BackendFactory('redis')
    aclog = logging.getLogger('activity')

    def fin():
        keys = rbackend.rdb.keys()
        aclog.warning('[RBACKEND FINISH] %s - %s', keys, len(keys))
        aclog.warning('[RBACKEND FINISH] module %s, function: %s',
                      request.module, request.function)
        if keys:
            rv = rbackend.rdb.delete(*keys)
            aclog.warning('[RBACKEND CLEAN UP] deleted %s items.', rv)
    request.addfinalizer(fin)

    return rbackend


@pytest.fixture(scope='function')
def arb(request):
    rbackend = BackendFactory('aioredis')
    aclog = logging.getLogger('activity')

    def fin():
        rdb = StrictRedis(host=rdconf['host'], port=rdconf['port'],
                          db=rdconf['db'])
        keys = rdb.keys()
        aclog.warning('[RBACKEND FINISH] %s - %s', keys, len(keys))
        aclog.warning('[RBACKEND FINISH] module %s, function: %s',
                      request.module, request.function)
        if keys:
            rv = rdb.delete(*keys)
            aclog.warning('[RBACKEND CLEAN UP] deleted %s items.', rv)
    request.addfinalizer(fin)

    return rbackend


@pytest.fixture(scope='function')
def cachectl(request):
    def fin():
        CacheCtl.collmap = {}
    request.addfinalizer(fin)

    return CacheCtl


@pytest.fixture(scope='function')
def loop(request):
    # We want explicit loops
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def fin():
        loop.stop()
        loop.close()

    return loop


@pytest.fixture
def reader():
    m = mock.MagicMock()
    return m


@pytest.fixture
def writer():
    m = mock.MagicMock()
    m.drain = CoroWraps()
    m.get_extra_info.return_value = 'test'
    return m
