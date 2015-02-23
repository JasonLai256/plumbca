# -*- coding:utf-8 -*-
"""
    tests.collection
    ~~~~~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import pytest

from plumbca.collection import IncreaseCollection
from plumbca.config import DefaultConf
import plumbca.log


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
def metadata_t():
    return [
        [143372, 'hello', 123],
        [143372, 'hello', 1234],
        [168011, 'hello', 123],
        [168072, 'hello', 123],
        [188072, 'hello', 123],
        [228072, 'hello', 123]
    ]
