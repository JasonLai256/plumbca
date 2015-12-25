# -*- coding:utf-8 -*-
"""
    tests.worker
    ~~~~~~~~~~~~

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from unittest import mock

import pytest
from plumbca.protocol import PlumbcaCmdProtocol

from utils import CoroWraps, req, call_first_args


@pytest.mark.incremental
def test_worker_basic(loop, reader, writer):
    # patch worker and the method will call
    m = mock.MagicMock()
    m.run_command = CoroWraps('OK')

    pcp = PlumbcaCmdProtocol()
    pcp.handler = m
    reader.readline = CoroWraps(req('command', 'test.test'))
    coro = pcp.plumbca_cmd_handle(reader, writer)
    loop.run_until_complete(coro)

    assert call_first_args(writer.write) == 'OK'
    assert writer.get_extra_info.called
