# -*- coding: utf-8 -*-
"""
    plumbca.protocol
    ~~~~~~~~~~~~~~~~

    Implements the protocol support for Plumbca.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import logging
import asyncio

from .message import Request
from .worker import Worker


actlog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


class PlumbcaCmdProtocol:

    def __init__(self):
        self.handler = Worker()

    async def plumbca_cmd_handle(self, reader, writer):
        """Simple plumbca command protocol implementation.

        plumbca_cmd_handle handles incoming command request.
        """
        data = await reader.read()
        req = Request(data)
        addr = writer.get_extra_info('peername')
        actlog.info("<Server> Received %r from %r", req.command, addr)

        # drive the command process
        resp = self.handler.run_command(req)

        writer.write(req.args)
        await writer.drain()

        actlog.info("Close the client %r socket", addr)
        writer.close()
