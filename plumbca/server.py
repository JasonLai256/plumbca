# -*- coding: utf-8 -*-
"""
    plumbca.server
    ~~~~~~~~~~~~~~

    Implements the serving support for Plumbca.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import asyncio
import logging

from .config import DefaultConf
from .protocol import PlumbcaCmdProtocol


aclog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


def runserver():
    loop = asyncio.get_event_loop()
    pcp = PlumbcaCmdProtocol()
    coro = asyncio.start_server(pcp.plumbca_cmd_handle, DefaultConf['bind'],
                                DefaultConf['port'], loop=loop)
    server = loop.run_until_complete(coro)

    # Serve requests until terminate signal is received
    aclog.info('Serving on %s', server.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
