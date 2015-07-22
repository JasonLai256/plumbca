# -*- coding: utf-8 -*-
"""
    plumbca.message
    ~~~~~~~~~~~~~~~

    Implements the message transport layer.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import logging
import msgpack

from .exceptions import MessageFormatError


actlog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


message_process_success = 0
message_process_failure = 1


class Request(object):
    """Handler objects for client requests messages

    Frame Format:
    [req_addr, command, Message]

    Message:
    {
        'args': [...],
    }
    """
    def __init__(self, raw_message):
        try:
            self.addr = raw_message[0]
            self.command = raw_message[1]
            self._message = msgpack.unpackb(raw_message[2])

            actlog.debug('<Request %s - %s>', self.command, self._message)
            # __getitem__ will raise if key not exists
            self.args = self._message['args']
        except KeyError:
            errlog.exception("Invalid request message : %s" % raw_message)
            raise MessageFormatError("Invalid request message : %r" % raw_message)


class Response(tuple):
    """Handler objects for responses messages

    Format:
    {
        'meta': {
            'status': 1|0|-1,
            'err_code': null|0|1|[...],
            'err_msg': '',
        },
        'datas': [...],
    }
    """
    def __new__(cls, *args, **kwargs):
        try:
            response = {
                'headers': {
                    'status': kwargs.pop('status', message_process_success),
                    'err_msg': kwargs.pop('err_msg', None),
                },
                'datas': kwargs.pop('datas'),
            }
            actlog.debug('<Response %s - %s> ', response['headers']['status'],
                         len(response['datas']))
            errlog.debug('<Response %s - %s> ', response['headers']['status'],
                         len(response['datas']))
            msg = msgpack.packb(response)
        except KeyError:
            errlog.exception("Invalid response message.")
            raise
            raise MessageFormatError("Invalid Response message.")

        return msg
