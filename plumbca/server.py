# -*- coding: utf-8 -*-
"""
    plumbca.server
    ~~~~~~~~~~~~~~

    Implements the serving support for Plumbca.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import zmq
import logging
import traceback

from .config import DefaultConf
from .message import Response
from .helpers import frame2str
from . import constants


aclog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


class Broker:

    def __init__(self):
        self.frontend = constants.ZCONTEXT.socket(zmq.ROUTER)
        endpoint = '{}:{}'.format(DefaultConf['bind'], DefaultConf['port'])
        self.host = self._gen_bind_adress(DefaultConf['transport'], endpoint)
        self.frontend.bind(self.host)
        aclog.info('Broker frontend bind to %s' % self.host)

        self.backend = constants.ZCONTEXT.socket(zmq.ROUTER)
        self.backend.bind(constants.BACKEND_IPC)
        aclog.info('Broker backend bind to %s', constants.BACKEND_IPC)

        self.service_list = ['worker']
        self.service_map = {}
        self.worker_cmd = ['store', 'query', 'fetch', 'wping', 'dump',
                           'get_collections', 'ensure_collection']
        self.server_cmd = ['ping', 'register_service']

    def submit_message(self, message):
        command = frame2str(message[1])
        # print(command, type(command))
        if command in self.server_cmd:
            getattr(self, command)(message)
        elif command in self.worker_cmd:
            self.backend.send_multipart(self.service_map['worker'] + message)
        else:
            errlog.error('Broker found unknown command - %s, ignore it.', command)

    def register_service(self, msg):
        """The register message frames should be: [worker_addr, worker_type, 'READY']
        """
        worker_addr, cmd, wtype, case = msg[:4]
        wtype, case = frame2str(wtype), frame2str(case)
        if wtype in self.service_list and wtype not in self.service_map and \
           case == 'ready':
            self.service_map[wtype] = [worker_addr]
            aclog.info('Broker register `%s`.', wtype)
        else:
            aclog.info('Forbidden register `%s` in broker.', wtype)

    def ping(self, msg):
        aclog.info('<Broker> handling PING command.')
        req_addr = msg[0]
        r = Response(datas='SERVER OK')
        self.frontend.send_multipart([req_addr, r])

    def __del__(self):
        self.frontend.close()

    def _gen_bind_adress(self, transport, endpoint):
        if transport == 'ipc':
            if not DefaultConf['unixsocket']:
                err_msg = "Ipc transport layer was selected, but no unixsocket " \
                          "path could be found in conf file"
                errlog.error(err_msg)
                raise KeyError(err_msg)
            return '{0}://{1}'.format(transport, self.config['unixsocket'])

        else:  # consider it's tcp
            return '{0}://{1}'.format(transport, endpoint)


def runserver():
    broker = Broker()

    poller = zmq.Poller()
    poller.register(broker.frontend, zmq.POLLIN)
    poller.register(broker.backend, zmq.POLLIN)

    aclog.info('Server started on %s.', broker.host)
    while True:
        try:
            sockets = dict(poller.poll())
            if sockets.get(broker.backend) == zmq.POLLIN:
                msg = broker.backend.recv_multipart(copy=False)
                # there is use a trick condition, should be refactor it
                if len(msg) != 3:
                    command = frame2str(msg[1])
                    if command in broker.server_cmd:
                        broker.submit_message(msg)
                else:
                    broker.frontend.send_multipart(msg[1:])

            if sockets.get(broker.frontend) == zmq.POLLIN:
                msg = broker.frontend.recv_multipart(copy=False)
                if not msg or len(msg) != 3:
                    continue
                broker.submit_message(msg)

        except Exception as err:
            error_track = traceback.format_exc()
            errmsg = '%s\n%s' % (err, error_track)
            errlog.error('<SERVER> Unknown situation occur: %s', errmsg)
