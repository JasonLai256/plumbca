# -*- coding:utf-8 -*-
"""
    plumbca.worker
    ~~~~~~~~~~~~~~

    Implements helper class for worker control.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import traceback
import logging
import time
from threading import Thread

import zmq

from .collection import IncreaseCollection
from .cache import CacheCtl
from .message import Request, Response, message_process_failure
from . import constants


wrtlog = logging.getLogger('write-opes')
actlog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


class Worker(Thread):
    """
    Class that handles commands server side.
    Translates, messages commands to it's methods calls.
    """
    def __init__(self):
        self.sock = constants.ZCONTEXT.socket(zmq.DEALER)
        super().__init__()

    def __del__(self):
        self.sock.close()

    def run(self):
        time.sleep(1)
        self.sock.connect(constants.BACKEND_IPC)
        actlog.info('<WORKER> socket connect to %s', constants.BACKEND_IPC)

        # register service
        actlog.info('<WORKER> try to register worker service.')
        self.sock.send_multipart([b'register_service', b'worker', b'READY'])
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        actlog.info('<WORKER> starting the worker service ...')
        while True:
            try:
                poller.poll()
                msg = self.sock.recv_multipart(copy=False)
                req = Request(msg)
                func = getattr(self, req.command)
                response = func(*req.args)
                self.sock.send_multipart([req.addr, response])
            except Exception as err:
                error_track = traceback.format_exc()
                errmsg = '%s\n%s' % (err, error_track)
                errmsg = '<WORKER> Unknown situation occur: %s' % errmsg
                errlog.error(errmsg)

                response = Response(datas=errmsg,
                                    status=message_process_failure)
                self.sock.send_multipart([req.addr, response])

    def wping(self):
        actlog.info('<WORKER> handling Wping command ...')
        return Response(datas='WORKER OK')

    def dump(self):
        """
        Handles Dumps message command.
        Executes dump operation for all of the collections in CacheCtl.
        """
        actlog.info('<WORKER> handling Dump command ...')
        # Nothing todo
        return Response(datas='DUMP OK')

    def store(self, collection, *args, **kwargs):
        """
        Handles Store message command.
        Executes a Store operation over the specific collection.

        collection   =>     Collection object name
        timestamp    =>     The data storing time
        tagging      =>     The tagging of the data
        value        =>     Data value
        expire       =>     Data expiring time
        """
        wrtlog.info('<WORKER> handling Store command - %s, %s ... %s ...',
                    collection, args[:2], len(args[2]))
        coll = CacheCtl.get_collection(collection)
        coll.store(*args, **kwargs)
        return Response(datas='Store OK')

    def query(self, collection, *args, **kwargs):
        """
        Handles Query message command.
        Executes a Put operation over the plumbca backend.

        collection   =>     Collection object name
        start_time   =>     The starting time of the query
        end_time     =>     The end time of the query
        tagging      =>     The tagging of the data
        """
        actlog.info('<WORKER> handling Query command - %s, %s ...',
                    collection, args)
        coll = CacheCtl.get_collection(collection)
        rv = coll.query(*args, **kwargs)
        rv = list(rv) if rv else []
        return Response(datas=rv)

    def fetch(self, collection, *args, **kwargs):
        """
        Handles Fetch message command
        Executes a Delete operation over the plumbca backend.

        collection   =>      Collection object name
        tagging      =>      The tagging of the data
        d            =>      Should be delete the fetching data
        e            =>      whether only contain the expired data
        """
        actlog.info('<WORKER> handling Fetch command - %s, %s ...',
                    collection, args)
        coll = CacheCtl.get_collection(collection)
        rv = coll.fetch(*args, **kwargs)
        rv = list(rv) if rv else []
        return Response(datas=rv)

    def get_collections(self):
        """
        """
        actlog.info('<WORKER> handling Get_collections command ...')
        rv = list(CacheCtl.collmap.keys())
        return Response(datas=rv)

    def ensure_collection(self, name, coll_type='IncreaseCollection',
                          expired=3600):
        actlog.info('<WORKER> handling ENSURE_COLLECTION command - %s, %s, %s ...',
                    name, coll_type, expired)
        CacheCtl.ensure_collection(name, coll_type, expired)
        assert name in CacheCtl.collmap
        return Response(datas='Ensure OK')

    def _gen_response(self, request, cmd_status, cmd_value):
        if cmd_status == FAILURE_STATUS:
            header = ResponseHeader(status=cmd_status, err_code=cmd_value[0], err_msg=cmd_value[1])
            content = ResponseContent(datas=None)
        else:
            if 'compression' in request.meta:
                compression = request.meta['compression']
            else:
                compression = False

            header = ResponseHeader(status=cmd_status, compression=compression)
            content = ResponseContent(datas=cmd_value, compression=compression)

        return header, content
