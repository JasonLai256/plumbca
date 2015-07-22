# -*- coding:utf-8 -*-
"""
    plumbca.worker
    ~~~~~~~~~~~~~~

    Implements helper class for worker control.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import logging
import msgpack
import zmq

from .collection import IncreaseCollection
from .cache import CacheCtl
from .message import Request, Response, message_process_failure
from .constants import ZCONTEXT
from . import constants


actlog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


class Worker(object):
    """
    Class that handles commands server side.
    Translates, messages commands to it's methods calls.
    """
    def __init__(self):
        self.sock = ZCONTEXT.socket(zmq.DEALER)
        self.sock.connect(constants.BACKEND_IPC)

    def __del__(self):
        self.sock.close()

    def _ensure_collection(self, name, coll_type='IncreaseCollection'):
        CacheCtl.ensure_collection(name, coll_type)
        return CacheCtl.collmap[name]

    def run(self):
        # register service
        self.sock.send_multipart(['register_service', 'worker', 'READY'])
        poller = zmq.Poller()
        poller.register(broker.frontend, zmq.POLLIN)
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
                errmsg = '%s\n%s' % (err.message, error_track)
                errmsg = '<WORKER> Unknown situation occur: %s', errmsg
                errlog.error(errmsg)

                response = Response(datas=errmsg, status=message_process_failure)
                self.sock.send_multipart([req.addr, response])

    def wping(self):
        return Response(datas='WORKER OK')

    def dumps(self):
        """
        Handles Dumps message command.
        Executes dump operation for all of the collections in CacheCtl.
        """
        CacheCtl.dump_collections()
        return Response(datas='Dumps OK')

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
        coll = self._ensure_collection(collection)
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
        coll = self._ensure_collection(collection)
        rv = coll.query(*args, **kwargs)
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
        coll = self._ensure_collection(collection)
        rv = coll.fetch(*args, **kwargs)
        return Response(datas=rv)

    def get_collection(self, collection):
        """
        """
        pass

    def get_collection_info(self, collection):
        """
        """
        pass

    def get_collections(self):
        """
        """
        rv = list(CacheCtl.collmap.keys())
        return Response(datas=rv)

    def get_info(self):
        """
        """
        pass

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
