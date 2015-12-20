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

from .collection import IncreaseCollection
from .cache import CacheCtl
from .message import Request, Response, message_process_failure
from . import constants


wrtlog = logging.getLogger('write-opes')
actlog = logging.getLogger('activity')
errlog = logging.getLogger('errors')


class Worker:
    """
    Class that handles commands server side.
    Translates, messages commands to it's methods calls.
    """
    def __init__(self):
        pass

    def __del__(self):
        self.sock.close()

    async def run_command(self, req):
        try:
            func = getattr(self, req.command)
            response = func(*req.args)
        except Exception as err:
            error_track = traceback.format_exc()
            errmsg = '%s\n%s' % (err, error_track)
            errmsg = '<WORKER> Unknown situation occur: %s' % errmsg
            errlog.error(errmsg)

            response = Response(datas=errmsg,
                                status=message_process_failure)
        return response

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

    async def store(self, collection, *args, **kwargs):
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
        await coll.store(*args, **kwargs)
        return Response(datas='Store OK')

    async def query(self, collection, *args, **kwargs):
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
        rv = await coll.query(*args, **kwargs)
        rv = list(rv) if rv else []
        return Response(datas=rv)

    async def fetch(self, collection, *args, **kwargs):
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
        rv = await coll.fetch(*args, **kwargs)
        rv = list(rv) if rv else []
        return Response(datas=rv)

    def get_collections(self):
        """
        """
        actlog.info('<WORKER> handling Get_collections command ...')
        rv = list(CacheCtl.collmap.keys())
        return Response(datas=rv)

    async def ensure_collection(self, name, coll_type='IncreaseCollection',
                          expired=3600):
        actlog.info('<WORKER> handling ENSURE_COLLECTION command - %s, %s, %s ...',
                    name, coll_type, expired)
        await CacheCtl.ensure_collection(name, coll_type, expired)
        assert name in CacheCtl.collmap
        return Response(datas='Ensure OK')

    def _gen_response(self, request, cmd_status, cmd_value):
        if cmd_status == FAILURE_STATUS:
            header = ResponseHeader(status=cmd_status, err_code=cmd_value[0],
                                    err_msg=cmd_value[1])
            content = ResponseContent(datas=None)
        else:
            if 'compression' in request.meta:
                compression = request.meta['compression']
            else:
                compression = False

            header = ResponseHeader(status=cmd_status, compression=compression)
            content = ResponseContent(datas=cmd_value, compression=compression)

        return header, content
