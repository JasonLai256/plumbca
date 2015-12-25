# -*- coding:utf-8 -*-

from asyncio import coroutine, futures

from plumbca.helpers import packb


class CoroWraps:

    def __init__(self, s=None):
        self.s = s

    @coroutine
    def __call__(self, *args):
        future = futures.Future()
        h = future._loop.call_soon(future._set_result_unless_cancelled,
                                   self.s)
        try:
            return (yield from future)
        finally:
            h.cancel()


def req(command, args):
    command = command.encode('utf8')
    args = {
        'args': args
    }
    args = packb(args)
    return b' '.join([command, args])


def call_first_args(call):
    """The call objects in Mock.call_args and Mock.call_args_list are
    two-tuples of (positional args, keyword args)"""
    return call.call_args[0][0]
