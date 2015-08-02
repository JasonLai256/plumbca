# -*- coding:utf-8 -*-
"""
    plumbca.helpers
    ~~~~~~~~~~~~~~~

    Implements various helpers.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from bisect import bisect_left
from functools import partial

from msgpack import packb, unpackb


packb = partial(packb)
unpackb = partial(unpackb, encoding='utf-8')


def frame2str(frame):
    return frame.bytes.decode('utf8').lower()


def find_eq(a, x, ret_index=False):
    '''Find the leftmost item exactly equal to x'''
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i if ret_index else a[i]
    raise ValueError('Not found the value that == {}\n'.format(x))


def find_ge(a, x, ret_index=False):
    '''Find leftmost item greater than or equal to x'''
    i = bisect_left(a, x)
    if i != len(a):
        return i if ret_index else a[i]
    raise ValueError('Not found the value that >= {}\n'
                     '(The maximun item is {})'.format(x, a[-1]))


def find_lt(a, x, ret_index=False):
    '''Find rightmost value less than x'''
    i = bisect_left(a, x)
    if i:
        return i-1 if ret_index else a[i-1]
    raise ValueError('Not found the value that < {} \n'
                     '(The minimun item is {})'.format(x, a[0]))
