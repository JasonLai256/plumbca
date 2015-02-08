# -*- coding:utf-8 -*-

# Copyright (c) 2015 jason lai
#
# See the file LICENSE for copying permission.

from bisect import bisect_left


def find_ge(a, x):
    '''Find leftmost item greater than or equal to x'''
    i = bisect_left(a, x)
    if i != len(a):
        return a[i]
    raise ValueError('Not found the value that >= {}\n'
                     '(The maximun item is {})'.format(x, a[-1]))


def find_lt(a, x):
    '''Find rightmost value less than x'''
    i = bisect_left(a, x)
    if i:
        return a[i-1]
    raise ValueError('Not found the value that < {} \n'
                     '(The minimun item is {})'.format(x, a[0]))
