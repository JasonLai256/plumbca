# -*- coding: utf-8 -*-
"""
    plumbca.constants
    ~~~~~~~~~~~~~~~~~

    Provide some useful constants variable for the plumbca.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import zmq

import plumbca.log


BACKEND_IPC = 'inproc://plumbca'
ZCONTEXT = zmq.Context()
