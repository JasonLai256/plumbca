# -*- coding:utf-8 -*-

# Copyright (c) 2015 jason lai
#
# See the file LICENSE for copying permission.

from configparser import ConfigParser

from .exceptions import PlumbcaConfigNotFound


defaults = {
    'global': {
        'daemonize': 'no',
        'pidfile': '/var/run/plumbca.pid',
        'bind': '127.0.0.1',
        'port': '4273',
        'logfile': '/var/log/plumbca.log',
        'dumpfilename': 'dump.save',
        'dumpdir': '/var/lib/plumbca/',
    },
}


class Config(dict):

    def __init__(self, section):
        super().__init__()
        self._section = section
        self.update(defaults[section])

    def readFrom(self, f):
        config = ConfigParser()

        rv = config.read(f)
        if not rv:
            raise PlumbcaConfigNotFound('Failed to read the config file {}'.format(f))

        self.update({k: v for k, v in config.items(self._section)})


DefaultConf = Config('global')
