# -*- coding:utf-8 -*-
"""
    plumbca.config
    ~~~~~~~~~~~~~~

    Implements the configuration related objects.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

from configparser import ConfigParser

from .exceptions import PlumbcaConfigNotFound


defaults = {
    'global': {
        'debug': '',
        'daemonize': 'no',
        'pidfile': '/var/run/plumbca.pid',
        'bind': '127.0.0.1',
        'port': '4273',
        'transport': 'tcp',
        'unixsocket': '',
        'logfile': '/var/log/plumbca.log',
        'dumpfilename': 'dump.save',
        'dumpdir': '/var/lib/plumbca/',
        'activity_log': '/var/log/plumbca/plumbca.log',
        'errors_log': '/var/log/plumbca/plumbca_errors.log',
        # 'activity_log': 'plumbca_activity.log',
        # 'errors_log': 'plumbca_errors.log',
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
