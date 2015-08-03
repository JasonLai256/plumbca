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
        'dumpdir': '/var/lib/plumbca/',
        'write_log': '/var/log/plumbca/write-opes.log',
        'activity_log': '/var/log/plumbca/plumbca.log',
        'errors_log': '/var/log/plumbca/plumbca_errors.log',
        'mark_version': '1.0',
        'backend': 'redis',
    },
    'redis': {
        'host': '127.0.0.1',
        'port': '',
        'db': '0',
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

        self.update({k: [i.strip() for i in v.split(',')] if ',' in v else v.strip()
                         for k, v in config.items(self._section)})
        self['debug'] = bool(self.get('debug'))


CONFIG_PATH = '/etc/plumbca.conf'
DefaultConf = Config('global')
DefaultConf.readFrom(CONFIG_PATH)
RedisConf = Config('redis')
RedisConf.readFrom(CONFIG_PATH)
