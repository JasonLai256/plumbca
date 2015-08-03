# -*- coding: utf-8 -*-
"""
    plumbca.logging
    ~~~~~~~~~~~~~~~

    Implements the logging support for Plumbca.

    :copyright: (c) 2015 by Jason Lai.
    :license: BSD, see LICENSE for more details.
"""

import logging.config
import logging

from .config import DefaultConf


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'simple': {'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'},
        'debug': {
            'format': '-' * 80 + '\n' +
                      '[%(asctime)s] %(levelname)s in %(module)s [%(pathname)s:' +
                      '%(lineno)d]:\n%(message)s\n' +
                      '-' * 80
        }
    },
    'filters': {
        'special': {'foo': 'bar'}
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'activity_rotating_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'simple',
            'filename': DefaultConf['activity_log'],
            'backupCount': 9,
            'maxBytes': 52428800
        },
        'writing_rotating_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'simple',
            'filename': DefaultConf['write_log'],
            'backupCount': 19,
            'maxBytes': 52428800
        },
        'errors_rotating_file': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'debug',
            'filename': DefaultConf['errors_log'],
            'backupCount': 9,
            'maxBytes': 52428800
        },
    },
    'loggers': {
        'write-opes': {
            'handlers': ['console', 'writing_rotating_file'] if DefaultConf.get('debug', '') == 'yes' else ['writing_rotating_file'],
            'propagate': True,
            'level': 'INFO',
        },
        'activity': {
            'handlers': ['console', 'activity_rotating_file'] if DefaultConf.get('debug', '') == 'yes' else ['activity_rotating_file'],
            'propagate': True,
            'level': 'INFO',
        },
        'errors': {
            'handlers': ['console', 'errors_rotating_file'] if DefaultConf.get('debug', '') == 'yes' else ['errors_rotating_file'],
            'level': 'ERROR',
            'propagate': False,
        }
    }
}

logging.config.dictConfig(LOGGING)
# activity_logger = logging.getLogger('activity')
# errors_logger = logging.getLogger('errors')
