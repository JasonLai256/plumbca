# import sys

from setuptools import setup, find_packages

from plumbca import __version__

setup(
    name='plumbca',
    version=__version__,
    install_requires=[
        'pyzmq',
        'msgpack-python',
        'redis',
    ],
    scripts=['bin/plumbca-server'],
    packages=find_packages(exclude=["tests"]),
    license='BSD',
    author='Jason Lai',
    author_email='jasonlai256@gmail.com',
    description='.',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ]
)
