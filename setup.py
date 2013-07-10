#!/usr/bin/env python

from distutils.core import setup

setup(
    name='S3Lib',
    version='0.1.0',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['s3lib', 'stats.test'],
    scripts=['bin/copy_object', 'bin/head_object', 'bin/list_bucket', 'bin/put_object'],
    url='http://pypi.python.org/pypi/Stats/',
    license='LICENSE.txt',
    description='Library and utilities for interfacing wtih S3',
    long_description=open('README.txt').read(),
)
