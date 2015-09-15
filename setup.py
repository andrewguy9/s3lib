#!/usr/bin/env python

from distutils.core import setup

setup(
    name='S3Lib',
    version='0.1.0',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['s3lib', 's3lib.test'],
    scripts=['bin/s3cp', 'bin/s3head', 'bin/s3rm', 'bin/s3ls', 'bin/s3put'],
    url='http://pypi.python.org/pypi/S3Lib/',
    license='LICENSE.txt',
    description='Library and utilities for interfacing wtih S3',
    long_description=open('README.txt').read(),
)
