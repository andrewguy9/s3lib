#!/usr/bin/env python

from setuptools import setup

setup(
    name='S3Lib',
    version='0.3.0',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['s3lib', 's3lib.test'],
    entry_points = {
      'console_scripts': [
        's3ls   = s3lib.ui:ls_main',
        's3get  = s3lib.ui:get_main',
        's3cp   = s3lib.ui:cp_main',
        's3head = s3lib.ui:head_main',
        's3put  = s3lib.ui:put_main',
        's3rm   = s3lib.ui:rm_main',
        's3sign   = s3lib.ui:sign_main',
        ],
    },
    url='http://pypi.python.org/pypi/S3Lib/',
    license='LICENSE.txt',
    description='Library and utilities for interfacing wtih S3',
    long_description=open('README.txt').read(),
)
