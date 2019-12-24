#!/usr/bin/env python
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

requires =  ['safeoutput>=2.0', 'future']
test_requires = ['tox', 'pytest']

setup(
    name='S3Lib',
    version='1.0.0',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['s3lib'],
    install_requires = requires,
    tests_require = test_requires,
    entry_points = {
      'console_scripts': [
        's3ls   = s3lib.ui:ls_main',
        's3get  = s3lib.ui:get_main',
        's3cp   = s3lib.ui:cp_main',
        's3head = s3lib.ui:head_main',
        's3put  = s3lib.ui:put_main',
        's3rm   = s3lib.ui:rm_main',
        's3sign = s3lib.ui:sign_main',
        ],
    },
    url='http://pypi.python.org/pypi/S3Lib/',
    license='LICENSE.txt',
    description='Library and utilities for interfacing wtih S3',
    long_description_content_type='text/markdown',
    long_description=open('README.md').read(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 2.7',
    ],
)
