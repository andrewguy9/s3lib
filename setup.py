#!/usr/bin/env python
import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

requires =  ['safeoutput>=2.0']
test_requires = requires + ['pytest==4.6']

class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name='S3Lib',
    version='1.0.0',
    author='Andrew Thomson',
    author_email='athomsonguy@gmail.com',
    packages=['s3lib', 's3lib.test'],
    install_requires = requires,
    tests_require = test_requires,
    cmdclass = {'test': PyTest},
    test_suite = 'pytest',
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
    long_description=open('README.md').read(),
)
