#!/usr/bin/env python

from setuptools import setup

setup(
    name='target-sftp',
    version='1.0.0',
    description='hotglue target for exporting data to sftp',
    author='hotglue',
    url='https://hotglue.xyz',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_sftp'],
    install_requires=[
        'argparse==1.4.0',
        'singer-python==5.9.0',
        'paramiko==2.6.0',
        'backoff==1.8.0',
    ],
    entry_points='''
        [console_scripts]
        target-sftp=target_sftp:main
    ''',
    packages=['target_sftp']
)
