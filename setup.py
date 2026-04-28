#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mock',
      version='1.0.0',
      description='Singer.io tap for generating mock data for testing',
      author='Your Name',
      url='https://github.com/yourusername/tap-mock',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'hotglue-singer-sdk>=1.0.29',
      ],
      entry_points='''
          [console_scripts]
          tap-mock=tap_mock.tap:TapMock.cli
      ''',
      packages=['tap_mock'],
      include_package_data=True,
) 