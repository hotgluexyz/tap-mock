#!/usr/bin/env python

from setuptools import setup

setup(name='tap-mock',
      version='1.0.0',
      description='Singer.io tap for generating mock data for testing',
      author='Your Name',
      url='https://github.com/yourusername/tap-mock',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_mock'],
      install_requires=[
          'singer-python>=5.0.0',
      ],
      entry_points='''
          [console_scripts]
          tap-mock=tap_mock:main
      ''',
      packages=['tap_mock'],
      include_package_data=True,
) 