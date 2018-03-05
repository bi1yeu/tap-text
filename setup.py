#!/usr/bin/env python

from setuptools import setup

setup(name='tap-text',
      version='0.0.0',
      description='Singer.io tap for extracting data from text files',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_text'],
      install_requires=[
          'singer-python',
      ],
      entry_points='''
          [console_scripts]
          tap-text=tap_text:main
      ''',
      packages=['tap_text'],
)
