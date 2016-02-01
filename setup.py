#!/usr/bin/env python

from setuptools import setup, find_packages
version = 'v1.1 Beta'
setup(
	name = 'pys3io',
	version = version,
	description = 'A very helpful python -- S3 (AWS) input output that includes reading and streaming files line by line',
	author = 'kod5kod Datuman',
	url = 'https://github.com/kod5kod',
	include_package_data = True,
	packages = find_packages(),
	install_requires = [
            boto,
		]
)





