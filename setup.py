from setuptools import setup, find_packages


import tables

VERSION = tables.__version__

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
	name="tables-m2", 
	version=VERSION,
	author="Jay Kim",
	description="Hierarchical numpy memmap datasets for Python",
	long_description=long_description,
	long_description_content_type="text/x-rst",
	url="https://github.com/mozjay0619/tables-m2",
	license="DSB 3-clause",
	packages=find_packages(),
	install_requires=[]
	)
