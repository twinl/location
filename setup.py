#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

# To update the package version number, edit CITATION.cff
with open('CITATION.cff', 'r') as cff:
    for line in cff:
        if 'version:' in line:
            version = line.replace('version:', '').strip().strip('"')

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup(
    name='location',
    version=version,
    description="location",
    long_description=readme + '\n\n',
    author="Erik Tjong Kim Sang",
    author_email='e.tjongkimsang@esciencecenter.nl',
    url='https://github.com/twinl/location',
    packages=[
        'location',
    ],
    include_package_data=True,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='location',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    install_requires=[],  # FIXME: add your package's dependencies to this list
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'sphinx_rtd_theme',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    },
    data_files=[('citation/location', ['CITATION.cff'])]
)
