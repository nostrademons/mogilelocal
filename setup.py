#!/usr/bin/env python
from setuptools import setup

setup(
    name='MogileLocal',
    version='0.9.0',
    py_modules=['mogilelocal'],
    zip_safe=True,

    author='Jonathan Tang',
    author_email='jonathan.d.tang@gmail.com',
    license='MIT License',
    platforms='Any',
    url='http://jonathan.tang.name/code/mogilelocal',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python'
    ],
    description='MogileFS client that uses the local filesystem as backing storage',
    long_description="A library that's API-compatible with the Python MogileFS client, yet uses the local filesystem so you don't need a full tracker/storage distributed system.  Intended for testing, development, and small low-traffic websites."
)
