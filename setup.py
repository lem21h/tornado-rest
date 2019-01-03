#!/usr/bin/env python
# coding=utf-8
from setuptools import setup

with open("requirements.txt", "r") as file:
    requirements = file.readlines()

setup(
    name="tornado-rest",
    version="1.0",
    packages=['tornado-rest.core'],
    install_requires=requirements,
    python_requires='>=3.7',
    zip_safe=True,
    author="Maio",
    author_email="konradend@gmail.com",
    description="Tornado Rest Core module with Mongo integration",
)
