#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

with open("README.md") as f:
    long_description = f.read()


CLASSIFIERS = [
    "Development Status :: 1 - Planning",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Intended Audience :: Science/Research",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Topic :: Scientific/Engineering",
]

setup(
    name="pangeo-forge",
    description="Pipeline tools for building and publishing analysis ready datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    maintainer="Joe Hamman",
    maintainer_email="jhamman@ucar.edu",
    classifiers=CLASSIFIERS,
    url="https://github.com/pangeo-forge/pangeo-forge",
    packages=find_packages(),
    package_dir={"pangeo_forge": "pangeo_forge"},
    include_package_data=True,
    install_requires=install_requires,
    license="Apache",
    zip_safe=False,
    keywords=["pangeo", "data"],
    entry_points=dict(console_scripts=["pangeo-forge = pangeo_forge.cli:main"]),
)
