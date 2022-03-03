#!/usr/bin/env python

from setuptools import setup

with open("Readme.md", "r") as fh:
        long_description = fh.read()

setup(name='sociophysicsDataHandler',
      description="Data retriever package for the Sociophysics course at TU/Eindhoven.",
      version='1.2.5',
      author='Alessandro Corbetta; Cas Pouw; Joris Willems',
      author_email='a.corbetta@tue.nl',
      url='https://github.com/acorbe/sociophysicsDataHandler/archive/refs/tags/v1.2.5.tar.gz',
      long_description = long_description,
      long_description_content_type="text/markdown",
      packages=['sociophysicsDataHandler'],
      license="new BSD",
      install_requires=['numpy>=1.8', 'matplotlib>=2.0'
                        , 'scipy>=1.4', 'pyocclient', 'pyarrow'
                        , 'pandas', 'pillow']
)
