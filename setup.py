#!/usr/bin/env python

from setuptools import setup

# with open("README.md", "r") as fh:
#         long_description = fh.read()

setup(name='sociophysicsDataHandler',
      description="basic description",
      version='0.1',
      author='Alessandro Corbetta, Cas Pouw',
      author_email='a.corbetta@tue.nl',
      url='https://github.com/acorbe/autogpy',
      long_description="we will have a long description",
      #     long_description_content_type="text/markdown",
      packages=['sociophysicsDataHandler'],
      license="new BSD",
      install_requires=['numpy>=1.8', 'matplotlib>=2.0'
                        , 'scipy>=1.4', 'pyocclient']
)
