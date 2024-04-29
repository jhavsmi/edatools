from setuptools import setup, find_packages

setup(name='edatools',
      version='0.1',
      description='A set of functions for data cleaning and exploratory data analysis',
      author='Joel Smith',
      author_email='jhavsmi@pm.me',
      packages=find_packages(),
      install_requires=[
            'pandas', 'datetime','setuptools','wheel','twine'
      ]
)
