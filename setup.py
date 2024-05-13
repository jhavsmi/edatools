from setuptools import setup

setup(name='edatools',
      version='0.1',
      description='A set of functions for data cleaning and exploratory data analysis',
      author='Joel Smith',
      author_email='jhavsmi@pm.me',
      packages=['edatools'],
      install_requires=[
            'pandas', 'datetime'
      ]
)
