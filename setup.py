from setuptools import setup

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='py-lighthouse',
      version='0.0.1',
      description='Python Lighthouse',
      author='Dataminded',
      license="Apache-2.0",
      author_email='dev@dataminded.be',
      url='https://www.dataminded.be',
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=[
         "lighthouse"
      ],
      install_requires=[
          'Pipfile',
          'boto3',
          'pyspark'
      ])
