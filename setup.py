from setuptools import setup


setup(name='lighthouse',
      version='0.1',
      description='Python Lighthouse',
      author='Dataminded',
      author_email='dev@dataminded.be',
      url='www.dataminded.be',
      packages=[
         "lighthouse"
      ],
      install_requires=[
          'Pipfile',
          'boto3',
          'pyspark'
      ])
