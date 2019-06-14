from setuptools import setup


setup(name='py-lighthouse',
      version='0.1',
      description='Python Lighthouse',
      author='Dataminded',
      author_email='dev@dataminded.be',
      url='https://www.dataminded.be',
      packages=[
         "lighthouse"
      ],
      install_requires=[
          'Pipfile',
          'boto3',
          'pyspark'
      ])
