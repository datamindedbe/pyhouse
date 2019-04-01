from setuptools import setup


setup(name='lighthouse',
      version='0.1',
      description='Python Lighthouse',
      author='Dataminded',
      packages=[
         "lighthouse"
      ],
      install_requires=[
          'Pipfile',
          'boto3',
          'pyspark'
      ],
      include_package_data=True,
      zip_safe=False)
