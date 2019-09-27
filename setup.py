from setuptools import setup, find_packages
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

pfile = Project(chdir=False).parsed_pipfile
requirements = convert_deps_to_pip(pfile['packages'], r=False)
dev_requirements = convert_deps_to_pip(pfile['dev-packages'], r=False)

setup(
    name='pyhouse',
    version='0.0.9',
    description='Python Lighthouse',
    author='Dataminded',
    license="Apache-2.0",
    author_email='dev@dataminded.be',
    url='https://www.dataminded.be',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=requirements,
    test_requires=dev_requirements,
    package_data={
        'pyhouse': ['py.typed'],
    },
    zip_safe=False,
)
