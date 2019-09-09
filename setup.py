from setuptools import setup
from setuptools import find_packages, setup

setup(
    name='datalake_cli',
    version='0.1',
    py_modules=['datalake_cli'],
    packages=find_packages(exclude=[
        'tests',
        # Exclude packages that will be covered by PEP420 or nspkg
        'azure',
        'azure.datalake',
        'azure.datalake.client'
    ]),
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        datalake_cli=azure.datalake.client.sample.datalake_cli:cli
    ''',
)