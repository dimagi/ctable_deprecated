import os.path
import glob
from setuptools import setup, find_packages

setup(
    name='ctable',
    version='0.0.1',
    description='CouchDB view to SQL table',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/ctable',
    packages=['ctable'],
    include_package_data=True,
    test_suite='tests',
    test_loader='unittest2:TestLoader',
    license='MIT',
    install_requires=[
        'SQLAlchemy>=0.8.1',
        'django>=1.3.1',
        'couchdbkit>=0.5.7',
        'six>=1.2.0',
        'alembic>=0.5.0',
        'celery>=3.0.15',
        'psycopg2>=2.4.1',
        'pillowfluff>=0.0.1',
    ],
    tests_require=[
        'unittest2',
        'fakecouch',
    ],
    dependency_links=[
        'http://github.com/dimagi/fluff/tarball/master#egg=pillowfluff-0.0.1',
    ],
)
