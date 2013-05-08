try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='ctable',
    version='0.1.0',
    description='Basic CouchDB view to SQL table ETL',
    author='Simon Kelly',
    author_email='skelly@dimagi.com',
    url='http://github.com/dimagi/ctable',
    packages=['ctable'],
    license='MIT',
    install_requires=[
        'SQLAlchemy',
        'django',
        'couchdbkit',
        'six',
        'alembic'
    ],
    tests_require=[
        'dimagi-test-utils'
    ],
    dependency_links=[
        'http://github.com/dimagi/test-utils/tarball/master#egg=dimagi-test-utils-1.0.0',
    ],
)