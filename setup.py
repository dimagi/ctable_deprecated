try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='ctable',
    version='0.0.1',
    description='CouchDB view to SQL table',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/ctable',
    packages=['ctable'],
    test_suite='tests',
    test_loader='unittest2:TestLoader',
    license='MIT',
    install_requires=[
        'SQLAlchemy',
        'django',
        'couchdbkit',
        'six',
        'alembic'
    ],
    tests_require=[
        'unittest2',
        'fakecouch',
    ],
    dependency_links=[
        'http://github.com/dimagi/fakecouch/tarball/master#egg=fakecouch-0.0.1',
    ],
)
