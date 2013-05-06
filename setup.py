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
        'SQLAlchemy>=0.8.0',
        'django>=1.3.1',
        'couchdbkit>=0.5.7',
        'six>=1.3.0',
        'alembic>=0.5.0'
    ]
)