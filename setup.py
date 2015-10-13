from setuptools import setup

setup(
    name='ctable',
    version='0.0.7',
    description='CouchDB view to SQL table',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/ctable',
    packages=['ctable', 'ctable_view'],
    include_package_data=True,
    license='MIT',
    install_requires=[
        'SQLAlchemy==0.8.2',
        'django<1.9',
        'jsonobject-couchdbkit>=0.7.0.1',
        'six>=1.2.0',
        'alembic==0.6.0',
        'celery>=3.0.15',
        'psycopg2>=2.4.1',
        'pillowfluff>=0.0.4',
    ],
    tests_require=[
        'fakecouch==0.0.6',
        'mock>=0.0.8'
    ]
)
