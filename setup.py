from distutils.core import setup

setup(name='dbsync',
      version='0.4.0',
      url='https://github.com/bintlabs/python-sync-db',
      author='Bint',
      packages=['dbsync', 'dbsync.client', 'dbsync.server', 'dbsync.messages'],
      description='Centralized database synchronization for SQLAlchemy',
      install_requires=['sqlalchemy>=0.8.0', 'requests'],
      license='MIT',)
