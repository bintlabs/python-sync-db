from distutils.core import setup

setup(name='dbsync',
      version='0.1.0',
      author='Bint',
      packages=['dbsync', 'dbsync.client', 'dbsync.server', 'dbsync.messages'],
      description='Centralized database synchronization for SQLAlchemy',)
