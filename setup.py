from distutils.core import setup
import dbsync

setup(name='dbsync',
      version=dbsync.__version__,
      url='https://github.com/bintlabs/python-sync-db',
      author='Bint',
      packages=['dbsync', 'dbsync.client', 'dbsync.server', 'dbsync.messages'],
      description='Centralized database synchronization for SQLAlchemy',
      install_requires=['sqlalchemy>=0.8.0', 'requests'],
      license='MIT',)
