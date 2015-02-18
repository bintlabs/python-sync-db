"""
.. module:: dbsync.dialects
   :synopsis: DBMS-dependent statements.
"""

from sqlalchemy import func

from dbsync.utils import class_mapper, get_pk


def begin_transaction(session):
    """
    Returns information of the state the database was on before the
    transaction began.
    """
    engine = session.bind
    dialect = engine.name
    if dialect == 'sqlite':
        cursor = engine.execute("PRAGMA foreign_keys;")
        state = cursor.fetchone()[0]
        cursor.close()
        engine.execute("PRAGMA foreign_keys = OFF;")
        engine.execute("BEGIN EXCLUSIVE TRANSACTION;")
        return state
    if dialect == 'mysql':
        # temporal by default
        # see http://dev.mysql.com/doc/refman/5.7/en/using-system-variables.html
        engine.execute("SET foreign_key_checks = 0;")
        return None
    return None


def end_transaction(state, session):
    """
    *state* is whatever was returned by :func:`begin_transaction`
    """
    engine = session.bind
    dialect = engine.name
    if dialect == 'sqlite':
        if state not in (0, 1): state = 1
        engine.execute("PRAGMA foreign_keys = {0}".format(int(state)))


def max_local(sa_class, session):
    """
    Returns the maximum primary key used for the given table.
    """
    engine = session.bind
    dialect = engine.name
    table_name = class_mapper(sa_class).mapped_table.name
    if dialect == 'sqlite':
        cursor = engine.execute("SELECT seq FROM sqlite_sequence WHERE name = ?",
                                table_name)
        result = cursor.fetchone()[0]
        cursor.close()
        return result
    # default, strictly incorrect query
    return session.query(func.max(getattr(sa_class, get_pk(sa_class)))).scalar()
