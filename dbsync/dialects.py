"""
.. module:: dbsync.dialects
   :synopsis: DBMS-dependent statements.
"""


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
