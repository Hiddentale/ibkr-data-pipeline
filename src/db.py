"""Database connection factory.

Single source of truth for the default connection parameters.
All modules that need a database connection import from here.
"""

from __future__ import annotations

import psycopg2

_DEFAULT_CONFIG: dict = {
    "host": "localhost",
    "database": "trading",
    "user": "postgres",
    "password": "trading123",
    "port": 5433,
}


def get_connection(config: dict | None = None) -> psycopg2.extensions.connection:
    """Open and return a new psycopg2 connection.

    Parameters
    ----------
    config:
        Connection parameters.  Defaults to _DEFAULT_CONFIG if omitted.
    """
    return psycopg2.connect(**(config or _DEFAULT_CONFIG))
