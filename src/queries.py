"""Database query functions for retrieving and analysing OHLC data.

All public functions return Polars DataFrames.  Each function opens and closes
its own connection, so callers do not need to manage connection lifetime.
"""

from __future__ import annotations

import logging

import polars as pl
import psycopg2

from db import get_connection

# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _execute_query(
    conn: psycopg2.extensions.connection,
    query: str,
    params: tuple = (),
) -> pl.DataFrame:
    """Execute *query* and return results as a Polars DataFrame.

    Uses the raw psycopg2 cursor directly — no dependency on pandas or on
    Polars' connection-string backend, both of which have version-dependent
    behaviour.
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        if cur.description is None:
            return pl.DataFrame()
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        return pl.DataFrame({col: [] for col in columns})

    # Transpose list-of-tuples → dict-of-lists for the Polars constructor.
    return pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    )


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


def get_recent_bars(
    symbol: str = "DAX",
    timeframe: str = "1H",
    days: int = 30,
) -> pl.DataFrame:
    """Return recent OHLC bars sorted ascending by timestamp."""
    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlc
        WHERE symbol = %s
          AND timeframe = %s
          AND timestamp > NOW() - (%s * INTERVAL '1 day')
        ORDER BY timestamp
    """
    conn = get_connection()
    try:
        return _execute_query(conn, query, (symbol, timeframe, days))
    finally:
        conn.close()


def get_all_timeframes(symbol: str = "DAX", days: int = 7) -> pl.DataFrame:
    """Return bars across all stored timeframes for *symbol*."""
    query = """
        SELECT timestamp, timeframe, open, high, low, close, volume
        FROM ohlc
        WHERE symbol = %s
          AND timestamp > NOW() - (%s * INTERVAL '1 day')
        ORDER BY timestamp
    """
    conn = get_connection()
    try:
        return _execute_query(conn, query, (symbol, days))
    finally:
        conn.close()


def calculate_daily_returns(symbol: str = "DAX", days: int = 30) -> pl.DataFrame:
    """Return daily close prices with period returns and cumulative returns.

    *return* is expressed in percent.
    *cumulative_return* is expressed as a fraction (0.05 = +5 %).
    The first row (where *return* is null) is dropped.
    """
    df = get_recent_bars(symbol, "1D", days)
    df = df.with_columns(
        [
            (
                (pl.col("close") - pl.col("close").shift(1))
                / pl.col("close").shift(1)
                * 100
            ).alias("return"),
            (pl.col("close") / pl.col("close").first() - 1).alias("cumulative_return"),
        ]
    )
    return df.select(["timestamp", "close", "return", "cumulative_return"]).drop_nulls(
        subset=["return"]
    )


def get_data_coverage(symbol: str = "DAX") -> pl.DataFrame:
    """Return per-timeframe row counts and timestamp range for *symbol*."""
    query = """
        SELECT
            timeframe,
            MIN(timestamp) AS earliest,
            MAX(timestamp) AS latest,
            COUNT(*)       AS bar_count
        FROM ohlc
        WHERE symbol = %s
        GROUP BY timeframe
        ORDER BY timeframe
    """
    conn = get_connection()
    try:
        return _execute_query(conn, query, (symbol,))
    finally:
        conn.close()


def find_largest_moves(
    symbol: str = "DAX",
    timeframe: str = "1H",
    limit: int = 10,
) -> pl.DataFrame:
    """Return the *limit* bars with the largest high-to-low range (% basis)."""
    query = """
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            (high - low)             AS range,
            (high - low) / low * 100 AS range_pct
        FROM ohlc
        WHERE symbol = %s AND timeframe = %s
        ORDER BY range_pct DESC
        LIMIT %s
    """
    conn = get_connection()
    try:
        return _execute_query(conn, query, (symbol, timeframe, limit))
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    coverage = get_data_coverage()
    logging.info("Data coverage:\n%s", coverage)

    recent_1h = get_recent_bars("DAX", "1H", days=1).tail(5)
    logging.info("Recent 1H bars (last 5):\n%s", recent_1h)

    big_moves = find_largest_moves("DAX", "1H", limit=5)
    logging.info(
        "Largest 1H moves:\n%s", big_moves.select(["timestamp", "range", "range_pct"])
    )
