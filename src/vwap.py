"""Session-anchored VWAP computation and storage.

VWAP resets at 09:00 CET each trading day and is derived from 1m bars.
Standard-deviation bands (±1σ, ±2σ) use volume-weighted variance so that
high-volume bars contribute more to the dispersion estimate.

No external statistics libraries are used.  All computation is performed
with Polars built-ins and the König–Huygens variance identity:

    Var(tp) = E[tp²] - E[tp]²          (volume-weighted expectations)

where the expectations are updated cumulatively bar-by-bar within each
trading session.
"""

from __future__ import annotations

import logging

import polars as pl
import psycopg2
from psycopg2.extras import execute_values

from db import get_connection

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def create_vwap_table(cursor) -> None:
    """Create the vwap table (and its hypertable) if they do not yet exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vwap (
            timestamp    TIMESTAMPTZ  NOT NULL,
            symbol       TEXT         NOT NULL,
            session_date DATE         NOT NULL,
            vwap         NUMERIC(18,5),
            upper_1      NUMERIC(18,5),
            lower_1      NUMERIC(18,5),
            upper_2      NUMERIC(18,5),
            lower_2      NUMERIC(18,5),
            PRIMARY KEY (timestamp, symbol)
        )
    """)
    try:
        cursor.execute("""
            SELECT create_hypertable('vwap', 'timestamp', if_not_exists => TRUE)
        """)
    except Exception as exc:
        log.warning("Could not create vwap hypertable: %s", exc)


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def _compute_cumulative_weighted_sums(df: pl.DataFrame) -> pl.DataFrame:
    """Add typical price, per-bar weighted values, and session-cumulative sums.

    Adds columns: tp, tp_vol, tp2_vol, vol_f, cum_tp_vol, cum_tp2_vol, cum_vol.
    These are the inputs required to compute VWAP and its variance.
    """
    df = df.with_columns(
        ((pl.col("high") + pl.col("low") + pl.col("close")) / 3).alias("tp")
    )
    df = df.with_columns([
        (pl.col("tp") * pl.col("volume")).alias("tp_vol"),
        (pl.col("tp") ** 2 * pl.col("volume")).alias("tp2_vol"),
        pl.col("volume").cast(pl.Float64).alias("vol_f"),
    ])
    # Cumulative sums partition-by session; row order within each partition
    # follows the DataFrame sort order (ascending timestamp).
    return df.with_columns([
        pl.col("tp_vol").cum_sum().over("session_date").alias("cum_tp_vol"),
        pl.col("tp2_vol").cum_sum().over("session_date").alias("cum_tp2_vol"),
        pl.col("vol_f").cum_sum().over("session_date").alias("cum_vol"),
    ])


def _compute_std_bands(df: pl.DataFrame) -> pl.DataFrame:
    """Add ±1σ / ±2σ bands from an existing *vwap* column.

    Uses the König–Huygens identity to derive volume-weighted variance:
        variance = cum_tp2_vol / cum_vol - vwap²
    Variance is clamped to zero to absorb floating-point noise.
    """
    df = df.with_columns(
        (pl.col("cum_tp2_vol") / pl.col("cum_vol") - pl.col("vwap") ** 2)
        .clip(lower_bound=0)
        .alias("variance")
    )
    df = df.with_columns(pl.col("variance").sqrt().alias("std"))
    return df.with_columns([
        (pl.col("vwap") + pl.col("std")).alias("upper_1"),
        (pl.col("vwap") - pl.col("std")).alias("lower_1"),
        (pl.col("vwap") + 2 * pl.col("std")).alias("upper_2"),
        (pl.col("vwap") - 2 * pl.col("std")).alias("lower_2"),
    ])


def compute_session_vwap(df: pl.DataFrame) -> pl.DataFrame:
    """Compute session-anchored VWAP and ±1σ / ±2σ bands from 1m bars.

    Parameters
    ----------
    df:
        Must contain columns: timestamp, session_date, high, low, close, volume.
        Rows must already be sorted ascending by timestamp (the SQL query in
        _fetch_1m_bars guarantees this).  Zero-volume bars must be removed
        before calling (also handled in _fetch_1m_bars).

    Returns
    -------
    DataFrame with columns:
        timestamp, session_date, vwap, upper_1, lower_1, upper_2, lower_2.
    """
    df = _compute_cumulative_weighted_sums(df)
    df = df.with_columns(
        (pl.col("cum_tp_vol") / pl.col("cum_vol")).alias("vwap")
    )
    df = _compute_std_bands(df)
    return df.select([
        "timestamp", "session_date",
        "vwap", "upper_1", "lower_1", "upper_2", "lower_2",
    ])


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _fetch_1m_bars(conn: psycopg2.extensions.connection, symbol: str) -> pl.DataFrame:
    """Load all 1m bars for *symbol* with session_date computed in CET/CEST.

    Only bars from 09:00 CET onwards are returned (regular trading hours).
    Zero-volume bars are excluded before the DataFrame is returned so that
    compute_session_vwap never divides by zero.

    session_date is derived entirely in SQL to avoid timezone-conversion
    edge cases in Python — particularly around CET/CEST daylight-saving
    transitions.
    """
    query = """
        SELECT
            timestamp,
            high,
            low,
            close,
            volume,
            DATE(timestamp AT TIME ZONE 'Europe/Berlin')              AS session_date,
            EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Europe/Berlin') AS hour_cet
        FROM ohlc
        WHERE symbol = %s
          AND timeframe = '1m'
          AND volume > 0
        ORDER BY timestamp
    """
    with conn.cursor() as cur:
        cur.execute(query, (symbol,))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})

    # Cast NUMERIC columns to Float64 for arithmetic in compute_session_vwap
    df = df.with_columns([
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    ])

    # Keep only regular-trading-hours bars (session opens at 09:00 CET)
    return df.filter(pl.col("hour_cet") >= 9).drop("hour_cet")


def _insert_vwap(
    conn: psycopg2.extensions.connection,
    df: pl.DataFrame,
    symbol: str,
) -> None:
    """Bulk-upsert computed VWAP rows into the database."""
    rows = [
        (
            row["timestamp"],
            symbol,
            row["session_date"],
            row["vwap"],
            row["upper_1"],
            row["lower_1"],
            row["upper_2"],
            row["lower_2"],
        )
        for row in df.iter_rows(named=True)
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO vwap
                (timestamp, symbol, session_date, vwap, upper_1, lower_1, upper_2, lower_2)
            VALUES %s
            ON CONFLICT (timestamp, symbol) DO UPDATE SET
                session_date = EXCLUDED.session_date,
                vwap         = EXCLUDED.vwap,
                upper_1      = EXCLUDED.upper_1,
                lower_1      = EXCLUDED.lower_1,
                upper_2      = EXCLUDED.upper_2,
                lower_2      = EXCLUDED.lower_2
            """,
            rows,
        )


def backfill_vwap(
    conn: psycopg2.extensions.connection,
    symbol: str = "DAX",
    volume_symbol: str = "FDAX",
) -> None:
    """Compute session-anchored VWAP from stored 1m bars and persist results.

    Creates the vwap table if it does not yet exist, then fetches 1m bars from
    *volume_symbol* (FDAX futures, which carry real traded volume), computes
    VWAP and bands session-by-session, and upserts the results tagged with
    *symbol* (the cash index the VWAP is being computed for).

    Parameters
    ----------
    symbol:
        The instrument the VWAP rows will be tagged with in the vwap table.
    volume_symbol:
        The instrument whose 1m bars supply the volume data.  Must be a
        symbol with non-zero volume (e.g. 'FDAX', not the cash 'DAX' index).
    """
    with conn.cursor() as cur:
        create_vwap_table(cur)

    log.info("Loading 1m bars from %s for VWAP computation...", volume_symbol)
    df = _fetch_1m_bars(conn, volume_symbol)

    if df.is_empty():
        log.warning("No 1m bars found for %s; skipping VWAP backfill.", volume_symbol)
        return

    n_sessions = df["session_date"].n_unique()
    log.info(
        "Computing VWAP over %d bars across %d sessions...", len(df), n_sessions
    )
    vwap_df = compute_session_vwap(df)

    log.info("Inserting %d VWAP rows...", len(vwap_df))
    _insert_vwap(conn, vwap_df, symbol)
    log.info("VWAP backfill complete for %s.", symbol)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = get_connection()
    conn.autocommit = True
    try:
        backfill_vwap(conn)
    finally:
        conn.close()
