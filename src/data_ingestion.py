"""Historical OHLC data ingestion from IBKR into PostgreSQL / TimescaleDB."""

from __future__ import annotations

import logging
from datetime import date

import psycopg2
from ib_async import IB, Contract, Future, Index
from psycopg2.extras import execute_values

from db import _DEFAULT_CONFIG

log = logging.getLogger(__name__)

# Timeframes fetched with a single request each.
# Format: (timeframe_name, IBKR bar-size string, IBKR duration string).
# 1m, 5m, 15m are absent — IBKR enforces per-request duration caps that vary
# by bar size; those three are fetched by _fetch_paged (see below).
_TIMEFRAME_CONFIG: list[tuple[str, str, str]] = [
    ("1H",  "1 hour",  "4 Y"),
    ("1D",  "1 day",   "5 Y"),
]

# Timeframes that require paging because their duration exceeds IBKR's
# per-request cap.  Per IBKR docs: 5 mins ≤ 10 W, 15 mins ≤ 20 W per request.
# Format: (timeframe_name, bar_size_setting, page_duration, n_pages)
_PAGED_TIMEFRAME_CONFIG: list[tuple[str, str, str, int]] = [
    ("5m",  "5 mins",  "8 W",  7),   # 7 × 8 weeks ≈ 1 year
    ("15m", "15 mins", "16 W", 7),   # 7 × 16 weeks ≈ 2 years
]

_1M_PAGE_DAYS: int = 30      # IBKR's per-request limit for 1m data
_1M_TARGET_MONTHS: int = 3   # how many 30-day pages to request

# IBKR pacing: a brief pause after the heavy 1m paging session before
# issuing further requests.  5m/15m now use their own paged requests so
# this only guards against gateway-level throttling after large downloads.
_POST_1M_SLEEP: int = 30     # seconds to wait after the 1m paging session
_INTER_REQUEST_SLEEP: int = 10  # seconds to wait between successful requests
_AFTER_TIMEOUT_SLEEP: int = 60  # seconds to wait after a failed / empty request


def _fdax_front_month() -> str:
    """Return the front-month FDAX quarterly expiry in YYYYMM format.

    FDAX expires on the third Friday of March, June, September, and December.
    Day 20 is used as a conservative roll cutoff — the third Friday always
    falls between the 15th and 21st, so by day 20 the contract has expired
    and the next quarter becomes the front month.
    """
    today = date.today()
    for month in (3, 6, 9, 12):
        if today.month < month or (today.month == month and today.day <= 20):
            return f"{today.year}{month:02d}"
    return f"{today.year + 1}03"  # past December 20 → roll to March next year


class IBKRDataCollector:
    def __init__(
        self,
        db_config: dict | None = None,
        ibkr_host: str = "127.0.0.1",
        ibkr_port: int = 4002,
        client_id: int = 1,
    ) -> None:
        """
        Args:
            db_config: Dict with keys host, database, user, password, port.
                       Defaults to localhost if omitted.
            ibkr_host: IB Gateway / TWS host.
            ibkr_port: IB Gateway port (4002 = paper, 4001 = live).
            client_id: Unique client ID for this connection.
        """
        self.conn = psycopg2.connect(**(db_config or _DEFAULT_CONFIG))
        self.conn.autocommit = True
        self._create_schema()

        self.ib = IB()
        self.ib.connect(ibkr_host, ibkr_port, clientId=client_id)
        log.info("Connected to IBKR: %s", self.ib.client.isConnected())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_historical_data(self, symbol: str = "DAX") -> None:
        """Pull IBKR historical data for all configured timeframes and store it."""
        log.info("Starting historical data fetch for %s", symbol)

        contract = self._resolve_contract(symbol)
        self.ib.qualifyContracts(contract)
        log.info("Contract qualified: %s", contract)

        self._fetch_paged(
            contract, symbol, "1m", "1 min", f"{_1M_PAGE_DAYS} D", _1M_TARGET_MONTHS
        )

        log.info(
            "Pausing %d s after 1m download to clear IBKR pacing window...",
            _POST_1M_SLEEP,
        )
        self.ib.sleep(_POST_1M_SLEEP)

        for timeframe_name, bar_size, page_duration, n_pages in _PAGED_TIMEFRAME_CONFIG:
            log.info("Starting paged fetch for %s...", timeframe_name)
            self._fetch_paged(contract, symbol, timeframe_name, bar_size, page_duration, n_pages)
            self.ib.sleep(_INTER_REQUEST_SLEEP)

        for timeframe_name, bar_size, duration in _TIMEFRAME_CONFIG:
            if self._fetch_single_timeframe(contract, symbol, timeframe_name, bar_size, duration):
                self.ib.sleep(_INTER_REQUEST_SLEEP)

        log.info("Resampling 1H -> 3H...")
        self._resample(symbol, "1H", "3H", "3 hours")
        log.info("Ingestion complete for %s", symbol)

    def fetch_fdax_1m_data(self) -> None:
        """Fetch FDAX front-month 1m bars and store them as symbol='FDAX'.

        Only 1m data is fetched — the sole purpose of this series is to supply
        real traded volume for the session-anchored VWAP computation.  The
        front-month contract is resolved automatically by IBKR at qualification
        time; no rollover handling is required because the paging window
        (_1M_PAGE_DAYS × _1M_TARGET_MONTHS) stays within one contract cycle.
        """
        contract = self._resolve_contract("FDAX")
        self.ib.qualifyContracts(contract)
        log.info("FDAX contract qualified: %s", contract)

        self._fetch_paged(
            contract, "FDAX", "1m", "1 min", f"{_1M_PAGE_DAYS} D", _1M_TARGET_MONTHS
        )
        log.info("FDAX 1m ingestion complete.")

    def get_latest_timestamp(self, symbol: str, timeframe: str):
        """Return the most recent stored timestamp for a symbol/timeframe pair."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(timestamp) FROM ohlc WHERE symbol = %s AND timeframe = %s",
                (symbol, timeframe),
            )
            return cur.fetchone()[0]

    def close(self) -> None:
        """Disconnect from IBKR and close the database connection."""
        self.ib.disconnect()
        self.conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_contract(self, symbol: str) -> Contract:
        if symbol == "DAX":
            return Index("DAX", "EUREX", "EUR")
        if symbol == "FDAX":
            return Future(
                symbol="DAX",
                lastTradeDateOrContractMonth=_fdax_front_month(),
                exchange="EUREX",
                currency="EUR",
                tradingClass="FDAX",
            )
        raise ValueError(f"Unsupported symbol: {symbol!r}")

    def _fetch_single_timeframe(
        self,
        contract: Contract,
        symbol: str,
        timeframe_name: str,
        bar_size: str,
        duration: str,
    ) -> bool:
        """Request one non-paged IBKR historical data batch and store it.

        Returns True on success.  On timeout or empty response, sleeps
        _AFTER_TIMEOUT_SLEEP and returns False so the caller can skip the
        normal inter-request delay.
        """
        log.info(
            "Requesting %s data (bar_size=%s, duration=%s)...",
            timeframe_name, bar_size, duration,
        )
        try:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
        except Exception as exc:
            # Error 162 (query cancelled) and timeouts both land here when
            # IBKR's rate-limit bucket is not yet refilled.
            log.warning(
                "Request failed for %s: %s — waiting %d s then continuing",
                timeframe_name, exc, _AFTER_TIMEOUT_SLEEP,
            )
            self.ib.sleep(_AFTER_TIMEOUT_SLEEP)
            return False

        if not bars:
            log.warning(
                "No data returned for %s — waiting %d s then continuing",
                timeframe_name, _AFTER_TIMEOUT_SLEEP,
            )
            self.ib.sleep(_AFTER_TIMEOUT_SLEEP)
            return False

        log.info("Storing %d %s bars...", len(bars), timeframe_name)
        self._store_bars(bars, timeframe_name, symbol)
        return True

    def _fetch_paged(
        self,
        contract: Contract,
        symbol: str,
        timeframe_name: str,
        bar_size: str,
        page_duration: str,
        n_pages: int,
    ) -> None:
        """Collect bars by paging backwards through IBKR's per-request duration cap.

        IBKR enforces a maximum duration per reqHistoricalData call that varies
        by bar size (e.g. ~10 W for 5 mins, ~20 W for 15 mins, ~30 D for 1 min).
        This method issues n_pages sequential requests, stepping the end pointer
        back to just before the earliest bar of each previous batch.
        """
        end_dt = ""
        all_bars: list = []

        for page in range(n_pages):
            log.info(
                "Fetching %s page %d/%d (endDateTime=%r)...",
                timeframe_name, page + 1, n_pages, end_dt or "now",
            )
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=page_duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            if not bars:
                log.info("No more %s bars available at page %d", timeframe_name, page + 1)
                break
            all_bars.extend(bars)
            end_dt = bars[0].date  # step the window backwards
            self.ib.sleep(1)       # respect IBKR rate limits

        if all_bars:
            log.info("Storing %d %s bars...", len(all_bars), timeframe_name)
            self._store_bars(all_bars, timeframe_name, symbol)
        else:
            log.warning("No %s bars collected for %s", timeframe_name, symbol)

    def _store_bars(self, bars: list, timeframe: str, symbol: str) -> None:
        """Bulk-upsert OHLC bars into the database."""
        if not bars:
            return

        data = [
            (
                bar.date,
                symbol,
                timeframe,
                float(bar.open),
                float(bar.high),
                float(bar.low),
                float(bar.close),
                int(bar.volume),
            )
            for bar in bars
        ]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO ohlc (timestamp, symbol, timeframe, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (timestamp, symbol, timeframe) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                data,
            )

    def _resample(
        self, symbol: str, source_tf: str, target_tf: str, bucket_interval: str
    ) -> None:
        """Derive target_tf bars from source_tf bars using TimescaleDB time_bucket.

        All four values are passed as query parameters so no string interpolation
        touches the SQL.  bucket_interval must be a valid PostgreSQL interval
        literal (e.g. '5 minutes', '15 minutes', '3 hours').
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ohlc (timestamp, symbol, timeframe, open, high, low, close, volume)
                SELECT
                    time_bucket(%s::interval, timestamp)           AS bucket,
                    symbol,
                    %s                                             AS timeframe,
                    (array_agg(open  ORDER BY timestamp))[1]       AS open,
                    MAX(high)                                      AS high,
                    MIN(low)                                       AS low,
                    (array_agg(close ORDER BY timestamp DESC))[1]  AS close,
                    SUM(volume)                                    AS volume
                FROM ohlc
                WHERE timeframe = %s AND symbol = %s
                GROUP BY bucket, symbol
                ON CONFLICT (timestamp, symbol, timeframe) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                (bucket_interval, target_tf, source_tf, symbol),
            )

    def _create_schema(self) -> None:
        """Create tables and indexes if they do not yet exist."""
        with self.conn.cursor() as cursor:
            create_main_ohlc_table(cursor)
            create_indexes_for_common_queries(cursor)
            try:
                create_hypertable(cursor)
            except Exception as exc:
                log.warning("Could not create hypertable: %s", exc)
            try:
                enable_compression(cursor)
            except Exception as exc:
                log.warning("Could not enable compression policy: %s", exc)


# ---------------------------------------------------------------------------
# Schema helpers — module-level so they can be imported by other modules
# ---------------------------------------------------------------------------


def create_main_ohlc_table(cursor) -> None:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ohlc (
            timestamp  TIMESTAMPTZ   NOT NULL,
            symbol     TEXT          NOT NULL,
            timeframe  TEXT          NOT NULL,
            open       NUMERIC(18,5),
            high       NUMERIC(18,5),
            low        NUMERIC(18,5),
            close      NUMERIC(18,5),
            volume     BIGINT,
            PRIMARY KEY (timestamp, symbol, timeframe)
        )
    """)


def create_indexes_for_common_queries(cursor) -> None:
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_timeframe_time
        ON ohlc (symbol, timeframe, timestamp DESC)
    """)


def create_hypertable(cursor) -> None:
    cursor.execute("""
        SELECT create_hypertable('ohlc', 'timestamp', if_not_exists => TRUE)
    """)


def enable_compression(cursor, age_days: int = 7) -> None:
    cursor.execute("""
        ALTER TABLE ohlc SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol,timeframe'
        )
    """)
    cursor.execute(f"""
        SELECT add_compression_policy('ohlc', INTERVAL '{age_days} days', if_not_exists => TRUE)
    """)
