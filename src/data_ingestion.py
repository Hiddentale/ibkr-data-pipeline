import psycopg2
from ib_async import IB, Index
from psycopg2.extras import execute_values


class IBKRDataCollector:
    def __init__(
        self, db_config=None, ibkr_host="127.0.0.1", ibkr_port=4002, client_id=1
    ):
        """
        Initialize data collector with IBKR API and PostgreSQL + TimescaleDB

        Args:
            db_config: Dict with keys: host, database, user, password, port
                      If None, uses localhost defaults
            ibkr_host: IBKR Gateway/TWS host
            ibkr_port: IBKR Gateway port
            client_id: Unique client ID for this connection
        """
        config = db_config or {
            "host": "localhost",
            "database": "trading",
            "user": "postgres",
            "password": "trading123",
            "port": 5432,
        }
        self.conn = psycopg2.connect(**config)
        self.conn.autocommit = True
        self._create_schema()

        self.ib = IB()
        self.ib.connect(ibkr_host, ibkr_port, clientId=client_id)
        print(f"Connected to IBKR: {self.ib.client.isConnected()}")

    def _create_schema(self):
        """Create tables with TimescaleDB optimizations"""
        with self.conn.cursor() as cursor:
            create_main_ohlc_table(cursor)
            create_indexes_for_common_queries(cursor)
            try:
                create_hypertable(cursor)
                enable_compression(cursor)
            except Exception as e:
                print(f"TimescaleDB features not enabled: {e}")

    def fetch_historical_data(self, symbol="DAX", years=2):
        """Pull years of EUREX data for all timeframes"""

        if symbol == "DAX":
            contract = Index("DAX", "EUREX", "EUR")
        else:
            raise ValueError(
                f"Unsupported symbol: {symbol}. Currently only DAX is supported."
            )
        self.ib.qualifyContracts(contract)

        for timeframe_name, bar_size in [
            ("1H", "1 hour"),
            ("1D", "1 day"),
        ]:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=f"{years} Y",
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )

            if not bars:
                print(f"No data returned for {timeframe_name}")
                continue

            self._store_bars(bars, timeframe_name, symbol)
            print(f"Stored {len(bars)} {timeframe_name} bars")

        self._resample_to_3h(symbol)

    def _store_bars(self, bars, timeframe, symbol):
        """Store OHLC bars in database using bulk insert"""
        if len(bars) == 0:
            return

        with self.conn.cursor() as cursor:
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

            execute_values(
                cursor,
                """
                INSERT INTO ohlc (timestamp, symbol, timeframe, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (timestamp, symbol, timeframe) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                data,
            )

    def _resample_to_3h(self, symbol):
        """Resample 1H bars to 3H using TimescaleDB's time_bucket"""

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ohlc (timestamp, symbol, timeframe, open, high, low, close, volume)
                SELECT
                    time_bucket('3 hours', timestamp) AS bucket,
                    symbol,
                    '3H' AS timeframe,
                    (array_agg(open ORDER BY timestamp))[1] AS open,
                    max(high) AS high,
                    min(low) AS low,
                    (array_agg(close ORDER BY timestamp DESC))[1] AS close,
                    sum(volume) AS volume
                FROM ohlc
                WHERE timeframe = '1H' AND symbol = %s
                GROUP BY bucket, symbol
                ON CONFLICT (timestamp, symbol, timeframe) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """,
                (symbol,),
            )

    def get_latest_timestamp(self, symbol, timeframe):
        """Get the most recent timestamp for a symbol/timeframe"""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(timestamp) FROM ohlc
                WHERE symbol = %s AND timeframe = %s
            """,
                (symbol, timeframe),
            )
            result = cur.fetchone()[0]
            return result

    def close(self):
        """Close IBKR and database connections"""
        self.ib.disconnect()
        self.conn.close()


def create_main_ohlc_table(cursor):
    cursor.execute(
        """
                CREATE TABLE IF NOT EXISTS ohlc (
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open NUMERIC(18, 5),
                    high NUMERIC(18, 5),
                    low NUMERIC(18, 5),
                    close NUMERIC(18, 5),
                    volume BIGINT,
                    PRIMARY KEY (timestamp, symbol, timeframe)
                );
            """
    )


def create_indexes_for_common_queries(cursor):
    cursor.execute(
        """
                CREATE INDEX IF NOT EXISTS idx_ohlc_symbol_timeframe_time
                ON ohlc (symbol, timeframe, timestamp DESC);
            """
    )


def create_hypertable(cursor):
    cursor.execute(
        """
                    SELECT create_hypertable('ohlc', 'timestamp',
                                              if_not_exists => TRUE);
                """
    )


def enable_compression(cursor, age=7):
    cursor.execute(
        """
                    ALTER TABLE ohlc SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'symbol,timeframe'
                    );
                """
    )

    cursor.execute(
        f"""
                    SELECT add_compression_policy('ohlc', INTERVAL '{age} days');
                """
    )
