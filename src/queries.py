import polars as pl
import psycopg2


def get_connection():
    """Create database connection"""
    return psycopg2.connect(
        host="localhost",
        database="trading",
        user="postgres",
        password="trading123",
        port=5432,
    )


def get_recent_bars(symbol="DAX40", timeframe="1H", days=30):
    """Get recent OHLC bars as polars DataFrame"""
    conn = get_connection()

    query = """
        SELECT timestamp, open, high, low, close, volume
        FROM ohlc
        WHERE symbol = %s
          AND timeframe = %s
          AND timestamp > NOW() - INTERVAL '%s days'
        ORDER BY timestamp
    """

    df = pl.read_database(query, conn, params=(symbol, timeframe, days))
    conn.close()

    df["timestamp"] = pl.Expr.str.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    return df


def get_all_timeframes(symbol="DAX40", days=7):
    """Get all timeframes for comparison"""
    conn = get_connection()

    query = """
        SELECT timestamp, timeframe, open, high, low, close, volume
        FROM ohlc
        WHERE symbol = %s
          AND timestamp > NOW() - INTERVAL '%s days'
        ORDER BY timestamp
    """

    df = pl.read_database(query, conn, params=(symbol, days))
    conn.close()

    df["timestamp"] = pl.Expr.str.to_datetime(df["timestamp"])

    return df


def calculate_daily_returns(symbol="DAX40", days=30):
    """Calculate daily returns for the last N days"""
    df = get_recent_bars(symbol, "1D", days)
    df["return"] = df["close"].pct_change() * 100
    df["cumulative_return"] = (1 + df["close"].pct_change()).cumprod() - 1

    return df[["close", "return", "cumulative_return"]].dropna()


def get_data_coverage(symbol="DAX40"):
    """Check how much data you have"""
    conn = get_connection()

    query = """
        SELECT
            timeframe,
            MIN(timestamp) as earliest,
            MAX(timestamp) as latest,
            COUNT(*) as bar_count
        FROM ohlc
        WHERE symbol = %s
        GROUP BY timeframe
        ORDER BY timeframe
    """

    df = pl.read_database(query, conn, params=(symbol,))
    conn.close()

    return df


def find_largest_moves(symbol="DAX40", timeframe="1H", limit=10):
    """Find the largest price moves (high to low) in the dataset"""
    conn = get_connection()

    query = """
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            (high - low) as range,
            ((high - low) / low * 100) as range_pct
        FROM ohlc
        WHERE symbol = %s AND timeframe = %s
        ORDER BY range_pct DESC
        LIMIT %s
    """

    df = pl.read_database(query, conn, params=(symbol, timeframe, limit))
    conn.close()

    df["timestamp"] = pl.Expr.str.to_datetime(df["timestamp"])

    return df


if __name__ == "__main__":
    coverage = get_data_coverage()
    print(coverage.to_string(index=False))

    recent_1h = get_recent_bars("DAX40", "1H", days=1).tail(5)
    print(recent_1h)

    big_moves = find_largest_moves("DAX40", "1H", limit=5)
    print(big_moves[["timestamp", "range", "range_pct"]].to_string(index=False))
