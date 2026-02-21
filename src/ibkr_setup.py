import logging

from ib_async import Index

from data_ingestion import IBKRDataCollector
from queries import get_data_coverage


def test_connection():
    """Verify IBKR connectivity and contract qualification.

    Does not request live market data — that requires an open session and
    blocks indefinitely outside trading hours.
    """
    collector = IBKRDataCollector(ibkr_host="127.0.0.1", ibkr_port=4002, client_id=1)

    logging.info("Connection status: %s", collector.ib.client.isConnected())

    dax = Index("DAX", "EUREX", "EUR")
    qualified = collector.ib.qualifyContracts(dax)
    logging.info("Contract qualified: %s", qualified[0] if qualified else "FAILED")

    collector.close()
    logging.info("Connection closed successfully")


def fetch_historical():
    """Fetch 2 years of historical DAX data"""

    collector = IBKRDataCollector(ibkr_port=4002, client_id=2)

    try:
        collector.fetch_historical_data(symbol="DAX")

    except Exception as e:
        logging.error("Error fetching data: %s", e)

    finally:
        collector.close()


def fetch_fdax_1m():
    """Fetch FDAX front-month 1m bars (volume source for VWAP)."""
    collector = IBKRDataCollector(ibkr_port=4002, client_id=3)
    try:
        collector.fetch_fdax_1m_data()
    except Exception as e:
        logging.error("Error fetching FDAX 1m data: %s", e)
    finally:
        collector.close()


def check_data_coverage():
    """Check what data is stored in the database"""
    coverage = get_data_coverage(symbol="DAX")
    logging.info("Data coverage:\n%s", coverage)


# Bar sizes to probe, paired with a short fixed duration so each attempt
# either returns data quickly or fails within ib_async's 60-second timeout.
# 1m and 1H are included as known-good references.
_PROBE_TARGETS: list[tuple[str, str]] = [
    ("1 min", "1 D"),
    ("5 mins", "1 D"),
    ("15 mins", "1 D"),
    ("1 hour", "1 D"),
    ("1 day", "5 D"),
]


def probe_bar_availability(symbol: str = "DAX", client_id: int = 4) -> None:
    """Probe which TRADES bar sizes IBKR will serve for *symbol*.

    Requests one day of data for each bar size in _PROBE_TARGETS.  A short
    duration is used deliberately: if IBKR cancels the query for a 1-day
    window, it will also cancel it for longer durations, so the data is
    simply not available for that bar size on this instrument.

    Expected output for the DAX cash Index:
        1 min   -> OK
        5 mins  -> OK
        15 mins -> OK
        1 hour  -> OK
        1 day   -> OK

    All bar sizes are available for a 1-day window.  If any fail, that bar
    size is unsupported for this instrument and should be removed from
    _PAGED_TIMEFRAME_CONFIG or _TIMEFRAME_CONFIG.

    Run this interactively before changing _TIMEFRAME_CONFIG or _resample
    calls to verify IBKR's current data availability.
    """
    collector = IBKRDataCollector(ibkr_port=4002, client_id=client_id)
    contract = Index(symbol, "EUREX", "EUR")
    collector.ib.qualifyContracts(contract)

    try:
        for bar_size, duration in _PROBE_TARGETS:
            logging.info("Probing TRADES / %s / duration=%s...", bar_size, duration)
            bars = collector.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            if bars:
                logging.info("  -> OK: %d bars returned", len(bars))
            else:
                logging.warning("  -> FAILED: no bars (timeout or query cancelled)")
            collector.ib.sleep(5)
    finally:
        collector.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    test_connection()
    response = input("\nFetch full historical data? [y/N]: ").strip().lower()

    if response == "y":
        fetch_historical()
        fetch_fdax_1m()
        check_data_coverage()
    else:
        logging.info("Skipping historical data fetch")
