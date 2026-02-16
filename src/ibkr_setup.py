from ib_async import Index

from data_ingestion import IBKRDataCollector


def test_connection():
    """Test IBKR connection and fetch sample data"""

    collector = IBKRDataCollector(ibkr_host="127.0.0.1", ibkr_port=4002, client_id=1)

    print(f"Connection status: {collector.ib.client.isConnected()}")

    dax = Index("DAX", "EUREX", "EUR")
    collector.ib.qualifyContracts(dax)

    ticker = collector.ib.reqMktData(dax)
    collector.ib.sleep(2)
    print(f"Current DAX price: {ticker.last if ticker.last else 'Waiting for data...'}")

    collector.close()
    print("Connection closed successfully")


def fetch_historical():
    """Fetch 2 years of historical DAX data"""

    collector = IBKRDataCollector(ibkr_port=4002, client_id=2)

    try:
        collector.fetch_historical_data(symbol="DAX", years=2)

    except Exception as e:
        print(f"Error fetching data: {e}")

    finally:
        collector.close()


def check_data_coverage():
    """Check what data is stored in the database"""
    from queries import get_data_coverage

    coverage = get_data_coverage(symbol="DAX")
    print(f"\nData coverage: {coverage}")


if __name__ == "__main__":
    test_connection()
    response = input().strip().lower()

    if response == "y":
        fetch_historical()
        check_data_coverage()
    else:
        print("Skipping historical data fetch")
