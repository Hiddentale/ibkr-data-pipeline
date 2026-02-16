# IBKR Data Pipeline

Data pipeline and foundation for automated trading system. Ingests OHLC data from MetaTrader 5 API, stores in PostgreSQL/TimescaleDB, and provides exploratory analysis tools.

## Features

- Fetch historical OHLC data from IBKR (1H, 3H, 1D timeframes)
- Store data in PostgreSQL with TimescaleDB optimizations
- Query and analyze data using Polars DataFrames
- Automatic resampling (1H → 3H aggregation)
- Data compression for long-term storage

## Prerequisites

- Python 3.10+
- IBKR Gateway (only needed during data collection)
- Docker (for PostgreSQL/TimescaleDB)

## Quick Start

### 1. Install Dependencies

Using UV (recommended):
```bash
# Install UV
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# Install dependencies
uv venv
uv pip install -e .
```

### 2. Setup Database

```bash
docker run -d --name trading-db -p 5432:5432 \
    -e POSTGRES_PASSWORD=trading123 \
    -e POSTGRES_DB=trading \
    -v trading-data:/var/lib/postgresql/data \
    timescale/timescaledb:latest-pg16
```

### 3. Collect Historical Data

**Important:** IBKR terminal must be open and logged in for this step only.

```bash
python src/test_data_collection.py
```

### 4. Query Data

Once data is in the database, IBKR doesn't need to be running:

```bash
python src/query.py
```