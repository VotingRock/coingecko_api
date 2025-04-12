# Coingecko API Pipeline

This project implements a Bitcoin price analysis pipeline using PySpark, the CoinGecko API, and SQLite to store and process data. The result is a 5-day moving average calculation and a visualization of its behavior (for further information).

---

## 📦 Project structure
```plaintext
├── main.py               # Main code with pipeline functions
├── db/
│   └── bitcoin_prices.db # SQLite database with prices and moving averages
├── log/
│   └── cripto_api.log    # Detailed execution logs
├── image.png             # Moving average chart
└── README.md             # This file
```

## 🛠️ Requirements

- Python 3.13 (driver) y 3.13 (worker) *[must match]*  
- PySpark
- SQLite JDBC Driver (`SQLITE_JDBC_JAR`)
- Anaconda or virtual environment recommended

### Install dependencies (example with `conda`):

```bash
conda create -n spark_test python=3.12
conda activate spark_test
```

## 🛠️ Configuration

Set the environment variables before running:
```bash
export API_KEY='TU_API_KEY_DE_COINGECKO'
export SQLITE_JDBC_JAR='/ruta/completa/sqlite-jdbc-3.36.0.3.jar'
```

## 👍 Executable

```bash
python main.py
```

The script will do the following:
- Call the CoinGecko API and download historical prices.
- Calculate the 5-day moving average.
- Save the results to SQLite.

## 🔎 Integration testing 
```bash
python test_integration.py
```
- Ensure the .db file is created correctly.
- Validate that the {coin}_prices_v2 and {coin}_ma_5d tables contain data.
- Test that the coin_prices() function returns a DataFrame with valid prices.

