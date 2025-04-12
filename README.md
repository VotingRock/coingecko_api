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

