import unittest
import os
import sqlite3
from main import save_to_db, average_spark, coin_prices

class TestIntegrationPipeline(unittest.TestCase):

    def setUp(self):
        # Paths y params
        self.db_path = './db/test_bitcoin.db'
        self.jdbc_url = f'jdbc:sqlite:{self.db_path}'
        self.coin = 'bitcoin'
        self.window_time = 5
        self.start_date = '2025-01-01'
        self.end_date = '2025-03-31'
        self.vs_currency = 'usd'

    def test_pipeline_creates_tables_and_data(self):
        # 1. Execute pipeline
        df = coin_prices(self.start_date, self.end_date, self.coin, self.vs_currency, self.window_time)
        save_to_db(df, self.jdbc_url, self.coin)
        average_spark(self.jdbc_url, self.coin, self.window_time)

        # 2. Validate SQl conection
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Validate the data tables 
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        self.assertIn('bitcoin_prices_v2', tables)
        self.assertIn('bitcoin_ma_5d', tables)

        # Validate the tables have data
        cursor.execute("SELECT COUNT(*) FROM bitcoin_prices_v2")
        count_prices = cursor.fetchone()[0]
        self.assertGreater(count_prices, 0)

        cursor.execute("SELECT COUNT(*) FROM bitcoin_ma_5d")
        count_ma = cursor.fetchone()[0]
        self.assertGreater(count_ma, 0)

        conn.close()

    def tearDown(self):
        # Delete temporal data base
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

if __name__ == '__main__':
    unittest.main()