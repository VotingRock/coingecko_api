import logging
import time
import os
import requests as r
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import date_format, col, min as min_

python_path = '/opt/anaconda3/envs/spark_test/bin/python' # which python

os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

# Local path
JDBC_URL = 'jdbc:sqlite:./db/bitcoin_prices.db'
JDBC_JAR_PATH = os.getenv('SQLITE_JDBC_JAR')
API_KEY = os.getenv('API_KEY')

logging.basicConfig(
        filename='./log/cripto_api_steps.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
)

# Spark session
spark = SparkSession.builder.appName('CoinGecko Data')  \
        .config('spark.jars', JDBC_JAR_PATH) \
        .getOrCreate()

logging.info('-------------START PIPELINE RUNNING-------------')

'''def coin_list():
    url = 'https://api.coingecko.com/api/v3/coins/list'

    try: 
        print ('Collecting coin list...')
        response = r.get(url)
        data_coins = response.json()

        coins_list = [{'id': record['id'],
              'name': record['name'],
              'symbol': record['symbol']}
             for record in coins[:][:]
             ]

        return coins_list
    
    except r.exceptions.RequestException as e:
        logging.error(f'CoinGecko call fails: {e}')
        print('Something fails in the call')
    except KeyError as e:
        logging.error(f'CoinGecko key missing: {e}')
        print('Something fails with the paramethers')
    except Exception as e:
        logging.error(f'Unexpected error: {e}')
        print('Something fails unexpected')
'''
def coin_prices(start: str, end: str, coin: str, vs_currency: str, window_time: int):

    logging.info('-------------API CONECTION-------------')

    # Dates to UNIX
    start_adjusted = datetime.strptime(start, '%Y-%m-%d') - timedelta(days=window_time) # We will need data from the 5 previous days
    start_ts = int(time.mktime(start_adjusted.timetuple()))
    end_ts = int(time.mktime(datetime.strptime(end, '%Y-%m-%d').timetuple()))

    # API definition
    url = f'https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency={vs_currency}&from={start_ts}&to={end_ts}'
    headers = {
        'accept': 'application/json',
        'x-cg-demo-api-key': API_KEY
        }
    
    logging.info('Calling CoinGecko...')

    try: 
        response = r.get(url, headers=headers)
        data_reponse = response.json()
         
        logging.info('Conection works')
        logging.info('Start creating Dataframe')

        # Schema
        schema = StructType([
            StructField('timestamp', LongType(), True),
            StructField('price', DoubleType(), True)
        ])
        spark.conf.set('spark.sql.session.timeZone', 'UTC')

        # DataFrame
        df_spark = spark.createDataFrame(data_reponse['prices'], schema=schema)

        logging.info('Spark dataframe ready')

        return df_spark

    except r.exceptions.RequestException as e:
        logging.error(f'CoinGecko call fails: {e}')
        print('Something fails in the call')
    except KeyError as e:
        logging.error(f'CoinGecko key missing: {e}')
        print('Something fails with the paramethers')
    except Exception as e:
        logging.error(f'Unexpected error API: {e}')
        print('Something fails unexpected API')

def save_to_db(df, jdbc_url, coin):

    logging.info('-------------SAVE DATA-------------')

    try:
        logging.info('Starting saving in to DB...')
        # IMPORTANT: The dates are in UTC timezone
        df.withColumn('date', (col('timestamp')/1000).cast('timestamp')) \
            .withColumn('date', date_format('date', 'yyyy-MM-dd HH:mm:ssZ')) \
            .select('date', 'price') \
            .write.format('jdbc') \
            .option('url', jdbc_url) \
            .option('dbtable', f'{coin}_prices_v2') \
            .option('driver', 'org.sqlite.JDBC') \
            .mode('overwrite') \
            .save()
        
        logging.info('Data successfully saved')
        print('Data successfully saved')

    except Exception as e:
        logging.error(f'Unexpected error DB: {e}')
        print(f'Something fails {e}')

def average_spark(jdbc_url, coin, window_time: int):

    logging.info('-------------5 DAY AVG-------------')

    try: 
        logging.info('Conecting to the DB...')

        df_spark = spark.read.format('jdbc') \
            .option('url', jdbc_url) \
            .option('dbtable', f'{coin}_prices_v2') \
            .option('driver', 'org.sqlite.JDBC') \
            .load()

        logging.info('Conection works')

        df_spark = df_spark.withColumn('date', col('date').cast('timestamp'))

        window_spec = Window.orderBy('date').rowsBetween(-window_time + 1, 0)
        df_spark_ma = df_spark.withColumn(f'{window_time}_day_ma', avg('price').over(window_spec))

        date_min= df_spark_ma.select(min_('date').alias('date_min')).collect()
        lower_date = date_min[0]['date_min']
        start_date = lower_date + timedelta(days=window_time)

        logging.info('Finish the window creation. Save data to DB...')

        # Save in DB for visualization
        # IMPORTANT: The dates are in CST timezone
        df_spark_ma.filter(col('date') >= start_date) \
            .withColumn('date', date_format('date', 'yyyy-MM-dd HH:mm:ss')) \
            .select('date', 'price', f'{window_time}_day_ma') \
            .write \
            .format('jdbc') \
            .option('url', jdbc_url) \
            .option('dbtable', f'{coin}_ma_5d') \
            .option('driver', 'org.sqlite.JDBC') \
            .mode('overwrite') \
            .save()

        logging.info('The data is update')
        
    except Exception as e:
        logging.error(f'Unexpected error: {e}')
        print(f'Something fails unexpected {e}')


def main():
    logging.basicConfig(
        filename='./log/cripto_api.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    start_date = '2025-01-01'
    end_date = '2025-03-31'
    coin = 'bitcoin'
    vs_currency ='usd'
    window_time = 5

    save_to_db(coin_prices(start_date, end_date, coin, vs_currency, window_time), JDBC_URL, coin)
    average_spark(JDBC_URL, coin, window_time)
if __name__ == '__main__':
    main()