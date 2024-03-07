""" This script is used to combine 3 csv, using Pandas dataframes, then
    load the data into an existing table in a PostgreSQL database.

    NOTE: The headers in the csv file must match the columen headers in
    your datbase table.  I clear the existing data programatically each time
    for testing purposes.

    History:

"""

__author__ = "<name>"
__credits__ = ["<name>"]
__version__ = "1.0.1"
__maintainer__ = "<name>"
__email__ = "<email>"
__status__ = "Development"


import os
import psycopg2
import pandas as pd
import time
import logging

from dotenv import load_dotenv
from logging.config import dictConfig
from datetime import datetime
from pytz import timezone


load_dotenv()  # Load environment variable from the .env file in same dir
est = timezone('America/New_York')  # Set to my local timezone

# Logging
# debug settings
debug = eval(os.environ.get("DEBUG", "False"))

dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
        },
        "access": {
            "format": "%(message)s",
        }
    },
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": "/path/to/save/log/file",
            "maxBytes": 10000000,
            "backupCount": 5,
            "delay": "True",
        }
    },
    "root": {
        "level": "DEBUG" if debug else "INFO",
        "handlers": ["file"] if debug else ["file"],
    }
})


def delete_devices():

    """
    Delete all records prior to inserting devices
    """
    logging.info('Attempting to delete devices...')
    try:

        connection = psycopg2.connect(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DATABASE_NAME")
        )
        logging.info('Postgres connection established!')

        cur = connection.cursor()

        delete_existing_records = (
                """TRUNCATE <table_name> RESTART IDENTITY CASCADE;"""
            )
        cur.execute(delete_existing_records)
        connection.commit()
        logging.info('Existing records deleted!')
        cur.close()
        connection.close()
        logging.info('Connectin to db closed!')

        result = True

    except psycopg2.Error as e:
        logging.info(
            f'Failed to connect to db and delete existing records: {e}'
        )
        result = False

    return result


def insert_data():

    """ Insert device into db """
    logging.info('Attempting to insert data...')

    try:

        connection = psycopg2.connect(
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DATABASE_NAME")
        )
        logging.info('Postgres connection established!')

        cur = connection.cursor()

        f = open('path/to/combined/csv/file', 'r')
        cur.copy_from(f, '<table_name>', sep=',')
        connection.commit()
        logging.info('Data inserted into db!')
        cur.close()
        connection.close()
        logging.info('Connectin to db closed!')
        result = True

    except psycopg2.Error as e:
        logging.error(f'Failed to inserted data: {e}')
        result = False

    return result


def main():

    logging.info(
            'Script started that combines 3 csv files and insert data into db.'
    )

    # Start timer
    start = time.perf_counter()

    # Load 3 csv files into dataframe
    # Create 1st dataframe
    df1 = pd.read_csv('/path/to/1st/csv/file', index_col=False)

    # Create 2nd dataframe
    df2 = pd.read_csv('/path/to/2nd/csv/file', index_col=False)

    # Create 3rd dataframe
    df3 = pd.read_csv('/path/to/3rd/csv/file', index_col=False)

    # Combine dataframes
    frames = [df1, df2, df3]
    df = pd.concat(frames)

    # Add timestamp
    df['inserted_date'] = datetime.now(tz=est)

    # resetting index
    df.reset_index(inplace=True)

    # Start index at 1, instead of default of 0
    df.index += 1

    # Rename index
    df.index.names = ['id']

    # Rearrange columns to match table in db (if necessary)
    df = df[
            [
                'col1',
                'col2',
                'col3',
                'col4',
                'col5'
            ]
        ]

    logging.info('Data collected!')

    # Store combined data as csv
    df.to_csv('/path/to/temporary/csv/file/location', header=False)
    logging.info('CSV file generated!')

    # Delete existing devices
    deletion = delete_devices()

    if deletion:
        load_data = insert_data()

        if load_data:
            try:
                # Delete temporary csv file
                os.remove('/path/to/temporary/csv/file/location')
                logging.info(
                        'Temporary file deleted successfully!')

                # End timer
                end = time.perf_counter()
                logging.info(
                        f'Finished inserting data in {round(end-start, 2)}'
                        'second(s)')
            except Exception as e:
                logging.error(
                        'Temporary file not deleted!')
                logging.error(f'Exception: {e}')
        else:
            logging.error('Unable to delete devices, so aborted process!')

    else:
        logging.error('File missing!')


if __name__ == '__main__':
    main()
