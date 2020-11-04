import os
from pathlib import Path

class config():
    # Postgres database connection info
    DB_NAME = os.environ.get('DB_NAME') or 'climatedb'
    DB_USER = os.environ.get('DB_USER') or 'postgres'
    DB_HOST = os.environ.get('DB_HOST') or '192.168.86.32'
    DB_PORT = os.environ.get('DB_PORT') or 5432

    # Prefect flow options
    PREFECT_ENV = os.environ.get('PREFECT_ENV') or 'prod'
    
    # Directory options
    #if PREFECT_ENV == 'local':
    #    NOAA_TEMP_CSV_DIR = Path.cwd() / 'data_downloads'/ 'noaa_daily_avg_temps'
    #else:
    NOAA_TEMP_CSV_DIR = os.environ.get('NOAA_TEMP_CSV_DIR') or Path.home() / 'data_downloads'/ 'noaa_daily_avg_temps'

global local_config
local_config = config()