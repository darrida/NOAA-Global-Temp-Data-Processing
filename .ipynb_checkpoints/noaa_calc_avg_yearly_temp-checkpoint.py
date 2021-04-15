import sys
sys.settrace

# Standard
from pathlib import Path
from datetime import timedelta, datetime
import os

import pandas as pd


class config():
    # Prefect flow options
    PREFECT_ENV = os.environ.get('PREFECT_ENV') or 'prod'
    
    # Directory options
    if os.environ.get('ENV_TEST') == 'true':
        NOAA_TEMP_CSV_DIR = Path.home() / 'github' / 'NOAA-Global-Temp-Data-Processing' / 'test' / 'data_downloads'/ 'noaa_daily_avg_temps'
    else: 
        #NOAA_TEMP_CSV_DIR = os.environ.get('NOAA_TEMP_CSV_DIR') or Path('/') / 'mnt' / 'sda1' / 'data_downloads' / 'noaa_daily_avg_temps'
        NOAA_TEMP_CSV_DIR = os.environ.get('NOAA_TEMP_CSV_DIR') or Path('/') / 'media' / 'share' / 'store_240a' / 'data_downloads' / 'noaa_daily_avg_temps'

local_config = config
print(local_config.NOAA_TEMP_CSV_DIR)

# PyPI
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch, PostgresExecuteMany
from prefect.schedules import IntervalSchedule
from prefect.tasks.secrets import PrefectSecret
from prefect.engine.signals import LOOP
#from prefect.engine.executors import LocalDaskExecutor
#from prefect.executors import LocalDaskExecutor
from prefect.executors.dask import LocalDaskExecutor
import psycopg2 as pg
from psycopg2.errors import UniqueViolation, InvalidTextRepresentation # pylint: disable=no-name-in-module

# url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
# export PREFECT__CONTEXT__SECRETS__MY_KEY="MY_VALUE"
# export PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS="prefect.engine.executors.LocalDaskExecutor"
# prefect agent start --name dask_test
# prefect register flow --file psql_sample.py --name psql_test_v2 --project 
# ? add_default_labels=False

# local testing: export NOAA_TEMP_CSV_DIR=$PWD/test/data_downloads/noaa_daily_avg_temps


def unique_values_only_one(column: str):
    value_l = column.unique()
    if len(value_l) > 1:
        return 'X'
    return value_l[0]


@task(log_stdout=True)
def list_year_folders():
    return os.listdir(local_config.NOAA_TEMP_CSV_DIR)


@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def calculate_year_csv(year_folder):
    folder_dir_path = local_config.NOAA_TEMP_CSV_DIR
    with open(f'results_{year_folder}.csv', 'w', newline='') as f:
        sites_in_folder = os.listdir(folder_dir_path / year_folder)
        f.write('SITE_NUMBER,LATITUDE,LONGITUDE,ELEVATION,AVERAGE_TEMP\n')
        for site in sites_in_folder:
            df1 = pd.read_csv(folder_dir_path / year_folder / site)
            average_temp = df1['TEMP'].mean()
            site_number = unique_values_only_one(df1['STATION'])
            latitude = unique_values_only_one(df1['LATITUDE'])
            longitude = unique_values_only_one(df1['LONGITUDE'])
            elevation = unique_values_only_one(df1['ELEVATION'])
            if site_number == 'X' \
                    or latitude == 'X' \
                    or longitude == 'X' \
                    or elevation == 'X':
                print(f'Non-unique column:', folder_dir_path, year_folder, site)
            f.write(f'{site_number},{latitude},{longitude},{elevation},{average_temp}\n')           


schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(seconds=10),
)

#schedule = IntervalSchedule(interval=timedelta(minutes=2))
executor=LocalDaskExecutor(scheduler="processes", num_workers=7)#, local_processes=True)
with Flow(name="NOAA Temps: Process CSVs", executor=executor, schedule=schedule) as flow:
    # folder_path_flow = Parameter('folder_path_flow', default=os.environ.get('NOAA_TEMP_CSV_DIR') or Path('/') / 'media' / 'share' / 'store_240a' / 'data_downloads' / 'noaa_daily_avg_temps')
    # job_size = Parameter('JOB_SIZE', default=200)
    folders = list_year_folders()
    calculate_year_csv.map(year_folder=folders)#list_of_tuples=t3_records)#, waiting_for=t4_stations)


if __name__ == '__main__':
    #flow.register(project_name="Global Warming Data")
    state = flow.run()#executor=LocalDaskExecutor(scheduler="processes", num_workers=6))#, local_processes=True)
    assert state.is_successful()
