import sys
sys.settrace

# Standard
from csv import reader
from pathlib import Path
from pprint import pprint
import os

# Local
#from config import local_config
class config():
    # Postgres database connection info
    DB_NAME = os.environ.get('DB_NAME') or 'climatedb'
    DB_USER = os.environ.get('DB_USER') or 'ben'
    DB_HOST = os.environ.get('DB_HOST') or 'localhost'
    DB_PORT = os.environ.get('DB_PORT') or 5432

    # Prefect flow options
    PREFECT_ENV = os.environ.get('PREFECT_ENV') or 'prod'
    
    # Directory options
    if os.environ.get('ENV_TEST') == 'true':
        NOAA_TEMP_CSV_DIR = Path.home() / 'github' / 'NOAA-Global-Temp-Data-Processing' / 'test' / 'data_downloads'/ 'noaa_daily_avg_temps'
    else: 
        NOAA_TEMP_CSV_DIR = os.environ.get('NOAA_TEMP_CSV_DIR') or Path('/') / 'mnt' / 'sda1' / 'data_downloads' / 'noaa_daily_avg_temps'

local_config = config
print(local_config.NOAA_TEMP_CSV_DIR)

# PyPI
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch, PostgresExecuteMany
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

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def list_csvs():
    csv_list = []
    data_dir = Path(config.NOAA_TEMP_CSV_DIR)
    for year in os.listdir(path=data_dir):
        csv_folder = (data_dir / str(year)).rglob('*.csv')
        csv_list = csv_list + [str(x) for x in csv_folder]
    return csv_list

@task(log_stdout=True) # pylist: disable=no-value-for-parameter
def list_db_years(waiting_for: str) -> list: #list of sets
    db_years = PostgresFetch(
        db_name=local_config.DB_NAME, #'climatedb', 
        user=local_config.DB_USER, #'postgres', 
        host=local_config.DB_HOST, #'192.168.86.32', 
        port=local_config.DB_PORT, #5432, 
        fetch="all",
        query="""
        select distinct year, date_update from climate.csv_checker
        order by date_update
        """
    ).run(password=PrefectSecret('NOAA_LOCAL_DB').run())
    db_years.insert(0, db_years.pop())   # Move last item in the list to the first
                                         # - We want to check the most recent year first, since csvs in that dir
                                         #   may not be complete (we are not doing the full number of csvs for some dirs
                                         #   with each run)
                                         # - Then we move to the oldest checked folder in the list to move forward
    print(db_years)
    return db_years

@task(log_stdout=True) # pylist: disable=no-value-for-parameter
def select_session_csvs(local_csvs: list, job_size: int) -> list:
    return_list = []

    # LOCAL SET
    csv_set = set()
    for csv in local_csvs:
        csv_list = csv.split('/') if '/' in csv else csv.split('\\')
        csv_str = f'{csv_list[-2]}-{csv_list[-1]}'
        csv_set.add(csv_str)

    year_db_csvs = PostgresFetch(
        db_name=local_config.DB_NAME, #'climatedb', 
        user=local_config.DB_USER, #'postgres', 
        host=local_config.DB_HOST, #'192.168.86.32', 
        port=local_config.DB_PORT, #5432, 
        fetch="all",
        query=f"""
        select year, station from climate.csv_checker
        order by date_update
        """
    ).run(password=PrefectSecret('NOAA_LOCAL_DB').run())

    # DB SET
    year_db_set = set()
    for year_db in year_db_csvs:
        year_db_str = f'{year_db[0]}-{year_db[1]}'
        year_db_set.add(year_db_str)

    # SET DIFF, SORT
    new_set = csv_set.difference(year_db_set)
    new_set = sorted(new_set)

    # CONVERT TO LIST, SELECT SHORT SUBSET
    new_list = []
    while len(new_list) < job_size:
        new_list.append(new_set.pop())
    new_list = [x.split('-') for x in new_set]
    new_list = new_list[:job_size]

    # REBUILD LIST OF FILE PATH LOCATIONS
    data_dir = Path(config.NOAA_TEMP_CSV_DIR)
    return_list = [f'{data_dir}/{x[0]}/{x[1]}' for x in new_list]

    return return_list
                

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def open_csv(filename: str):
    print(filename)
    with open(filename) as read_obj:
        csv_reader = reader(read_obj)
        # Get all rows of csv from csv_reader object as list of tuples
        return list(map(tuple, csv_reader))
    raise LOOP(message)

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_stations(list_of_tuples: list):#, password: str):
    insert = 0
    unique_key_violation = 0

    #print(len(list_of_tuples))
    insert = 0
    unique_key_violation = 0
    for row in list_of_tuples[1:2]:
        station = row[0]
        latitude = row[2] if row[2] != '' else None
        longitude = row[3] if row[3] != '' else None
        elevation = row[4] if row[4] != '' else None
        name = row[5]
        try:
            PostgresExecute(
                db_name=local_config.DB_NAME, #'climatedb', 
                user=local_config.DB_USER, #'postgres', 
                host=local_config.DB_HOST, #'192.168.86.32', 
                port=local_config.DB_PORT, #5432, 
                query="""
                insert into climate.noaa_global_temp_sites 
                    (station, latitude, longitude, elevation, name)
                values (%s, %s, %s, %s, %s)
                """, 
                data=(station, latitude, longitude, elevation, name), 
                commit=True,
            ).run(password=PrefectSecret('NOAA_LOCAL_DB').run())
            insert += 1
        except UniqueViolation:
            unique_key_violation += 1
        except InvalidTextRepresentation as e:
            print(e)
    print(f'STATION INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_records(list_of_tuples: list):#, waiting_for):
    #insert = 0
    unique_key_violation = 0
    new_list = []
    for row in list_of_tuples[1:]:
        date=row[1]
        station=row[0]
        temp=row[6]
        temp_attributes=row[7]
        dewp=row[8]
        dewp_attributes=row[9]
        slp=row[10]
        slp_attributes=row[11]
        stp=row[12]
        stp_attributes=row[13]
        visib=row[14] 
        visib_attributes=row[15]
        wdsp=row[16]
        wdsp_attributes=row[17]
        mxspd=row[18]
        gust=row[19]
        max_v=row[20]
        max_attributes=row[21]
        min_v=row[22]
        min_attributes=row[23]
        prcp=row[24] 
        prcp_attributes=row[25]
        sndp=row[26]
        frshtt=row[27]
        new_tuple = (date, station, temp, temp_attributes, dewp, dewp_attributes, slp, slp_attributes, 
                    stp, stp_attributes, visib, visib_attributes, wdsp, wdsp_attributes, mxspd, gust, 
                    max_v, max_attributes, min_v, min_attributes, prcp, prcp_attributes, sndp, frshtt)
        new_list.append(new_tuple)
        insert = len(new_list)
    try:
        PostgresExecuteMany(
            db_name=local_config.DB_NAME, #'climatedb', 
            user=local_config.DB_USER, #'postgres', 
            host=local_config.DB_HOST, #'192.168.86.32', 
            port=local_config.DB_PORT, #5432,  
            query="""
            insert into climate.noaa_global_daily_temps 
                (date, station, temp, temp_attributes, dewp, dewp_attributes, slp, slp_attributes, 
                    stp, stp_attributes, visib, visib_attributes, wdsp, wdsp_attributes, mxspd, gust, 
                    max, max_attributes, min, min_attributes, prcp, prcp_attributes, sndp, frshtt)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            data=new_list,
                 #(date, station, temp, temp_attributes, dewp, dewp_attributes, slp, slp_attributes, 
                 #   stp, stp_attributes, visib, visib_attributes, wdsp, wdsp_attributes, mxspd, gust, 
                 #   max_v, max_attributes, min_v, min_attributes, prcp, prcp_attributes, sndp, frshtt),
            commit=True,
        ).run(password=PrefectSecret('NOAA_LOCAL_DB').run())
        # insert += 1
    except UniqueViolation:
        unique_key_violation += 1
    try:
        csv_filename = station + '.csv'
        PostgresExecute(
            db_name=local_config.DB_NAME, #'climatedb', 
            user=local_config.DB_USER, #'postgres', 
            host=local_config.DB_HOST, #'192.168.86.32', 
            port=local_config.DB_PORT, #5432,  
            query="""
            insert into climate.csv_checker 
                (station, date_create, date_update, year)
            values (%s, CURRENT_DATE, CURRENT_DATE, %s)
            """, 
            data=(csv_filename, date[0:4]),
            commit=True,
        ).run(password=PrefectSecret('NOAA_LOCAL_DB').run())
    except UniqueViolation:
        pass
    print(f'RECORD INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

executor=LocalDaskExecutor(scheduler="processes", num_workers=16)#, local_processes=True)
with Flow(name="NOAA Temps: Process CSVs", executor=executor) as flow:
    t1_csvs = list_csvs()
    t2_session = select_session_csvs(local_csvs=t1_csvs, job_size=50)
    t3_records = open_csv.map(filename=t2_session)
    #t4_stations = insert_stations.map(list_of_tuples=t3_records)
    t5_records = insert_records.map(list_of_tuples=t3_records)#, waiting_for=t4_stations)


if __name__ == '__main__':
    # if os.environ.get('PREFECT_LOCAL') == 'true':
    #     prefect.config.cloud.use_local_secrets = True
    state = flow.run()#executor=LocalDaskExecutor(scheduler="processes", num_workers=6))#, local_processes=True)
    assert state.is_successful()
