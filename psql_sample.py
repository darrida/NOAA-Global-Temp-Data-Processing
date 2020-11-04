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
    DB_USER = os.environ.get('DB_USER') or 'postgres'
    DB_HOST = os.environ.get('DB_HOST') or '192.168.86.32'
    DB_PORT = os.environ.get('DB_PORT') or 5432

    # Prefect flow options
    PREFECT_ENV = os.environ.get('PREFECT_ENV') or 'prod'
    
    # Directory options
    #NOAA_TEMP_CSV_DIR = Path.home() / 'github' / 'NOAA-Global-Temp-Data-Processing' / 'test' / 'data_downloads'/ 'noaa_daily_avg_temps'
    NOAA_TEMP_CSV_DIR = os.environ.get('NOAA_TEMP_CSV_DIR') or Path.home() / 'data_downloads'/ 'noaa_daily_avg_temps'

local_config = config
print(local_config.NOAA_TEMP_CSV_DIR)

# PyPI
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
from prefect.tasks.secrets import EnvVarSecret, PrefectSecret
from prefect.engine.signals import LOOP
from prefect.engine.executors import LocalDaskExecutor
import psycopg2 as pg
from psycopg2.errors import UniqueViolation, InvalidTextRepresentation # pylint: disable=no-name-in-module

# url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'
# export PREFECT__CONTEXT__SECRETS__MY_KEY="MY_VALUE"
# export PREFECT__ENGINE__EXECUTOR__DEFAULT_CLASS="prefect.engine.executors.LocalDaskExecutor"

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def list_folders(data_dir: str):
    year_folders = os.listdir(path=data_dir)
    print(year_folders)
    return year_folders

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def list_csvs():
    #print(year)
    csv_list = []
    data_dir = Path(config.NOAA_TEMP_CSV_DIR)
    for year in os.listdir(path=data_dir):
        print(year)
        #csv_folder = (data_dir / str('1920')).rglob('*.csv') #Path(Path.cwd() / 'data' / str(year)).rglob('*.csv')
        csv_folder = (data_dir / str(year)).rglob('*.csv')
        csv_list = csv_list + [str(x) for x in csv_folder]
        print(type(csv_list))
        for i in csv_list:
            print(i)
    return csv_list

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

    print(len(list_of_tuples))
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
            ).run(password=PrefectSecret('DB'))
            insert += 1
        except UniqueViolation:
            unique_key_violation += 1
        except InvalidTextRepresentation as e:
            print(e)
    print(f'INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_records(list_of_tuples: list, waiting_for):
    insert = 0
    unique_key_violation = 0
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
        try:
            PostgresExecute(
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
                data=(date, station, temp, temp_attributes, dewp, dewp_attributes, slp, slp_attributes, 
                     stp, stp_attributes, visib, visib_attributes, wdsp, wdsp_attributes, mxspd, gust, 
                     max_v, max_attributes, min_v, min_attributes, prcp, prcp_attributes, sndp, frshtt),
                commit=True,
            ).run(password=PrefectSecret('DB'))
            insert += 1
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
        ).run(password=PrefectSecret('DB'))
    except UniqueViolation:
        pass
    print(f'INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

with Flow(name="psql_test") as flow:
    #p = PrefectSecret('DB')
    #data_dir = Parameter('data_dir', default=local_config.NOAA_TEMP_CSV_DIR)
    #t0_years = list_folders(data_dir=data_dir)
    t1_csvs = list_csvs()#.map(year=t0_years)
    #t0_CSVs = list_csvs.map(data_dir=data_dir)
    #t1 = open_csv(filename='01023099999.csv')
    t2_records = open_csv.map(filename=t1_csvs)
    t3_stations = insert_stations.map(list_of_tuples=t2_records)
    t4_records = insert_records.map(list_of_tuples=t2_records, waiting_for=t3_stations)

#flow.register(project_name="Test")#, executor=LocalDaskExecutor(scheduler="processes", local_processes=True, num_workers=4))

if __name__ == '__main__':
    state = flow.run(executor=LocalDaskExecutor(scheduler="processes", local_processes=True, num_workers=4))#address="tcp://192.168.169.61:8786"))
    #flow.register(project_name="Test")#, executor=LocalDaskExecutor(scheduler="processes", local_processes=True, num_workers=4))
    assert state.is_successful()
# if os.environ.get('PREFECT_ENV') == 'test':
#     schedule = None#IntervalSchedule(interval=timedelta(minutes=0.1))
# else:
#     schedule = IntervalSchedule(interval=timedelta(days=3))

# with Flow('NOAA Global Temp Data Processing', schedule=schedule) as flow:
#     year = Parameter('year', default=date.today().year)
#      ...

# if local_config.PREFECT_ENV in ('local', 'test'):
#     flow.run()
# else:
#     flow.register(project_name="Global Warming Data")