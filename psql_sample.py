# Standard
from csv import reader
from pathlib import Path
from pprint import pprint

# Local
from config import config
print(config.NOAA_TEMP_CSV_DIR)

# PyPI
import prefect
from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
from prefect.tasks.secrets import EnvVarSecret, PrefectSecret
from prefect.engine.signals import LOOP
import psycopg2 as pg
from psycopg2.errors import UniqueViolation, InvalidTextRepresentation # pylint: disable=no-name-in-module

# url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def list_csvs(data_dir: str):
    data_dir = Path(data_dir)
    csv_folder = (data_dir / str('1920')).rglob('*.csv') #Path(Path.cwd() / 'data' / str(year)).rglob('*.csv')
    csv_local_list = [str(x) for x in csv_folder]
    print(type(csv_local_list))
    for i in csv_local_list:
        print(i)
    return csv_local_list

# @task(log_stdout=True) # pylint: disable=no-value-for-parameter
# def open_csv(filename: str):
#     with open(filename) as read_obj:
#         csv_reader = reader(read_obj)
#         # Get all rows of csv from csv_reader object as list of tuples
#         return list(map(tuple, csv_reader))

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def open_csv(filename):
    #loop_payload = prefect.context.get("task_loop_result", {})
    #n = loop_payload.get("n", 0)
    print(filename)
    with open(filename) as read_obj:
        csv_reader = reader(read_obj)
        # Get all rows of csv from csv_reader object as list of tuples
        return list(map(tuple, csv_reader))
    #raise LOOP(message=f"Next file: {filename}")#, result=dict(n=n + 1))

# @task(log_stdout=True) # pylint: disable=no-value-for-parameter
# def insert_stations(list_of_tuples: list):#, password: str):
#     insert = 0
#     unique_key_violation = 0
#     loop_payload = prefect.context.get("task_loop_result", {})
#     n_sites = loop_payload.get("n_sites", 0)
#     n_records = loop_payload.get("n_records", 0)
    
#     site_records = list_of_tuples[n_sites]
#     row = site_records[n_records]

#     print(len(list_of_tuples[8]))
#     insert = 0
#     unique_key_violation = 0
#     exit()
#     for row in list_of_tuples[0]:
#         #print(row)
#         station = row[0]
#         latitude = row[2]
#         longitude = row[3]
#         elevation = row[4]
#         name = row[5]
#         #print(station, latitude, longitude, elevation, name)
#         try:
#             PostgresExecute(
#                 db_name=config.DB_NAME, #'climatedb', 
#                 user=config.DB_USER, #'postgres', 
#                 host=config.DB_HOST, #'192.168.86.32', 
#                 port=config.DB_PORT, #5432, 
#                 query="""
#                 insert into climate.noaa_global_temp_sites 
#                     (station, latitude, longitude, elevation, name)
#                 values (%s, %s, %s, %s, %s)
#                 """, 
#                 data=(station, latitude, longitude, elevation, name), 
#                 commit=True,
#             ).run(password=PrefectSecret('DB'))
#             insert += 1
#         except UniqueViolation:
#             unique_key_violation += 1
#         except InvalidTextRepresentation:
#             pass
#     print(f'INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_stations(list_of_tuples: list):#, password: str):
    insert = 0
    unique_key_violation = 0

    print(len(list_of_tuples))
    insert = 0
    unique_key_violation = 0
    #exit()
    for row in list_of_tuples[1:2]:
        #print(row)
        station = row[0]
        latitude = row[2]
        longitude = row[3]
        elevation = row[4]
        name = row[5]
        #print(station, latitude, longitude, elevation, name)
        try:
            PostgresExecute(
                db_name=config.DB_NAME, #'climatedb', 
                user=config.DB_USER, #'postgres', 
                host=config.DB_HOST, #'192.168.86.32', 
                port=config.DB_PORT, #5432, 
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
        except InvalidTextRepresentation:
            pass
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
                db_name=config.DB_NAME, #'climatedb', 
                user=config.DB_USER, #'postgres', 
                host=config.DB_HOST, #'192.168.86.32', 
                port=config.DB_PORT, #5432,  
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
    print(f'INSERT RESULT: inserted {insert} records | {unique_key_violation} duplicates')

with Flow("psql test") as flow:
    #p = PrefectSecret('DB')
    data_dir = Parameter('data_dir', default=config.NOAA_TEMP_CSV_DIR)
    t0_CSVs = list_csvs(data_dir=data_dir)
    #t1 = open_csv(filename='01023099999.csv')
    t1 = open_csv.map(t0_CSVs)
    t2 = insert_stations.map(list_of_tuples=t1)
    t3 = insert_records(list_of_tuples=t1, waiting_for=t2)

flow.run()

# if os.environ.get('PREFECT_ENV') == 'test':
#     schedule = None#IntervalSchedule(interval=timedelta(minutes=0.1))
# else:
#     schedule = IntervalSchedule(interval=timedelta(days=3))

# with Flow('NOAA Global Temp Data Processing', schedule=schedule) as flow:
#     year = Parameter('year', default=date.today().year)
#      ...

# if config.PREFECT_ENV in ('local', 'test'):
#     flow.run()
# else:
#     flow.register(project_name="Global Warming Data")