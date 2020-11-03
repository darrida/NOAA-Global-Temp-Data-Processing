from prefect import task, Flow, Parameter
from prefect.tasks.postgres import PostgresExecute, PostgresFetch
from prefect.tasks.secrets import EnvVarSecret, PrefectSecret
import psycopg2 as pg
from psycopg2.errors import UniqueViolation # pylint: disable=no-name-in-module
from csv import reader

# url = 'https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/'

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def open_csv(filename: str):
    with open(filename) as read_obj:
        csv_reader = reader(read_obj)
        # Get all rows of csv from csv_reader object as list of tuples
        return list(map(tuple, csv_reader))

@task(log_stdout=True) # pylint: disable=no-value-for-parameter
def insert_stations(list_of_tuples: list):#, password: str):
    insert = 0
    unique_key_violation = 0
    for row in list_of_tuples[1:2]:
        station = row[0]
        latitude = row[2]
        longitude = row[3]
        elevation = row[4]
        name = row[5]
        try:
            PostgresExecute(
                db_name='climatedb', 
                user='postgres', 
                host='192.168.86.32', 
                port=5432, 
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
                db_name='climatedb', 
                user='postgres', 
                host='192.168.86.32', 
                port=5432, 
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
    t1 = open_csv(filename='01023099999.csv')
    t2 = insert_stations(list_of_tuples=t1)
    t3 = insert_records(list_of_tuples=t1, waiting_for=t2)

flow.run()

# if os.environ.get('PREFECT_ENV') == 'test':
#     schedule = None#IntervalSchedule(interval=timedelta(minutes=0.1))
# else:
#     schedule = IntervalSchedule(interval=timedelta(days=3))

# with Flow('NOAA Daily Avg Current Year', schedule=schedule) as flow:
#     year = Parameter('year', default=date.today().year)
#      ...

# if os.environ.get('PREFECT_ENV') in ('local', 'test'):
#     flow.run()
# else:
#     flow.register(project_name="Global Warming Data")