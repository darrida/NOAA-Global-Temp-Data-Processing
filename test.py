import psycopg2 as pg

# db_years = PostgresFetch(
#         db_name=local_config.DB_NAME, #'climatedb', 
#         user=local_config.DB_USER, #'postgres', 
#         host=local_config.DB_HOST, #'192.168.86.32', 
#         port=local_config.DB_PORT, #5432, 
#         fetch="all",
#         query="""
#         select distinct year, date_update from climate.csv_checker
#         order by date_update
#         """
#     )


connection = pg.connect(user = "postgres",
                password = "port estimate emission",
                host = "localhost",#"localhost",
                port = "5432",
                database = "climatedb")
cursor = connection.cursor()
cursor.execute('SELECT count(*) FROM "climate"."csv_checker" LIMIT 1000')
print(cursor.fetchone())

cursor.execute("select count(*) from climate.noaa_global_temp_sites")
print(cursor.fetchone())

cursor.execute("select count(*) from climate.noaa_global_daily_temps")
print(cursor.fetchone())
# print(result)