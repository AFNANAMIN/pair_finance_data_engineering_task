from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from queries import *

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        print('The maximum temperatures measured for every device per hours.')
        max_temp(psql_engine,mysql_engine)
        print('The amount of data points aggregated for every device per hours.')
        count_data(psql_engine,mysql_engine)
        print('Total distance of device movement for every device per hours.')
        total_distance(psql_engine,mysql_engine)
        break
    except OperationalError:
        sleep(0.1)





