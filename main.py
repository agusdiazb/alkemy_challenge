import logging
from sqlalchemy import create_engine
from datos import tabla_unica, tabla2, tabla_cine


logging.info('Creacion de base de datos')
engine = create_engine('postgresql://postgres:1234@localhost:5432/postgres')


logging.info('Upload tabla 1: tabla_unica a PostgreSQL')

tabla_unica.to_sql('tabla_unica', con = engine, if_exists = 'replace')

logging.info('Upload tabla 2 a PostgreSQL')

tabla2.to_sql('tabla2', con = engine, if_exists = 'replace')

logging.info('Upload tabla 3: tabla_cine a PostgreSQL')

tabla_cine.to_sql('tabla_cine', con = engine, if_exists = 'replace')