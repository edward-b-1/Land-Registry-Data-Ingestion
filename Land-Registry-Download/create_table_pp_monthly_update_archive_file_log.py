
from sqlalchemy import create_engine
from sqlalchemy import text
from lib_land_registry_data.lib_db import LandRegistryBase

# TODO: use env file
url = 'postgresql://postgres:adminpassword@192.168.0.232/postgres'
engine = create_engine(url)

with engine.connect() as connection:
    connection.execute(text('create schema if not exists land_registry_2'))
    connection.commit()

print(f'list of tables')
for table in LandRegistryBase.metadata.tables.keys():
    print(table)

LandRegistryBase.metadata.tables['land_registry_2.pp_monthly_update_archive_file_log'].create(engine)
