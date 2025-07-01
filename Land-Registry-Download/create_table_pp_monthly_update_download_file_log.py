
from sqlalchemy import create_engine
from sqlalchemy import text
from lib_land_registry_data.lib_db import LandRegistryBase


def main(recreate: bool):
    # TODO: use env file
    url = 'postgresql://postgres:adminpassword@192.168.0.232/postgres'
    engine = create_engine(url)

    with engine.connect() as connection:
        connection.execute(text('create schema if not exists land_registry_2'))
        connection.commit()

    print(f'list of tables')
    for table in LandRegistryBase.metadata.tables.keys():
        print(table)

    if recreate:
        LandRegistryBase.metadata.tables['land_registry_2.pp_monthly_update_download_file_log'].drop(engine)

    LandRegistryBase.metadata.tables['land_registry_2.pp_monthly_update_download_file_log'].create(engine)


if __name__ == '__main__':
    main(recreate=False)

