
FROM python:3.12-bookworm

WORKDIR /land-registry-data

RUN python3 -m venv venv

RUN ./venv/bin/pip3 install confluent-kafka jsons sqlalchemy psycopg2-binary croniter typeguard --upgrade --no-cache-dir

COPY ./lib_land_registry_data ./lib_land_registry_data
COPY ./land_registry_cron_trigger.py ./land_registry_cron_trigger.py

#ENTRYPOINT ["./venv/bin/python3", "land_registry_cron_trigger.py"]
CMD ["./venv/bin/python3", "land_registry_cron_trigger.py"]
