
FROM python:3.12-bookworm

WORKDIR /land-registry-data

RUN python3 -m venv venv

RUN ./venv/bin/pip3 install confluent-kafka jsons sqlalchemy psycopg2-binary boto3 botocore typeguard requests pandas --upgrade --no-cache-dir

COPY ./lib_land_registry_data ./lib_land_registry_data
COPY ./land_registry_pp_monthly_update_downloader.py ./land_registry_pp_monthly_update_downloader.py

#ENTRYPOINT ["./venv/bin/python3", "land_registry_pp_monthly_update_downloader.py"]
CMD ["./venv/bin/python3", "land_registry_pp_monthly_update_downloader.py"]
