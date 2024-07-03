#!/usr/bin/bash

export POSTGRES_ADDRESS=192.168.0.232
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=adminpassword
export POSTGRES_DATABASE=postgres

./land_registry_database_verify.py --input-file /data-land-registry/pp-complete/pp-complete-2024-07-02.txt #--fix-database