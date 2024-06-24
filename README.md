# Land-Registry-Data-Ingestion
Land Registry Data Ingestion System


# What?

This is a data ingestion system which downloads data from the Land Registry and ingests this data into a database. The Land Registry dataset contains price paid data for properties sold in the UK.


# Why?

The easiest way to obtain the Land Registry data would be to simply download the complete file from the website. (`pp-complete.txt`) It would be relatively trivial to build a single process which simply downloads the data on a per-day or per-month basis. However, how would we implement such a data source with a database?

The intention is to store this data in a PostgreSQL database such that it can be queried. The alternative would be to build an analysis code which starts by loading the entire file from disk into a Pandas dataframe. This is not only slow, but consumes significant quantities of memory. Storing the data in a queryable database is useful as often an analysis will focus on one category of data, for example data for flats or properties sold during a particular period of time. Therefore this system is designed to ingest data to a SQL database.


# How?

The Land Registry publish the complete dataset `pp-complete.txt` each month. A monthly update dataset is also published. This dataset contains a list of rows to insert, modify or delete. By applying these operations on a monthly basis, the complete data can be maintained across time.

#### Why not just upload the complete dataset each month?

This would be possible, but not particularly convenient. In particular, uploading the entire dataset takes about an hour, and the number of rows grows each month. While this method would work, it would create a period of downtime where the data in the database is not accessible because it has been deleted pending the upload of a new dataset.

For a personal project, this is not really a concern, however part of the purpose of this is to show an example of a production-grade system which could be used by analysts 24/7 without downtime.

There is also another more important point. Data from the Land Registry is published monthly. However, this does not guarantee that once any months data has been published, that no new data will be added for that month at a later date. Indeed, in most cases, there is some unknown and variable delay between a property being sold, and the price paid data being updated in the dataset available here. In addition, changes can be made at any point in the future.

From a practical standpoint of performing data analysis, there is no certain way to know as to what date data can be considered "reliable" or "mostly complete". Put another way, if the complete data file were to be downloaded on the 2024-07-01, there would likely be transactions for all (working) days before this date. However, if the data file is downloaded the next month, it is usually the case that a significant number of new rows will appear for transaction dates prior to 2024-07-01.

It is only by recording information about the date when data becomes available that an analysis can be performed to measure the expected distribution of delay times between a reported transaction date, and the data becoming available in the downloaded file. The file only contains transaction dates. It does not declare when any particular row of data was added. This is the primary reason why I built this system.

The following figures may help to explain this concept in more detail:

The first figure shows the count of the number of properties by *delay time*. Delay time is defined as the number of days between the data on which the dataset was published and the transaction date.

![Count of number of properties by measured delay time](image/normalized_delay_count.png)

At the time of writing, the first captured publish date was 2023-09-10. This date does not co-incide with a scheduled release date for the data. The next captured publish date was 2023-09-29. When the data was downloaded on this date, new rows appeared for the month of September. These new rows were not present in the previously downloaded file. Therefore, from this date onwards, it is possible to measure the delay time between a transaction occuring (transaction date value of a row) and rows appearing in the published dataset. (This is not known exactly but approximated. The monthly release schedule is known, and by downloading the data each day it is possible to record a "created datetime" for each row.)

Note that data beyond approximately 250 days is not valid, as this corresponds to a transaction date of before September 2023.

The data suggests that a significant number of transactions are delayed by more than 9 months.

The housing market data contains significant seasonal effects, as more properties are sold during the spring and less are sold in the first few months of the year. It is difficult to account for seasonal effects.

The data shown in the below figure is corrected for daily transaction volume by producing a weight which is inversely proportional to the number of transactions which occur each day. This corrects for bias in the above figure caused by variations in the number of sales across time. As an additional bonus, since there are a large number of rows with transaction dates before September 2023, these rows have very small associated weights. The result is that data older than September 2023 is diminished in the normalized figure.

![Normalized distribution of properties by measured delay time](image/normalized_delay_weight.png)


# System Archetecture

The system consists of a data ingestion pipeline. This is a number of processes which operate on a target data file in sequence. Processes communicate with each other by sending messages via Kafka. Some auxillary data is stored in database tables when it is useful to have this data in tabular format. The list of processes, in their order of operation are:

- Cron Trigger: (`land_registry_cron_trigger.py`) This process emits a message to Kafka triggered by a CRON schedule. Rather than using the Linux CRON system, a CRON Python library is used to generate a target datetime for when the process should next dispatch a message. The process uses `sleep` to wait until the target time.
- Complete Download: (`land_registry_complete_downloader.py`) This process downloads the complete dataset `pp-complete.txt` when triggered by receiving a message from Kafka.
- Monthly Update Download: (`land_registry_monthly_update_downloader.py`) This process downloads the incremental monthly update dataset when triggered by receiving a message from Kafka. This file then becomes the target file for the processes which follow.
- SHA-256 Calculator: (`land_registry_monthly_update_sha256_calculator.py`) This process calcualtes the sha-256 sum of the target file. This is used to compare pairs of files across days. If the shasum of a downloaded file differs from the previous days file, then this implies the file contains new data which should be ingested to the database table.
- Data Decision: (`land_registry_monthly_update_data_decision.py`) This process contains the logic to make a decision about what to do with the most recently downloaded file. It uses the calculated shasum values to decide if a file should be uploaded to the database or ignored and deleted.
- Database Upload: (`land_registry_monthly_update_database_uploader.py`) This process reads the monthly update file and updates the database table with the new data if the previous data decision process has marked the file to be uploaded. Otherwise, the file is ignored, and a message is forwared to trigger the garbage collector.
- Garbage Collector: (`land_registry_monthly_update_garbage_collector.py`) This process performs garbage collection of old files which were marked as containing no new data. Copies of the complete data (`pp-complete.txt`) are not currently deleted by this process. These files must be removed manually.
- Data Verification: (`land_registry_database_verify.py`) This process is not part of the chain of processes which run automatically. It does not communicate via Kafka. It is a manually run process which verifies that the data in the complete data file (`pp-complete.txt`) matches that of the database. It does not attempt to perform any reconcilliation in the case of any differences, since the updates issued by the Land Registry to the complete file are not always synchronized with the release of a new monthly update file.

The CRON interval is 1 day, and the trigger fires at midnight each day. Therefore the data files are downloaded everyday.

In many cases, logic from more than one process could have easily been combined into a single process. Separating as much logic as possible into individual processes has some advantages.

- Debugging a single process generally becomes easier because it is simpler (not that any of these processes are particularly complex)
- It is easier to replace a process, or substitute it for something else. The processes and Kafka messaging system form a directed graph. This choice of archetecture leads to a flexibile system, new components can easily be added by reading the control messgaes from Kafka.
- For example, it would be easy to add another garbage collection process to delete old `pp-complete.txt` files automatically, keeping only the latest one. This might be a fun challenge for anyone who wanted to play with this system.


## Database Tables

Here is a list of database tables with some explanation as to their purpose:

- `price_paid_data`. This is the main data table which contains the ingested price paid data
- `price_paid_data_log`. This table records information about how many rows were inserted, updated or deleted when the database upload process runs.
- `price_paid_data_monthly_update_file_log`. This table contains auxillary data produced by processes in the data pipeline chain. It records filenames of downloaded files, the shasum, the creation datetime, the datetime when the data decision process ran, the result of the data decision process, and either the upload or delete datetime, depending on the decision which was made.
- `price_paid_data_monthly_update_database_updater_operation_log`. This is a more detailed breakdown of the statistics summarized in the `price_paid_data_log` table. It is mostly useful for debugging, and reports detailed statistics about the operations performed by the database upload process.

## System Requirements

Ideally, this should be run on Linux. Systemd is used to manage processes, so while the code should run on any operating system (Python is cross platform) the install script will only work on Linux. You will also need:

- a Kafka cluster (or single Kafka broker) for inter-process messaging and communication
- a Postgres database
- systemd for process management
- bash shell

# Setup Instructions

The database and Kafka connection details need to be entered into the environments configuration file. This can be found in the `config` subdirectory.

- `KAFKA_BOOTSTRAP_SERVERS`: Single string containing the Kafka Bootstrap Server addresses in the standard format accepted by Kafka
- `POSTGRES_ADDRESS`: IP address of Postgres database
- `POSTGRES_USER`: Postgres username, default `postgres`
- `POSTGRES_PASSWORD`: Postgres password for user specified by `POSTGRES_USER`
- `POSTGRES_DATABASE`: Postgres database name, default `postgres`

Installed software requirements include Python, Pip and the `venv` Python module.

```
$ cd Land-Registry-Download
$ # this will create a virtual environment
$ python3 -m venv .venv
$ # there is a symbolic link `activate` which links to `.venv/bin/python3`
$ ./activate # or source .venv/bin/python3
$ pip3 install -r requirements.txt
```

The processes can then either be run individually, or you can run the installer script to install the whole system. This script will create required users, groups and setup systemd with scripts to run each process.

# Installation

```
$ cd install
$ sudo ./install.sh
```

TODO: add full SQL code requried for creation of the database tables

# Project Structure

The subdirectories of this repository are described here:

- `/`: Root directory containing Python executables, and pip `requirements.txt`
- `/initialize_database`: Contains process to perform the initial database initialization using a copy of `pp-complete.txt`
- `/data`: Contains the column header names for tables. TODO: check if still used
- `/install`: Contains installer bash script
- `/lib_land_registry_download`: Python package (library) for common code required by each process
- `/sql`: SQL scripts for database table creation
- `/systemd-scripts`: Helper scripts for systemd
- `/systemd-unit-service-files`: Service files for systemd units, one for each process managed by systemd