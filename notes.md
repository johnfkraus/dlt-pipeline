

/Users/blauerbock/workspaces/dlt-pipeline/.venv/bin/python /Users/blauerbock/workspaces/dlt-pipeline/bronze_c01.py 
2026-02-27 06:00:25,624|[WARNING]|91171|8319623360|dlt|_adbc_jobs.py|_loader_file_format_selector:124|parquet file format was requested for table c01_bronze but ADBC driver for postgresql was not installed:
 No module named 'adbc_driver_manager'
 Read more: https://dlthub.com/docs/dlt-ecosystem/destinations/postgres#fast-loading-with-arrow-tables-and-parquet
2026-02-27 06:00:25,624|[WARNING]|91171|8319623360|dlt|worker.py|_get_items_normalizer:144|The configured value `parquet` for `loader_file_format` is not supported for table `c01_bronze` and will be ignored. dlt will use a supported format instead.
2026-02-27 06:00:25,625|[WARNING]|91171|8319623360|dlt|validate.py|verify_normalized_table:91|In schema `bronze_c01`: The following columns in table 'c01_bronze' did not receive any data during this load and therefore could not have their types inferred:
  - target

Unless type hints are provided, these columns will not be materialized in the destination.
One way to provide type hints is to use the 'columns' argument in the '@dlt.resource' decorator.  For example:

@dlt.resource(columns={'target': {'data_type': 'text'}})

Pipeline bronze_c01 load step completed in 0.09 seconds
1 load package(s) were loaded to destination postgres and into dataset bronze_20260227110025
The postgres destination used postgresql://postgres:***@localhost:5432/comms location to store data
Load package 1772190025.5754838 is LOADED and contains no failed jobs

Process finished with exit code 0
