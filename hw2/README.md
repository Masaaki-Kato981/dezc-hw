Code used for Homework 2

### Quiz Questions
0. For the quiz questions below, used the `gcp_taxi_scheduled.yaml` flow in kestra to answer using backfills.
1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
    * Ran the yellow taxi backfill from 12/01/2020 to 12/31/2020
    * `128.3 MB`
2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
    * Run green taxi backfill from 04/01/2020 to 04/30/2020
    * `green_tripdata_2020-04.csv`
3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
    * Run yellow taxi backfill from 01/01/2020 to 12/30/2020
    * `24,648,499`
    * SQL query to double check
    ```sql
    SELECT COUNT(*)
    FROM `project_id.zoomcamp.yellow_tripdata` 
    WHERE filename LIKE '%2020%'
    ```
4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
    * Run green taxi backfill from 01/01/2020 to 12/30/2020
    * `1,734,051`
    * SQL query to double check
    ```sql
    SELECT COUNT(*)
    FROM `project_id.zoomcamp.green_tripdata` 
    WHERE filename LIKE '%2020%'
    ```
5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
    * Run yellow taxi backfrill from 03/01/2021 to 03/30/2021
    * `1,925,152`
    * SQL query to double check
    ```sql
    SELECT COUNT(*)
    FROM `project_id.zoomcamp.yellow_tripdata` 
    WHERE filename LIKE '%2021-03%'
    ```
6. How would you configure the timezone to New York in a Schedule trigger?
    * Add a timezone property set to America/New_York in the Schedule trigger configuration