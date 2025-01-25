Code used for Homework 1

### Question 1. Understanding docker first run
Run in command line, to run python container with bash. Then get pip version once container is running
```bash
docker run -it --entrypoint=bash # run python container with bash
pip --version # pip 24.3.1
```

### Question 2. Understanding Docker networking and docker-compose
Code isn't needed for this question. Answer: db:5432

### Prepare Postgres
The `Dockerfile` and `ingest_data.py` were used to create an image that processes the data. The `docker-compose.yaml` file was used to run the postgres database and pgadmin (used for SQL later on). The data processing container and compose containers are connected using the same network, `my-network`
Then run this codeblock in the command line
```bash
# Build Python Data Processing Image
docker build -t hw1:v1 .

# Run Postgres database and pgadmin 
docker-compose up

# Load green taxi trip and zone lookup datasets into postgres database
docker run -it --rm \
    --network my-network \
    hw1:v1 \
    --user=postgres \
    --password=postgres \
    --host=db \
    --port=5432 \
    --db=ny_taxi
```

### Question 3. Trip Segmentation Count
Using pgadmin to query for trips in October 2019

```sql
-- Up to 1 mile (104,802)
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND
	  lpep_dropoff_datetime < '2019-11-01' AND
	  trip_distance <= 1

-- In between 1 (exclusive) and 3 miles (inclusive) (198,924)
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND
	  lpep_dropoff_datetime < '2019-11-01' AND
	  trip_distance > 1 AND
	  trip_distance <= 3

-- In between 3 (exclusive) and 7 miles (inclusive) (109,603)
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND
	  lpep_dropoff_datetime < '2019-11-01' AND
	  trip_distance > 3 AND
	  trip_distance <= 7

-- In between 7 (exclusive) and 10 miles (inclusive) (27,678)
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND
	  lpep_dropoff_datetime < '2019-11-01' AND
	  trip_distance > 7 AND
	  trip_distance <= 10

-- Over 10 miles (35,189)
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND
	  lpep_dropoff_datetime < '2019-11-01' AND
	  trip_distance > 10
```

### Question 4. Longest trip for each day
```sql
-- Pick up day with longest trip distance (2019-10-31)
SELECT
	CAST(lpep_pickup_datetime AS DATE) AS "pickup_day",
	MAX(trip_distance)
FROM green_taxi_trips
GROUP BY
	CAST(lpep_pickup_datetime AS DATE)
ORDER BY MAX(trip_distance) DESC
LIMIT 1
```

### Question 5. Three biggest pickup zones

```sql
-- Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
-- East Harlemn North, East Harlem South, Morningside Heights
WITH temp_table AS (
	SELECT
		CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
		"PULocationID" AS location_id,
		total_amount,
		z."Zone" AS zone
	FROM green_taxi_trips AS g
	LEFT JOIN zone_lookup AS z
	ON g."PULocationID" = z."LocationID"
	WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-10-18'
)
SELECT zone, sum(total_amount) AS sum_amount
FROM temp_table
GROUP BY zone
HAVING sum(total_amount) >= 13000

```

### Question 6. Largest Tip

```sql
-- For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
-- JFK Airport
WITH temp_table AS (
	SELECT
		CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
		"PULocationID" AS pulocation_id,
		"DOLocationID" AS dolocation_id,
		zpu."Zone" AS pu_zone,
		zdo."Zone" AS do_zone,
		tip_amount
	FROM green_taxi_trips AS g,
		 zone_lookup as zpu,
		 zone_lookup as zdo
	WHERE g."PULocationID" = zpu."LocationID" AND
		  g."DOLocationID" = zdo."LocationID" AND
		  zpu."Zone" = 'East Harlem North' AND
		  g.lpep_pickup_datetime BETWEEN '2019-10-01' AND '2019-10-31'
)
SELECT do_zone, MAX(tip_amount) AS max_tip
FROM temp_table
GROUP BY do_zone
ORDER BY MAX(tip_amount) DESC
LIMIT 1
```

### Terraform
Credentials stored locally and do not need to use gcloud auth. Used `main.tf` and `variables.tf` for terraform work.
Run following in command line to create a GCP Bucket and Big Query Dataset, and then destroy it
```bash
terraform init # install plugins
terraform plan # make sure changes are correct
terraform apply # create bucket and query dataset
terraform destroy # destroy infrastructure from account
```

### Question 7. Terraform Workflow
No need for code.
1. Downloading the provider plugins and setting up back end - `terraform init`
2. Generating proposed changes and auto-executing the plan - `terraform apply -auto-approve`
3. Remove all resources managed by terraform - `terraform destroy`








