
## Question 2
import dlt
import duckdb
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name = "rides")
def ny_taxi():
    client = RESTClient(
        base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator = PageNumberPaginator(
            base_page = 1,
            total_path = None
        )
    )
    for page in client.paginate("data_engineer_zoomcamp_api"):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(ny_taxi)
print(load_info)

#from google.colab import data_table
#data_table.enable_dataframe_formatter()

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

# Question 3

# inspect the table ride
df = pipeline.dataset(dataset_type="default").rides.df()
df

# Question 4
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)