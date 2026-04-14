import requests
from dagster_essentials.defs.assets import constants
import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource
import os
from dagster._utils.backoff import backoff
from dagster_essentials.defs.partitions import monthly_partition
import pandas as pd


@dg.asset(
    partitions_def=monthly_partition,
    group_name="raw_files",
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # to rm -DD from date YYYY-MM-DD and match partition_key
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
      
    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return dg.MaterializeResult(
        metadata={
            'Number of records': dg.MetadataValue.int(num_rows)
        }
    )


@dg.asset(
    group_name="raw_files",
)
def taxi_zones_file() -> dg.MaterializeResult:
    """
      The raw parquet files for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get("https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv")

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)
    
    num_rows = len(pd.read_csv(constants.TAXI_ZONES_FILE_PATH))

    return dg.MaterializeResult(
        metadata={
            'Number of records': dg.MetadataValue.int(num_rows)
        }
    )


@dg.asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="ingested",
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
      drop table trips;

      create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
      );

      delete from trips where partition_date = '{month_to_fetch}';

      insert into trips
      select
        VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
        tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

    with database.get_connection() as conn:
        conn.execute(query)

    # check if rows have been pushed correctly
    query_all_results = "select * from trips;"
    with database.get_connection() as conn:
        results = conn.execute(query_all_results).fetch_df()
    print("ALL RESULTS")
    print(results)

    query_january_si_results = query = f"""
        select
          date_part('hour', pickup_datetime) as hour_of_day,
          date_part('dayofweek', pickup_datetime) as day_of_week_num,
          case date_part('dayofweek', pickup_datetime)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
          end as day_of_week,
          count(*) as num_trips
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where pickup_datetime >= '2023-01-11'
        and pickup_datetime < '2023-01-25'
        and pickup_zone_id in (
          select zone_id
          from zones
          where borough = 'Staten Island'
        )
        group by 1, 2
        order by 1, 2 asc
    """
    with database.get_connection() as conn:
        results = conn.execute(query_january_si_results).fetch_df()
    print("JANUARY STATEN ISLAND AGG RESULTS")
    print(results)

@dg.asset(
    deps=["taxi_zones_file"],
    group_name="ingested",
)
def taxi_zones(database: DuckDBResource) -> None:
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)