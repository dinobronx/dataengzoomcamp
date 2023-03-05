#!/usr/bin/env python
# coding: utf-8
 
import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# sample command:

# python local_spark.py \
#     --input_green=data/pq/green/2020/*/ \
#     --input_yellow=data/pq/yellow/2020/*/ \
#     --output=data/report-2020



# for multiple clusters, we might have different masters
# we can parameterize master like this
# in practice this is how we initialize spark master, thru spark submit
# URL="spark://de-zoomcamp.us-east5-a.c.alien-handler-376020.internal:7077"
# spark-submit \
#     --master="${URL}" \
#     local_spark.py \
#         --input_green=data/pq/green/2020/*/ \
#         --input_yellow=data/pq/yellow/2020/*/ \
#         --output=data/report-2020

# running dataproc thru gcloud sdk
# gcloud dataproc jobs submit pyspark \
#     --cluster="dataeng-zoomcamp-cluster" \
#     --region="us-east5" \
#     "gs://dtc_data_lake_alien-handler-376020/code/local_spark.py" \
#     -- \
#         --input_green=gs://dtc_data_lake_alien-handler-376020/pq/green/2021/*/ \
#         --input_yellow=gs://dtc_data_lake_alien-handler-376020/pq/yellow/2021/*/ \
#         --output=gs://dtc_data_lake_alien-handler-376020/report-2021

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

df_green = spark.read.parquet(input_green)
df_green = df_green \
            .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


df_yellow = spark.read.parquet(input_yellow)

df_yellow = df_yellow \
            .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
            .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_columns = [
 'VendorID',
 'pickup_datetime',
 'dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge'
]

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
    SELECT
    -- Revenue grouping
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    
    service_type,

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_COUNT) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

    FROM 
        trips_data
    GROUP BY revenue_zone, revenue_month, service_type
""")

df_result.coalesce(1).write.parquet(output, mode='overwrite')
