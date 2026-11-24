"""Schémas explicites partagés entre batch (Partie 3) et streaming (Parties 4-5)."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

trip_schema = StructType([
    StructField("trip_id",       StringType(),  True),
    StructField("user_id",       StringType(),  True),
    StructField("vehicle_id",    StringType(),  True),
    StructField("vehicle_type",  StringType(),  True),
    StructField("start_station", StringType(),  True),
    StructField("end_station",   StringType(),  True),
    StructField("city",          StringType(),  True),
    StructField("status",        StringType(),  True),
    StructField("duration_s",    IntegerType(), True),
    StructField("distance_km",   DoubleType(),  True),
    StructField("battery_pct",   IntegerType(), True),
    StructField("price_mad",     DoubleType(),  True),
    StructField("event_time",    StringType(),  True),
])

station_schema = StructType([
    StructField("station_id", StringType(),  True),
    StructField("name",       StringType(),  True),
    StructField("city",       StringType(),  True),
    StructField("district",   StringType(),  True),
    StructField("zone_type",  StringType(),  True),
    StructField("capacity",   IntegerType(), True),
    StructField("latitude",   DoubleType(),  True),
    StructField("longitude",  DoubleType(),  True),
])
