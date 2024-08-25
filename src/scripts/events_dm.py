import sys
import os
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.window import Window

os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"


def commaToDot(df, column) -> 'df':
    df = df.withColumn(column, F.regexp_replace(column, ',', '.'))
    df = df.withColumn(column, df[column].cast("float"))
    return df


def main():
    date = sys.argv[1]
    path_to_geo_events = sys.argv[2]
    path_to_geo_city = sys.argv[3] 
    output_base_path = sys.argv[4]
    earth_radius = 6371

    spark = SparkSession.builder.appName("Project").getOrCreate()

    geo_df = spark.read.csv(path_to_geo_city,
                            sep=';',
                            header=True,
                            inferSchema=True)

    geo_df = commaToDot(geo_df, 'lat')
    geo_df = commaToDot(geo_df, 'lng')

    events_df = spark.read.parquet(f'{path_to_geo_events}/date={date}')

    df = events_df.crossJoin(geo_df.select(F.col('id').alias('zone_id'),
                                           F.col('city'),
                                           F.col('lat').alias('lat_city'),
                                           F.col('lng').alias('lon_city'),
                                           F.col('timezone')))

    home_address_df = events_df.crossJoin(geo_df.select(F.col('id').alias('zone_id'),
                                                                F.col('city'),
                                                                F.col('lat').alias('lat_city'),
                                                                F.col('lng').alias('lon_city'),
                                                                F.col('timezone')))

    df = df.withColumn("distance", F.lit(2) * F.lit(earth_radius)
                                            * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat')) - F.radians(F.col('lat_city')))/F.lit(2)), 2)
                                            + F.cos(F.radians(F.col('lat')))
                                            * F.cos(F.radians(F.col('lat_city')))
                                            * F.pow(F.sin((F.radians(F.col('lon')) - F.radians(F.col('lon_city')))/F.lit(2)), 2))))

    df = df.select('event', 'event_type', 'zone_id', 'city', 'distance')
    df = df.join(df.select('event', 'distance').groupBy('event').agg(F.min('distance')), 'event')
    df = df.filter((F.col('distance') == F.col('min(distance)')) & (F.col('event.user').isNotNull())) 

    win = Window.partitionBy("event.user").orderBy(F.desc("event.datetime"))
    df = df.withColumn("row_number", F.row_number().over(win))
    true_city_df = df.filter(df.row_number == 1).select(F.col('event.user').alias('user_id'), F.col('city').alias('act_city'))
    df = df.drop("row_number")

    df1 = home_address_df.select(F.col('event.message_from').alias('user_id'),"city",F.date_trunc("day",F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date"))
    home_city_df = (df1.withColumn("row_date",F.row_number().over(
            Window.partitionBy("user_id", "city").orderBy(F.col("date").desc())))
            .filter(F.col("row_date") >= 27)
            .withColumn("row_by_row_date", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("row_date").desc())))
            .filter(F.col("row_by_row_date") == 1)
            .withColumnRenamed("user_id", "user_id")
            .withColumnRenamed("city", "home_city")
            .drop("row_date", "row_by_row_date", "date"))

    users_dm = true_city_df.join(home_city_df, "user_id", "left")


    win2 = Window.partitionBy('event.user').orderBy('event.datetime')
    df2 = df.withColumn('lag_city', F.lag('city').over(win2))
    df2 = df2.withColumn('lead_city', F.lead('city').over(win2))
    df3 = df2.filter((df2['lag_city'] != df2['city']) | (df2['lead_city'] != df2['city']))
    df3_count = df3.groupBy(F.col('event.user').alias('user_id')).agg(F.count('*').alias('travel_count'))
    df3_array = df3.groupBy(F.col('event.user').alias('user_id')).agg(F.collect_list('city').alias('travel_array'))

    users_dm = users_dm.join(df3_count, 'user_id', 'left') \
                                       .join(df3_array, 'user_id', 'left')

    local_time_df = true_city_df.join(events_df.select(F.col('event.user').alias('user_id'),
                                                      F.date_format('event.datetime', "HH:mm:ss") \
                                                       .alias('time_utc')), 'user_id', "left")
    local_time_df = local_time_df.join(geo_df, local_time_df['act_city'] == geo_df['city'], "left")
    local_time_df = local_time_df.withColumn("local_time", F.from_utc_timestamp(F.col("time_utc"), F.col("timezone")))
    local_time_df = local_time_df.select('user_id', 'local_time')

    users_dm = users_dm.join(local_time_df, 'user_id', 'left')

    users_dm.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")


if __name__ == "__main__":
    main()