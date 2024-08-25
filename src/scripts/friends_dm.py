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

    spark = SparkSession.builder.appName("Project").getOrCreate()
    spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
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

    df = df.withColumn("distance", F.lit(2) * F.lit(6371)
                                            * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('lat')) - F.radians(F.col('lat_city')))/F.lit(2)), 2)
                                            + F.cos(F.radians(F.col('lat')))
                                            * F.cos(F.radians(F.col('lat_city')))
                                            * F.pow(F.sin((F.radians(F.col('lon')) - F.radians(F.col('lon_city')))/F.lit(2)), 2))))

    df = df.select('event', 'event_type', 'zone_id', 'city', 'distance', 'lat', 'lon', 'timezone')
    df = df.join(df.select('event', 'distance').groupBy('event').agg(F.min('distance')), 'event')
    df = df.filter((F.col('distance') == F.col('min(distance)')) & (F.col('event.message_from').isNotNull())) 

    one_group_users = df.select('event.subscription_channel', F.col('event.user').alias('user_left'))\
                           .filter(df.event.subscription_channel.isNotNull() & df.event.user.isNotNull())

    one_group_users = one_group_users.join(one_group_users.select('subscription_channel', F.col('user_left').alias('user_right')), 'subscription_channel')
    one_group_users = one_group_users.filter(one_group_users.user_left != one_group_users.user_right)

    match_users = df.groupBy("event.message_from", "event.message_to")\
                         .count().filter(F.col("count") > 1).select(
        F.col("message_from").alias("user_left"),
        F.col("message_to").alias("user_right")
    )

    recomentadion_df = one_group_users.join(match_users,
                                                (one_group_users["user_left"].contains(match_users["user_left"])) &
                                                (one_group_users["user_right"].contains(match_users["user_right"])),
                                                "leftanti")

    win = Window.partitionBy('event.user').orderBy(F.col('event.datetime').desc())

    user_geo = df.withColumn('rank', F.row_number().over(win)).filter(F.col('rank') == 1).select('event.user', 'lat', 'lon') 

    df1 = recomentadion_df.join(user_geo.select('user', F.col('lat').alias('ul_lat'), F.col('lon').alias('ul_lon')),
                                       recomentadion_df.user_left == user_geo.user, "left") \
                                               .join(user_geo.select('user', F.col('lat').alias('ur_lat'), F.col('lon').alias('ur_lon')),
                                                     recomentadion_df.user_right == user_geo.user, "left")

    df1 = df1.withColumn("distance", F.lit(2) * F.lit(6371) 
                                                          * F.asin(F.sqrt(F.pow(F.sin((F.radians(F.col('ul_lat')) - F.radians(F.col('ur_lat')))/F.lit(2)), 2)
                                                          + F.cos(F.radians(F.col('ul_lat'))) * F.cos(F.radians(F.col('ur_lat')))
                                                          * F.pow(F.sin((F.radians(F.col('ul_lon')) - F.radians(F.col('ur_lon')))/F.lit(2)), 2))))

    recomentadion_df = df1.select('user_left', 'user_right').filter(F.col('distance') <= 1)

    recomentadion_df = recomentadion_df.withColumn("processed_dttm", F.current_timestamp())

    user_zone = df.select('event.user',  'zone_id') 
    recomentadion_df = recomentadion_df.join(user_zone, F.col("user_left") == F.col("user"), "left").drop('user')

    local_time_df = df.select('zone_id', 'timezone', F.date_format('event.datetime', "HH:mm:ss").alias('time_utc'))
    recomentadion_df = recomentadion_df.join(local_time_df, recomentadion_df.zone_id == local_time_df.zone_id)
    recomentadion_df = recomentadion_df.withColumn("local_time", F.from_utc_timestamp(F.col("time_utc"), F.col("timezone")))\
                                         .drop('local_time_df.zone_id', 'timezone', 'time_utc')

    recomentadion_df.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")


if __name__ == "__main__":
    main()