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

    df = df.join(df.select('event', 'distance').groupBy('event').agg(F.min('distance')), 'event')

    df = df.where(F.col('distance') == F.col('min(distance)')).select('event', 'event_type', 'zone_id')

    df = df.withColumn('month', F.trunc('event.datetime', 'month'))
    df = df.withColumn('week', F.trunc('event.datetime', 'week'))

    subscription_df = df.filter(F.col('event_type') == 'subscription')
    reaction_df = df.filter(df['event_type'] == 'reaction')
    message_df = df.filter(df['event_type'] == 'message')

    subscription_df_grouped = subscription_df.groupBy('zone_id', 'month', 'week').agg(F.count('*').alias('week_subscription'))
    reaction_df_grouped = reaction_df.groupBy('zone_id', 'month', 'week').agg(F.count('*').alias('week_reaction'))
    message_df_grouped = message_df.groupBy('zone_id', 'month', 'week').agg(F.count('*').alias('week_message'))
    df_user_grouped = df.groupBy('zone_id', 'month', 'week').agg(F.countDistinct('event.message_from').alias('week_user'))

    subscription_df_grouped = subscription_df_grouped.withColumn('month_subscription', F.sum('week_subscription').over(Window.partitionBy('zone_id', 'month')))
    reaction_df_grouped = reaction_df_grouped.withColumn('month_reaction', F.sum('week_reaction').over(Window.partitionBy('zone_id', 'month')))
    message_df_grouped = message_df_grouped.withColumn('month_message', F.sum('week_message').over(Window.partitionBy('zone_id', 'month')))
    df_user_grouped = df_user_grouped.withColumn('month_user', F.sum('week_user').over(Window.partitionBy('zone_id', 'month')))

    dm = subscription_df_grouped.join(reaction_df_grouped, on=['zone_id', 'month', 'week'], how='left')
    dm = dm.join(message_df_grouped, on=['zone_id', 'month', 'week'], how='left')
    dm = dm.join(df_user_grouped, on=['zone_id', 'month', 'week'], how='left')

    dm.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")


if __name__ == "__main__":
    main()