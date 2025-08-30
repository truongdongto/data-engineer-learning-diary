from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, lit, coalesce, when,
    regexp_replace,
    # date
    to_date, year, date_format, current_date, date_sub, 
    row_number, dense_rank,
)
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import os
import zipfile

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    #extract and load csv files into Dataframe
    zips_dir = './data'
    extract_to = './temp'
    zip_paths = [os.path.join(zips_dir, f) for f in os.listdir(zips_dir) if f.endswith('.zip')]
    csv_paths = []
    os.makedirs(extract_to, exist_ok=True)

    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()
            zip_ref.extractall(extract_to)
            
        csv_file = [f for f in extracted_files if f.endswith('.csv')][0]
        csv_path = os.path.join(extract_to, csv_file)
        csv_paths.append(csv_path)

    df = spark.read.csv(csv_paths[0], header=True, inferSchema=True)
    for path in csv_paths[1:]:
        df_temp = spark.read.csv(path, header=True, inferSchema=True)
        if df.schema == df_temp.schema:
            df = df.union(df_temp)
        else:
            print(f"Skipping {path} - schema mismatch")
            continue

    #preprocessing
    df = df.dropna()
    df = df.withColumn('trip_duration',
                    coalesce(
                        when(regexp_replace(col('tripduration'), ",", "").cast("float").isNotNull(),
                             regexp_replace(col('tripduration'), ",", "").cast(FloatType())),
                        lit(None)
                    ))\
           .drop('tripduration')
    
    #what is the average trip duration per day?
    avg_trip_duration = df.groupBy(to_date(col('start_time')).alias('date'))\
                          .agg(avg('trip_duration').alias('avg_trip_duration'))\
                          .orderBy('date')
    avg_trip_duration.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/avg_trip_duration')
    
    #how many trips were taken each day?
    trips_per_day = df.groupBy(to_date(col('start_time')).alias('date'))\
                      .agg(count('*').alias('n_trips'))\
                      .orderBy('date')
    trips_per_day.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/trips_per_day')

    #what was the most popular starting trip station for each month?
    num_of_starting_stations = df.groupBy(date_format(col('start_time'), 'yyyy-MM').alias('year_month'), col('from_station_name').alias('station_name')) \
                                 .agg(count('*').alias('n_stations')) \
                                 .orderBy(['year_month', 'n_stations'], ascending=[True, False])
    window = Window.partitionBy('year_month').orderBy(col('n_stations').desc())
    max_num_of_starting_stations = num_of_starting_stations.withColumn('rnk', row_number().over(window)) \
                                                           .filter(col('rnk') == 1) \
                                                           .drop('rnk')
    max_num_of_starting_stations.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/max_num_of_starting_stations')
    
    #what were the top 3 trip stations each day for the last two weeks?
    max_date = df.select(spark_max(col("start_time"))).collect()[0][0]
    start_date = date_sub(lit(max_date), 14)
    df_last_2_weeks = df.filter(col("start_time") >= start_date)
    num_of_starting_stations = df_last_2_weeks.groupBy(date_format(col('start_time'), 'yyyy-MM-dd').alias('start_date'), col('from_station_name').alias('station_name')) \
                                              .agg(count('*').alias('n_stations')) \
                                              .orderBy(['start_date', 'n_stations'], ascending=[True, False])
    window = Window.partitionBy('start_date').orderBy(col('n_stations').desc())
    top_3_stations = num_of_starting_stations.withColumn('rnk', dense_rank().over(window))\
                                             .filter(col('rnk') <= 3)\
                                             .drop('rnk')
    top_3_stations.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/top_3_stations')

    #do `Male`s or `Female`s take longer trips on average?
    avg_duration_per_gender = df.groupBy('gender').agg(avg('trip_duration').alias('avg_trip_duration'))
    avg_duration_per_gender.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/avg_duration_per_gender')

    #what is the top 10 ages of those that take the longest trips, and shortest?
    df = df.withColumn('age', year(current_date()) - col('birthyear'))

    longest_window = Window.orderBy(col('trip_duration').desc())
    longest_trips = df.withColumn('rnk', row_number().over(longest_window)) \
                    .filter(col('rnk') <= 10) \
                    .select('age', 'trip_duration') \
                    .orderBy(col('trip_duration').desc())
    longest_trips.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/longest_trips')

    shortest_window = Window.orderBy(col('trip_duration').asc())
    shortest_trips = df.withColumn('rnk', row_number().over(shortest_window)) \
                    .filter(col('rnk') <= 10) \
                    .select('age', 'trip_duration') \
                    .orderBy(col('trip_duration').asc())
    shortest_trips.coalesce(1).write.mode('overwrite').option('header', True).csv('./reports/shortest_trips')

if __name__ == "__main__":
    main()
