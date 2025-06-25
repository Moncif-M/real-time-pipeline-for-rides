from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, to_date, expr

'''
./spark-submit \
--master local[4] \
--packages io.delta:delta-spark_2.13:4.0.0 \
/home/Viber/kafka_project/gold_layer.py
'''

spark = SparkSession\
    .builder\
    .appName('gold_aggregation')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()


silver_df = spark.read\
    .format('delta')\
    .load('/home/Viber/kafka_project/silver/rides')

df_processed = silver_df\
    .withColumn('ride_date',to_date('pickup_time'))\
    .withColumn('longitude',expr("split(split(pickup_location, '[()]')[1], ' ')[0]").cast('double'))\
    .withColumn("latitude", expr("split(split(pickup_location, '[()]')[1], ' ')[1]").cast("double"))
                
gold_df = df_processed\
    .groupBy('ride_date','longitude','latitude')\
    .agg(
        count('*').alias('total_rides'),
        avg('price').alias('average_price')
    )


gold_df.write\
    .format('delta')\
    .mode('overwrite')\
    .save('/home/Viber/kafka_project/gold/rides_daily_summary_delta')

gold_df.write.mode("overwrite").parquet('/home/Viber/kafka_project/gold/rides_daily_summary_delta')