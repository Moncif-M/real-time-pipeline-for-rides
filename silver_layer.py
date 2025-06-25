from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession\
    .builder\
    .appName('SilverProcess')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

bronze_df = spark.readStream\
    .format('delta')\
    .load('/home/Viber/kafka_project/bronze/rides')

silver_df = bronze_df\
.filter(bronze_df['timestamp'].isNotNull())\
.dropDuplicates(['ride_id'])\
.withColumnRenamed('timestamp','pickup_time')

silver_df.writeStream\
.format('delta')\
.outputMode('append')\
.option('checkpointLocation', '/home/Viber/kafka_project/silver/_checkpoints/rides')\
.start('/home/Viber/kafka_project/silver/rides')\
.awaitTermination()