from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
builder = SparkSession\
        .builder\
        .appName('BronzeIngest')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = builder.getOrCreate()

raw_data = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('subscribe','rides')\
        .option('startingOffsets',"latest")\
        .load()\
        .selectExpr('CAST(value AS STRING) as str')

schema = StructType()\
        .add('ride_id', StringType())\
        .add('timestamp', StringType())\
        .add('pickup_location', StringType())\
        .add('dropoff_location', StringType())\
        .add('passanger_count', IntegerType())\
        .add('price', DoubleType())\
        .add('driver_id', IntegerType())


bronze_df = raw_data.withColumn('data',from_json('str',schema))\
        .select('data.*')
bronze_df.writeStream \
    .format('delta')\
    .outputMode('append') \
    .option('checkpointLocation', '/home/Viber/kafka_project/bronze/_checkpoints/rides')\
    .start('/home/Viber/kafka_project/bronze/rides')\
    .awaitTermination()






