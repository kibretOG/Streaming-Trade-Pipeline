from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

def create_spark_ss():
    try:
        s_conn = SparkSession.builder \
            .appName('trade_streaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")
        return None

if __name__ == "__main__":
    ss = create_spark_ss()
    
    if ss is not None:
        try:
            df = ss.readStream \
                .format('kafka') \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "trade_occurance_raw") \
                .option("startingOffsets", "earliest") \
                .load()

            schema = StructType() \
                .add('id', StringType()) \
                .add('exchange', StringType()) \
                .add('quoteType', IntegerType()) \
                .add('price', DoubleType()) \
                .add('timestamp', TimestampType()) \
                .add('markethours', IntegerType()) \
                .add('changePercent', DoubleType()) \
                .add('dayVolume', IntegerType()) \
                .add('change', DoubleType()) \
                .add('priceHint', IntegerType())

            sel = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("trade_data")) \
                .select("trade_data.*")

            query = sel.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .start()

            query.awaitTermination()

        except Exception as e:
            print(f"Streaming failed due to: {e}")
