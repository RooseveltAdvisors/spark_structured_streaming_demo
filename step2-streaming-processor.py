from __future__ import print_function
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
import uuid
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
import time
from datetime import datetime
import json
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-rh', '--host', default="127.0.0.1:9092")
    parser.add_argument('-t', '--topic', default='logs')
    args = parser.parse_args()
    print('Starting the process...\n')


    # Create spark context
    print("Starting a spark context")
    #os.environ["PYSPARK_PYTHON"] = "/home/yuan/anaconda3/envs/phdata-env/bin/python"
    conf = SparkConf().\
            setAppName("phdata-ddos-detection").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars.packages','io.delta:delta-core_2.11:0.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3')

    spark = SparkSession.builder.\
        config(conf=conf).\
        getOrCreate()
    sc = spark.sparkContext
    # Creating Kafka input stream
    print("Creating kafka input stream")
    spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

    raw_logs_df = (spark.readStream                        # Get the DataStreamReader
      .format("kafka")                                 # Specify the source format as "kafka"
      .option("kafka.bootstrap.servers", args.host)  # Configure the Kafka server name and port
      .option("subscribe", args.topic)                       # Subscribe to the "en" Kafka topic
      .option("startingOffsets", "earliest")           # Rewind stream to beginning when we restart notebook
      .option("maxOffsetsPerTrigger", 4000)            # Throttle Kafka's processing of the streams
      .load()                                          # Load the DataFrame
      .select(F.col("value").cast("STRING"))             # Cast the "value" column to STRING
    )
    
    schema = T.StructType([
      T.StructField("remote_host", T.StringType(), True),
      T.StructField("user-identifier", T.StringType(), True),
      T.StructField("frank", T.StringType(), True),
      T.StructField("time_received", T.StringType(), True),
      T.StructField("request_first_line", T.StringType(), True),
      T.StructField("status", T.StringType(), True),
      T.StructField("size_bytes", T.StringType(), True),
      T.StructField("request_header_referer", T.StringType(), True),
      T.StructField("request_header_user_agent", T.StringType(), True)
    ])
    
    # Parsing Kafka input stream
    raw_logs_json_df = raw_logs_df.select(
      F.from_json("value", schema).alias("json"))  # Parse the column "value" and name it "json"

    logs_df = (raw_logs_json_df
      .select(F.col("json.remote_host").alias("remote_host"),      # Promoting from sub-field to column
              F.col("json.user-identifier").alias("user_identifier"),
              F.col("json.frank").alias("frank"),
              F.to_timestamp(F.col("json.time_received"), "d/MMM/yyyy:HH:mm:ss +SSSS").alias("time_received"), # Promoting and converting to a timestamp
              F.col("json.request_first_line").alias("request_first_line"),
              F.col("json.status").alias("status"),
              F.col("json.size_bytes").cast("int").alias("size_bytes"),
              F.col("json.request_header_referer").alias("request_header_referer"),
              F.col("json.request_header_user_agent").alias("request_header_user_agent"))
    )
    
    # Aggregations
    logs_watermarked_df = (logs_df
      .withWatermark("time_received", "10 seconds")         # Specify a 10 seconds watermark
      .groupBy(F.col("remote_host"),                     # Aggregate by remote_host...
               F.window(F.col("time_received"), "10 seconds", "5 seconds"))     # ...then by a 10 secs window, sliding every 5 secs
      .agg(F.count('status').alias('req_count'), # For each aggregate, produce a count
           F.approx_count_distinct('request_header_user_agent').alias('unique_ua')
          )                                  # For each aggregate, produce a unique user-agent count
      .select(F.col("window.start").alias("start"), # Elevate field to column
              F.col("req_count"),                       # Include count
              F.col("unique_ua"),                   # Include unique user agent types
              F.col("remote_host"))                      # Include action
    )
    
    # Suspicious IP criteria
    suspicious_ip_df = logs_watermarked_df.filter("req_count > 5 or unique_ua > 3")
    
    # Outputing suspicious IPs to the File sink
    base_path = "."
    data_path = "{}/data".format(base_path)
    chk_path = "{}/chk".format(base_path)
    
    file_output_stream = (suspicious_ip_df.select("remote_host")
      .writeStream                                   # Write the stream
      .format("delta")                               # Use the delta format
      .trigger(processingTime='2 seconds')           # ProcessingTime trigger with two-seconds micro-batch interval
      .option("checkpointLocation", chk_path)  # Specify where to log metadata
      .option("path", data_path)                    # Specify the output path
      .outputMode("append")                          # Complete mode
      .queryName("file_output_stream")           # The name of the stream
      .start()                                       # Start the operation
      .awaitTermination()
    )
   