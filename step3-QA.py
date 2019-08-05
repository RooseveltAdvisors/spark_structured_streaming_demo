from __future__ import print_function
import sys
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
import time
from time import sleep
from datetime import datetime
import json
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', default="./data")
    args = parser.parse_args()
    print('Starting the process...\n')


    # Create spark context
    print("Starting a spark context")
    #os.environ["PYSPARK_PYTHON"] = "/home/yuan/anaconda3/envs/phdata-env/bin/python"
    conf = SparkConf().\
            setAppName("phdata-ddos-detection-QA").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars.packages','io.delta:delta-core_2.11:0.3.0,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3')

    spark = SparkSession.builder.\
        config(conf=conf).\
        getOrCreate()
    sc = spark.sparkContext
    # Creating Kafka input stream
    
    while True:
        result_df = spark.read.format('delta').load(args.path)
        print(f"=>Total: {result_df.count()}")
        print("=>Sample:")
        print(result_df.show(5))
        print("===================")
        sleep(1)
   