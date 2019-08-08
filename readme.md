# Spark Log Analytics

## 1. Overview
A Log Analytics demo based on Spark Structured Streaming + Kafka


## 2. Get started

### 2.1 Setup Conda env

```bash
conda create -y -n yz_spark_kafka_demo python=3.6
source activate spark_kafka_demo
pip install kafka-python
pip install pyspark
pip install pandas

```


### 2.2 Run Kafka in Docker

```bash
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka
```


### 2.3 Run Kafka producer

Open a terminal and run the following commands (please unarchive `apache-access-log.txt.gz` first)

```bash
source activate yz_spark_kafka_demo
gunzip apache-access-log.txt.gz
python step1-kafka-producer.py -i ./apache-access-log.txt --host 127.0.0.1:9092 --topic logs
```


### 2.4 Streaming processor

Open a new terminal and run the following commands

```bash
source activate yz_spark_kafka_demo
python step2-streaming-processor.py --host 127.0.0.1:9092 --topic logs
```

### 2.5 QA

Open a new terminal and run the following commands

```bash
source activate yz_spark_kafka_demo
python step3-QA.py --path ./data
```
