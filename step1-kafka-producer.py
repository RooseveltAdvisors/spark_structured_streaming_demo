from __future__ import print_function
import time
import re
import json
import argparse
import sys
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from time import sleep

def send_message(producer, topic, input):
     
    with open(input, 'r') as ins:
        for line in ins:
            value1 = list(map(''.join, re.findall(r'\"(.*?)\"|\[(.*?)\]|(\S+)', line)))
            json_data = json.dumps({'remote_host': value1[0],
                                    'user-identifier': value1[1],
                                    'frank': value1[2],
                                    'time_received': value1[3],
                                    'request_first_line': value1[4],
                                    'status': value1[5],
                                    'size_bytes': value1[6],
                                    'request_header_referer': value1[7],
                                    'request_header_user_agent': value1[8]}).encode('utf-8')
            producer.send(topic,json_data)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-rh', '--host', default="127.0.0.1:9092")
    parser.add_argument('-t', '--topic', default='logs')
    parser.add_argument('-i', '--input', required=True)
    args = parser.parse_args()
    
    # create kafka topics
    print(f"Create Kafka topics: {args.topic}")
    admin_client = KafkaAdminClient(bootstrap_servers=args.host, client_id='test')
    topic_list = []
    topic_list.append(NewTopic(name=args.topic, num_partitions=8, replication_factor=1))
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except TopicAlreadyExistsError as err:
        print("topics already exisit...")

    # push data to kafka
    print(f"Pushing data to Kafka topic: {args.topic}")
    producer = KafkaProducer(bootstrap_servers=args.host)
    send_message(producer, args.topic, args.input)
    