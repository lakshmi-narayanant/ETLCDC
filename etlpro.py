from pymongo import MongoClient
from kafka import KafkaProducer
import json
from time import sleep
from bson import json_util
# MongoDB Connection
mongo_client = MongoClient('mongodb+srv://atsreports:atsreports@cluster0.3xaqdyl.mongodb.net')
mongo_db = mongo_client['sample_mflix']
mongo_collection = mongo_db['movies']
cursor = mongo_collection.find()

for document in cursor:
    print(document)

# Close the MongoDB connection

# Kafka Connection
#kafka_producer = KafkaProducer(bootstrap_servers='your-kafka-broker')
kafka_producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])
topic_name='user'
# Change Stream to capture changes in MongoDB
pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]
change_stream = mongo_collection.watch(pipeline)

for change in change_stream:
    # Send the change event to Kafka
    print(change)
    kafka_producer.send(topic_name, json.dumps(change).encode('utf-8'))
    sleep(5)
    print("sleep 5")

