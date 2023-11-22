from pymongo import MongoClient
from kafka import KafkaProducer
from bson import json_util
import json
from time import sleep
# MongoDB Connection
mongo_client = MongoClient('mongodb+srv://atsreports:atsreports@cluster0.3xaqdyl.mongodb.net')
mongo_db = mongo_client['sample_training']
mongo_collection = mongo_db['zips']
cursor = mongo_collection.find()
topic_name='user'
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
kafka_producer = KafkaProducer(bootstrap_servers=['localhost: 9092'])

for document in cursor:
    print(document)
    #document_bytes = json.dumps(document).encode('utf-8')
    document_bytes = json.dumps(document, default=json_util.default).encode('utf-8')
    #producer.send(topic_name, value=document)
    #kafka_producer.send(topic_name, json.dumps(document).encode('utf-8'))
    #kafka_producer.send(topic_name,value=document)
    kafka_producer.send(topic_name, value=document_bytes)




