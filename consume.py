from kafka import KafkaConsumer
from mysql.connector import connect
import json

machine_ip='192.168.1.97'
# Kafka Connection
kafka_consumer = KafkaConsumer('user', bootstrap_servers=[f'{machine_ip}:9092'])
print("finished")

# MySQL Connection
mysql_connection = connect(
    host='localhost',
    user='root',
    password='Sight@9841',
    database='sql-target'
)
mysql_cursor = mysql_connection.cursor()

# Kafka Consumer loop to process messages
for message in kafka_consumer:
    change_event = json.loads(message.value.decode('utf-8'))

    # Extract relevant information from the change event
    # Implement your logic for transformation here

    # Apply the changes to MySQL
    # Implement your logic for applying changes to MySQL here