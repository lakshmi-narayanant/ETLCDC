from kafka import KafkaConsumer
import json
import pymysql

# Kafka Consumer
kafka_consumer = KafkaConsumer('user',bootstrap_servers=['localhost: 9092'])

# MySQL Connection
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'Sight@9841'
mysql_db = 'sql_target'

# Establish MySQL connection
mysql_connection = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, db=mysql_db)

# Create a cursor object
cursor = mysql_connection.cursor()

# Print a message when the consumer is ready
print("Kafka Consumer is ready. Waiting for messages...")

# Continuously poll for new messages
for message in kafka_consumer:
    # Decode and print the message value
    received_message = json.loads(message.value.decode('utf-8'))
    print("Received message:", received_message)

    # Extract data from the message
    city = received_message.get('city', '')
    zip_code = received_message.get('zip', '')
    latitude = received_message.get('loc', {}).get('y', 0.0)
    longitude = received_message.get('loc', {}).get('x', 0.0)
    population = received_message.get('pop', 0)
    state = received_message.get('state', '')

    # Insert data into MySQL table
    insert_query = f"INSERT INTO mongodbextract (city, zip, latitude, longitude, population, state) VALUES " \
                   f"('{city}', '{zip_code}', {latitude}, {longitude}, {population}, '{state}')"

    try:
        # Execute the query
        cursor.execute(insert_query)

        # Commit the changes
        mysql_connection.commit()

        #print("Data inserted into MySQL successfully.")
    except Exception as e:
        # Rollback in case of an error
        mysql_connection.rollback()
        print(f"Error: {e}")

# Close the cursor and MySQL connection
cursor.close()

