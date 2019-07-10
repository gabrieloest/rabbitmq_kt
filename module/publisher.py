# example_publisher.py
import pika, os, logging

logging.basicConfig()

# Parse CLODUAMQP_URL (fallback to localhost)
url = os.environ.get('RMQ_DEV_URL', 'amqp://rabbitmq_kt:rabbitmq_kt@rmqnlbdev.mtvi.com:5672/%2f')
params = pika.URLParameters(url)
params.socket_timeout = 5

connection = pika.BlockingConnection(params)  # Connect to CloudAMQP
channel = connection.channel()  # start a channel
channel.queue_declare(queue='pdfprocess', durable=True)  # Declare a queue
# send a message

channel.basic_publish(exchange='', routing_key='pdfprocess', body='User information')
print("[x] Message sent to consumer")
connection.close()
