# example_consumer.py
import pika, os, time


def pdf_process_function(msg):
    print(" PDF processing")
    print(" Received %s" % msg)

    time.sleep(5)  # delays for 5 seconds
    print(" PDF processing finished")
    return


url = os.environ.get('RMQ_DEV_URL', 'amqp://rabbitmq_kt:rabbitmq_kt@rmqnlbdev.mtvi.com:5672/%2f')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel()  # start a channel
channel.queue_declare(queue='pdfprocess', durable=True)  # Declare a queue


# create a function which is called on incoming messages
def callback(ch, method, properties, body):
    pdf_process_function(body)


# set up subscription on the queue
channel.basic_consume(queue='pdfprocess', on_message_callback=callback, auto_ack=True)

# start consuming (blocks)
channel.start_consuming()
connection.close()
