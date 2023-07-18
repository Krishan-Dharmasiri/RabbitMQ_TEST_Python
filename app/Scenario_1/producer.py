import pika
import json
import time

# Establish a connection to RabbitMQ
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare a queue you want to send messages to (if does not exist)
channel.queue_declare(queue='portfolio_queue')

# Just hardcoding for Demo purposes, should come from another service like a Web API
messages = [
    {
        'Id' : 1,
        'financial_algorithm' : 'MARKOWITZ',
        'file_location' : {
            'data_file' : 'location1'
        }
    },
    {
        'Id' : 2,
        'financial_algorithm' : 'HRP',
        'file_location' : {
            'data_file' : 'location2'
        }
    },
    {
        'Id' : 3,
        'financial_algorithm' : 'RESAMPLING',
        'file_location' : {
            'data_file' : 'location3',
            'b_file' : 'location33'
        }
    },
    {
        'Id' : 4,
        'financial_algorithm' : 'MARKOWITZ',
        'file_location' : {
            'data_file' : 'location4'
        }
    },
    {
        'Id' : 5,
        'financial_algorithm' : 'MARKOWITZ',
        'file_location' : {
            'data_file' : 'location5'
        }
    }
]

# Publish the messages one at time to the Queue

for message in messages:
    json_message = json.dumps(message)
    # exchange parameter is empty because we are using the default exchange
    channel.basic_publish(exchange='', routing_key='portfolio_queue', body=json_message)
    print(f'Sent Message is : {json_message}')

    # Wait a second before sending the next message
    time.sleep(5)



# Close the connection to RabbitMQ
connection.close()
