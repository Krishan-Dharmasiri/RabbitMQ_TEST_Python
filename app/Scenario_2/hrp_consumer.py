import pika
import json


# Establish a connection to RabbitMQ
connection_parameters = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_parameters)
channel = connection.channel()

# Declare the Queue you want to consume messages from
channel.queue_declare(queue='portfolio_queue')

# Callback function 
def on_message_received(ch, method, properties, body):
    '''
        The call back function that gets called every time a messsage is recieved from the RabbitMQ
    '''
    # Convert the message to JSON from string (Deserialization)
    message = json.loads(body)

    # Check the message is intended for using MARKOWITZ algorithm
    algorithm_type = message.get('financial_algorithm')
    job_id = message.get('Id')

    if algorithm_type == 'HRP':
        print(f'Message Accepted, the message is : {message}')

        # Acknowledge the message to remove it from the Queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    else:
        print(f'Message with Algorithm type : {algorithm_type} Rejected. Job Id : {job_id} Requeued')
        # Reject and requeue the message if it doesn't meet the condition
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)


# Ensures that consumer only recieves one message at a time 
# and processes it before recieving the next one    
channel.basic_qos(prefetch_count=1)

# Start consuming messages from the Queue
channel.basic_consume(queue='portfolio_queue', 
                      on_message_callback=on_message_received)
print('Starting Consuming Messages ...')

# Starts an infinite loop that listenes for new messages and invokes the call back function
# for each recieved message
channel.start_consuming()


# Close the connection to RabbitMQ
# connection.close()
