
from consumer_interface import mqConsumerInterface
import pika
import os
import json

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        
        self.binding_key = binding_key
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # return super().setupRMQConnection()
        
        #Build our connection to the RMQ Connection.
        #The AMPQ_URL is a string which tells pika the package the URL of our AMPQ service in this scenario RabbitMQ.
        
        #Establish connection to the RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()

        #declare a queue and exchange
        self.channel.exchange_declare(self.exchange_name)
        self.channel.queue_declare(self.queue_name)

        #bind the binding key to the queue on the exchange 
        self.channel.queue_bind(
            queue= self.queue_name,
            routing_key= self.binding_key,
            exchange=self.exchange_name,
        )

        #and finally set up a callback function for receiving messages
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )
    
    def onMessageCallback(self, json_msg) -> None:
        #Print the UTF-8 string message and then close the connection

        string_msg = json.loads(json_msg)
        print(string_msg)
        self.connection.close()

        # #We can then publish data to that exchange using the basic_publish method
        # channel.basic_publish('Test Exchange', 'Test_route', 'Hi',...)


    def startConsuming(self) -> None:
        self.channel.start_consuming()

    def Del(self) -> None:
        
        self.channel.close()
        self.connection.close()