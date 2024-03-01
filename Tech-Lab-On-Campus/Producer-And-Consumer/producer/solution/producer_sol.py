from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.rk = routing_key
        self.string_msg = exchange_name
        self.setupRMQConnection()
    
    def setupRMQConnection(self):
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.string_msg)

    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(exchange=self.string_msg,routing_key = self.rk,body=message)
        print("Sent messages!")
        self.channel.close()
        self.connection.close()
