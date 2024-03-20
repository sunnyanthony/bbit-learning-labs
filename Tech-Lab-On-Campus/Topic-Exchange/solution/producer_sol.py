import pika, os
from producer_interface import mqProducerInterface
class mqProducer(mqProducerInterface):

    def __init__(self, routing_key: str, exchange_name: str):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ.get('AMQP_URL'))
        self.connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = self.connection.channel()

    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)