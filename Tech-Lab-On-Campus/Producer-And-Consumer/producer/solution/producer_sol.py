import pika
import os
from producer import mqProducerInterface
class mqProducer:

    def __init__(self):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ.get('AMQP_URL'))
        connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        channel = connection.channel()
        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange="Our Exchange")
        channel.basic_publish(
            exchange="Our Exchange",
            routing_key="Routing Key",
            body="Hello there",
        )
        pass