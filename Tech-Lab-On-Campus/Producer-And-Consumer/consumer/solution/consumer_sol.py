from consumer_interface import mqConsumerInterface
import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection = None

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ.get('AMQP_URL'))
        #credentials = pika.PlainCredentials('guest', 'guest')172.21.0.3:56216 
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.basic_consume(queue=self.queue_name,
                      auto_ack=True,
                      on_message_callback=self.on_message_callback)
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        self.channel.queue_bind(queue=self.queue_name, exchange=self.exchange_name, routing_key = self.binding_key)

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        print(f"{body}")

    def startConsuming(self) -> None:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def __del__(self) -> None:
        if self.connection:
            self.connection.close()
        print("Consumer Ending...")