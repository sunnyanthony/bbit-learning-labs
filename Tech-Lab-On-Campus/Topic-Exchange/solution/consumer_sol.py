from consumer_interface import mqConsumerInterface
import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, exchange_name: str,
        binding_key: str,
        queue_name: str
    ) -> None:
        self.exchange_name = exchange_name
        self.connection = None

        self.setupRMQConnection()
        self.createQueue(queue_name)
        self.bindQueueToExchange(queue_name, binding_key)

    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ.get('AMQP_URL'))
        #credentials = pika.PlainCredentials('guest', 'guest')172.21.0.3:56216 
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(queue=queueName, exchange=self.exchange_name, routing_key = topic)

    def createQueue(self, queueName: str) -> None:
        self.channel.queue_declare(queue=queueName)
        self.channel.basic_consume(queue=queueName,
                      auto_ack=True,
                      on_message_callback=self.on_message_callback)

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