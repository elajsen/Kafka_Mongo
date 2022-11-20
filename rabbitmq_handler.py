from mongo_handler import MongoHandler
import pika, sys, os, json


class RabbitHandler:
    def __init__(self):
        # Initialize the mongodb
        mongo_handler = MongoHandler()

        # Declare the queuee
        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        channel = connection.channel()

        channel.queue_declare(queue="hello")

        channel.basic_consume(queue="hello",
                              auto_ack=True,
                              on_message_callback=self.callback)

        print("Handler running... waiting for messages!")
        channel.start_consuming()

    def callback(self, ch, method, properties, serialized_body):
        body = json.loads(serialized_body)
        print(f" [x] Received {body}")


if __name__ == '__main__':
    try:
        RH = RabbitHandler()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)