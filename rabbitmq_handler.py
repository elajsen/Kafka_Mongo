from mongo_handler import MongoHandler
import pika, sys, os, json
from datetime import datetime


class RabbitHandler:
    def __init__(self):
        # Initialize the mongodb
        self.mongo_handler = MongoHandler()
        self.IP = "localhost"
        self.queue = "trading_bot_backend"

        self.trading_db = "trading_db"
        self.holdings_collection = "holdings"

        # Declare the queuee
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.IP))
        channel = connection.channel()

        channel.queue_declare(queue=self.queue)

        channel.basic_consume(queue=self.queue,
                              auto_ack=True,
                              on_message_callback=self.callback)

        print("Handler running... waiting for messages!")
        channel.start_consuming()

    def callback(self, ch, method, properties, serialized_body):
        body = json.loads(serialized_body)
        type = body.get("type")
        res = getattr(self, type)(selling_price=body.get("price"),
                                  objects=body.object_ids)

    def buy(self, ticker, amount, price):
        print("Buying")
        values = [{
            "time": datetime.now(),
            "ticker": ticker,
            "amount": amount,
            "price": price
        }]

        self.mongo_handler.insert_value(db=self.trading_db,
                                        collection=self.holdings_collection,
                                        values=values)

    def sell(self, selling_price, objects):
        print("Selling")
        assert isinstance(objects, list), "Objects needs to be a list of objects to sell"

        self.mongo_handler.sell_items(db=self.trading_db,
                                      collection=self.holdings_collection,
                                      object_ids=objects,
                                      selling_price=selling_price)

if __name__ == '__main__':
    try:
        RH = RabbitHandler()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
