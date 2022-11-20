import pika, json

connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
channel = connection.channel()

channel.queue_declare(queue="hello")

channel.basic_publish(exchange="",
                      routing_key="hello",
                      body=json.dumps({"name": "Elias", "age": 25}))

print(" [x] Sent")

connection.close()