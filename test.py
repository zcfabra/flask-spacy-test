import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# channel.queue_declare(queue='reply', durable=True)
def cb(ch,method,properties,body):
    print(body)

channel.queue_declare("test", durable=True)
channel.basic_publish(exchange="", routing_key="test", body="YES")
print("SENT")

    
# channel.basic_consume(queue="reply", on_message_callback=cb)
# channel.start_consuming()