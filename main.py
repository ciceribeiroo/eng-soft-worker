import pika, os, time, requests, json


def process_function(msg):
    my_json = msg.decode('utf8')
    print(my_json)
    data = json.loads(my_json)

    bName = data.get("bucketName")
    fName = data.get("fileName")
    rId = data.get("idApp")

    URL = f"http://converter:8000/convert/{bName}/{fName}"

    r = requests.get(url=URL)

    dataMsg = r.json()

    if dataMsg.get("message") == "Converted":
        channel_send.basic_publish(exchange='', routing_key='converter.queue.ready', body=bytes(str(rId), 'utf-8'))
    else:
        channel_send.basic_publish(exchange='', routing_key='converter.queue.error', body=bytes(str(rId), 'utf-8'))

    print("processing finished")
    return


def callback(ch, method, properties, body):
    process_function(body)


while True:
    connection = None
    try:
        url = os.environ.get('CLOUDAMQP_URL', 'amqp://myuser:mypassword@rabbitmq:5672/%2f')
        params = pika.URLParameters(url)

        connection = pika.BlockingConnection(params)
        channel_consume = connection.channel()
        channel_send = connection.channel()
        channel_send.queue_declare("converter.queue.ready", durable=True)
        channel_send.queue_declare("converter.queue.error", durable=True)

        channel_consume.basic_consume('converter.queue.send',
                                      callback,
                                      auto_ack=True)

        channel_consume.start_consuming()
        connection.close()
    except KeyboardInterrupt:
        if connection:
            connection.close()
        break
