import os
import amqpstorm
from amqpstorm import Message
from urllib.parse import urlparse

# Función para parsear la URL de CloudAMQP
def parse_amqp_url(url):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 5671 if parsed.scheme == 'amqps' else 5672
    username = parsed.username
    password = parsed.password
    virtual_host = parsed.path[1:] if parsed.path else '/'
    return host, port, username, password, virtual_host

def on_request(message):
    payload = message.body.decode('utf-8')
    print(f" [.] Procesando: {payload}")
    response_data = f"Respuesta desde el servidor: {payload.upper()}"
    response = Message.create(message.channel, response_data)
    response.correlation_id = message.correlation_id
    response.publish(routing_key=message.reply_to)
    message.ack()

amqp_url = os.environ.get('CLOUDAMQP_URL')
if amqp_url:
    host, port, username, password, virtual_host = parse_amqp_url(amqp_url)
    connection = amqpstorm.Connection(host, username, password, port=port, virtual_host=virtual_host)
else:
    connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')

channel = connection.channel()
channel.queue.declare(queue='rpc_queue', durable=True, auto_delete=False, exclusive=False)
channel.basic.consume(on_request, queue='rpc_queue')

print(" [x] Servidor RPC iniciado. Esperando peticiones...")
channel.start_consuming()