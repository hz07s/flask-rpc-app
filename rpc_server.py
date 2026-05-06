import os
import sys
from urllib.parse import urlparse
import amqpstorm
from amqpstorm import Message

# ------------------------------------------------------------
# 1. Parseo de la URL de CloudAMQP
# ------------------------------------------------------------
def parse_amqp_url(url):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (5671 if parsed.scheme == 'amqps' else 5672)
    username = parsed.username
    password = parsed.password
    vhost = parsed.path[1:] if parsed.path and parsed.path != '/' else '/'
    return host, port, username, password, vhost

# ------------------------------------------------------------
# 2. Callback que atiende las solicitudes
# ------------------------------------------------------------
def on_request(message):
    payload = message.body.decode('utf-8')
    print(f" [.] Procesando: {payload}")
    response_data = f"Respuesta desde el servidor: {payload.upper()}"
    response = Message.create(message.channel, response_data)
    response.correlation_id = message.correlation_id
    response.publish(routing_key=message.reply_to)
    message.ack()

# ------------------------------------------------------------
# 3. Conexión a RabbitMQ / CloudAMQP
# ------------------------------------------------------------
amqp_url = os.environ.get('CLOUDAMQP_URL')
if amqp_url:
    host, port, username, password, vhost = parse_amqp_url(amqp_url)
    print(f"Conectando worker a CloudAMQP: {host}:{port}", file=sys.stderr)
    connection = amqpstorm.Connection(host, username, password,
                                      port=port, virtual_host=vhost)
else:
    print("Modo local: conectando a RabbitMQ en 127.0.0.1", file=sys.stderr)
    connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')

channel = connection.channel()
# Cola durable para compatibilidad con RabbitMQ 4.x
channel.queue.declare(queue='rpc_queue', durable=True, auto_delete=False, exclusive=False)
channel.basic.consume(on_request, queue='rpc_queue')

print(" [x] Servidor RPC iniciado. Esperando peticiones...", file=sys.stderr)
channel.start_consuming()